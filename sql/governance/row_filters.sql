-- Attribute-driven row-level security (PDF section G).
--
-- A row filter POLICY (not a per-table filter) is bound to a governed tag:
--   * the column that carries a row's region code is tagged `region_code` (set from maven_market.governance)
--   * the policy hands that column's value to region_allowed(), which looks the querying user up in
--     audit.user_region_mapping and keeps the row only when the user is mapped to that region.
--
-- Fail-closed: a user who is not in the mapping table gets ZERO rows.
-- Exempt (see every policy): `admin` (platform admins and the pipeline run-as identities, which MUST NOT
-- be filtered or masked or they would write filtered data into the tables they build) and `data_compliance`.

-- Mapping table: one row per (user, region). A user may have several rows (multi-region managers).
-- It is created EMPTY here and never wiped by a re-run. The rows hold real login names, so they are not
-- in git: load them from sql/governance/user_region_mapping.seed.sql (git-ignored; copy the .example file).
CREATE TABLE IF NOT EXISTS {catalog}.{audit}.user_region_mapping (
  user_email STRING NOT NULL COMMENT 'Login as returned by current_user()',
  region     STRING NOT NULL COMMENT 'EU, NA or APAC (the data_region vocabulary)',
  note       STRING COMMENT 'Who/why'
) COMMENT 'Maps users to the data regions they may see. Drives the region row-filter policy.';

-- The filter itself. Comparison is case-insensitive. Keep the lookup table small: the function runs
-- for every protected query.
CREATE OR REPLACE FUNCTION {catalog}.{audit}.region_allowed(row_region STRING)
RETURNS BOOLEAN
RETURN EXISTS (
  SELECT 1 FROM {catalog}.{audit}.user_region_mapping m
  WHERE lower(m.user_email) = lower(current_user())
    AND m.region = row_region
);

-- One policy for the whole catalog: every table that has a column tagged `region_code` is filtered.
-- Tables without such a column are untouched by this policy.
--
-- WHEN: only tables that carry BOTH required governance tags. Two DIFFERENT row filters on one table
-- make Unity Catalog raise an error instead of returning zero rows, and structural_rules.sql adds a
-- blocking row filter for tables LACKING those tags. The two conditions are exact opposites, so a
-- table can never match both.
CREATE OR REPLACE POLICY regional_row_filter
ON CATALOG {catalog}
COMMENT 'Regional managers see only rows of the regions mapped to them in audit.user_region_mapping'
ROW FILTER {catalog}.{audit}.region_allowed
TO `account users` EXCEPT `admin`, `data_compliance`
FOR TABLES
WHEN has_tag('owner_team') AND has_tag('data_sensitivity')
MATCH COLUMNS has_tag('region_code') AS r
USING COLUMNS (r);
