-- Structural rule (PDF section G): no table in the gold schema may be used unless it carries valid
-- `owner_team` AND `data_sensitivity` tags.
--
-- Unity Catalog has no DDL that rejects CREATE TABLE when a tag is missing, so the rule is enforced
-- in two layers:
--   1. THIS policy: a gold table without both tags returns ZERO rows to everyone outside `admin` and
--      `data_compliance`. A new, untagged table is therefore unusable until a steward tags it
--      (secure by default).
--   2. A pipeline gate (maven_market.jobs.governance_gate) that FAILS the run when a gold table is
--      untagged, so the omission is caught at deploy/refresh time, not by a confused analyst.
--
-- IMPORTANT: tags are set on each TABLE, never on the gold SCHEMA. Tags inherit downward for ABAC, so
-- a schema-level owner_team would make every new table look compliant and this rule would never fire.
--
-- The governed tags only accept their allowed values, so "valid" is enforced by Unity Catalog itself
-- (an attempt to set data_sensitivity = 'top_secret' is rejected).

CREATE OR REPLACE FUNCTION {catalog}.{audit}.block_all()
RETURNS BOOLEAN
RETURN FALSE;

CREATE OR REPLACE POLICY block_untagged_gold
ON SCHEMA {catalog}.{gold}
COMMENT 'Gold tables must declare owner_team and data_sensitivity; otherwise they return no rows'
ROW FILTER {catalog}.{audit}.block_all
TO `account users` EXCEPT `admin`, `data_compliance`
FOR TABLES
WHEN NOT (has_tag('owner_team') AND has_tag('data_sensitivity'));

GRANT EXECUTE ON FUNCTION {catalog}.{audit}.block_all TO `account users`;
