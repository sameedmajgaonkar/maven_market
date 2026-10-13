-- Privileges for the personas. ABAC masks and filters what a user may SEE; these grants decide
-- whether they can query a table at all. Group names are the ACCOUNT groups from the account console.
-- Placeholders ({catalog}, {bronze}, ...) come from config/defaults.yml (see maven_market.governance).

-- Everyone who works with the platform can browse the catalog.
GRANT USE CATALOG ON CATALOG {catalog} TO `data_engineers`;
GRANT USE CATALOG ON CATALOG {catalog} TO `data_compliance`;
GRANT USE CATALOG ON CATALOG {catalog} TO `business_analysts`;
GRANT USE CATALOG ON CATALOG {catalog} TO `regional_managers_NA`;
GRANT USE CATALOG ON CATALOG {catalog} TO `regional_managers_EU`;
GRANT USE CATALOG ON CATALOG {catalog} TO `regional_managers_APAC`;
GRANT USE CATALOG ON CATALOG {catalog} TO `admin`;

-- Data engineers and compliance read every layer (masked unless exempt).
GRANT USE SCHEMA, SELECT ON SCHEMA {catalog}.{bronze} TO `data_engineers`;
GRANT USE SCHEMA, SELECT ON SCHEMA {catalog}.{silver} TO `data_engineers`;
GRANT USE SCHEMA, SELECT ON SCHEMA {catalog}.{gold}   TO `data_engineers`;
GRANT USE SCHEMA, SELECT ON SCHEMA {catalog}.{bronze} TO `data_compliance`;
GRANT USE SCHEMA, SELECT ON SCHEMA {catalog}.{silver} TO `data_compliance`;
GRANT USE SCHEMA, SELECT ON SCHEMA {catalog}.{gold}   TO `data_compliance`;
GRANT USE SCHEMA, SELECT ON SCHEMA {catalog}.{audit}  TO `data_compliance`;
GRANT USE SCHEMA, SELECT ON SCHEMA {catalog}.{audit}  TO `data_engineers`;

-- Analysts and regional managers only see the curated gold layer.
GRANT USE SCHEMA, SELECT ON SCHEMA {catalog}.{gold} TO `business_analysts`;
GRANT USE SCHEMA, SELECT ON SCHEMA {catalog}.{gold} TO `regional_managers_NA`;
GRANT USE SCHEMA, SELECT ON SCHEMA {catalog}.{gold} TO `regional_managers_EU`;
GRANT USE SCHEMA, SELECT ON SCHEMA {catalog}.{gold} TO `regional_managers_APAC`;

-- The identities that run the governance job (the `admin` group holds the deploying user and the
-- github-cicd service principal) must be able to see the gold tables and tag them. Without SELECT the
-- gate's information_schema query would list no tables and pass without checking anything. Tagging a
-- governed tag additionally needs ASSIGN on the tag itself: scripts/grant_tag_assign.py.
GRANT SELECT ON SCHEMA {catalog}.{gold} TO `admin`;
GRANT APPLY TAG ON CATALOG {catalog} TO `admin`;

-- The row-filter function looks users up in the mapping table, so everyone it applies to needs EXECUTE.
-- (The grant on the function itself is in row_filters.sql, after the function exists.)
