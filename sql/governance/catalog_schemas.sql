-- T2.4: catalog + schemas.
--
-- Run in the Databricks SQL Editor. The editor shows one widget per :parameter below:
--   catalog        = maven_market_uc
--   bronze_schema  = bronze
--   silver_schema  = silver
--   gold_schema    = gold
--   audit_schema   = audit
-- (same values as config/defaults.yml and the databricks.yml variables)
--
-- MANAGED LOCATION is the one place a literal string is used: it must be written out as a
-- string literal (a :parameter marker there is, as far as I know, not accepted). It must sit
-- inside an EXTERNAL LOCATION you own (maven_market_ext covers the whole unity-catalog container).
-- Replace the account/container if yours differ.

CREATE CATALOG IF NOT EXISTS IDENTIFIER(:catalog)
  MANAGED LOCATION 'abfss://unity-catalog@sgtmavenmarket.dfs.core.windows.net/catalog'
  COMMENT 'Maven Market secure retail analytics lakehouse';

USE CATALOG IDENTIFIER(:catalog);

CREATE SCHEMA IF NOT EXISTS IDENTIFIER(:bronze_schema) COMMENT 'Raw landing (append-only)';
CREATE SCHEMA IF NOT EXISTS IDENTIFIER(:silver_schema) COMMENT 'Cleansed, conformed, SCD2 dimensions';
CREATE SCHEMA IF NOT EXISTS IDENTIFIER(:gold_schema)   COMMENT 'Star schema + aggregates for BI';
CREATE SCHEMA IF NOT EXISTS IDENTIFIER(:audit_schema)  COMMENT 'Pipeline run logs / observability';

-- Verify
SHOW SCHEMAS IN IDENTIFIER(:catalog);
