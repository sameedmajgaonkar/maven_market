-- Dynamic column-level ABAC (PDF section G): ONE tag-based mask policy for the whole catalog.
--
-- Any column tagged classification=PII is masked for everyone except the `admin` and `data_compliance`
-- groups. The policy never names a column, so tagging a new column is all it takes to protect it.
--
-- The pipelines read these tables to build Silver and Gold. They run as members of `admin`
-- (the deploying user and the github-cicd service principal), which is EXEMPT. If the run-as identity
-- were masked, masked values would be written permanently into the tables it builds.

-- One function for every column type. Databricks casts the column to VARIANT before calling it and
-- casts the result back to the column type, so strings, integers, longs and dates all work.
-- Integer types widen to BIGINT inside a VARIANT, hence the BIGINT branch.
CREATE OR REPLACE FUNCTION {catalog}.{audit}.mask_pii(val VARIANT)
RETURNS VARIANT
RETURN CASE
  WHEN schema_of_variant(val) = 'BIGINT' THEN 0::VARIANT
  WHEN schema_of_variant(val) = 'DATE'   THEN DATE'1970-01-01'::VARIANT
  WHEN schema_of_variant(val) = 'DOUBLE' THEN 0.0::VARIANT
  WHEN schema_of_variant(val) = 'STRING' THEN '***'::VARIANT
  ELSE NULL::VARIANT
END;

CREATE OR REPLACE POLICY mask_pii
ON CATALOG {catalog}
COMMENT 'Mask every column tagged classification=PII for users outside admin and data_compliance'
COLUMN MASK {catalog}.{audit}.mask_pii
TO `account users` EXCEPT `admin`, `data_compliance`
FOR TABLES
MATCH COLUMNS has_tag_value('classification', 'PII') AS pii
ON COLUMN pii;
