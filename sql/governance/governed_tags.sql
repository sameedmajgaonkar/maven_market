-- Governed tag taxonomy (Public Preview). Run as an account / metastore admin.
-- Governed tags are value-constrained and have their own CREATE / MANAGE / ASSIGN privileges.
-- Verified against a live workspace: each statement below executes as written.

-- ---- Data classification: drives the single PII column-mask policy -------------------------
CREATE GOVERNED TAG classification
  DESCRIPTION 'Data sensitivity class used by ABAC masking policies'
  VALUES ('PII', 'Financial', 'Public');

-- ---- Region vocabulary used by the row-level policy and the user_region_mapping table --------
CREATE GOVERNED TAG data_region
  DESCRIPTION 'Geographic region the data belongs to; drives row-level filtering'
  VALUES ('EU', 'NA', 'APAC');

-- Key-only tag: marks the COLUMN that carries each row's region code (EU/NA/APAC). A policy can only
-- find columns through tags, never by name, and a column cannot carry one fixed data_region value
-- because a fact table holds rows from every region.
CREATE GOVERNED TAG region_code
  DESCRIPTION 'Marks the column holding a row region code (EU/NA/APAC); drives the row filter';

-- ---- Structural rule: every gold table must declare an owner and a sensitivity -----------------
CREATE GOVERNED TAG owner_team
  DESCRIPTION 'Team accountable for the data product'
  VALUES ('data_platform', 'analytics', 'compliance', 'regional_sales');

CREATE GOVERNED TAG data_sensitivity
  DESCRIPTION 'Business sensitivity of the table'
  VALUES ('public', 'internal', 'confidential', 'restricted');

-- ---- Secure-by-default control tag ------------------------------------------------------------
CREATE GOVERNED TAG review_status
  DESCRIPTION 'Steward review state; pending objects are masked by default'
  VALUES ('pending', 'reviewed');

-- Split of duties (optional, once a steward group exists): admins keep CREATE/MANAGE, stewards only ASSIGN.
-- GRANT ASSIGN ON GOVERNED TAG classification TO `data_steward`;
