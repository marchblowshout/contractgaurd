CREATE TABLE IF NOT EXISTS DAG_CONTRACT_RESULTS (
  object_name STRING,
  is_compliant BOOLEAN,
  missing_dependencies ARRAY,
  unexpected_dependencies ARRAY,
  last_checked TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP
);

