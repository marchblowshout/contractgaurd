MERGE INTO DATA_CONTRACTS AS target
USING DATA_CONTRACTS_STAGING AS src
  ON target.object_name = src.object_name
WHEN MATCHED THEN
  UPDATE SET
    target.expected_schema = src.expected_schema,
    target.expected_dependencies = src.expected_dependencies,
    target.target_lag = src.target_lag,
    target.updated_at = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN
  INSERT (
    object_name,
    expected_schema,
    expected_dependencies,
    target_lag,
    updated_at
  ) VALUES (
    src.object_name,
    src.expected_schema,
    src.expected_dependencies,
    src.target_lag,
    CURRENT_TIMESTAMP()
  );
