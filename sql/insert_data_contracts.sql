INSERT INTO DATA_CONTRACTS (
  object_name,
  object_type,
  expected_schema,
  expected_dependencies,
  target_lag,
  enforced
)
VALUES (
  'MY_DB.MY_SCHEMA.MY_DYNAMIC_TABLE',
  'DYNAMIC_TABLE',
  PARSE_JSON('[{"name": "id", "type": "NUMBER"}, {"name": "name", "type": "STRING"}, {"name": "created_at", "type": "TIMESTAMP_LTZ"}]'),
  ARRAY_CONSTRUCT('MY_DB.MY_SCHEMA.SOURCE_TABLE'),
  '5 minutes',
  TRUE
);
