CREATE TABLE IF NOT EXISTS DATA_CONTRACTS (
    object_name STRING NOT NULL,
    object_type STRING NOT NULL,
    expected_schema VARIANT,
    expected_dependencies ARRAY,
    target_lag STRING,
    enforced BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    updated_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

