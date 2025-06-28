-- Create table to store schema drift results
CREATE TABLE IF NOT EXISTS SCHEMA_DRIFT_RESULTS (
    object_name STRING,
    is_compliant BOOLEAN,
    missing_columns ARRAY,
    extra_columns ARRAY,
    mismatched_types ARRAY,
    last_checked TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP
);

-- Stored procedure to check schema drift for enforced data contracts
CREATE OR REPLACE PROCEDURE CHECK_SCHEMA_DRIFT()
RETURNS TABLE (
    object_name STRING,
    is_compliant BOOLEAN,
    missing_columns ARRAY,
    extra_columns ARRAY,
    mismatched_types ARRAY,
    last_checked TIMESTAMP_LTZ
)
LANGUAGE JAVASCRIPT
AS
$$
var results = [];
var contract_stmt = snowflake.createStatement({sqlText:
    `SELECT object_name, expected_schema
       FROM DATA_CONTRACTS
      WHERE enforced = TRUE`});
var contract_rs = contract_stmt.execute();
while (contract_rs.next()) {
    var objName = contract_rs.getColumnValue(1);
    var expectedSchema = JSON.parse(contract_rs.getColumnValue(2));

    var tokens = objName.split('.');
    var db = tokens[0];
    var sch = tokens[1];
    var table = tokens[2];

    var actual_stmt = snowflake.createStatement({sqlText:
        `SELECT column_name, data_type
           FROM "${db}".information_schema.columns
          WHERE table_schema = '${sch}' AND table_name = '${table}'`});
    var actual_rs = actual_stmt.execute();
    var actualMap = {};
    while (actual_rs.next()) {
        actualMap[actual_rs.getColumnValue(1).toUpperCase()] = actual_rs.getColumnValue(2).toUpperCase();
    }

    var expectedMap = {};
    for (var i = 0; i < expectedSchema.length; i++) {
        var col = expectedSchema[i];
        expectedMap[col.name.toUpperCase()] = col.type.toUpperCase();
    }

    var missing = [];
    var mismatched = [];
    for (var key in expectedMap) {
        if (!(key in actualMap)) {
            missing.push(key);
        } else if (expectedMap[key] !== actualMap[key]) {
            mismatched.push(key);
        }
    }

    var extra = [];
    for (var key in actualMap) {
        if (!(key in expectedMap)) {
            extra.push(key);
        }
    }

    var compliant = (missing.length === 0 && extra.length === 0 && mismatched.length === 0);

    snowflake.createStatement({
        sqlText: `INSERT INTO SCHEMA_DRIFT_RESULTS(
                    object_name, is_compliant, missing_columns,
                    extra_columns, mismatched_types, last_checked)
                  SELECT ?, ?, PARSE_JSON(?), PARSE_JSON(?), PARSE_JSON(?), CURRENT_TIMESTAMP()`,
        binds: [objName, compliant, JSON.stringify(missing), JSON.stringify(extra), JSON.stringify(mismatched)]
    }).execute();

    results.push({
        object_name: objName,
        is_compliant: compliant,
        missing_columns: missing,
        extra_columns: extra,
        mismatched_types: mismatched,
        last_checked: new Date()
    });
}
return results;
$$;
