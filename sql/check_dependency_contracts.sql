-- Create table to store dependency contract results
CREATE TABLE IF NOT EXISTS DAG_CONTRACT_RESULTS (
    object_name STRING,
    is_compliant BOOLEAN,
    missing_dependencies ARRAY,
    unexpected_dependencies ARRAY,
    last_checked TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP
);

-- Stored procedure to check dependency contracts
CREATE OR REPLACE PROCEDURE CHECK_DEPENDENCY_CONTRACTS()
RETURNS TABLE (
    object_name STRING,
    is_compliant BOOLEAN,
    missing_dependencies ARRAY,
    unexpected_dependencies ARRAY,
    last_checked TIMESTAMP_LTZ
)
LANGUAGE JAVASCRIPT
AS
$$
var results = [];
var contracts_stmt = snowflake.createStatement({sqlText:
    `SELECT object_name, expected_dependencies
       FROM DATA_CONTRACTS
      WHERE enforced = TRUE`});
var contracts_rs = contracts_stmt.execute();
while (contracts_rs.next()) {
    var objName = contracts_rs.getColumnValue(1);
    var expectedDeps = contracts_rs.getColumnValue(2);
    if (typeof expectedDeps === 'string') {
        expectedDeps = JSON.parse(expectedDeps);
    }
    if (!Array.isArray(expectedDeps)) {
        expectedDeps = [];
    }

    var parts = objName.split('.');
    var db = parts[0];
    var sch = parts[1];
    var name = parts[2];

    var deps_stmt = snowflake.createStatement({sqlText:
        `SELECT DISTINCT referenced_object_database || '.' || referenced_object_schema || '.' || referenced_object_name
           FROM SNOWFLAKE.ACCOUNT_USAGE.OBJECT_DEPENDENCIES
          WHERE object_database = ? AND object_schema = ? AND object_name = ?`,
        binds: [db, sch, name]});
    var deps_rs = deps_stmt.execute();
    var actualDeps = [];
    while (deps_rs.next()) {
        actualDeps.push(deps_rs.getColumnValue(1));
    }

    var missing = [];
    expectedDeps.forEach(function(d) {
        if (actualDeps.indexOf(d) === -1) {
            missing.push(d);
        }
    });

    var unexpected = [];
    actualDeps.forEach(function(d) {
        if (expectedDeps.indexOf(d) === -1) {
            unexpected.push(d);
        }
    });

    var compliant = (missing.length === 0 && unexpected.length === 0);

    snowflake.createStatement({
        sqlText: `INSERT INTO DAG_CONTRACT_RESULTS(
                    object_name, is_compliant, missing_dependencies,
                    unexpected_dependencies, last_checked)
                  SELECT ?, ?, PARSE_JSON(?), PARSE_JSON(?), CURRENT_TIMESTAMP()`,
        binds: [objName, compliant, JSON.stringify(missing), JSON.stringify(unexpected)]
    }).execute();

    results.push({
        object_name: objName,
        is_compliant: compliant,
        missing_dependencies: missing,
        unexpected_dependencies: unexpected,
        last_checked: new Date()
    });
}
return results;
$$;
