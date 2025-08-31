DECLARE datasets ARRAY<STRING> DEFAULT ['dataset_1', 'dataset_2'];

CREATE TABLE IF NOT EXISTS `{{params.destination_dataset}}.data_quality_duplication`
(
    test_datetime       DATETIME NOT NULL OPTIONS (DESCRIPTION = 'This is the datetime when the duplication test was executed'),
    test_name           STRING   NOT NULL OPTIONS (DESCRIPTION = 'This is the name of the data quality test'),
    table_path          STRING   NOT NULL OPTIONS (DESCRIPTION = 'This is the affected table path'),
    affected_field_name STRING   NOT NULL OPTIONS (DESCRIPTION = 'This is the name of the affected fields'),
    affected_record     INT64    NOT NULL OPTIONS (DESCRIPTION = 'This is the number of affected records'))
    PARTITION BY DATE(test_datetime)
    OPTIONS (DESCRIPTION = "Contains records of all the failed data quality duplication tests",
        LABELS = [("table_type", "data_quality"), ("time_zone", "local_timezone")]);

CREATE OR REPLACE TEMPORARY TABLE meta_data
(
    primary_key_field STRING,
    table_path        STRING
);

FOR record IN (SELECT * FROM UNNEST(datasets) AS dataset)
    DO
        EXECUTE IMMEDIATE FORMAT("""
        INSERT INTO meta_data
        SELECT
            STRING_AGG(column_name, ', ')         AS primary_key_field,
            CONCAT(table_schema, '.', table_name) AS table_path
        FROM `{{params.destination_project}}.%s.INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE`
        WHERE SPLIT(constraint_name, '.')[OFFSET(1)] = 'pk$'
        GROUP BY ALL
    """, record.dataset);
    END FOR;

CREATE OR REPLACE TEMPORARY TABLE duplication_test
(
    test_datetime       DATETIME,
    test_name           STRING,
    table_path          STRING,
    affected_field_name STRING,
    affected_record     INT64
);

FOR current_table IN (SELECT table_path, primary_key_field
                      FROM meta_data)
    DO
        BEGIN
            EXECUTE IMMEDIATE FORMAT("""
            INSERT INTO duplication_test
            WITH duplication AS (
            SELECT
              %s,
              COUNT(1) as affected_record
            FROM `{{params.destination_project}}.%s`
            GROUP BY ALL
            HAVING COUNT(1) > 1)

            SELECT
              CURRENT_DATETIME('CET') AS test_datetime,
              'duplication' AS test_name,
              '%s' AS table_path,
              '%s' AS affected_field_name,
              COUNT(affected_record) AS affected_record
            FROM duplication
            GROUP BY ALL
            HAVING affected_record > 0""",
         current_table.primary_key_field, current_table.table_path,
         current_table.table_path, current_table.primary_key_field);
        END;
    END FOR;

INSERT INTO `{{params.destination_dataset}}.data_quality_duplication` (test_datetime,
                                                                       test_name,
                                                                       table_path,
                                                                       affected_field_name,
                                                                       affected_record)

SELECT
    test_datetime,
    test_name,
    table_path,
    affected_field_name,
    affected_record
FROM duplication_test;
