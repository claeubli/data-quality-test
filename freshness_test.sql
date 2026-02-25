CREATE OR REPLACE PROCEDURE `{{params.destination_project}}`.`{{ params.destination_dataset }}.data_freshness`(
    datasets ARRAY<STRING>,
    temporary_table_name STRING
)
    OPTIONS ( DESCRIPTION = "This procedure will create a temporary table containing one record for each table in a dataset" )

BEGIN
EXECUTE IMMEDIATE FORMAT("""
CREATE OR REPLACE TEMPORARY TABLE `%s`
(
    table_path              STRING,
    last_modified_datetime  DATETIME
);
""", temporary_table_name);

FOR record IN (SELECT * FROM UNNEST(datasets) AS dataset)
    DO
        EXECUTE IMMEDIATE FORMAT("""
        INSERT INTO %s
        SELECT
        CONCAT(project_id, '.', dataset_id, '.', table_id) AS table_path,
        DATETIME(TIMESTAMP_MILLIS(last_modified_time), 'CET') AS last_modified_datetime
        FROM `{{params.destination_project}}.%s.__TABLES__`;
    """, temporary_table_name, record.dataset);
    END FOR;
END;