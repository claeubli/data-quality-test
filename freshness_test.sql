DECLARE projects ARRAY<STRING> DEFAULT ['dataset_1', 'dataset_2'];
DECLARE delete_date DATE DEFAULT DATE_TRUNC(
        DATE_SUB(CURRENT_DATE, INTERVAL CAST('{{params.date_range_months}}' AS INT) MONTH), YEAR);

CREATE TABLE IF NOT EXISTS `{{params.destination_dataset}}.freshness`
(
    test_datetime       DATETIME DEFAULT CURRENT_DATETIME('CET') NOT NULL OPTIONS (DESCRIPTION = 'This is the datetime when the freshness test was executed'),
    test_name           STRING   NOT NULL OPTIONS (DESCRIPTION = 'This is the name of the data quality test'),
    table_path          STRING   NOT NULL OPTIONS (DESCRIPTION = 'This is the affected table path'),
    last_modified_datetime DATETIME NOT NULL OPTIONS (DESCRIPTION = 'This is the last datetime the metadata of the table was modified'))
    PARTITION BY DATE(test_datetime)
    OPTIONS (DESCRIPTION = "Contains records of all tables that weren't refreshed in the last 2 days",
        LABELS = [("table_type", "data_quality"), ("time_zone", "local_timezone")]);

CREATE OR REPLACE TEMPORARY TABLE freshness_meta_data
(
    table_path        STRING,
    last_modified_datetime DATETIME
);

FOR record IN (SELECT * FROM UNNEST(projects) AS project)
    DO
        EXECUTE IMMEDIATE FORMAT("""
        INSERT INTO freshness_meta_data
        SELECT
        CONCAT(project_id, '.', dataset_id, '.', table_id) AS table_path,
        DATETIME(TIMESTAMP_MILLIS(last_modified_time), 'CET') AS last_modified_datetime
        FROM `{{params.destination_project}}.%s.__TABLES__`
        WHERE TIMESTAMP_DIFF(CURRENT_TIMESTAMP, TIMESTAMP_MILLIS(last_modified_time), DAY) > 1;
    """, record.project);
    END FOR;

BEGIN TRANSACTION;

DELETE
FROM `{{params.destination_dataset}}.freshness`
WHERE DATE(test_datetime) < delete_date;

INSERT INTO `{{params.destination_dataset}}.freshness`  (test_name,
                                                         table_path,
                                                         last_modified_datetime)

SELECT
    'freshness' AS test_name,
    table_path,
    last_modified_datetime
FROM freshness_meta_data;

ASSERT (SELECT IF(COUNT(1) > 0, FALSE, TRUE)
        FROM freshness_meta_data
        WHERE DATETIME_DIFF(CURRENT_DATETIME('CET'), last_modified_datetime, DAY) = 2)
    AS 'Data freshness test failed: To see which tables were not refreshed in the last two days run the following query:\nSELECT * FROM `{{params.destination_dataset}}.freshness` ORDER BY last_modified_datetime DESC LIMIT 100';

COMMIT TRANSACTION;
