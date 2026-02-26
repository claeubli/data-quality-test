CREATE OR REPLACE PROCEDURE `{{params.destination_project}}`.`{{ params.destination_dataset }}.data_duplication`(
    project_dataset ARRAY<STRING>,
    temporary_table_name STRING
)
    OPTIONS ( DESCRIPTION = "This procedure will create a temporary table containing one record for each table in a dataset containing duplications" )

BEGIN
IF temporary_table_name = 'temp_meta_data_table_name' THEN
    RAISE USING MESSAGE = 'Please choose a different temporary_table_name';
    END IF;

CREATE OR REPLACE TEMPORARY TABLE temp_meta_data_table_name
(
    primary_key_field STRING,
    table_path        STRING
);

FOR record IN (SELECT * FROM UNNEST(project_dataset) AS dataset)
    DO
        EXECUTE IMMEDIATE FORMAT("""
        INSERT INTO temp_meta_data_table_name
        SELECT
            STRING_AGG(column_name, ', ')                             AS primary_key_field,
            CONCAT(table_catalog, '.', table_schema, '.', table_name) AS table_path
        FROM `%s.INFORMATION_SCHEMA.KEY_COLUMN_USAGE`
        WHERE SPLIT(constraint_name, '.')[OFFSET(1)] = 'pk$'
        GROUP BY ALL
    """, record.dataset);
    END FOR;

EXECUTE IMMEDIATE FORMAT("""
CREATE OR REPLACE TEMPORARY TABLE `%s`
(
    table_path          STRING,
    affected_field_name STRING,
    affected_record     INT64
);
""", temporary_table_name);

FOR current_table IN (SELECT table_path, primary_key_field
                      FROM temp_meta_data_table_name)
    DO
        BEGIN
            EXECUTE IMMEDIATE FORMAT("""
            INSERT INTO %s
            WITH duplication AS (
            SELECT
              %s,
              COUNT(1) as affected_record
            FROM `%s`
            GROUP BY ALL
            HAVING COUNT(1) > 1)

            SELECT
              '%s' AS table_path,
              '%s' AS affected_field_name,
              COUNT(affected_record) AS affected_record
            FROM duplication
            GROUP BY ALL
            HAVING affected_record > 0""",
         temporary_table_name, current_table.primary_key_field, current_table.table_path,
         current_table.table_path, current_table.primary_key_field);
        END;
    END FOR;
END;