DECLARE domain_constraints ARRAY<STRUCT<
    table_path STRING,
    affected_field_name STRING,
    expected_value ARRAY<STRING>,
    field_type STRING>>;

SET domain_constraints = [
('project_name.dataset_name.table_name', 'field_name', ['pending', 'approved', 'declined'], 'categoric'),
('project_name.dataset_name.table_name', 'field_name', ['0', '1'], 'numeric')];

CREATE OR REPLACE PROCEDURE `{{params.destination_project}}`.`{{ params.destination_dataset }}.data_domain`(
    domain_constraints ARRAY<STRUCT<
        table_path STRING,
        affected_field_name STRING,
        expected_value ARRAY<STRING>,
        field_type STRING>>,
    temporary_table_name STRING
)
    OPTIONS ( DESCRIPTION = "This procedure will create a temporary table containing one record for each field not in line with the domain constraints" )

BEGIN
EXECUTE IMMEDIATE FORMAT("""
CREATE OR REPLACE TEMPORARY TABLE `%s`
(
    table_path              STRING,
    affected_field_name     STRING,
    unexpected_field_value  ARRAY<STRING>
);
""", temporary_table_name);

FOR record IN (SELECT * FROM UNNEST(domain_constraints) AS domain_constraint)
    DO
        EXECUTE IMMEDIATE FORMAT("""
        INSERT INTO %s
        SELECT '%s' AS table_path, '%s' AS affected_field_name, ARRAY_AGG(DISTINCT CAST(%s AS STRING)) AS unexpected_field_value
        FROM `%s`
        WHERE '%s' = 'categoric' AND CAST(%s AS STRING) NOT IN UNNEST(SPLIT('%s', ','))
        HAVING ARRAY_LENGTH(ARRAY_AGG(DISTINCT %s)) > 0
        UNION ALL
        SELECT '%s' AS table_path, '%s' AS affected_field_name, ARRAY_AGG(DISTINCT CAST(%s AS STRING)) AS unexpected_field_value
        FROM `%s`
        WHERE '%s' = 'numeric' AND SAFE_CAST(%s AS FLOAT64) NOT BETWEEN
        SAFE_CAST(SPLIT('%s', ',')[OFFSET(0)] AS FLOAT64) AND
        SAFE_CAST(SPLIT('%s', ',')[OFFSET(1)] AS FLOAT64)
        HAVING ARRAY_LENGTH(ARRAY_AGG(DISTINCT %s)) > 0
    """, temporary_table_name, record.table_path, record.affected_field_name, record.affected_field_name,
                                 record.table_path,
                                 record.field_type, record.affected_field_name, ARRAY_TO_STRING(record.expected_value, ','),
                                 record.affected_field_name,
                                 record.table_path, record.affected_field_name, record.affected_field_name,
                                 record.table_path,
                                 record.field_type, record.affected_field_name,
                                 ARRAY_TO_STRING(record.expected_value, ','),
                                 ARRAY_TO_STRING(record.expected_value, ','),
                                 record.affected_field_name);
    END FOR;
    END;