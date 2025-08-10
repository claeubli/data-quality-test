CREATE TABLE IF NOT EXISTS `{{params.destination_dataset}}.staging_data_quality_domain`
(
    table_path          STRING NOT NULL OPTIONS (DESCRIPTION = 'This is the affected table path'),
    affected_field_name STRING NOT NULL OPTIONS (DESCRIPTION = 'This is the name of the affected field'),
    expected_value      ARRAY<STRING> OPTIONS (DESCRIPTION = 'These are the expected values for the field'),
    type                STRING NOT NULL OPTIONS (DESCRIPTION = 'Has to be one of the following: "categoric", "numeric"'),
    creation_datetime   DATETIME DEFAULT CURRENT_DATETIME() NOT NULL OPTIONS (DESCRIPTION = 'The time when the record was created')
)
    OPTIONS (DESCRIPTION = "Contains meta data used to run the data quality domain test",
        LABELS = [("table_type", "data_quality")]);

INSERT INTO `{{params.destination_dataset}}.staging_data_quality_domain`
(
    table_path,
    affected_field_name,
    expected_value,
    type
)

VALUES
('project_name.dataset_name.table_name', 'field_name', ['pending', 'approved', 'declined'], 'categoric'),
('project_name.dataset_name.table_name', 'field_name', ['0', '1'], 'numeric');

CREATE TABLE IF NOT EXISTS `{{params.destination_dataset}}.data_quality_domain`
(
    table_path             STRING NOT NULL OPTIONS (DESCRIPTION = 'This is the affected table path'),
    affected_field_name    STRING NOT NULL OPTIONS (DESCRIPTION = 'This is the name of the affected field'),
    unexpected_field_value ARRAY <STRING> OPTIONS (DESCRIPTION = 'These are the unexpected values for the field'),
    creation_datetime      DATETIME DEFAULT CURRENT_DATETIME() NOT NULL OPTIONS (DESCRIPTION = 'The time when the record was created')
)
  OPTIONS (DESCRIPTION = "Contains domain test data on fields with unexpected values",
        LABELS = [("table_type", "data_quality")]);

FOR record IN (SELECT * FROM `{{params.destination_dataset}}.staging_data_quality_domain`)
    DO
        EXECUTE IMMEDIATE FORMAT("""
        INSERT INTO `{{params.destination_dataset}}.data_quality_domain`
        SELECT '%s' AS table_path, '%s' AS affected_field_name, ARRAY_AGG(DISTINCT CAST(%s AS STRING)) AS unexpected_field_value, CURRENT_DATETIME AS creation_datetime
        FROM `%s`
        WHERE '%s' = 'categoric' AND CAST(%s AS STRING) NOT IN UNNEST(SPLIT('%s', ','))
        HAVING ARRAY_LENGTH(ARRAY_AGG(DISTINCT %s)) > 0
        UNION ALL
        SELECT '%s' AS table_path, '%s' AS affected_field_name, ARRAY_AGG(DISTINCT CAST(%s AS STRING)) AS unexpected_field_value, CURRENT_DATETIME AS creation_datetime
        FROM `%s`
        WHERE '%s' = 'numeric' AND SAFE_CAST(%s AS FLOAT64) NOT BETWEEN
        SAFE_CAST(SPLIT('%s', ',')[OFFSET(0)] AS FLOAT64) AND
        SAFE_CAST(SPLIT('%s', ',')[OFFSET(1)] AS FLOAT64)
        HAVING ARRAY_LENGTH(ARRAY_AGG(DISTINCT %s)) > 0
    """, record.table_path, record.affected_field_name, record.affected_field_name,
                                 record.table_path,
                                 record.type, record.affected_field_name, ARRAY_TO_STRING(record.expected_value, ','),
                                 record.affected_field_name,
                                 record.table_path, record.affected_field_name, record.affected_field_name,
                                 record.table_path,
                                 record.type, record.affected_field_name,
                                 ARRAY_TO_STRING(record.expected_value, ','),
                                 ARRAY_TO_STRING(record.expected_value, ','),
                                 record.affected_field_name);
    END FOR;
