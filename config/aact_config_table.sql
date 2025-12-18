CREATE OR REPLACE TABLE CONFIG_SCH.INGESTION_SOURCE_ENV (
    source_name        STRING,        -- aact, citeline
    env_name            STRING,        -- dev / prod
    target_database     STRING,
    target_schema       STRING,
    stage_name          STRING,        -- @kipi_acct_stg
    current_env_flag    BOOLEAN        -- TRUE for active env
);


CREATE TABLE IF NOT EXISTS CONFIG_SCH.INGESTION_DATASET_CONFIG (
    dataset_name        STRING,
    data_source         STRING,

    table_name          STRING,
    s3_path_template    STRING,
    file_name           STRING,

    file_type           STRING,   -- CSV | JSON
    file_format_object  STRING,

    copy_options        VARIANT,
    query_tag           VARIANT,

    is_active           BOOLEAN,

    created_at          TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP,
    updated_at          TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP
);



CREATE TABLE IF NOT EXISTS CONFIG_SCH.INGESTION_ADHOC_CONFIG (
    adhoc_id            STRING,
    dataset_name        STRING,
    data_source         STRING,

    table_name          STRING,
    s3_path_template    STRING,
    file_name           STRING,

    file_type           STRING,
    file_format_object  STRING,

    copy_options        VARIANT,
    query_tag           VARIANT,

    status              STRING DEFAULT 'PENDING', -- PENDING | COMPLETED | FAILED

    created_at          TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP
);




INSERT INTO CONFIG_SCH.INGESTION_SOURCE_ENV VALUES
('aact','dev','CT_PROTOCOL','LINKML','@kipi_acct_stg', TRUE),
('aact','prod','RAW','STAGE_RAW','@prod_stage', FALSE);



INSERT INTO CONFIG_SCH.INGESTION_DATASET_CONFIG (
    dataset_name, data_source, table_name,
    s3_path_template, file_name,
    file_type, file_format_object,
    copy_options, query_tag, is_active
)
VALUES (
    'ctgov_all_browse_conditions',
    'aact',
    'browse_conditions',
    'year={year}/month={month}/',
    'ctgov_all_browse_conditions.csv',
    'CSV',
    'FF_AACT_CSV',
    OBJECT_CONSTRUCT('match_by_column_name','CASE_INSENSITIVE'),
    OBJECT_CONSTRUCT(
        'app','data_ingestion',
        'dataset','browse_conditions',
        'data_source','aact'
    ),
    TRUE
);



INSERT INTO CONFIG_SCH.INGESTION_DATASET_CONFIG (
    dataset_name, data_source, table_name,
    s3_path_template, file_name,
    file_type, file_format_object,
    copy_options, query_tag, is_active
)
VALUES (
    'ctgov_all_browse_interventions',
    'aact',
    'browse_interventions',
    'year={year}/month={month}/',
    'ctgov_all_browse_interventions.csv',
    'CSV',
    'FF_AACT_CSV',
    OBJECT_CONSTRUCT('match_by_column_name','CASE_INSENSITIVE'),
    OBJECT_CONSTRUCT(
        'app','data_ingestion',
        'dataset','browse_interventions',
        'data_source','aact'
    ),
    TRUE
);

INSERT INTO CONFIG_SCH.INGESTION_ADHOC_CONFIG (
    adhoc_id, dataset_name, data_source,
    table_name, s3_path_template, file_name,
    file_type, file_format_object,
    copy_options, query_tag
)
VALUES (
    'adhoc_001',
    'browse_conditions_adhoc',
    'aact',
    'browse_conditions',
    'year=2024/month=01/',
    'ctgov_all_browse_conditions.csv',
    'CSV',
    'FF_AACT_CSV',
    OBJECT_CONSTRUCT('force',TRUE),
    OBJECT_CONSTRUCT('app','data_ingestion','run_type','adhoc')
);
