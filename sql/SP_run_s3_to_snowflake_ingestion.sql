CREATE OR REPLACE PROCEDURE run_s3_to_snowflake_ingestion(
    p_config_file STRING,   -- e.g. 'aact_ingest_config.yaml'
    p_data_source STRING,
    p_adhoc_id    STRING
)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION = 3.10
PACKAGES = ('snowflake-snowpark-python', 'pyyaml')
IMPORTS = (
  '@CT_PROTOCOL.LINKML.S3_TO_SF_GIT_REPO/branches/dev/ingestion/aws_s3_snf_ingestion.py',
  '@CT_PROTOCOL.LINKML.S3_TO_SF_GIT_REPO/branches/dev/config/aact_ingest_config.yaml')
HANDLER = 'main'
EXECUTE AS CALLER
AS
$$
from ingestion.aws_s3_snf_ingestion import run

def main(session, p_config_file, p_data_source, p_adhoc_id):
    return run(session, p_config_file, p_data_source, p_adhoc_id)
$$;





-- call run_s3_to_snowflake_ingestion('aact',null);