CREATE OR REPLACE PROCEDURE RUN_S3_TO_SNOWFLAKE_INGESTION(
    P_DATA_SOURCE STRING,
    P_ADHOC_ID STRING
)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION = 3.10
PACKAGES = ('snowflake-snowpark-python')
IMPORTS = (
    '@CT_PROTOCOL.LINKML.S3_TO_SF_GIT_REPO/branches/dev/ingestion/aws_s3_snf_ingestion.py'
)
HANDLER = 'main'
EXECUTE AS CALLER
AS
$$
from aws_s3_snf_ingestion import run

def main(session, p_data_source, p_adhoc_id):
    return run(
        session=session,
        data_source=p_data_source,
        adhoc_id=p_adhoc_id
    )
$$; 