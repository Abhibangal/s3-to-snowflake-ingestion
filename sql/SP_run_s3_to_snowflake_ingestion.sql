CREATE OR REPLACE PROCEDURE run_s3_to_snowflake_ingestion(
    p_data_source STRING,   -- e.g. 'aact', 'citeline', or NULL
    p_adhoc_id    STRING    -- e.g. 'adhoc_001' or NULL
)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION = 3.10
PACKAGES = (
    'snowflake-snowpark-python',
    'pyyaml'
)
IMPORTS = (
    -- CI/CD will replace __BRANCH__ with dev or main
    '@CT_PROTOCOL.LINKML.S3_TO_SF_GIT_REPO/branches/dev/ingestion/aws_s3_snf_ingestion.py',
    '@CT_PROTOCOL.LINKML.S3_TO_SF_GIT_REPO/branches/dev/config/aact_ingest_config.yaml'
)
HANDLER = 'main'
EXECUTE AS CALLER
AS
$$
import yaml
from ingestion.aws_s3_snf_ingestion import run

def main(session, p_data_source, p_adhoc_id):
    """
    Snowflake native ingestion entry point
    """

    # --------------------------------------------------
    # Load YAML (stage + target DB/schema only)
    # --------------------------------------------------
    with open('aact_ingest_config.yaml', 'r') as f:
        cfg = yaml.safe_load(f)

    # --------------------------------------------------
    # Execute ingestion engine
    # --------------------------------------------------
    stats = run(
        session=session,
        cfg=cfg,
        data_source=p_data_source,
        adhoc_id=p_adhoc_id
    )

    return stats
$$;
