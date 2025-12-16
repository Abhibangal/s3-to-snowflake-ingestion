CREATE OR REPLACE PROCEDURE run_s3_to_snowflake_ingestion()
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION = 3.10
PACKAGES = ('snowflake-snowpark-python', 'pyyaml')
IMPORTS = ('@INGEST_REPO')   -- Git repository
HANDLER = 'main'
EXECUTE AS CALLER
AS
$$
import yaml
from ingestion.ingest_runner import run

def main(session):
    with open('config/ingest_config.yaml', 'r') as f:
        cfg = yaml.safe_load(f)
    return run(session, cfg)
$$;
