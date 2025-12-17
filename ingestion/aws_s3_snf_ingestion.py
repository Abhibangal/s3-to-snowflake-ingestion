import json
import uuid
from datetime import datetime

# ------------------------
# Helpers
# ------------------------
def normalize_stage(stage):
    return stage if stage.startswith("@") else f"@{stage}"

def detect_env(cfg):
    branch = cfg.get("current_branch", "dev")
    return cfg["branch_map"].get(branch, "dev")

def get_env_cfg(cfg):
    env = detect_env(cfg)
    return cfg["environments"][env]

def build_copy_options_sql(copy_opts):
    clauses = []
    for k, v in (copy_opts or {}).items():
        key = k.upper()
        if isinstance(v, bool):
            clauses.append(f"{key} = {str(v).upper()}")
        elif isinstance(v, (int, float)):
            clauses.append(f"{key} = {v}")
        else:
            clauses.append(f"{key} = '{v}'")
    return "\n".join(clauses)

# ------------------------
# Fetch configs
# ------------------------
def get_dataset_configs(session, data_source=None,database=None, adhoc_id=None):

    if adhoc_id:
        return session.sql(f"""
            SELECT *
            FROM {database}.CONFIG_SCH.INGESTION_ADHOC_CONFIG
            WHERE adhoc_id = %s
              AND status = 'PENDING'
        """, [adhoc_id]).collect()

    if data_source:
        return session.sql(f"""
            SELECT *
            FROM {database}.CONFIG_SCH.INGESTION_DATASET_CONFIG
            WHERE is_active = TRUE
              AND data_source = %s
        """, [data_source]).collect()

    return session.sql("""
        SELECT *
        FROM {database}.CONFIG_SCH.INGESTION_DATASET_CONFIG
        WHERE is_active = TRUE
    """).collect()

# ------------------------
# Main Runner
# ------------------------
def run(session, cfg, data_source=None, adhoc_id=None):

    env_cfg = get_env_cfg(cfg)
    snow_cfg = env_cfg["snowflake"]
    stage_cfg = env_cfg["stage"]

    database = snow_cfg["database"]
    schema = snow_cfg["schema"]

    stage = normalize_stage(stage_cfg["name"])
    root = (stage_cfg.get("root_path") or "").strip("/")

    year = str(datetime.utcnow().year)
    month = str(datetime.utcnow().month).zfill(2)

    stats = {
        "files_attempted": 0,
        "files_loaded": 0,
        "files_failed": 0,
        "files_already_loaded": 0
    }

    datasets = get_dataset_configs(session, data_source,database, adhoc_id)

    for ds in datasets:
        stats["files_attempted"] += 1
        event_id = str(uuid.uuid4())
        started = datetime.utcnow()

        table_fqn = f"{database}.{schema}.{ds['TABLE_NAME']}"
        prefix = ds["S3_PATH_TEMPLATE"].format(year=year, month=month).rstrip("/")
        file_path = f"{prefix}/{ds['FILE_NAME']}"
        full_path = f"{root}/{file_path}" if root else file_path

        try:
            # ------------------------
            # CREATE TABLE
            # ------------------------
            if ds["FILE_TYPE"].upper() == "JSON":
                session.sql(f"""
                    CREATE TABLE IF NOT EXISTS {table_fqn} (RAW VARIANT)
                """).collect()
            else:
                session.sql(f"""
                    CREATE TABLE IF NOT EXISTS {table_fqn}
                    USING TEMPLATE (
                      SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*))
                      FROM TABLE(
                        INFER_SCHEMA(
                          LOCATION => '{stage}/{full_path}',
                          FILE_FORMAT => '{ds["FILE_FORMAT_OBJECT"]}'
                        )
                      )
                    )
                """).collect()

            session.sql(
                f"ALTER TABLE {table_fqn} SET ENABLE_SCHEMA_EVOLUTION = TRUE"
            ).collect()

            # ------------------------
            # QUERY TAG
            # ------------------------
            if ds["QUERY_TAG"]:
                session.sql(
                    "ALTER SESSION SET QUERY_TAG = %s",
                    [json.dumps(ds["QUERY_TAG"])]
                ).collect()

            # ------------------------
            # COPY
            # ------------------------
            copy_sql = f"""
                COPY INTO {table_fqn}
                FROM '{stage}/{full_path}'
                FILE_FORMAT = {ds["FILE_FORMAT_OBJECT"]}
                {build_copy_options_sql(ds["COPY_OPTIONS"])}
            """
            session.sql(copy_sql).collect()

            result = session.sql(
                "SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))"
            ).collect()

            rows_loaded = max(
                [r["ROWS_LOADED"] for r in result if r["ROWS_LOADED"] is not None],
                default=0
            )

            if rows_loaded > 0:
                stats["files_loaded"] += 1
            else:
                stats["files_already_loaded"] += 1

        except Exception:
            stats["files_failed"] += 1

        finally:
            session.sql("ALTER SESSION UNSET QUERY_TAG").collect()

    return stats
