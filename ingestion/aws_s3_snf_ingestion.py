import json
import uuid
from datetime import datetime

# ------------------------
# Helpers (RESTORED)
# ------------------------
def normalize_stage(stage):
    return stage if stage.startswith("@") else f"@{stage}"

def sql_string_literal(val: str) -> str:
    return val.replace("'", "''")

def normalize_variant(val):
    """
    Snowpark VARIANT may come as dict or JSON string.
    Normalize to Python dict.
    """
    if val is None:
        return {}
    if isinstance(val, dict):
        return val
    if isinstance(val, str):
        return json.loads(val)
    raise TypeError(f"Unsupported VARIANT type: {type(val)}")

def build_copy_options_sql(copy_opts):
    copy_opts = normalize_variant(copy_opts)

    clauses = []
    for k, v in copy_opts.items():
        key = k.upper()
        if isinstance(v, bool):
            clauses.append(f"{key} = {str(v).upper()}")
        elif isinstance(v, (int, float)):
            clauses.append(f"{key} = {v}")
        else:
            clauses.append(f"{key} = '{sql_string_literal(str(v))}'")
    return "\n".join(clauses)

# ------------------------
# Fetch environment config
# ------------------------
def get_env_config(session, data_source):
    rows = session.sql("""
        SELECT target_database,
               target_schema,
               stage_name
        FROM CONFIG_SCH.INGESTION_SOURCE_ENV
        WHERE source_name = ?
          AND current_env_flag = TRUE
    """, [data_source]).collect()

    if not rows:
        raise Exception(f"No active env config for source '{data_source}'")

    return rows[0]

# ------------------------
# Fetch dataset configs
# ------------------------
def get_dataset_configs(session, database, data_source=None, adhoc_id=None):

    if adhoc_id:
        return session.sql(f"""
            SELECT *
            FROM {database}.CONFIG_SCH.INGESTION_ADHOC_CONFIG
            WHERE adhoc_id = ?
              AND status = 'PENDING'
        """, [adhoc_id]).collect()

    return session.sql(f"""
        SELECT *
        FROM {database}.CONFIG_SCH.INGESTION_DATASET_CONFIG
        WHERE data_source = ?
    """, [data_source]).collect()

# ------------------------
# Main Runner
# ------------------------
def run(session, data_source, adhoc_id=None):

    # --------------------------------------------------
    # ENV CONFIG
    # --------------------------------------------------
    env_cfg = get_env_config(session, data_source)

    database = env_cfg["TARGET_DATABASE"]
    schema   = env_cfg["TARGET_SCHEMA"]
    stage    = normalize_stage(env_cfg["STAGE_NAME"])

    year = str(datetime.utcnow().year)
    month = str(datetime.utcnow().month).zfill(2)

    stats = {
        "files_attempted": 0,
        "files_loaded": 0,
        "files_failed": 0,
        "files_already_loaded": 0
    }

    datasets = get_dataset_configs(session, database, data_source, adhoc_id)

    for ds in datasets:
        stats["files_attempted"] += 1

        table_fqn = f"{database}.{schema}.{ds['TABLE_NAME']}"

        if adhoc_id:
            file_path = f"{ds['S3_PATH']}/{ds['FILE_NAME']}"
        else:
            prefix = ds["S3_PATH_TEMPLATE"].format(
                year=year,
                month=month
            ).rstrip("/")
            file_path = f"{prefix}/{ds['FILE_NAME']}"

        try:
            # ------------------------
            # CREATE TABLE
            # ------------------------
            if ds["FILE_TYPE"].upper() == "JSON":
                session.sql(
                    f"CREATE TABLE IF NOT EXISTS {table_fqn} (RAW VARIANT)"
                ).collect()
            else:
                session.sql(f"""
                    CREATE TABLE IF NOT EXISTS {table_fqn}
                    USING TEMPLATE (
                      SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*))
                      FROM TABLE(
                        INFER_SCHEMA(
                          LOCATION => '{stage}/{file_path}',
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
            query_tag = normalize_variant(ds["QUERY_TAG"])
            if query_tag:
                session.sql(
                    f"ALTER SESSION SET QUERY_TAG = "
                    f"'{sql_string_literal(json.dumps(query_tag))}'"
                ).collect()

            # ------------------------
            # COPY
            # ------------------------
            copy_sql = f"""
                COPY INTO {table_fqn}
                FROM '{stage}/{file_path}'
                FILE_FORMAT = {ds["FILE_FORMAT_OBJECT"]}
                {build_copy_options_sql(ds["COPY_OPTIONS"])}
            """
            session.sql(copy_sql).collect()

            # ------------------------
            # COPY RESULT (FIXED LOGIC)
            # ------------------------
            result = session.sql(
                "SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))"
            ).collect()

            status_msg = None
            rows_loaded = 0

            for r in result:
                if "STATUS" in r and r["STATUS"]:
                    status_msg = r["STATUS"]
                if "ROWS_LOADED" in r and r["ROWS_LOADED"] is not None:
                    rows_loaded = max(rows_loaded, int(r["ROWS_LOADED"]))

            if status_msg and "0 files processed" in status_msg.lower():
                stats["files_already_loaded"] += 1

            elif rows_loaded > 0:
                stats["files_loaded"] += 1

            else:
                stats["files_failed"] += 1

        except Exception:
            stats["files_failed"] += 1
            raise

        finally:
            session.sql("ALTER SESSION UNSET QUERY_TAG").collect()

    return stats
