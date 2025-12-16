import json
import uuid
from datetime import datetime

# ------------------------
# Helpers
# ------------------------
def normalize_stage(stage):
    return stage if stage.startswith("@") else f"@{stage}"

def qualify_file_format(database, schema, ff):
    return ff if "." in ff else f"{database}.{schema}.{ff}"

def merge_copy_options(defaults, overrides):
    result = {}
    if defaults:
        result.update(defaults)
    if overrides:
        result.update(overrides)
    return result

def build_copy_options_sql(copy_opts):
    clauses = []
    for k, v in copy_opts.items():
        key = k.upper()
        if isinstance(v, bool):
            clauses.append(f"{key} = {str(v).upper()}")
        elif isinstance(v, (int, float)):
            clauses.append(f"{key} = {v}")
        else:
            clauses.append(f"{key} = '{v}'")
    return "\n".join(clauses)

# ------------------------
# Main Runner
# ------------------------
def run(session, cfg):

    env = "dev"   # branch logic can be added later
    env_cfg = cfg["environments"][env]
    snow_cfg = env_cfg["snowflake"]
    stage_cfg = env_cfg["stage"]
    defaults = cfg.get("defaults", {})

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

    for ds in cfg["datasets"]:
        stats["files_attempted"] += 1
        started = datetime.utcnow()
        event_id = str(uuid.uuid4())

        table_fqn = f"{database}.{schema}.{ds['table']}"
        prefix = ds["s3_path_template"].format(year=year, month=month).rstrip("/")
        file_path = f"{prefix}/{ds['file_name']}"

        ff = qualify_file_format(database, schema, ds["file_format_object"])
        copy_opts = merge_copy_options(
            defaults.get("copy_options"),
            ds.get("copy_options")
        )

        try:
            # ------------------------
            # Create table if not exists
            # ------------------------
            session.sql(f"""
            CREATE TABLE IF NOT EXISTS {table_fqn}
            USING TEMPLATE (
              SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*))
              FROM TABLE(
                INFER_SCHEMA(
                  LOCATION => '{stage}/{file_path}',
                  FILE_FORMAT => '{ff}'
                )
              )
            );
            """).collect()

            # ------------------------
            # QUERY TAG
            # ------------------------
            if "query_tag" in ds:
                session.sql(
                    "ALTER SESSION SET QUERY_TAG = %s",
                    [json.dumps(ds["query_tag"])]
                ).collect()

            # ------------------------
            # COPY
            # ------------------------
            copy_sql = f"""
            COPY INTO {table_fqn}
            FROM '{stage}/{file_path}'
            FILE_FORMAT = {ff}
            {build_copy_options_sql(copy_opts)}
            """
            session.sql(copy_sql).collect()

            copy_qid = session.sql("SELECT LAST_QUERY_ID()").collect()[0][0]

            # ------------------------
            # COPY RESULT
            # ------------------------
            result = session.sql(
                "SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))"
            ).collect()

            if result == 'Copy executed with 0 files processed.':
                status = "ALREADY_LOADED"
                rows_loaded = 0
                stats["files_already_loaded"] += 1
            else:
                rows_loaded = max(
                    [r["ROWS_LOADED"] for r in result if r["ROWS_LOADED"] is not None],
                    default=0
                )
                if rows_loaded > 0:
                    status = "LOADED"
                    stats["files_loaded"] += 1
                else:
                    status = "PARTIAL"

            # ------------------------
            # VALIDATE rejected records
            # ------------------------
            errors = session.sql(f"""
            SELECT ERROR_CODE, ERROR_MESSAGE, ROW_CONTENT
            FROM TABLE(
              VALIDATE({table_fqn}, JOB_ID => '{copy_qid}')
            )
            """).collect()

            for e in errors:
                session.sql("""
                INSERT INTO INGESTION_ERROR_RECORDS
                (event_id, app_name, dataset_name, target_table,
                 file_name, file_path, error_code, error_message, rejected_record)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
                """, [
                    str(uuid.uuid4()),
                    ds["query_tag"]["app"],
                    ds["name"],
                    ds["table"],
                    ds["file_name"],
                    file_path,
                    e["ERROR_CODE"],
                    e["ERROR_MESSAGE"],
                    e["ROW_CONTENT"]
                ]).collect()

            # ------------------------
            # INGESTION EVENT
            # ------------------------
            session.sql("""
            INSERT INTO INGESTION_EVENTS
            (event_id, dataset_name, table_name, file_name, file_path,
             started_at, finished_at, status, rows_loaded, error_message, query_tag)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,NULL,%s)
            """, [
                event_id,
                ds["name"],
                ds["table"],
                ds["file_name"],
                file_path,
                started,
                datetime.utcnow(),
                status,
                rows_loaded,
                json.dumps(ds.get("query_tag"))
            ]).collect()

        except Exception as ex:
            stats["files_failed"] += 1
            session.sql("""
            INSERT INTO INGESTION_EVENTS
            VALUES (%s,%s,%s,%s,%s,%s,%s,'FAILED',0,%s,NULL)
            """, [
                event_id,
                ds["name"],
                ds["table"],
                ds["file_name"],
                file_path,
                started,
                datetime.utcnow(),
                str(ex)
            ]).collect()

        finally:
            session.sql("ALTER SESSION UNSET QUERY_TAG").collect()

    return stats
