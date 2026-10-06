"""Look up the file settings for one partner transaction in BigQuery."""

import os
from pathlib import Path
from prefect import get_run_logger
from .config_utils import Config

try:
    from google.cloud import bigquery as bq_lib
    BIGQUERY_AVAILABLE = True
except ImportError:
    BIGQUERY_AVAILABLE = False

class PartnerConfig:
    def __init__(self):
        self.partner_name: str = ""
        self.pos_inv_flag: str = ""
        self.sheet_name:   str = ""
        self.skip_rows:    int = 0
        self.preprocessing_required: bool = False
        self.preprocessing_script_path: str = ""


def fetch_partner_config(cfg: Config, partner_id: str, *, strict: bool = False) -> PartnerConfig:
    pc = PartnerConfig()

    gac = cfg.get("credentials_json", section="bigquery", fallback="")
    credentials_path = Path(gac)
    if gac and not credentials_path.is_absolute():
        credentials_path = cfg.path.parent / credentials_path
    if gac and credentials_path.is_file():
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = str(credentials_path.resolve())
    else:
        log.warning(
            "BigQuery credentials_json path not found: '%s'\n"
            "Check the configured BigQuery credentials path.",
            credentials_path,
        )

    if not BIGQUERY_AVAILABLE:
        if strict:
            raise RuntimeError("google-cloud-bigquery is required for partner lookup")
        get_run_logger().warning("google-cloud-bigquery not installed — skipping partner_master_v2 lookup.")
        return pc

    project = cfg.get("project", section="bigquery", fallback="")
    dataset = cfg.get("dataset", section="bigquery", fallback="dbo")

    if not project:
        if strict:
            raise ValueError("[bigquery] project is required for partner lookup")
        get_run_logger().warning("[bigquery] project not set in config.ini — skipping partner_master_v2 lookup.")
        return pc

    parts         = partner_id.split("_", 1)
    bq_partner_id = parts[0]
    txn_type      = parts[1].upper() if len(parts) > 1 else ""

    print(f"\n{'─' * 72}")
    print(f"  partner_master_v2 lookup")
    print(f"  Input argument        : {partner_id}")
    print(f"  partner_id  (filter)  : {bq_partner_id}")
    print(f"  txn_type    (filter)  : {txn_type}")
    print(f"  Table                 : {project}.{dataset}.partner_master_v2")
    print(f"{'─' * 72}")

    try:
        client = bq_lib.Client(project=project)
        query = f"""
            SELECT
                partner_id,
                partner_name,
                transaction_type_identifier,
                inputsheetname,
                skiprows,
                preprocessing_required,
                preprocessing_script_path
            FROM `{project}.{dataset}.partner_master_v2`
            WHERE partner_id                  = @partner_id
            AND transaction_type_identifier = @txn_type
            LIMIT 1
            """
        job_cfg = bq_lib.QueryJobConfig(
            query_parameters=[
                bq_lib.ScalarQueryParameter("partner_id", "STRING", bq_partner_id),
                bq_lib.ScalarQueryParameter("txn_type",   "STRING", txn_type),
            ]
        )
        rows = list(client.query(query, job_config=job_cfg).result())

        if not rows:
            if strict:
                raise ValueError(f"No partner configuration found for {partner_id!r}")
            get_run_logger().warning(
                "No row in partner_master_v2 for partner_id='%s' AND "
                "transaction_type_identifier='%s' — using defaults.",
                bq_partner_id, txn_type,
            )
            print(f"  ⚠️  No matching row found — defaults will be used.")
            return pc

        row = rows[0]

        pc.partner_name = str(row.get("partner_name")                or "")
        pc.pos_inv_flag = str(row.get("transaction_type_identifier") or "").strip().upper()
        pc.sheet_name   = str(row.get("inputsheetname")               or "").strip()

        raw_skiprows = row.get("skiprows")
        try:
            skiprows = int(raw_skiprows) if raw_skiprows is not None else 0
        except (TypeError, ValueError):
            skiprows = 0

        pc.skip_rows = max(0, skiprows)
        raw_required = row.get("preprocessing_required")
        pc.preprocessing_required = (
            raw_required.strip().lower() in ("1", "true", "yes", "y")
            if isinstance(raw_required, str) else bool(raw_required)
        )
        pc.preprocessing_script_path = str(row.get("preprocessing_script_path") or "").strip()

        print(f"\n  ✅ Found: partner_id={row.get('partner_id','')}  name={pc.partner_name}  "
              f"txn_type={pc.pos_inv_flag}  sheet={pc.sheet_name or '(first sheet)'}  "
              f"skiprows={pc.skip_rows}\n")

        get_run_logger().info(
            "partner_master_v2 → name='%s'  txn_type='%s'  sheet='%s'  header_row_index=%d (skiprows=%d)",
            pc.partner_name, pc.pos_inv_flag, pc.sheet_name, pc.skip_rows, skiprows,
        )

    except Exception as exc:
        if strict:
            raise
        get_run_logger().error("partner_master_v2 query failed: %s — using defaults.", exc)

    return pc
