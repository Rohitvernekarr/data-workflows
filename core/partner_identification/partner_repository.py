"""Load partner configuration from BigQuery."""

from __future__ import annotations

import os
from google.cloud import bigquery

if __package__ == "core.partner_identification":
    from ..utils.config_utils import Config
    from .partner_models import PartnerConfig
else:
    import sys
    from pathlib import Path

    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
    from utils.config_utils import Config
    from partner_identification.partner_models import PartnerConfig


config = Config()

def _bool(val) -> bool:
    """Safely coerce various truthy representations to bool."""
    if isinstance(val, bool):
        return val
    if isinstance(val, int):
        return val != 0
    if isinstance(val, str):
        return val.strip().lower() in ("1", "true", "yes", "y")
    return bool(val)


def _str(val) -> str:
    return str(val).strip() if val is not None else ""


def _int(val, default: int = 0) -> int:
    try:
        return int(val)
    except (TypeError, ValueError):
        return default

def _bq_client() -> bigquery.Client:
    """
    Returns an authenticated BigQuery client.

    Priority:
      1. credentials_json in config.ini  → service account key file
      2. GOOGLE_APPLICATION_CREDENTIALS env var
      3. gcloud Application Default Credentials
    """
    project = config.get("project", section="bigquery")
    creds_path = config.get("credentials_json", section="bigquery", fallback="").strip()

    if creds_path:
        if not os.path.exists(creds_path):
            raise FileNotFoundError(
                f"credentials_json path not found: {creds_path}\n"
                f"Check the path in config.ini."
            )
        from google.oauth2 import service_account

        credentials = service_account.Credentials.from_service_account_file(
            creds_path,
            scopes=["https://www.googleapis.com/auth/bigquery"],
        )
        # get_run_logger().debug("BigQuery auth: service account key (%s)", creds_path)
        return bigquery.Client(project=project, credentials=credentials)

    env_creds = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", "").strip()
    if env_creds:
        # get_run_logger().debug("BigQuery auth: GOOGLE_APPLICATION_CREDENTIALS env var")
        return bigquery.Client(project=project)

    try:
        client = bigquery.Client(project=project)
        # get_run_logger().debug("BigQuery auth: Application Default Credentials (gcloud)")
        return client
    except Exception:
        raise RuntimeError(
            "\n\nBigQuery authentication failed. Fix it with ONE of these options:\n\n"
            "  OPTION A — Add your service account key to config.ini:\n"
            "    [bigquery]\n"
            "    credentials_json = C:\\path\\to\\your-key.json\n\n"
            "  OPTION B — Set environment variable before running:\n"
            "    set GOOGLE_APPLICATION_CREDENTIALS=C:\\path\\to\\your-key.json\n\n"
            "  OPTION C — Login with gcloud CLI (run once in terminal):\n"
            "    gcloud auth application-default login\n\n"
            "Get a service account key: GCP Console → IAM → Service Accounts → Keys\n"
            "Required roles: BigQuery Data Viewer + BigQuery Job User\n"
        )



class PartnerRepository:
    @staticmethod
    def load_all_partners() -> list[PartnerConfig]:
        """Loads all rows from PartnerMaster."""
        bq = _bq_client()
        project = config.get("project", section="bigquery")
        dataset = config.get("dataset", section="bigquery")

        rows = list(bq.query(f"""
            SELECT *
            FROM `{project}.{dataset}.partner_master_v2`
        """).result())

        partners = []
        for r in rows:
            subject_kw = _str(r.get("subject_contains"))
            file_name_pl = _str(r.get("file_name"))
            sheet = _str(r.get("sheet_name"))

            partners.append(
                PartnerConfig(
                    client_name=_str(r.get("client_name")),
                    partner_id=_str(r.get("partner_id")),
                    partner_name=_str(r.get("partner_name")),
                    partner_type=_str(r.get("partner_type")),
                    partner_tier=_str(r.get("partner_tier")),
                    transaction_type_identifier=_str(r.get("transaction_type_identifier")),
                    from_email=_str(r.get("from_email")),
                    subject_contains=subject_kw,
                    file_name=file_name_pl,
                    attachment_extension=_str(r.get("attachment_extension")),
                    file_type_identifier=_str(r.get("file_type_identifier")),
                    send_output_to_email=_str(r.get("send_output_to_email")),
                    send_error_to_email=_str(r.get("send_error_to_email")),
                    country_code=_str(r.get("country_code")),
                    sheet_name=sheet,
                    input_file_start_index=_str(r.get("input_file_start_index")),
                    pre_header_rows=_int(r.get("pre_header_rows")),
                    site_use_id=_str(r.get("site_use_id")),
                    account_site_party_number=_str(r.get("account_site_party_number")),
                    delimiter=_str(r.get("delimiter")),
                    partner_status=_str(r.get("partner_status")),
                    tier1_channel=_str(r.get("tier1_channel")),
                    tier1_salesrep=_str(r.get("tier1_salesrep")),
                    pos_frequency=_str(r.get("pos_frequency")) or _str(r.get("frequency")),
                    inv_frequency=_str(r.get("inv_frequency")) or _str(r.get("frequency")),
                    preprocessing_script_path=_str(r.get("preprocessing_script_path"))
                    or _str(r.get("python_script")),
                    preprocessing_required=_bool(r.get("preprocessing_required")),
                    last_updated_date=_str(r.get("last_updated_date")),
                    last_updated_by=_str(r.get("last_updated_by")),
                    pos_count_header=_str(r.get("POS_Count")),
                    inv_count_header=_str(r.get("INV_Count")),
                    input_columns=[],
                )
            )

        # get_run_logger().debug("Loaded %d partners from PartnerMaster.", len(partners))
        return partners
