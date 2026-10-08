"""Load partner configuration from Postgres (partner_master + partner_feed_sources)."""

from __future__ import annotations

import psycopg2
import psycopg2.extras
from psycopg2 import sql

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

_QUERY = sql.SQL("""
    SELECT pm.partner_id, pm.partner_name, pm.partner_type, pm.partner_tier, pm.country_code,
           pfs.feed_type_identifier, pfs.from_email, pfs.subject_contains,
           pfs.file_name_pattern, pfs.file_extension, pfs.file_type_header,
           pfs.sheet_name, pfs.input_sheet_name, pfs.input_file_start_index,
           pfs.data_delimiter, pfs.preprocessing_script_path, pfs.preprocessing_required,
           pfs.pos_frequency, pfs.inv_frequency,
           pfs.last_updated_date, pfs.last_updated_by
    FROM {schema}.partner_master pm
    JOIN {schema}.partner_feed_sources pfs ON pfs.partner_id = pm.partner_id
    WHERE pm.is_active AND pfs.is_active
""")


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


def _rows() -> list[dict]:
    conn = psycopg2.connect(
        host=config.get("host", section="postgres"),
        port=config.get("port", section="postgres", datatype=int, fallback=5432),
        dbname=config.get("dbname", section="postgres"),
        user=config.get("user", section="postgres"),
        password=config.get("password", section="postgres"),
    )
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(_QUERY.format(
                schema=sql.Identifier(config.get("schema", section="postgres", fallback="public"))
            ))
            return [dict(r) for r in cur.fetchall()]
    finally:
        conn.close()


class PartnerRepository:
    @staticmethod
    def load_all_partners() -> list[PartnerConfig]:
        """One PartnerConfig per active partner feed."""
        client_name = _str(config.get("client_name", fallback=""))
        return [
            PartnerConfig(
                client_name=client_name,
                partner_id=_str(r["partner_id"]),
                partner_name=_str(r["partner_name"]),
                partner_type=_str(r["partner_type"]),
                partner_tier=_str(r["partner_tier"]),
                transaction_type_identifier=_str(r["feed_type_identifier"]),
                from_email=_str(r["from_email"]),
                subject_contains=_str(r["subject_contains"]),
                file_name=_str(r["file_name_pattern"]),
                attachment_extension=_str(r["file_extension"]),
                file_type_identifier=_str(r["file_type_header"]),
                send_output_to_email="",
                send_error_to_email="",
                country_code=_str(r["country_code"]),
                sheet_name=_str(r["sheet_name"]),
                input_file_start_index=_str(r["input_file_start_index"]),
                pre_header_rows=_int(r["input_file_start_index"]),
                site_use_id="",
                account_site_party_number="",
                delimiter=_str(r["data_delimiter"]),
                partner_status="Active",
                tier1_channel="",
                tier1_salesrep="",
                pos_frequency=_str(r["pos_frequency"]),
                inv_frequency=_str(r["inv_frequency"]),
                preprocessing_script_path=_str(r["preprocessing_script_path"]),
                preprocessing_required=_bool(r["preprocessing_required"]),
                last_updated_date=_str(r["last_updated_date"]),
                last_updated_by=_str(r["last_updated_by"]),
                pos_count_header="",
                inv_count_header="",
                input_columns=[],
                input_sheet_name=_str(r["input_sheet_name"]),
            )
            for r in _rows()
        ]