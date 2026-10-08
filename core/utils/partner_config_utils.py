"""Look up the file settings for one partner transaction in the partner master."""

from prefect import get_run_logger

from .config_utils import Config


class PartnerConfig:
    def __init__(self):
        self.partner_name: str = ""
        self.pos_inv_flag: str = ""
        self.sheet_name:   str = ""
        self.skip_rows:    int = 0
        self.preprocessing_required: bool = False
        self.preprocessing_script_path: str = ""


def fetch_partner_config(cfg: Config, partner_id: str, *, strict: bool = False) -> PartnerConfig:
    """partner_id is '<partner_id>_<feed_type_identifier>' (cfg is kept for existing callers)."""
    from ..partner_identification.partner_repository import PartnerRepository

    pc = PartnerConfig()
    base_id, _, txn_type = partner_id.partition("_")
    txn_type = txn_type.upper()

    match = next(
        (
            p for p in PartnerRepository.load_all_partners()
            if p.partner_id == base_id and p.transaction_type_identifier.strip().upper() == txn_type
        ),
        None,
    )
    if match is None:
        if strict:
            raise ValueError(f"No partner configuration found for {partner_id!r}")
        get_run_logger().warning(
            "No partner master row for partner_id='%s' txn_type='%s' — using defaults.",
            base_id, txn_type,
        )
        return pc

    pc.partner_name = match.partner_name
    pc.pos_inv_flag = txn_type
    pc.sheet_name = match.input_sheet_name
    pc.skip_rows = max(0, match.pre_header_rows)
    pc.preprocessing_required = match.preprocessing_required
    pc.preprocessing_script_path = match.preprocessing_script_path
    get_run_logger().info(
        "partner master → name='%s'  txn_type='%s'  sheet='%s'  skiprows=%d",
        pc.partner_name, pc.pos_inv_flag, pc.sheet_name, pc.skip_rows,
    )
    return pc