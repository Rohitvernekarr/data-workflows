"""Partner records and resolution results."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional


@dataclass
class PartnerConfig:
    """Maps directly to columns in PartnerMaster."""

    # ── Partner identity ──────────────────────────────────────────────────────
    client_name: str
    partner_id: str
    partner_name: str
    partner_type: str
    partner_tier: str
    transaction_type_identifier: str
    from_email: str
    subject_contains: str
    file_name: str
    attachment_extension: str
    file_type_identifier: str
    send_output_to_email: str
    send_error_to_email: str
    country_code: str
    sheet_name: str
    input_file_start_index: str
    pre_header_rows: int
    site_use_id: str
    account_site_party_number: str
    delimiter: str
    partner_status: str
    tier1_channel: str
    tier1_salesrep: str
    pos_frequency: str
    inv_frequency: str
    preprocessing_script_path: str
    preprocessing_required: bool
    last_updated_date: str
    last_updated_by: str
    pos_count_header: str
    inv_count_header: str

    # ── Populated separately (not in BQ table) ────────────────────────────────
    input_columns: list[str] = field(default_factory=list)


@dataclass
class ResolutionResult:
    partner: Optional[PartnerConfig]
    score: int
    matched_signals: list[str] = field(default_factory=list)
    issues: list[str] = field(default_factory=list)
