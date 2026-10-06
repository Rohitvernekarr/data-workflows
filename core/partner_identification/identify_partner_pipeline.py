"""Load input files, identify partners, and send notifications."""

from __future__ import annotations

import html as html_lib
import json
import os
import sys

import paramiko
from core.utils.file_utils import build_input_file_path
from core.utils.sftp_utils import copy_sftp_file
from prefect import get_run_logger

if __package__ == "core.partner_identification":
    from ..utils.config_utils import Config
    from ..utils.email_utils import EmailUtils
    from ..utils.file_utils import FileUtils
    from .get_partner import _fuzzy, _norm, resolve_partner
    from .partner_models import PartnerConfig, ResolutionResult
    from .partner_repository import PartnerRepository
else:
    from pathlib import Path

    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
    from utils.config_utils import Config
    from utils.email_utils import EmailUtils
    from utils.file_utils import FileUtils
    from partner_identification.get_partner import _fuzzy, _norm, resolve_partner
    from partner_identification.partner_models import PartnerConfig, ResolutionResult
    from partner_identification.partner_repository import PartnerRepository

config = Config()

def _bool(val) -> bool:
    if isinstance(val, bool):
        return val
    if isinstance(val, int):
        return val != 0
    if isinstance(val, str):
        return val.strip().lower() in ("1", "true", "yes", "y")
    return bool(val)


def _print_json(payload: dict) -> None:
    print(json.dumps(payload, indent=2, default=str))


def _process(
    sender: str,
    subject: str,
    file_reference: str,
    file_display_name: str,
    file_bytes: bytes,
    partners: list[PartnerConfig],
) -> list[dict]:
    # get_run_logger().debug("Processing — from=%s  subject='%s'  file=%s", sender, subject, filename)

    results, sheet_names, matched_headers = resolve_partner(
        sender, subject, file_reference, file_bytes, partners
    )
    # Keep backward-compatible single-result handling: use the first result
    result = (
        results[0]
        if results
        else ResolutionResult(partner=None, score=0, matched_signals=[])
    )

    # Overview headers at row 1 (for alert email when no partner matched)
    _, overview_headers = FileUtils.extract_file_info(file_bytes, skip_rows=0)

    # ── Unresolved → alert ops ────────────────────────────────────────────────
    if result.partner is None:
        # get_run_logger().warning("UNRESOLVED — score=%d/~80  signals=%s", result.score, result.matched_signals)

        # _print_json({
        #     "event": "unresolved",
        #     "incoming": {
        #         "sender":        sender,
        #         "subject":       subject,
        #         "filename":      filename,
        #         "sheets_found":  sheet_names,
        #         "headers_found": overview_headers,
        #     },
        #     "match": {
        #         "score":     result.score,
        #         "score_max": 80,
        #         "threshold": config.get("match_threshold", section="pipeline", datatype=float),
        #         "signals":   result.matched_signals,
        #     },
        # })

        EmailUtils.send(
            to=config.get("ops_alert_email", section="pipeline"),
            subject=f"[PIPELINE ALERT] Could not identify partner — {file_display_name}",
            body=(
                f"The pipeline received a file but could not match it to any known partner.\n\n"
                f"── Incoming email details ──────────────────────────────────\n"
                f"  Sender   : {sender}\n"
                f"  Subject  : {subject}\n"
                f"  Filename : {file_display_name}\n"
                f"  Sheets   : {', '.join(sheet_names) or '—'}\n"
                f"  Headers  : {', '.join(overview_headers[:10]) or '—'}"
                f"{'…' if len(overview_headers) > 10 else ''}\n\n"
                f"── Scoring result ──────────────────────────────────────────\n"
                f"  Best score   : {result.score} / ~80  (threshold: {config.get('match_threshold', section='pipeline', datatype=float)})\n"
                f"  Matched on   : {', '.join(result.matched_signals) or 'none'}\n\n"
                f"Action required: verify partner configuration in PartnerMaster.\n"
                f"The original file is attached for inspection."
            ),
            attachment_bytes=file_bytes,
            attachment_name=file_display_name,
        )
        return []

    # ── Matched → send "file picked" notification ─────────────────────────────
    partner = result.partner

    fti_status = "not_configured"
    if partner.file_type_identifier:
        found_fti = any(
            _norm(partner.file_type_identifier) == _norm(h)
            or _fuzzy(partner.file_type_identifier, h) >= 0.75
            for h in matched_headers
        )
        fti_status = "found" if found_fti else "not_found"

    # #get_run_logger().debug("Matched partner '%s' — sending file-picked notific ation.", partner.partner_name)

    # Print one JSON object per resolution result (preserves backward
    # compatibility when there's a single match, but supports two-match
    # (P/I) scenarios by emitting an array of objects).
    out_payloads = []
    matched_partners = []
    for res in results:
        p = res.partner
        if p is None:
            # Skip None partners here; unresolved branch handled above.
            continue
        matched_partners.append(p)
        get_run_logger().info("Matched partner '%s' (score=%d) for file '%s'", p.partner_name, res.score, file_display_name)
        partner_specific_input_folder = build_input_file_path(p.partner_name)
        get_run_logger().info("Copying file '%s' to partner-specific input folder: %s", file_display_name, partner_specific_input_folder)
        partner_input_file_path = copy_sftp_file(
            file_display_name,
            config.get("source_files_location", section="sftp"),
            partner_specific_input_folder,
            config=config
        )

        out_payloads.append(
            {
                "account_site_party_number": p.account_site_party_number,
                "attachment_extension": p.attachment_extension,
                "client_name": p.client_name,
                "delimiter": p.delimiter,
                "from_email": p.from_email,
                "file_name": p.file_name,
                "partner_input_file_path": partner_input_file_path,
                "file_type_identifier": p.file_type_identifier,
                "input_file_start_index": p.input_file_start_index,
                "inv_count_header": p.inv_count_header,
                "partner_id": p.partner_id,
                "partner_name": p.partner_name,
                "partner_tier": p.partner_tier,
                "partner_type": p.partner_type,
                "pos_count_header": p.pos_count_header,
                "pre_header_rows": p.pre_header_rows,
                "country_code": p.country_code,
                "send_error_to_email": p.send_error_to_email,
                "send_output_to_email": p.send_output_to_email,
                "sheet_name": p.sheet_name,
                "site_use_id": p.site_use_id,
                "subject_contains": p.subject_contains,
                "transaction_type_identifier": p.transaction_type_identifier,
                "preprocessing_required": p.preprocessing_required,
                "preprocessing_script_path": p.preprocessing_script_path,
                "pos_frequency": p.pos_frequency,
                "inv_frequency": p.inv_frequency,
                "last_updated_date": p.last_updated_date,
                "last_updated_by": p.last_updated_by,
            }
        )

    _print_json(out_payloads)

    def esc(value: object) -> str:
        return html_lib.escape(str(value))

    fti_display = (
        "✔ Found"
        if fti_status == "found"
        else "✘ NOT FOUND" if fti_status == "not_found" else "—"
    )
    headers_display = esc(", ".join(matched_headers[:10]) or "—") + (
        "…" if len(matched_headers) > 10 else ""
    )

    EmailUtils.send(
        to=config.get("ops_alert_email", section="pipeline"),
        subject=f"[PIPELINE] File picked — {partner.partner_name} | {file_display_name}",
        body=(
            "<html><body style=\"font-family:Arial,sans-serif; font-size:13px;\">"
            "<p>A file has been successfully identified and picked up by the pipeline.</p>"
            f"<h3>Matched partner{'s' if len(matched_partners) != 1 else ''}</h3>"
            f"{EmailUtils.partner_detail_block(matched_partners)}"
            f"<h3>File inspection (at pre_header_rows={esc(partner.pre_header_rows)})</h3>"
            "<pre>"
            f"  Sheet used       : {esc(partner.sheet_name or '(active)')}\n"
            f"  Headers found    : {headers_display}\n"
            f"  File_Type_Identifier ('{esc(partner.file_type_identifier)}'): {fti_display}"
            "</pre>"
            "<h3>Incoming email details</h3>"
            "<pre>"
            f"  Sender   : {esc(sender)}\n"
            f"  Subject  : {esc(subject)}\n"
            f"  Filename : {esc(file_display_name)}\n"
            f"  Sheets   : {esc(', '.join(sheet_names) or '—')}"
            "</pre>"
            "<h3>Match quality</h3>"
            "<pre>"
            f"  Score        : {esc(result.score)} / ~80\n"
            f"  Matched on   : {esc(', '.join(result.matched_signals))}"
            "</pre>"
            "</body></html>"
        ),
        is_html=True,
    )
    # get_run_logger().debug("File-picked notification sent to %s", config.get("ops_alert_email", section="pipeline"))
    return out_payloads

def execute(sender: str = "", subject: str = "", filename: str = "") -> list[dict]:
    partners = PartnerRepository.load_all_partners()
    sftp_remote = _bool(config.get("remote", section="sftp"))
    sftp_host = config.get("host", section="sftp")
    sftp_port = config.get("port", section="sftp", datatype=int)
    sftp_username = config.get("username", section="sftp")
    sftp_password = config.get("password", section="sftp")
    source_files_location = config.get("source_files_location", section="sftp")

    # TODO:PBC - Consider adding a check for missing SFTP settings and raise an error if any are missing.
    # MATCH_THRESHOLD = config.get("match_threshold", section="pipeline")
    # FUZZY_CUTOFF = config.get("fuzzy_header_cutoff", section="pipeline")
    # OPS_ALERT_EMAIL = config.get("ops_alert_email", section="pipeline")

    get_run_logger().debug(
        "Using sftp configuration — remote: %s  host: %s  port: %d  username: %s  source_files_location: %s",
        sftp_remote,
        sftp_host,
        sftp_port,
        sftp_username,
        source_files_location,
    )

    sftp = None
    transport = None

    resolved_partners = []
    try:
        # files = sftp.listdir(inbox_folder)
        # excel_files = [f for f in files if os.path.splitext(f)[1].lower() in SUPPORTED_EXT]
        # get_run_logger().debug("%d Excel file(s) found in %s", len(input_files), inbox_folder)

        input_files = [filename] if filename else []
        get_run_logger().debug("%d file(s) to process: %s", len(input_files), input_files)

        resolved_files = [
            FileUtils.resolve_file_reference(i_file, source_files_location, sftp_remote)
            for i_file in input_files
        ]
        # Only open SFTP when we actually need it for one or more resolved files.
        needs_sftp = any(
            not os.path.exists(file_reference) for file_reference in resolved_files
        )

        if needs_sftp:
            transport = paramiko.Transport((sftp_host, sftp_port))
            transport.connect(username=sftp_username, password=sftp_password)
            sftp = paramiko.SFTPClient.from_transport(transport)

            try:
                sftp.stat(source_files_location)
            except (OSError, IOError):
                get_run_logger().error(
                    "SFTP source_files_location does not exist: %s",
                    source_files_location,
                )
                return []

        for i_file, file_reference in zip(input_files, resolved_files):
            # file_reference is the actual local/SFTP path used to open the file.
            # file_display_name is only for logs, emails, and notifications.
            file_display_name =os.path.basename(file_reference.rstrip("/\\")) or i_file
            try:
                # Read locally when the resolved path exists here; otherwise fetch through SFTP.
                if os.path.exists(file_reference):
                    file_bytes = FileUtils.read_file_bytes(file_reference)
                else:
                    file_bytes = FileUtils.read_file_bytes(file_reference, sftp=sftp)

                # SFTP has no sender/subject — pass empty strings; scoring
                # will rely on filename, sheet names, headers, and FTI only.
                # TODO:PBC Check the case where the file can have data from multiple partners (e.g., P/I) and handle accordingly.
                resolved_partner_result = _process(
                                        sender=sender,
                                        subject=subject,
                                        file_reference=file_reference,
                                        file_display_name=file_display_name,
                                        file_bytes=file_bytes,
                                        partners=partners,
                                    )
                resolved_partners.extend(resolved_partner_result)

            except Exception:
                get_run_logger().exception("Failed processing file: %s", i_file)

        return resolved_partners

    finally:
        if sftp is not None:
            sftp.close()
        if transport is not None:
            transport.close()
            get_run_logger().debug("SFTP connection closed.")

if __name__ == "__main__":
    execute(sender=sys.argv[1], subject=sys.argv[2], filename=sys.argv[3])
