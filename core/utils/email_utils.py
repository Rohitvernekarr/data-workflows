"""SMTP delivery and partner details for notification emails."""

from __future__ import annotations

import html as html_lib
import smtplib
from email import encoders
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.utils import getaddresses
from typing import Callable, Optional

if __package__ == "core.utils":
    from .config_utils import Config
    from ..partner_identification.partner_models import PartnerConfig
else:
    from utils.config_utils import Config
    from partner_identification.partner_models import PartnerConfig


config = Config()

def _normalize_recipients(to: str) -> list[str]:
    if not to:
        return []

    normalized = to.replace(";", ",")
    parsed = getaddresses([normalized])
    recipients = [address for _, address in parsed if address]
    return recipients


def _send(
    to: str,
    subject: str,
    body: str,
    attachment_bytes: Optional[bytes] = None,
    attachment_name: Optional[str] = None,
    is_html: bool = False,
) -> None:
    from_addr = config.get("email", section="zoho")
    password = config.get("password", section="zoho")
    smtp_host = config.get("smtp_host", section="zoho")
    smtp_port = config.get("smtp_port", section="zoho", datatype=int)

    recipients = _normalize_recipients(to)
    if not recipients:
        raise ValueError("No valid recipients provided for email")

    msg = MIMEMultipart()
    msg["From"] = from_addr
    msg["To"] = to
    msg["Subject"] = subject
    msg.attach(MIMEText(body, "html" if is_html else "plain"))

    if attachment_bytes and attachment_name:
        part = MIMEBase("application", "octet-stream")
        part.set_payload(attachment_bytes)
        encoders.encode_base64(part)
        part.add_header(
            "Content-Disposition", f'attachment; filename="{attachment_name}"'
        )
        msg.attach(part)

    with smtplib.SMTP_SSL(smtp_host, smtp_port) as s:
        s.login(from_addr, password)
        s.sendmail(from_addr, recipients, msg.as_string())
    # get_run_logger().debug("Email sent → %s | subject: %s", to, subject)


_PARTNER_DETAIL_FIELDS: list[tuple[str, Callable[[PartnerConfig], object]]] = [
    ("Partner ID", lambda p: p.partner_id),
    ("Partner Name", lambda p: p.partner_name),
    ("Client Name", lambda p: p.client_name),
    ("Transaction Type Identifier", lambda p: p.transaction_type_identifier),
    ("Partner Type", lambda p: p.partner_type),
    # ("Partner Tier", lambda p: p.partner_tier),
    ("Partner Status", lambda p: p.partner_status),
    ("Country Code", lambda p: p.country_code),
    ("Expected Subject", lambda p: p.subject_contains),
    ("Expected Filename Pattern", lambda p: p.file_name),
    ("File Type Identifier", lambda p: p.file_type_identifier),
    ("Expected Sheet", lambda p: p.sheet_name),
    # ("Pre-Header Rows", lambda p: p.pre_header_rows),
    # ("Input File Start Index", lambda p: p.input_file_start_index),
    # ("Delimiter", lambda p: p.delimiter or "—"),
    # ("Attachment Extension", lambda p: p.attachment_extension),
    # ("From Email", lambda p: p.from_email),
    # ("Send Output To", lambda p: p.send_output_to_email),
    # ("Send Error To", lambda p: p.send_error_to_email),
    # ("Preprocessing Req.", lambda p: p.preprocessing_required),
    # ("Preprocessing Script Path", lambda p: p.preprocessing_script_path or "—"),
    # ("Site Use ID", lambda p: p.site_use_id or "—"),
    # ("Account Site Party Number", lambda p: p.account_site_party_number or "—"),
    # ("Tier1 Channel", lambda p: p.tier1_channel or "—"),
    # ("Tier1 Sales Rep", lambda p: p.tier1_salesrep or "—"),
    # ("POS Frequency", lambda p: p.pos_frequency or "—"),
    # ("INV Frequency", lambda p: p.inv_frequency or "—"),
    ("POS Count Header", lambda p: p.pos_count_header or "—"),
    ("INV Count Header", lambda p: p.inv_count_header or "—"),
    # ("Last Updated", lambda p: f"{p.last_updated_date} by {p.last_updated_by}"),
]


_TH_STYLE = (
    "border:1px solid #ccc; padding:6px 10px; text-align:left; "
    "background:#f2f2f2; font-family:Arial,sans-serif; font-size:13px;"
)
_TD_LABEL_STYLE = (
    "border:1px solid #ccc; padding:6px 10px; text-align:left; "
    "background:#f9f9f9; font-weight:bold; white-space:nowrap; "
    "font-family:Arial,sans-serif; font-size:13px;"
)
_TD_STYLE = (
    "border:1px solid #ccc; padding:6px 10px; text-align:left; "
    "font-family:Arial,sans-serif; font-size:13px;"
)


def _partner_detail_block(partners: list[PartnerConfig]) -> str:
    """Render one or more partners as an HTML table: fields as rows,
    one column per partner."""
    if not partners:
        return "<p>(no partner details available)</p>"

    def esc(value: object) -> str:
        return html_lib.escape(str(value))

    headers = [esc(f"{p.partner_name} ({p.partner_type})") for p in partners]

    header_cells = "".join(f'<th style="{_TH_STYLE}">{h}</th>' for h in headers)
    thead = f'<tr><th style="{_TH_STYLE}">Field</th>{header_cells}</tr>'

    body_rows = []
    for label, getter in _PARTNER_DETAIL_FIELDS:
        cells = "".join(
            f'<td style="{_TD_STYLE}">{esc(getter(p))}</td>' for p in partners
        )
        body_rows.append(
            f'<tr><td style="{_TD_LABEL_STYLE}">{esc(label)}</td>{cells}</tr>'
        )

    return (
        '<table style="border-collapse:collapse;" cellspacing="0" cellpadding="0">'
        f"<thead>{thead}</thead>"
        f"<tbody>{''.join(body_rows)}</tbody>"
        "</table>"
    )

class EmailUtils:
    """Stateless email delivery and message formatting helpers."""

    send = staticmethod(_send)
    partner_detail_block = staticmethod(_partner_detail_block)
