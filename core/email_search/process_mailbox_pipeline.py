"""Find spreadsheet emails, upload their attachments, and mark them processed."""

import imaplib
import re
import time
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from email import policy
from email.message import EmailMessage
from email.parser import BytesParser
from email.utils import parsedate_to_datetime, parseaddr
from pathlib import Path, PurePosixPath
from tempfile import TemporaryDirectory
from uuid import NAMESPACE_URL, uuid5

from ..utils.config_utils import Config, DEFAULT_CONFIG_PATH
from ..utils.sftp_utils import put_sftp
from prefect import get_run_logger


CONFIG_PATH = DEFAULT_CONFIG_PATH
SPREADSHEET_EXTENSIONS = {".xls", ".xlsx", ".csv"}
INTERNALDATE_PATTERN = re.compile(rb'\bINTERNALDATE "([^"]+)"')

@dataclass(frozen=True)
class InboxMessage:
    sender: str
    subject: str
    attachment_names: tuple[str, ...]
    uploaded_sftp_file_names: tuple[str, ...]
    received_at: datetime
    message: EmailMessage


def load_config(config_path: str | Path = CONFIG_PATH) -> tuple[str, str, str, int, str]:
    """Return validated mail settings and the destination SFTP directory."""
    config = Config(config_path)
    mail_keys = ("email_address", "password", "imap_host", "imap_port")
    mail_settings = {
        key: (config.get(key, section="zoho_mail", fallback="") or "").strip()
        for key in mail_keys
    }
    missing = [key for key, value in mail_settings.items() if not value]
    if missing:
        raise ValueError(f"Missing Zoho Mail settings: {', '.join(missing)}")

    address = mail_settings["email_address"]
    password = mail_settings["password"]
    if address == "you@example.com" or password == "your_app_password":
        raise ValueError(f"Set your Zoho Mail address and password in {config_path}")

    try:
        port = int(mail_settings["imap_port"])
    except ValueError as exc:
        raise ValueError("imap_port must be an integer") from exc
    if not 1 <= port <= 65535:
        raise ValueError("imap_port must be between 1 and 65535")

    location = (config.get("source_files_location", section="sftp", fallback="") or "").strip()
    if not location:
        raise ValueError("Missing SFTP setting: source_files_location")
    return address, password, mail_settings["imap_host"], port, location


def spreadsheet_attachments(message: EmailMessage) -> tuple[tuple[EmailMessage, str], ...]:
    """Return the spreadsheet MIME parts and their original filenames."""
    return tuple(
        (part, filename)
        for part in message.walk()
        if not part.is_multipart()
        if (filename := part.get_filename())
        if Path(filename).suffix.lower() in SPREADSHEET_EXTENSIONS
    )


def upload_attachments(
    attachments: tuple[tuple[EmailMessage, str], ...],
    source_files_location: str,
    config_path: str | Path,
) -> tuple[str, ...]:
    """Copy matching attachments from a message to the configured SFTP directory."""
    uploaded_names = []
    with TemporaryDirectory() as temporary_directory:
        for part, filename in attachments:
            safe_name = PurePosixPath(filename.replace("\\", "/")).name
            if not safe_name or safe_name in {".", ".."} or "\x00" in safe_name:
                raise ValueError(f"Invalid attachment filename: {filename!r}")
            content = part.get_payload(decode=True)
            if content is None:
                raise ValueError(f"Could not decode attachment: {filename}")

            local_path = Path(temporary_directory) / safe_name
            local_path.write_bytes(content)
            remote_path = str(PurePosixPath(source_files_location) / safe_name)
            put_sftp(local_path, remote_path, config=config_path)
            uploaded_names.append(remote_path)

    return tuple(uploaded_names)


def execute(config_path: str | Path = CONFIG_PATH) -> list[InboxMessage]:
    """Upload yesterday's matching attachments, then mark their emails PICKED."""
    address, password, host, port, source_files_location = load_config(config_path)
    get_run_logger().debug("Connecting to %s:%s as %s", host, port, address)
    messages = []

    with imaplib.IMAP4_SSL(host, port) as mailbox:
        mailbox.login(address, password)
        try:
            status, _ = mailbox.select("INBOX", readonly=False)
            if status != "OK":
                raise RuntimeError("Could not open the INBOX")

            DELTA_DAYS = 2
            from_date = date.today() - timedelta(days=DELTA_DAYS)  # Use yesterday to avoid timezone issues
            to_date = date.today() + timedelta(days=1)  # Use tomorrow to avoid timezone issues
            get_run_logger().info("Searching for unprocessed messages received from %s to %s", from_date.isoformat(), to_date.isoformat())
            # TODO: PBC Ensure we also parse the body of email when data is sent in body and no attachment is sent
            status, data = mailbox.uid(
                "search", None,
                "UNKEYWORD", "PICKED",
                "SINCE", from_date.strftime("%d-%b-%Y"),
                "BEFORE", to_date.strftime("%d-%b-%Y"),
            )
            if status != "OK":
                raise RuntimeError("Could not list INBOX messages")

            for uid in data[0].split():
                status, response = mailbox.uid("fetch", uid, "(BODY.PEEK[] INTERNALDATE)")
                if status != "OK":
                    raise RuntimeError(f"Could not fetch message UID {uid.decode()}")
                fetched = next((part for part in response if isinstance(part, tuple)), None)
                if fetched is None:
                    raise RuntimeError(f"No message data for UID {uid.decode()}")
                fetch_metadata, raw_message = fetched
                message = BytesParser(policy=policy.default).parsebytes(raw_message)
                attachments = spreadsheet_attachments(message)
                if not attachments:
                    continue
                attachment_names = tuple(filename for _, filename in attachments)

                internal_date = INTERNALDATE_PATTERN.search(fetch_metadata)
                if internal_date is None:
                    raise RuntimeError(f"No receipt timestamp for UID {uid.decode()}")
                received_at = parsedate_to_datetime(internal_date.group(1).decode("ascii"))
                # Note: An email can have multiple attachments. We need to upload all the attachments and return the list of uploaded files
                uploaded_safe_names = upload_attachments(attachments, source_files_location, config_path)
                inbox_message = InboxMessage(
                    sender=parseaddr(str(message.get("From", "")))[1],
                    subject=str(message.get("Subject", "")),
                    attachment_names=attachment_names,
                    uploaded_sftp_file_names=uploaded_safe_names,
                    received_at=received_at,
                    message=message,
                )
                status, _ = mailbox.uid("store", uid, "+FLAGS.SILENT", "(\\Seen)")
                if status != "OK":
                    raise RuntimeError(f"Could not mark message UID {uid.decode()} as read")
                status, _ = mailbox.uid("store", uid, "+FLAGS.SILENT", "(PICKED)")
                if status != "OK":
                    raise RuntimeError(f"Could not tag message UID {uid.decode()}")
                messages.append(inbox_message)
        finally:
            mailbox.logout()

    return messages


# ══════════════════════════════════════════════════════════════════════════════
# Event-driven flow support: one email at a time
# ══════════════════════════════════════════════════════════════════════════════

def _open_inbox(config_path: str | Path = CONFIG_PATH):
    address, password, host, port, source_files_location = load_config(config_path)
    mailbox = imaplib.IMAP4_SSL(host, port)
    mailbox.login(address, password)
    status, _ = mailbox.select("INBOX", readonly=False)
    if status != "OK":
        raise RuntimeError("Could not open the INBOX")
    return mailbox, source_files_location


def set_keyword(uid: str, keyword: str, config_path: str | Path = CONFIG_PATH) -> None:
    """Add an IMAP keyword (PICKED, FAILED, SKIPPED ...) to one message."""
    mailbox, _ = _open_inbox(config_path)
    try:
        status, _ = mailbox.uid("store", str(uid), "+FLAGS.SILENT", f"({keyword})")
        if status != "OK":
            raise RuntimeError(f"Could not tag UID {uid} with {keyword}")
    finally:
        mailbox.logout()


def process_uid(uid: str, config_path: str | Path = CONFIG_PATH) -> InboxMessage | None:
    """Handle ONE email by UID: upload spreadsheet attachments, then tag it PICKED.

    Returns None when the email was already PICKED, or has no spreadsheet
    attachment (it is then tagged SKIPPED so it is never re-triggered).
    """
    uid = str(uid)
    mailbox, source_files_location = _open_inbox(config_path)
    try:
        status, flag_data = mailbox.uid("fetch", uid, "(FLAGS)")
        if status != "OK":
            raise RuntimeError(f"Could not read flags for UID {uid}")
        if b"PICKED" in b" ".join(p for p in flag_data if isinstance(p, bytes)):
            return None

        status, response = mailbox.uid("fetch", uid, "(BODY.PEEK[] INTERNALDATE)")
        if status != "OK":
            raise RuntimeError(f"Could not fetch message UID {uid}")
        fetched = next((part for part in response if isinstance(part, tuple)), None)
        if fetched is None:
            raise RuntimeError(f"No message data for UID {uid}")
        fetch_metadata, raw_message = fetched
        message = BytesParser(policy=policy.default).parsebytes(raw_message)

        attachments = spreadsheet_attachments(message)
        if not attachments:
            mailbox.uid("store", uid, "+FLAGS.SILENT", "(SKIPPED)")
            return None

        internal_date = INTERNALDATE_PATTERN.search(fetch_metadata)
        if internal_date is None:
            raise RuntimeError(f"No receipt timestamp for UID {uid}")
        received_at = parsedate_to_datetime(internal_date.group(1).decode("ascii"))

        uploaded = upload_attachments(attachments, source_files_location, config_path)
        inbox_message = InboxMessage(
            sender=parseaddr(str(message.get("From", "")))[1],
            subject=str(message.get("Subject", "")),
            attachment_names=tuple(name for _, name in attachments),
            uploaded_sftp_file_names=uploaded,
            received_at=received_at,
            message=message,
        )
        for flag in ("(\\Seen)", "(PICKED)"):
            status, _ = mailbox.uid("store", uid, "+FLAGS.SILENT", flag)
            if status != "OK":
                raise RuntimeError(f"Could not tag UID {uid} with {flag}")
        return inbox_message
    finally:
        mailbox.logout()


def listen(config_path: str | Path = CONFIG_PATH, idle_timeout: int = 300) -> None:
    """Long-running listener: on new mail, tag QUEUED and emit a Prefect event."""
    from imapclient import IMAPClient
    from prefect.events import emit_event

    address, password, host, port, _ = load_config(config_path)
    client_name = Config(config_path).get("client_name", fallback="default").strip().lower()
    while True:
        try:
            with IMAPClient(host, port=port, ssl=True) as client:
                client.login(address, password)
                client.select_folder("INBOX")
                while True:
                    uids = client.search([
                        "UNKEYWORD", "PICKED", "UNKEYWORD", "FAILED",
                        "UNKEYWORD", "SKIPPED", "UNKEYWORD", "QUEUED",
                        "SINCE", date.today() - timedelta(days=2),
                    ])
                    for uid in uids:
                        client.add_flags([uid], ["QUEUED"])
                        emit_event(
                            event="bomisco.email.received",
                            resource={"prefect.resource.id": f"bomisco.{client_name}.email.{uid}"},
                            payload={"email_uid": str(uid)},
                            id=uuid5(NAMESPACE_URL, f"{address}/INBOX/{uid}"),
                        )
                        print(f"Emitted bomisco.email.received for UID {uid}", flush=True)
                    client.idle()
                    client.idle_check(timeout=idle_timeout)
                    client.idle_done()
        except Exception as exc:
            print(f"Listener error: {exc!r} — reconnecting in 30s", flush=True)
            time.sleep(30)


if __name__ == "__main__":
    listen()