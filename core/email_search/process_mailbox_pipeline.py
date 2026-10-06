"""Find spreadsheet emails, upload their attachments, and mark them processed."""

import imaplib
import re
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from email import policy
from email.message import EmailMessage
from email.parser import BytesParser
from email.utils import parsedate_to_datetime, parseaddr
from pathlib import Path, PurePosixPath
from tempfile import TemporaryDirectory

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


def main() -> None:
    for item in read_emails():
        body = item.message.get_body(preferencelist=("plain",))
        print(f"From: {item.sender}")
        print(f"Received: {item.received_at.isoformat()}")
        print(f"Subject: {item.subject}")
        print(f"Attachments: {', '.join(item.attachment_names)}")
        print(body.get_content() if body else "[No plain-text body]")
        print("-" * 60)


if __name__ == "__main__":
    main()
