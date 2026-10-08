"""Sonos: event-driven partner file processing on Prefect Cloud.

One email goes through:
    intake (upload attachments, tag PICKED)
      -> identify partner(s) from the partner master
      -> per partner: resolve sheet + headers (portal endpoint)
           -> ops review needed?  stop here (ops already emailed)
           -> otherwise: transform -> prepare input data
Any failure or crash tags the email FAILED and sends an alert email.

Everything client specific comes from the config file named by the
PIPELINE_CONFIG_PATH job variable (see prefect.yaml).
"""

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from pydantic import BaseModel, Field
from prefect import flow, task
from prefect.artifacts import create_markdown_artifact, create_table_artifact
from prefect.events import emit_event

from core.data_ingestion import prepare_input_data_pipeline
from core.data_transformation import transform_input_data_pipeline
from core.email_search import process_mailbox_pipeline
from core.header_mapping import resolve_headers_pipeline
from core.partner_identification import identify_partner_pipeline
from core.utils.prefect_utils import OpsAlreadyNotified, assert_client_config, on_flow_failure, slug

CLIENT_NAME = "sonos"


class RunConfig(BaseModel):
    """Typed flow parameters (rendered as a form in the Prefect UI)."""
    email_uid: str = Field(description="IMAP UID of the email in INBOX")
    skip_alerts: bool = False


# Retries are set per task. There are no flow-level retries: a re-run would find
# the email already PICKED and exit silently.

@task(retries=3, retry_delay_seconds=[30, 120, 300], tags=[CLIENT_NAME, "imap", "sftp"])
def intake_email(email_uid: str) -> dict | None:
    """Upload the spreadsheet attachments and tag the email PICKED."""
    message = process_mailbox_pipeline.process_uid(email_uid)
    if message is None:
        return None
    return {
        "sender": message.sender,
        "subject": message.subject,
        "received_at": message.received_at.isoformat(),
        "attachment_names": list(message.attachment_names),
        "uploaded_sftp_file_names": list(message.uploaded_sftp_file_names),
    }


@task(retries=2, retry_delay_seconds=[30, 120], tags=[CLIENT_NAME, "partner-master"])
def identify_partner(intake: dict) -> list[dict]:
    """Match the email and its first attachment to partner(s) in the partner master."""
    return identify_partner_pipeline.execute(
        sender=intake["sender"],
        subject=intake["subject"],
        filename=intake["uploaded_sftp_file_names"][0],
    )


# No retries: this step moves and deletes SFTP files, so a retry would not find the input.
@task(tags=[CLIENT_NAME, "sftp", "api"])
def resolve_headers(intake: dict, partner: dict) -> dict | None:
    """Resolve sheet + headers via the portal endpoint. None means ops review is needed."""
    partner_id = partner["partner_id"]
    if partner["transaction_type_identifier"]:
        partner_id = f"{partner_id}_{partner['transaction_type_identifier']}"
    return resolve_headers_pipeline.execute(
        partner_id=partner_id,
        file_name=intake["attachment_names"][0],
    )


@task(retries=2, retry_delay_seconds=[60, 300], tags=[CLIENT_NAME, "sftp"])
def transform_data(resolved: dict) -> dict:
    """Run the partner's preprocessing script when one is configured."""
    return transform_input_data_pipeline.execute(resolved)


@task(retries=1, retry_delay_seconds=60, tags=[CLIENT_NAME, "sftp"])
def prepare_input_data(transformed: dict) -> dict:
    """Map the transformed file's columns to input-table columns."""
    return prepare_input_data_pipeline.execute(transformed)


@flow(name=f"{CLIENT_NAME}-process-partner-file", log_prints=True)
def process_partner_file(intake: dict, partner: dict, email_uid: str) -> dict:
    partner_id = partner["partner_id"]

    resolved = resolve_headers(intake, partner)
    if resolved is None:
        emit_event(
            event=f"bomisco.{CLIENT_NAME}.header-mapping.review-required",
            resource={"prefect.resource.id": f"bomisco.{CLIENT_NAME}.partner.{partner_id}"},
            payload={"email_uid": email_uid, "partner_id": partner_id},
        )
        create_markdown_artifact(
            key=slug("review", email_uid, partner_id),
            markdown=(
                f"### Ops review needed\n"
                f"- **Partner:** {partner['partner_name']} ({partner_id})\n"
                f"- **File:** {intake['attachment_names'][0]}\n"
                f"The ops email has the link to the mapping screen."
            ),
            description="Header mapping is waiting for ops",
        )
        return {"status": "AWAITING_REVIEW", "partner_id": partner_id}

    if resolved.get("partner_mappings"):
        create_table_artifact(
            key=slug("mapping", email_uid, partner_id),
            table=resolved["partner_mappings"],
            description=f"Header mapping for {partner['partner_name']}",
        )

    transformed = transform_data(resolved)
    ingested = prepare_input_data(transformed)
    rows = ingested.get("row_count", 0)
    create_markdown_artifact(
        key=slug("load", email_uid, partner_id),
        markdown=(
            f"### Input data prepared\n"
            f"- **Partner:** {partner['partner_name']} ({partner_id})\n"
            f"- **File:** {transformed.get('file_name')}\n"
            f"- **Rows:** {rows}\n"
            f"- **Preview:** `{ingested.get('input_data_preview_path')}`"
        ),
        description="Input data load summary",
    )
    return {"status": "COMPLETED", "partner_id": partner_id, "row_count": rows}


@flow(name=f"{CLIENT_NAME}-file-orchestrator", log_prints=True,
      on_failure=[on_flow_failure], on_crashed=[on_flow_failure])
def sonos_file_orchestrator(config: RunConfig):
    """Entry point: started by the 'email received' automation, one run per email."""
    assert_client_config(CLIENT_NAME)
    uid = config.email_uid

    intake = intake_email(uid)
    if intake is None:
        print(f"UID {uid} already processed or has no spreadsheet attachment.")
        return {"status": "SKIPPED"}

    partners = identify_partner(intake)
    if not partners:
        # identify_partner_pipeline already emailed ops
        raise OpsAlreadyNotified(f"No partner matched email UID {uid}")

    create_table_artifact(
        key=slug("identified", uid),
        table=partners,
        description=f"Partner identification: {intake['subject']}",
    )

    results = [process_partner_file(intake, p, uid) for p in partners]
    create_markdown_artifact(
        key=slug("run", uid),
        markdown="### Run summary\n" + "\n".join(
            f"- {r['partner_id']}: **{r['status']}**" for r in results
        ),
        description="Run summary",
    )
    return results