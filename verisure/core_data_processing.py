import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from core.data_ingestion import prepare_input_data_pipeline
from core.data_transformation import transform_input_data_pipeline
from core.header_mapping import resolve_headers_pipeline
from core.partner_identification import identify_partner_pipeline
from core.email_search import process_mailbox_pipeline
from prefect import flow, task, get_run_logger

@task
def read_unprocessed_emails():
    messages = process_mailbox_pipeline.execute()
    get_run_logger().info("Found %d unprocessed email messages.", len(messages) if messages else 0)
    return messages

@task
def identify_partner(message):
    get_run_logger().info("Identifying partner for message from ")
    get_run_logger().info("sender: %s, subject: %s", message.sender, message.subject)
    get_run_logger().info("file: %s", message.uploaded_sftp_file_names[0] if message.uploaded_sftp_file_names else None)

    resolved_partners = identify_partner_pipeline.execute(sender=message.sender, subject=message.subject, filename=message.uploaded_sftp_file_names[0] if message.uploaded_sftp_file_names else None)
    output = {
        "sender": message.sender,
        "subject": message.subject,
        "received_at": message.received_at.isoformat(),
        "attachment_names": list(message.attachment_names),
        "uploaded_sftp_file_names": list(message.uploaded_sftp_file_names),
        "resolved_partners": [
            {
                "partner_id": partner["partner_id"],
                "partner_name": partner["partner_name"],
                "partner_type": partner["partner_type"],
                "sheet_name": partner["sheet_name"],
                "pre_header_rows": partner["pre_header_rows"],
                "transaction_type_identifier": partner["transaction_type_identifier"],
                "input_file_start_index": partner["input_file_start_index"],
                "attachment_extension": partner["attachment_extension"],
                "partner_input_file_path": partner["partner_input_file_path"],
            }
            for partner in resolved_partners
        ],
    }
    get_run_logger().debug("Partner identification output: %s", output)
    return output

@task
def resolve_sheet_and_headers(partner_output):
    get_run_logger().info("Resolving sheet and headers for partner output: %s", partner_output)
    partners = partner_output["resolved_partners"]
    if not partners:
        get_run_logger().info("No partner match found; skipping header mapping.")
        return None

    if len(partners) > 1:
        get_run_logger().info("Multiple partner matches found; using the first one: %s", partners)
    # TODO:PBC - Handle multiple partner matches as needed. For now, we will just use the first match.
    partner = partners[0]
    partner_id = str(partner["partner_id"]).strip()
    transaction_type = str(partner["transaction_type_identifier"]).strip()
    if transaction_type:
        partner_id = f"{partner_id}_{transaction_type}"
    resolved_sheets = resolve_headers_pipeline.execute(
        partner_id=partner_id,
        file_name=partner_output["attachment_names"][0] if partner_output["attachment_names"] else None,
        skip_rows=partner["pre_header_rows"],
        sheet=partner["sheet_name"] or None,
    )
    get_run_logger().info("Resolved sheets and headers: %s", resolved_sheets)
    return resolved_sheets

@task
def transform_raw_data(resolved_output):
    return transform_input_data_pipeline.execute(resolved_output)

@task
def map_rawdata_to_input_data(transformed_output):
    return prepare_input_data_pipeline.execute(transformed_output)

@task
def persist_input_data():
    return "Hello from macOS local environment!"

@flow
def core_data_workflow():
    unprocessed_messages = read_unprocessed_emails()
    get_run_logger().debug("Unprocessed messages: %s", unprocessed_messages)
    if(unprocessed_messages is None or len(unprocessed_messages) == 0):
        get_run_logger().info("No messages found in the inbox.")
        return
    ingestion_futures = []
    for message in unprocessed_messages:
        partner_identification_result_future = identify_partner.submit(message)
        get_run_logger().info("Partner identification result future: %s", partner_identification_result_future)
        mapping_future = resolve_sheet_and_headers.submit(partner_identification_result_future)
        transformation_future = transform_raw_data.submit(mapping_future)
        ingestion_futures.append(map_rawdata_to_input_data.submit(transformation_future))

    for future in ingestion_futures:
        future.result()

if __name__ == "__main__":
    core_data_workflow()
