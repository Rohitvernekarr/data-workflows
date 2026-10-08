"""Prefect helpers shared by client flows: failure hook, alert email, artifact keys."""

import logging
import re

from ..email_search import process_mailbox_pipeline
from .config_utils import Config
from .email_utils import EmailUtils

logger = logging.getLogger(__name__)


class OpsAlreadyNotified(Exception):
    """Raised when the failing step already emailed ops, so no second alert is sent."""


def slug(*parts) -> str:
    """Artifact keys allow only lowercase letters, digits and dashes."""
    return re.sub(r"[^a-z0-9]+", "-", "-".join(str(p) for p in parts).lower()).strip("-")


def assert_client_config(expected: str) -> None:
    """Refuse to run with another client's config (e.g. the verisure default)."""
    actual = (Config().get("client_name", fallback="") or "").strip().lower()
    if actual != expected:
        raise RuntimeError(
            f"Config client_name is {actual!r}, expected {expected!r}. "
            "Check the PIPELINE_CONFIG_PATH job variable."
        )


def send_alert(subject: str, body_html: str) -> None:
    cfg = Config()
    recipients = cfg.get("ops_alert_email", section="pipeline", fallback="")
    if not recipients:
        logger.warning("No ops_alert_email configured; alert not sent.")
        return
    client = (cfg.get("client_name", fallback="") or "").upper()
    try:
        EmailUtils.send(
            to=recipients,
            subject=f"[{client} PIPELINE ALERT] {subject}",
            body=body_html,
            is_html=True,
        )
    except Exception:
        logger.exception("Alert email failed")


def on_flow_failure(flow, flow_run, state) -> None:
    """Hook for Failed and Crashed runs: tag the email FAILED and alert ops."""
    params = flow_run.parameters or {}
    config = params.get("config") or {}
    uid = config.get("email_uid")
    try:
        exc = state.result(raise_on_failure=False)
    except Exception as err:
        exc = err
    if uid:
        try:
            process_mailbox_pipeline.set_keyword(uid, "FAILED")
        except Exception:
            logger.exception("Could not tag UID %s as FAILED", uid)
    if config.get("skip_alerts") or isinstance(exc, OpsAlreadyNotified):
        return
    send_alert(
        f"{flow.name} failed (email UID {uid})",
        f"<p>Flow run <b>{flow_run.name}</b> ended in state <b>{state.name}</b>.</p>"
        f"<p>Email UID: {uid}</p><pre>{exc!r}</pre>"
        f"<p>Flow run id: {flow_run.id}</p>",
    )