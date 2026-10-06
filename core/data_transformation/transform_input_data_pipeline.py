"""Run the configured partner preprocessing script after header resolution."""

import os
from pathlib import Path
import subprocess
import sys
from tempfile import TemporaryDirectory
from prefect import get_run_logger

from core.utils.config_utils import Config, DEFAULT_CONFIG_PATH
from core.utils.partner_config_utils import fetch_partner_config
from core.utils.sftp_utils import download_mapped_file_via_sftp, upload_transformed_file_via_sftp


def execute(
    resolved_output: dict | None,
    config: str | Path = DEFAULT_CONFIG_PATH,
) -> dict | None:
    """Stage, transform, and upload the exact mapped file for this flow.

    Scripts use the current interpreter and working directory, with no arguments.
    LOCAL_MAPPED_FILE_PATH is the local input; LOCAL_TRANSFORMED_FILE_PATH is the
    required output workbook (<input stem>_Transformed.xlsx). PARTNER_ID and the
    remote MAPPED_FILE_PATH remain available as context. Scripts must write the
    output before exiting successfully. Relative script paths resolve against
    the INI directory. Staging is cleaned up on success and failure.
    """
    if resolved_output is None:
        get_run_logger().info("No resolved input available; skipping transformation.")
        return None

    partner_id = resolved_output.get("partner_id")
    if not isinstance(partner_id, str) or not partner_id.strip():
        raise ValueError("Resolved output must contain a non-empty partner_id")
    partner_id = partner_id.strip()
    cfg = Config(config)
    partner = fetch_partner_config(cfg, partner_id, strict=True)
    mapped_file_path = resolved_output.get("mapped_file_path")
    if not isinstance(mapped_file_path, str) or not mapped_file_path.strip():
        raise ValueError("Resolved output must contain a non-empty mapped_file_path")

    if partner.preprocessing_required:
        if not partner.preprocessing_script_path:
            raise ValueError(f"Preprocessing script path is missing for partner {partner_id}")
        script_path = Path(partner.preprocessing_script_path).expanduser()
        if not script_path.is_absolute():
            script_path = cfg.path.parent / script_path
        script_path = script_path.resolve()
        if not script_path.is_file():
            raise FileNotFoundError(f"Preprocessing script not found: {script_path}")

    with TemporaryDirectory(prefix="transformation-") as directory:
        local_path = download_mapped_file_via_sftp(mapped_file_path, directory, config=cfg)
        output_path = local_path
        if partner.preprocessing_required:
            output_path = local_path.with_name(f"{local_path.stem}_Transformed.xlsx")
            logger = get_run_logger()
            logger.info("Running preprocessing for partner %s: %s", partner_id, script_path)
            script_env = {
                **os.environ,
                "PARTNER_ID": partner_id,
                "MAPPED_FILE_PATH": mapped_file_path,
                "LOCAL_MAPPED_FILE_PATH": str(local_path),
                "LOCAL_TRANSFORMED_FILE_PATH": str(output_path),
            }
            try:
                result = subprocess.run(
                    [sys.executable, str(script_path)],
                    check=True,
                    env=script_env,
                    capture_output=True,
                    text=True,
                    errors="replace",
                )
            except subprocess.CalledProcessError as exc:
                logger.error(
                    "Preprocessing failed for partner %s: %s (exit code %s)",
                    partner_id, script_path, exc.returncode,
                )
                if exc.stdout:
                    logger.info("Preprocessing stdout:\n%s", exc.stdout.rstrip())
                if exc.stderr:
                    logger.error("Preprocessing stderr:\n%s", exc.stderr.rstrip())
                raise
            except OSError:
                logger.exception(
                    "Could not start preprocessing for partner %s: %s", partner_id, script_path
                )
                raise
            else:
                if result.stdout:
                    logger.info("Preprocessing stdout:\n%s", result.stdout.rstrip())
                if result.stderr:
                    logger.warning("Preprocessing stderr:\n%s", result.stderr.rstrip())
            if not output_path.is_file():
                raise FileNotFoundError(f"Preprocessing script did not create output: {output_path}")
        else:
            get_run_logger().info("Preprocessing is not required for partner %s.", partner_id)

        transformed_path, backup_path = upload_transformed_file_via_sftp(
            output_path, partner.partner_name, config=cfg
        )
    get_run_logger().info("Transformed file saved to %s; backup saved to %s", transformed_path, backup_path)
    return {
        **resolved_output,
        "partner_name": partner.partner_name,
        "file_name": Path(transformed_path).name,
        "transformed_file_path": transformed_path,
        "transformed_file_backup_path": backup_path,
        # Preprocessing writes a new workbook with headers on the first sheet/row.
        **({"sheet_name": None, "skip_rows": 0} if partner.preprocessing_required else {}),
    }
