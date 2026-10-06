"""Transfer files to and from exact SFTP paths.

The config may be an INI path or a Config object. INI files must contain a [sftp] section with
host, username, and password. Port is optional and defaults to 22. Set
trust_on_first_use = true to save the first server key beside the INI file.
"""

import argparse
import posixpath
import shutil
import stat
import urllib.parse
from contextlib import contextmanager
from pathlib import Path
from typing import Generator
from core.utils.file_utils import build_input_file_path, build_mapped_file_path, build_mapping_review_file_path, build_transformed_files_backup_file_path
from prefect import get_run_logger

from .config_utils import Config

SUPPORTED_INPUT_EXTENSIONS = (".csv", ".xlsx", ".xls", ".tsv")
ConfigSource = Config | str | Path | None


def _resolve_config(config: ConfigSource) -> Config:
    """Use the supplied Config or load an INI file, defaulting to the project config."""
    if isinstance(config, Config):
        return config
    return Config() if config is None else Config(config)


@contextmanager
def _open_sftp(config: ConfigSource = None) -> Generator[object, None, None]:
    """Open SFTP using the same host-key checks for INI paths and Config objects."""
    config = _resolve_config(config)
    if "sftp" not in config.parser:
        raise ValueError(f"Missing [sftp] section in {config.path}")

    settings = config.parser["sftp"]
    missing = [key for key in ("host", "username", "password") if not settings.get(key, "").strip()]
    if missing:
        raise ValueError(f"Missing SFTP settings: {', '.join(missing)}")

    try:
        port = settings.getint("port", fallback=22)
    except ValueError as exc:
        raise ValueError("SFTP port must be an integer") from exc
    if not 1 <= port <= 65535:
        raise ValueError("SFTP port must be between 1 and 65535")

    try:
        trust_on_first_use = settings.getboolean("trust_on_first_use", fallback=False)
    except ValueError as exc:
        raise ValueError("trust_on_first_use must be true or false") from exc
    known_hosts_path = config.path.resolve().with_name("known_hosts")

    try:
        import paramiko
    except ImportError as exc:
        raise RuntimeError("Install paramiko to transfer files over SFTP") from exc

    with paramiko.SSHClient() as client:
        client.load_system_host_keys()
        if trust_on_first_use:
            known_hosts_path.touch(mode=0o600, exist_ok=True)
            client.load_host_keys(str(known_hosts_path))
            # Only the first connection may add a key. Later connections must match it.
            policy = (
                paramiko.AutoAddPolicy()
                if known_hosts_path.stat().st_size == 0
                else paramiko.RejectPolicy()
            )
            client.set_missing_host_key_policy(policy)
        else:
            client.set_missing_host_key_policy(paramiko.RejectPolicy())
        client.connect(
            hostname=settings["host"].strip(),
            port=port,
            username=settings["username"].strip(),
            password=settings["password"].strip(),
            look_for_keys=False,
            allow_agent=False,
        )
        with client.open_sftp() as sftp:
            yield sftp


def put_sftp(input_file: str | Path, sftp_path: str, config: ConfigSource = None) -> None:
    """Copy input_file to sftp_path using credentials from config."""
    source = Path(input_file)
    if not source.is_file():
        raise FileNotFoundError(f"Input file not found: {source}")
    if not sftp_path:
        raise ValueError("SFTP destination path must not be empty")


    get_run_logger().info("Uploading file via SFTP: %s → %s", source, sftp_path)
    with _open_sftp(config) as sftp:
        _ensure_sftp_directory(sftp, posixpath.dirname(sftp_path))
        sftp.put(str(source), sftp_path)


def get_sftp(sftp_path: str, output_file: str | Path, config: ConfigSource = None) -> None:
    """Copy sftp_path to output_file using credentials from config."""
    if not sftp_path:
        raise ValueError("SFTP source path must not be empty")
    destination = Path(output_file)
    if destination.is_dir():
        raise ValueError(f"Output path is a directory: {destination}")

    with _open_sftp(config) as sftp:
        sftp.get(sftp_path, str(destination))


def download_mapped_file_via_sftp(
    mapped_file_path: str,
    local_dir: str | Path,
    config: ConfigSource = None,
) -> Path:
    """Stage the exact mapped SFTP file selected by a flow; return its local path.

    The caller owns local_dir and its cleanup. Using the resolved remote path
    avoids selecting another flow's file from the partner folder.
    """
    filename = posixpath.basename(mapped_file_path)
    if not filename or filename in (".", ".."):
        raise ValueError("mapped_file_path must identify an SFTP file")
    destination = Path(local_dir) / filename
    destination.parent.mkdir(parents=True, exist_ok=True)
    get_sftp(mapped_file_path, destination, config=config)
    return destination


def upload_transformed_file_via_sftp(
    local_file_path: str | Path,
    partner_name: str,
    config: ConfigSource = None,
) -> tuple[str, str]:
    """Upload a local result to its configured partner folder and backup.

    Return (transformed_path, backup_path). Both copies retain the local filename;
    the caller owns local file cleanup. Transfer failures propagate.
    """
    from .file_utils import get_safe_name

    config = _resolve_config(config)
    source = Path(local_file_path)
    if not source.is_file():
        raise FileNotFoundError(f"Transformed file not found: {source}")
    partner_folder = get_safe_name(partner_name)
    if not partner_folder or partner_folder in (".", ".."):
        raise ValueError("A valid partner name is required for the transformed folder")
    locations = {}
    for key in ("transformed_files_location", "transformed_files_backup_location"):
        locations[key] = config.get(key, section="sftp", fallback="").strip()
        if not locations[key]:
            raise ValueError(f"[sftp] {key} must be configured")
    target_dir = posixpath.join(locations["transformed_files_location"], partner_folder)
    backup_dir = locations["transformed_files_backup_location"]
    if posixpath.normpath(target_dir) == posixpath.normpath(backup_dir):
        raise ValueError("Transformed destination and backup paths must differ")
    transformed_path = save_to_target(source, target_dir, source.name, config=config)
    backup_path = save_to_target(source, backup_dir, source.name, config=config)
    return transformed_path, backup_path


def _ensure_sftp_directory(sftp: object, directory: str) -> None:
    """Create a remote directory and any missing parents."""
    directory = posixpath.normpath(directory)
    if directory in (".", "/"):
        return

    try:
        attributes = sftp.stat(directory)
    except FileNotFoundError:
        _ensure_sftp_directory(sftp, posixpath.dirname(directory))
        sftp.mkdir(directory)
    else:
        if not stat.S_ISDIR(attributes.st_mode):
            raise NotADirectoryError(f"SFTP target path is not a directory: {directory}")


def copy_sftp_file(
    filename: str,
    source_dir: str,
    target_dir: str,
    config: ConfigSource = None,
) -> str:
    """Copy a file between SFTP directories, retaining its filename.

    Missing target directories are created. An existing target file is overwritten.
    Return the full target path; leave the source file in place.
    """
    if not filename or filename in (".", "..") or filename != posixpath.basename(filename):
        raise ValueError("filename must be a single file name")
    if not source_dir or not target_dir:
        raise ValueError("SFTP source and target directories must not be empty")

    source_path = posixpath.normpath(posixpath.join(source_dir, filename))
    target_path = posixpath.normpath(posixpath.join(target_dir, filename))
    if source_path == target_path:
        raise ValueError("SFTP source and target paths must differ")

    with _open_sftp(config) as sftp:
        with sftp.open(source_path, "rb") as source:
            _ensure_sftp_directory(sftp, target_dir)
            with sftp.open(target_path, "wb") as target:
                shutil.copyfileobj(source, target)

    return target_path


def remove_sftp(sftp_path: str, config: ConfigSource = None) -> None:
    """Delete one remote file."""
    if not sftp_path:
        raise ValueError("SFTP path must not be empty")
    with _open_sftp(config) as sftp:
        sftp.remove(sftp_path)


def copy_transformed_file(
    source_path: str,
    partner_name: str,
    config: ConfigSource = None,
) -> tuple[str, str]:
    """Copy transformed SFTP data to its partner folder and the backup directory.

    Both destinations retain the source filename; existing files are overwritten.
    Missing directories are created by copy_sftp_file. The source is retained.
    """
    from .file_utils import get_safe_name

    config = _resolve_config(config)
    partner_folder = get_safe_name(partner_name)
    if not partner_folder or partner_folder in (".", ".."):
        raise ValueError("A valid partner name is required for the transformed folder")
    locations = {}
    for key in ("transformed_files_location", "transformed_files_backup_location"):
        locations[key] = config.get(key, section="sftp", fallback="").strip()
        if not locations[key]:
            raise ValueError(f"[sftp] {key} must be configured")
    if not source_path or not posixpath.basename(source_path):
        raise ValueError("A transformed source file path is required")

    filename = posixpath.basename(source_path)
    source_dir = posixpath.dirname(source_path) or "."
    target_dir = posixpath.join(locations["transformed_files_location"], partner_folder)
    backup_dir = locations["transformed_files_backup_location"]
    paths = [posixpath.normpath(path) for path in (
        source_path, posixpath.join(target_dir, filename), posixpath.join(backup_dir, filename)
    )]
    if len(set(paths)) != 3:
        raise ValueError("Transformed source, destination, and backup paths must differ")

    transformed_path = copy_sftp_file(filename, source_dir, target_dir, config=config)
    backup_path = copy_sftp_file(filename, target_dir, backup_dir, config=config)
    return transformed_path, backup_path


def delete_input_file_via_sftp(remote_path: str, config: ConfigSource = None) -> None:
    """Delete a processed input file, logging failures without interrupting the caller."""
    if not remote_path:
        get_run_logger().warning("No remote path provided for deletion")
        return

    try:
        get_run_logger().info("Deleting input file from SFTP: %s", remote_path)
        remove_sftp(remote_path, config=config)
        get_run_logger().info("Input file deleted")
    except FileNotFoundError:
        get_run_logger().info("Input file already deleted or not found")
    except Exception as exc:
        get_run_logger().warning("Failed to delete input file %s: %s", remote_path, exc)


def save_to_output_via_sftp(
    local_file_path: str | Path,
    original_name: str,
    config: ConfigSource = None,
) -> str:
    return save_to_target(
        local_file_path=local_file_path,
        target_dir=_resolve_config(config).get("output_files_location", section="sftp", fallback="").rstrip("/"),
        original_name=original_name,
        config=config,
    )

def save_to_mapped_via_sftp(
    local_file_path: str | Path,
    original_name: str,
    partner_name:str,
    config: ConfigSource = None,
) -> str:

    return save_to_target(
        local_file_path=local_file_path,
        target_dir=build_mapped_file_path(partner_name),
        original_name=original_name,
        config=config,
    )

def save_to_mapping_review_via_sftp(
    local_file_path: str | Path,
    original_name: str,
    partner_name: str,
    config: ConfigSource = None,
) -> str:
    return save_to_target(
        local_file_path=local_file_path,
        target_dir=build_mapping_review_file_path(partner_name),
        original_name=original_name,
        config=config,
    )

def save_to_transformed_files_backup_via_sftp(
    local_file_path: str | Path,
    original_name: str,
    partner_name: str,
    config: ConfigSource = None,
) -> str:
    return save_to_target(
        local_file_path=local_file_path,
        target_dir=build_transformed_files_backup_file_path(partner_name),
        original_name=original_name,
        config=config,
    )

def save_to_target(
    local_file_path: str | Path,
    target_dir: str,
    original_name: str,
    config: ConfigSource = None,
) -> str:
    """Upload a processed file to the configured output directory under its original name."""
    config = _resolve_config(config)
    # target_dir = config.get("output_files_location", section="sftp", fallback="").rstrip("/")
    if not target_dir:
        raise RuntimeError("[sftp] output_files_location not set in config.ini")

    remote_path = f"{target_dir}/{original_name}"
    get_run_logger().info("Uploading output file via SFTP: %s → %s", local_file_path, remote_path)
    put_sftp(local_file_path, remote_path, config=config)
    get_run_logger().info("Output saved to SFTP → %s", remote_path)
    return remote_path

def download_input_file_via_sftp(
    local_dir: str | Path,
    partner_name: str = "",
    target_filename: str = "",
    config: ConfigSource = None,
) -> tuple[str, str, str]:
    """Download an exact input name, or the only supported file in the input dir."""
    config = _resolve_config(config)
    input_dir = config.get("input_files_location", section="sftp", fallback="").rstrip("/")
    if not input_dir:
        raise FileNotFoundError("input_files_location not set in config.ini")

    local_dir = Path(local_dir)
    local_dir.mkdir(parents=True, exist_ok=True)
    partner_specific_input_dir = build_input_file_path(partner_name)
    with _open_sftp(config) as sftp:
        try:
            entries = sftp.listdir_attr(partner_specific_input_dir)
        except FileNotFoundError as exc:
            raise FileNotFoundError(
                f"input_files_location does not exist on server: {partner_specific_input_dir}"
            ) from exc

        files = {
            entry.filename: entry for entry in entries
            if not entry.filename.startswith(".") and not stat.S_ISDIR(entry.st_mode)
        }
        if target_filename:
            if target_filename in files:
                chosen = target_filename
            else:
                lower_target = target_filename.strip().lower()
                chosen = next(
                    (name for name in files if name.strip().lower() == lower_target), None
                )
                if chosen is None:
                    raise FileNotFoundError(
                        f"Expected file '{target_filename}' not found in SFTP dir: "
                        f"{partner_specific_input_dir}\n"
                        f"Files present: {sorted(files.keys()) or '(none)'}"
                    )
        else:
            candidates = sorted(
                name for name in files
                if Path(name).suffix.lower() in SUPPORTED_INPUT_EXTENSIONS
            )
            if not candidates:
                raise FileNotFoundError(
                    f"No supported file found in SFTP dir: {partner_specific_input_dir}\n"
                    "Place the raw file there before running."
                )
            if len(candidates) > 1:
                raise FileNotFoundError(
                    f"Multiple supported files found in SFTP dir: {input_dir}\n"
                    f"  {candidates}\n"
                    "Pass --file <exact-name> to specify which one to process."
                )
            chosen = candidates[0]

        remote_path = f"{partner_specific_input_dir}/{chosen}"
        local_path = str(local_dir / chosen)
        get_run_logger().info("Downloading input file via SFTP: %s → %s", remote_path, local_path)
        sftp.get(remote_path, local_path)
        get_run_logger().info("Input file staged locally: %s (remote: %s)", local_path, remote_path)
        return local_path, chosen, remote_path


def build_sftp_link(remote_path: str, config: ConfigSource = None) -> str:
    """Build a display-only SFTP URI with no credentials."""
    config = _resolve_config(config)
    host = config.get("host", "sftp", fallback="")
    if not host or not remote_path:
        return ""
    port = config.get_int("port", "sftp", fallback=22)
    port_part = "" if port == 22 else f":{port}"
    safe_path = "/".join(
        urllib.parse.quote(segment, safe="")
        for segment in remote_path.strip("/").split("/")
    )
    return f"sftp://{host}{port_part}/{safe_path}"


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--get", action="store_true", help="Download instead of upload")
    parser.add_argument("local_file", help="Local input or output file")
    parser.add_argument("sftp_path", help="Full file path on the SFTP server")
    parser.add_argument("config_path", nargs="?", help="Path to the INI file containing [sftp] credentials")
    args = parser.parse_args()
    if args.get:
        get_sftp(args.sftp_path, args.local_file, config=args.config_path)
    else:
        put_sftp(args.local_file, args.sftp_path, config=args.config_path)


if __name__ == "__main__":
    main()
