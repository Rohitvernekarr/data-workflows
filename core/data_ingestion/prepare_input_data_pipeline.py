"""Read a transformed SFTP file and map its values to input-table columns."""

import csv
from pathlib import Path
import posixpath
from tempfile import NamedTemporaryFile, TemporaryDirectory

from core.utils.config_utils import Config, DEFAULT_CONFIG_PATH
from core.utils.api_utils import fetch_partner_mappings
from core.utils.sftp_utils import get_sftp
from prefect import get_run_logger


def _read_rows(path: Path, sheet_name: str | None, skip_rows: int, delimiter: str | None):
    """Read values without inferring numeric types for CSV identifiers."""
    extension = path.suffix.lower()
    if extension in (".csv", ".tsv"):
        with path.open(newline="", encoding="utf-8-sig") as stream:
            rows = list(csv.reader(stream, delimiter=delimiter or ("\t" if extension == ".tsv" else ",")))
    elif extension == ".xlsx":
        import openpyxl

        # Read-only mode trusts the worksheet's cached dimensions, which some
        # exporters leave stale and can truncate both headers and data. Normal
        # mode derives the bounds from the actual cells instead.
        workbook = openpyxl.load_workbook(path, read_only=False, data_only=True)
        try:
            sheet = workbook[sheet_name] if sheet_name else workbook.worksheets[0]
            rows = list(sheet.iter_rows(values_only=True))
        finally:
            workbook.close()
    elif extension == ".xls":
        import xlrd

        workbook = xlrd.open_workbook(str(path))
        try:
            sheet = workbook.sheet_by_name(sheet_name) if sheet_name else workbook.sheet_by_index(0)
            rows = []
            for index in range(sheet.nrows):
                row = []
                for cell in sheet.row(index):
                    if cell.ctype == xlrd.XL_CELL_DATE:
                        value = xlrd.xldate_as_datetime(cell.value, workbook.datemode)
                    elif cell.ctype in (xlrd.XL_CELL_EMPTY, xlrd.XL_CELL_BLANK):
                        value = None
                    elif cell.ctype == xlrd.XL_CELL_ERROR:
                        raise ValueError(f"Excel error in row {index + 1}")
                    else:
                        value = cell.value
                    row.append(value)
                rows.append(row)
        finally:
            workbook.release_resources()
    else:
        raise ValueError(f"Unsupported transformed file type: {extension}")
    if len(rows) <= skip_rows:
        raise ValueError("Transformed file has no header row")
    return rows[skip_rows], rows[skip_rows + 1:]


def prepare_records(
    headers, rows, partner_id: str, cfg: Config,
) -> tuple[list[str], list[dict]]:
    """Map expected_raw_file_field values to input_data table column names.

    Only mappings with a populated transformed_column_field are selected. Each
    selected mapping requires one matching header, ignoring case and surrounding
    whitespace. Blank cells become None; other values retain their types.
    """
    if not isinstance(partner_id, str) or not partner_id.strip():
        raise ValueError("partner_id must be a non-empty string")
    partner_mappings = fetch_partner_mappings(cfg, partner_id.strip())
    if isinstance(partner_mappings, dict):
        partner_mappings = partner_mappings.get("data")
    if not isinstance(partner_mappings, list) or not partner_mappings:
        raise ValueError("Partner mapping API must return a non-empty list or a data list")

    header_indexes = {}
    for index, header in enumerate(headers):
        normalized = str(header).strip().casefold() if header is not None else ""
        if normalized:
            header_indexes.setdefault(normalized, []).append(index)

    columns = []
    indexes = []
    for mapping in partner_mappings:
        if not isinstance(mapping, dict):
            raise ValueError("Each partner mapping must be an object")
        target = mapping.get("transformed_column_field")
        if not isinstance(target, str) or not target.strip():
            continue
        target = target.strip()
        source = mapping.get("expected_raw_file_field")
        if not isinstance(source, str) or not source.strip():
            raise ValueError(f"Mapping for {target!r} has no expected_raw_file_field")
        matches = header_indexes.get(source.strip().casefold(), [])
        if not matches:
            raise ValueError(f"Missing mandatory source column {source!r} for {target!r}")
        if len(matches) > 1:
            raise ValueError(f"Ambiguous source header {source!r} for {target!r}")
        if target in columns:
            raise ValueError(f"Duplicate mapping for input-table column {target!r}")
        columns.append(target)
        indexes.append(matches[0])
    if not columns:
        raise ValueError("No transformed columns match the input-table mappings")

    records = []
    for number, row in enumerate(rows, start=1):
        if all(value is None or value == "" for value in row):
            continue
        if len(row) > len(headers):
            raise ValueError(f"Data row {number} has more values than headers")
        record = {}
        for column, index in zip(columns, indexes):
            value = row[index] if index < len(row) else None
            record[column] = None if value is None or value == "" else value
        records.append(record)

    get_run_logger().info("Prepared %d input records for partner %s", len(records), partner_id)
    get_run_logger().debug("Prepared input columns: %s", columns)
    return columns, records


def execute(
    transformed_output: dict | None,
    config: str | Path = DEFAULT_CONFIG_PATH,
) -> dict | None:
    """Return input_columns, input_records, and row_count for DB persistence.

    Fetch mappings using the preceding step's partner_id. This stage prepares
    values; database writes and schema-specific type
    conversion belong to the persistence stage.
    """
    if transformed_output is None:
        return None
    cfg = Config(config)
    remote_path = transformed_output.get("transformed_file_path")
    filename = transformed_output.get("file_name")
    base_dir = cfg.get("transformed_files_location", section="sftp", fallback="").strip()
    if not base_dir or not isinstance(remote_path, str) or not remote_path:
        raise ValueError("transformed_files_location and transformed_file_path are required")
    remote_path = posixpath.normpath(remote_path)
    if posixpath.commonpath([base_dir, remote_path]) != posixpath.normpath(base_dir):
        raise ValueError("Transformed file must be inside transformed_files_location")
    if not filename or filename != posixpath.basename(remote_path):
        raise ValueError("file_name must match the transformed file path")
    skip_rows = transformed_output.get("skip_rows", 0)
    if isinstance(skip_rows, bool) or not isinstance(skip_rows, int) or skip_rows < 0:
        raise ValueError("skip_rows must be a non-negative integer")
    partner_id = transformed_output.get("partner_id")
    if not isinstance(partner_id, str) or not partner_id.strip():
        raise ValueError("Transformed output must contain a non-empty partner_id")

    with TemporaryDirectory(prefix="input-ingestion-") as directory:
        local_path = Path(directory) / filename
        get_sftp(remote_path, local_path, config=cfg)
        headers, rows = _read_rows(
            local_path, transformed_output.get("sheet_name"), skip_rows,
            transformed_output.get("delimiter"),
        )
        columns, records = prepare_records(headers, rows, partner_id, cfg)
    get_run_logger().info("Prepared %d input records for partner %s", len(records), partner_id)
    get_run_logger().debug("Prepared input columns: %s", columns)
    get_run_logger().debug("Prepared input records: %s", records)
    import pandas as pd

    dataframe = pd.DataFrame(records, columns=columns)
    # Keep the preview outside the staging directory so it survives this call.
    with NamedTemporaryFile(prefix="input-data-preview-", suffix=".xlsx", delete=False) as preview:
        preview_path = Path(preview.name)
    try:
        dataframe.to_excel(preview_path, index=False, sheet_name="input_data", engine="openpyxl")
    except Exception:
        preview_path.unlink(missing_ok=True)
        raise
    get_run_logger().info("Input data Excel preview: %s", preview_path)
    return {
        **transformed_output,
        "input_columns": columns,
        "input_records": records,
        "row_count": len(records),
        "input_data_preview_path": str(preview_path),
    }
