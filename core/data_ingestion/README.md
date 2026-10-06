# Input data preparation

`prepare_input_data_pipeline.execute(transformed_output, config=...)` downloads
the exact `file_name` at `transformed_file_path` from SFTP. The path must be under
`[sftp] transformed_files_location`. Temporary local files are removed on success
and failure. CSV, TSV, XLSX, and XLS are supported; `sheet_name`, `skip_rows`, and
an optional `delimiter` describe the transformed file's layout.

`prepare_records(headers, rows, partner_id, cfg)` fetches current partner mappings
using `fetch_partner_mappings`. The partner ID comes from the previous workflow
step's `partner_id`; mappings in the workflow payload are not used. The API
response may be a list or an object containing a `data` list. Each mapping uses
`transformed_column_field` as its ingestion target, with no legacy target fallback.
Mappings without a populated `transformed_column_field` are skipped. Every
populated target requires a matching source header, regardless of `is_mandatory`
or `source`. Preparation still fails if no columns can be mapped.
Source headers match only `expected_raw_file_field`. Values from that file column
populate the `input_data` column named by `transformed_column_field`. Matching
ignores case and surrounding whitespace. A selected source matching multiple
headers is rejected as ambiguous; duplicate unmapped headers are ignored. One
source can supply multiple distinct targets. Target column names
and data values retain their case. API failures and malformed mappings raise errors.
If preprocessing changes the sheet or header-row
layout, the corresponding output metadata must describe that new layout.

The result retains the preceding task's metadata and adds `input_columns`,
`input_records` (one dictionary per data row), and `row_count`. Unmapped columns
and entirely empty rows are omitted. Empty cells become `None`; CSV values stay
strings to preserve leading zeros. Missing mandatory headers and duplicate or
ambiguous mappings raise errors.

Before returning, the task exports those columns and records through a pandas
DataFrame to a temporary Excel workbook with an `input_data` sheet and no index
column. `input_data_preview_path` contains the local workbook path, which is also
logged. The preview remains after staging cleanup so it can be opened on the
workflow machine; delete it when no longer needed.

This task prepares records in memory for the persistence task. It does not write
to a database or infer database types; schema validation and type conversion
require the target table's schema. Files whose headers need manual review do not
reach this task. Partners without preprocessing still have their mapped file
copied to the transformed folder and backup before preparation.
