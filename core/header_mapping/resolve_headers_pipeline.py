import sys
import os
import re
import json
import shutil
import smtplib
import html
import math
import tempfile
import uuid
import urllib.parse
import urllib.request
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from datetime import datetime
from pathlib import Path
from core.utils.sftp_utils import save_to_mapped_via_sftp
from prefect import get_run_logger

# Support both `python resolve_headers_pipeline.py` and package imports.
if __package__:
    from ..utils.config_utils import Config, DEFAULT_CONFIG_PATH
    from ..utils.firebase_auth import auth_headers, get_firebase_token
    from .models import MappingDecision, SheetNotResolvedError
    from ..utils.partner_config_utils import fetch_partner_config
    from ..utils.sftp_utils import (
        build_sftp_link,
        delete_input_file_via_sftp,
        download_input_file_via_sftp,
        save_to_mapping_review_via_sftp,
        save_to_mapped_via_sftp,
    )
else:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
    from utils.config_utils import Config, DEFAULT_CONFIG_PATH
    from utils.firebase_auth import auth_headers, get_firebase_token
    from models import MappingDecision, SheetNotResolvedError
    from utils.partner_config_utils import fetch_partner_config
    from utils.sftp_utils import (
        build_sftp_link,
        delete_input_file_via_sftp,
        download_input_file_via_sftp,
        save_to_output_via_sftp,
        save_to_mapping_review_via_sftp
    )

# ── optional deps ──────────────────────────────────────────────────────────────
try:
    import requests
    REQUESTS_AVAILABLE = True
except ImportError:
    REQUESTS_AVAILABLE = False

try:
    import openpyxl
    OPENPYXL_AVAILABLE = True
except ImportError:
    OPENPYXL_AVAILABLE = False

try:
    import pandas as pd
    PANDAS_AVAILABLE = True
except ImportError:
    PANDAS_AVAILABLE = False

# ══════════════════════════════════════════════════════════════════════════════
# Sheet resolution (Case 3: ambiguous sheet — ported in, unchanged in logic)
# ══════════════════════════════════════════════════════════════════════════════

def _normalize_sheet_name(sheet_name: str) -> str:
    """Return the semantic form of a sheet name for tolerant matching."""
    return "".join(
        character
        for character in sheet_name.casefold()
        if character.isalnum()
    )


def resolve_sheet_name(file_path: str, configured_sheet_name: str) -> tuple[str, list[str]]:
    """Resolve which Excel sheet to use, without reading any data.

    Only meaningful for Excel workbooks — callers must branch on file
    extension before invoking this; CSV/TSV have no sheet concept and are
    handled entirely outside this function.

    Resolution order:
      1. Exactly one sheet in the workbook -> use it, regardless of what's
         configured (a single-sheet file is unambiguous no matter what
         PMC/partner_master_v2 says).
      2. Multiple sheets, exact match on configured_sheet_name -> use it.
         (Excel enforces unique sheet names per workbook, so an exact
         match can only ever be 0 or 1 sheet — no ambiguity possible here.)
      3. Multiple sheets, normalized match (case/space/punctuation-
         insensitive) -> use it, but ONLY if exactly one sheet normalizes
         to that value. Two sheets differing only by case (e.g. "Sales"
         and "SALES") must not be silently collapsed into one — that's
         genuine ambiguity, not resolution.
      4. Otherwise -> unresolved.

    Returns:
        (resolved_sheet_name, available_sheets) if resolved.

    Raises:
        SheetNotResolvedError: If no tier above yields a unique match.
    """
    if not OPENPYXL_AVAILABLE:
        raise RuntimeError("Cannot resolve Excel sheets — install openpyxl: pip install openpyxl")

    wb = openpyxl.load_workbook(file_path, read_only=True, data_only=True)
    try:
        available_sheets = wb.sheetnames

        if len(available_sheets) == 1:
            return available_sheets[0], available_sheets

        configured = (configured_sheet_name or "").strip()
        if configured:
            if configured in available_sheets:
                return configured, available_sheets

            normalized_configured = _normalize_sheet_name(configured)
            normalized_matches = [
                name for name in available_sheets
                if _normalize_sheet_name(name) == normalized_configured
            ]
            if len(normalized_matches) == 1:
                return normalized_matches[0], available_sheets

        raise SheetNotResolvedError(available_sheets, configured_sheet_name)
    finally:
        wb.close()


# ══════════════════════════════════════════════════════════════════════════════
# Read headers LOCALLY (from the file staged via SFTP)
# ══════════════════════════════════════════════════════════════════════════════

def read_headers_locally(
    file_path: str,
    skip_rows: int = 0,
    target_sheet: str = "",
) -> tuple[list[str], str]:
    ext = Path(file_path).suffix.lower()

    if ext in (".xlsx", ".xls"):
        if OPENPYXL_AVAILABLE:
            wb = openpyxl.load_workbook(file_path, read_only=True, data_only=True)
            sheet_names = wb.sheetnames
            sheet = None
            used_sheet_name = ""

            if target_sheet:
                if target_sheet in sheet_names:
                    sheet = wb[target_sheet]
                    used_sheet_name = target_sheet
                else:
                    for sn in sheet_names:
                        if sn.strip().lower() == target_sheet.strip().lower():
                            sheet = wb[sn]
                            used_sheet_name = sn
                            break
                if sheet is None:
                    get_run_logger().warning(
                        "Sheet '%s' not found (available: %s) — using active sheet.",
                        target_sheet, ", ".join(sheet_names),
                    )

            if sheet is None:
                sheet = wb.active
                used_sheet_name = sheet.title

            rows = list(sheet.iter_rows(values_only=True))
            wb.close()

            # ── FIX: guard against a stale/incorrect <dimension> tag ────

            if len(rows) <= max(skip_rows, 1):
                get_run_logger().warning(
                    "Only %d row(s) read in read_only mode (need > skip_rows=%d) "
                    "— sheet dimension may be stale. Reopening in normal mode.",
                    len(rows), skip_rows,
                )
                wb2 = openpyxl.load_workbook(file_path, read_only=False, data_only=True)
                sheet2 = (
                    wb2[used_sheet_name] if used_sheet_name in wb2.sheetnames
                    else wb2.active
                )
                rows = list(sheet2.iter_rows(values_only=True))
                wb2.close()
                get_run_logger().info("Recovered %d row(s) after reopening in normal mode.", len(rows))

            if not rows:
                raise ValueError(f"Sheet '{used_sheet_name}' is empty.")

            if skip_rows > 0:
                if skip_rows < len(rows):
                    header_row = rows[skip_rows]
                    get_run_logger().info(
                        "Using header at row %d (pre_header_rows=%d means %d metadata rows above).",
                        skip_rows + 1, skip_rows, skip_rows,
                    )
                else:
                    get_run_logger().warning("skip_rows=%d exceeds total rows (%d) - using auto-detect", skip_rows, len(rows))
                    header_row = []
                    for i, row in enumerate(rows):
                        non_empty = [
                            str(c).strip()
                            for c in row
                            if c is not None and str(c).strip() not in ("", "None")
                        ]
                        if len(non_empty) >= 2:
                            header_row = row
                            get_run_logger().info("Auto-detected header row at row %d.", i + 1)
                            break
                    if not header_row:
                        header_row = rows[0]
                        get_run_logger().warning("Could not auto-detect header row — using row 1.")
            else:
                header_row = rows[0] if rows else []
                get_run_logger().info("Using header at row 1 (pre_header_rows=0)")

            headers = [
                str(h).strip()
                for h in header_row
                if h is not None and str(h).strip() not in ("", "None")
            ]
            get_run_logger().info(
                "Headers from sheet '%s' row %d: %d columns",
                used_sheet_name, skip_rows + 1 if skip_rows > 0 else 1, len(headers),
            )
            return headers, used_sheet_name

        if PANDAS_AVAILABLE:
            xl = pd.ExcelFile(file_path)
            used_sheet_name = xl.sheet_names[0]
            if target_sheet:
                for sn in xl.sheet_names:
                    if sn.strip().lower() == target_sheet.strip().lower():
                        used_sheet_name = sn
                        break
            df = pd.read_excel(
                file_path, sheet_name=used_sheet_name, skiprows=skip_rows, nrows=0
            )
            headers = [str(c).strip() for c in df.columns if str(c).strip()]
            get_run_logger().info("Headers via pandas from '%s': %d columns", used_sheet_name, len(headers))
            return headers, used_sheet_name

        raise RuntimeError("Cannot read Excel — install openpyxl:  pip install openpyxl")

    # CSV / TSV
    import csv
    delimiter = "\t" if ext == ".tsv" else ","
    with open(file_path, newline="", encoding="utf-8-sig") as fh:
        for _ in range(skip_rows):
            next(fh, None)
        reader = csv.reader(fh, delimiter=delimiter)
        header_row = next(reader, [])
    headers = [h.strip() for h in header_row if h.strip()]
    get_run_logger().info("Headers via csv: %d columns", len(headers))
    return headers, "Sheet1"


# ══════════════════════════════════════════════════════════════════════════════
# HTTP helpers
# ══════════════════════════════════════════════════════════════════════════════

def _get(url: str, cfg: Config, timeout: int = 30) -> dict:
    headers = auth_headers(cfg)
    if REQUESTS_AVAILABLE:
        r = requests.get(url, headers=headers, timeout=timeout)
        r.raise_for_status()
        return r.json()
    req = urllib.request.Request(url, headers=headers)
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read().decode())


def _post_json(url: str, payload: dict, cfg: Config, timeout: int = 60) -> dict:
    headers = auth_headers(cfg)
    if REQUESTS_AVAILABLE:
        r = requests.post(url, json=payload, headers=headers, timeout=timeout)
        r.raise_for_status()
        return r.json()
    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(
        url, data=data,
        headers={"Content-Type": "application/json", **headers},
    )
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read().decode())


# ══════════════════════════════════════════════════════════════════════════════
# Header-field normalisation helpers
# ══════════════════════════════════════════════════════════════════════════════
#
# FIX (mirrors FileProcessing.js FIX #1 + label simplification):
#
#  1. output_field fallback — a suggestion can legitimately come back from
#     suggest-header-mappings with `suggested` set (the expected/matched
#     field) but `output_field` empty, for pass-through columns. Previously
#     every place in this script treated "no output_field" as "ignore this
#     column", which silently dropped valid pass-through renames (e.g. SKU)
#     out of the mapped/renamed set and into "Ignored". We now fall back
#     output_field -> suggested, exactly like FileProcessing.js does with
#     `pm.output_field?.trim() || pm.input_column_field?.trim() || expectedField`.
#
#  2. Only THREE mapping "kinds" are surfaced anywhere in this script's
#     output (console + email): Direct, AI, Manual. The old per-method
#     labels (Alias, Override, token_f1, sql_like, regex, fuzzy, semantic,
#     token, levenshtein, etc.) are collapsed into those three so the ops
#     team sees the same three states FileProcessing.js shows in the UI.
#
#  3. AI/fuzzy auto-acceptance threshold is 95% (AI_AUTO_ACCEPT_THRESHOLD).
#     It applies ONLY to the 'ai' kind. Exact/direct (100%) and approved
#     manual overrides (method == 'override') are always automatic.
#     Every accept / needs-attention decision comes from decide_mapping();
#     classify_suggestions(), the console table, the log lines and the email
#     all consume that one result.
# ══════════════════════════════════════════════════════════════════════════════

# ── Threshold: the ONLY place the AI/fuzzy auto-accept cutoff is defined ──────
AI_AUTO_ACCEPT_THRESHOLD     = 0.95
AI_AUTO_ACCEPT_THRESHOLD_PCT = 95   # display only; must equal the line above * 100

STATUS_AUTO_ACCEPTED   = "AUTO_ACCEPTED"
STATUS_NEEDS_ATTENTION = "NEEDS_ATTENTION"
STATUS_IGNORED         = "IGNORED"

_FLOAT_EPS = 1e-9


def effective_output_field(s: dict) -> str:
    """Returns the real output/target field for a suggestion, falling back
    to the suggested (expected) field name for pass-through renames."""
    output_field = (s.get("output_field") or "").strip()
    if output_field:
        return output_field
    suggested = (s.get("suggested") or "").strip()
    return suggested


def mapping_kind(s: dict) -> str:
    """Collapses method/source into exactly one of: 'direct', 'ai', 'manual'.

    - 'direct'  -> exact, case-perfect match (source == 'direct', confidence 100%)
    - 'manual'  -> a mapping that was manually confirmed/overridden previously
                   (method == 'override') — i.e. a human already fixed this once
    - 'ai'      -> everything else that has a suggested match (alias lookups,
                   fuzzy/semantic/token matches, etc.)
    """
    method     = (s.get("method") or "").strip().lower()
    source     = (s.get("source") or "").strip().lower()
    confidence = float(s.get("confidence") or 0)

    if source == "direct" and confidence >= 1.0:
        return "direct"
    if method == "override":
        return "manual"
    return "ai"


MAPPING_KIND_LABELS = {
    "direct": "Direct",
    "ai":     "AI",
    "manual": "Manual",
}

# Reason string emitted by suggest-header-mappings when a mandatory output
# field has no candidate column in the uploaded file at all (there's no
# `uploaded` column to point at — the field itself is what's missing).
_MANDATORY_FIELD_RE = re.compile(r'Mandatory field "([^"]+)" has no matching column')

# Reason string emitted when several uploaded columns matched the same
# expected field and the matcher couldn't pick one on its own. These share
# one reason across several columns, so they're grouped into badges under
# that single reason line instead of repeating the full sentence per row.
_DUPLICATE_MATCH_RE = re.compile(r'^Multiple headers matched', re.IGNORECASE)


def _confidence(s: dict) -> float:
    try:
        return float(s.get("confidence") or 0)
    except (TypeError, ValueError):
        return 0.0


def meets_ai_threshold(confidence: float) -> bool:
    """>= 95% passes. The epsilon only absorbs float noise (0.9499999999999),
    it does not let 94.9% through."""
    return confidence + _FLOAT_EPS >= AI_AUTO_ACCEPT_THRESHOLD


def pct_str(confidence: float) -> str:
    """Display percentage that can never disagree with meets_ai_threshold().

    round(0.946 * 100) would print '95%' next to a NEEDS_ATTENTION status.
    Flooring to one decimal prints '94.6%' instead; exact values print as
    '95%' / '100%'."""
    floored = math.floor(confidence * 1000 + 1e-6) / 10
    return f"{floored:g}%"


def decide_mapping(s: dict) -> MappingDecision:
    """SINGLE decision point for one suggestion. classify_suggestions(), the
    console table, the log lines and the email all consume this result."""
    uploaded       = s.get("uploaded") or ""
    source         = (s.get("source") or "").strip().lower()
    is_mandatory   = s.get("is_mandatory") is True
    suggested      = s.get("suggested") or ""
    output_field   = effective_output_field(s)
    confidence     = _confidence(s)
    kind           = mapping_kind(s)
    proposed       = bool(suggested and output_field)
    backend_reason = s.get("reason") or ""

    def make(status, reason="", duplicate=False):
        return MappingDecision(
            uploaded, suggested, output_field, kind, confidence,
            is_mandatory, status, reason, duplicate,
        )

    # Backend explicitly marked this column as intentionally unmapped.
    if source == "ignored":
        return make(STATUS_IGNORED, "column marked ignored")

    # ── Optional fields ───────────────────────────────────────────────────────
    # Unchanged: optional + no proposed AI mapping -> ignored (not a review item).
    # New: an optional field that carries an AI/fuzzy proposed mapping is NEVER
    # auto-accepted, however high the score; Operations decides.
    # Optional direct/override rows keep the old behaviour (ignored).
    if not is_mandatory:
        if proposed and kind == "ai":
            return make(
                STATUS_NEEDS_ATTENTION,
                f"Optional field: AI/fuzzy suggestion '{suggested}' "
                f"({pct_str(confidence)}) is not auto-accepted — Operations must "
                f"confirm it or leave the column unmapped",
            )
        return make(STATUS_IGNORED, "optional field, nothing requiring review")

    # ── Mandatory fields ──────────────────────────────────────────────────────
    if not proposed:
        return make(
            STATUS_NEEDS_ATTENTION,
            backend_reason or "no match found",
            duplicate=bool(_DUPLICATE_MATCH_RE.search(backend_reason)),
        )

    if kind == "direct":
        return make(STATUS_AUTO_ACCEPTED, "exact match")
    if kind == "manual":
        return make(STATUS_AUTO_ACCEPTED, "approved manual/historical override")

    # kind == "ai": the only branch the 95% threshold applies to
    if meets_ai_threshold(confidence):
        return make(STATUS_AUTO_ACCEPTED, "AI confidence at/above auto-acceptance threshold")

    is_dup = bool(_DUPLICATE_MATCH_RE.search(backend_reason))
    # Backend reasons that the email renderer / grouping parse must be kept verbatim.
    if is_dup or _MANDATORY_FIELD_RE.search(backend_reason):
        return make(STATUS_NEEDS_ATTENTION, backend_reason, duplicate=is_dup)
    return make(
        STATUS_NEEDS_ATTENTION,
        f"AI confidence {pct_str(confidence)} below "
        f"{AI_AUTO_ACCEPT_THRESHOLD_PCT}% auto-acceptance threshold "
        f"(suggested → '{suggested}')",
    )


def log_decision(d: MappingDecision) -> None:
    threshold = (
        f"{AI_AUTO_ACCEPT_THRESHOLD_PCT}%" if d.kind == "ai" else "n/a (not AI/fuzzy)"
    )
    get_run_logger().info(
        "\n  Header: %s\n  Suggested mapping: %s\n  Mapping type: %s\n"
        "  Confidence: %s\n  Threshold: %s\n  Status: %s\n  Reason: %s",
        d.uploaded or "(missing field)", d.suggested or "(none)",
        MAPPING_KIND_LABELS[d.kind], pct_str(d.confidence), threshold,
        d.status, d.reason or "-",
    )


def classify_suggestions(
    suggestions: list[dict],
) -> tuple[list[dict], list[dict], list[str], dict[str, list[str]]]:
    """Same signature / return shape as before; now a thin loop over
    decide_mapping() so no other code path decides acceptance on its own."""
    mapped, attention, ignored = [], [], []
    duplicate_groups: dict[str, list[str]] = {}

    for s in suggestions:
        d = decide_mapping(s)
        if d.status == STATUS_AUTO_ACCEPTED:
            mapped.append(s)
        elif d.status == STATUS_IGNORED:
            if d.uploaded:
                ignored.append(d.uploaded)
        elif d.duplicate:
            duplicate_groups.setdefault(d.reason, []).append(d.uploaded or "(missing field)")
        else:
            # copy so the email shows the same reason the log shows
            attention.append({**s, "reason": d.reason})

    return mapped, attention, ignored, duplicate_groups


def file_status_for(attention_items: list, duplicate_groups: dict) -> str:
    return (
        STATUS_NEEDS_ATTENTION
        if attention_items or duplicate_groups
        else STATUS_AUTO_ACCEPTED
    )


def require_auto_accepted(file_status: str) -> None:
    """Defensive guard: called immediately before the automatic rename/upload
    paths so a future refactor can't let a NEEDS_ATTENTION file through."""
    if file_status != STATUS_AUTO_ACCEPTED:
        raise RuntimeError(
            f"Refusing automatic rename/upload: file status is {file_status}"
        )


# ══════════════════════════════════════════════════════════════════════════════
# Step 5 — POST /api/workflow/suggest-header-mappings
# ══════════════════════════════════════════════════════════════════════════════

def call_suggest_header_mappings(
    cfg: Config,
    partner_id: str,
    headers: list[str],
) -> list[dict]:
    base_url = cfg.get('node_service_base_url', fallback='https://test.bomisco.ai/api').rstrip('/')
    url = f"{base_url}/workflow/suggest-header-mappings"

    print(f"\n{'─' * 72}")
    print(f"  suggest-header-mappings")
    print(f"  Partner ID : {partner_id}   Client ID : {cfg.get('client_id', fallback='')}")
    print(f"  Checking {len(headers)} uploaded column(s) against partner_mappings table:")
    for i, h in enumerate(headers, 1):
        print(f"    [{i:>3}] {h}")
    print(f"{'─' * 72}")

    get_run_logger().info("Calling suggest-header-mappings (%d headers) → %s", len(headers), url)
    try:
        resp = _post_json(url, {
            "partnerId":       partner_id,
            "clientId":        cfg.get('client_id', fallback=''),
            "uploadedHeaders": headers,
        }, cfg)

        if resp.get("success"):
            suggestions = resp.get("suggestions", [])

            filtered = [
                s for s in suggestions
                if s.get("is_mandatory") is True and (s.get("uploaded") or "").strip()
            ]

            print(f"\n  Mapped rows with uploaded data ({len(filtered)}):")
            print(f"  {'Uploaded':<20}  {'Suggested':<20}  {'Input Field':<28}  {'Score':<6}  {'Source'}")
            print(f"  {'─'*20}  {'─'*20}  {'─'*28}  {'─'*6}  {'─'*10}")
            for s in filtered:
                uploaded    = s.get("uploaded") or ""
                suggested   = s.get("suggested") or ""
                input_field = s.get("input_field") or ""
                score       = s.get("confidence") or 0
                source      = s.get("source") or ""
                print(f"  {uploaded:<20}  {suggested:<20}  {input_field:<28}  {score:<6}  {source}")
            print()

            get_run_logger().info("  %d suggestions returned (%d mapped w/ uploaded data)", len(suggestions), len(filtered))
            return suggestions
        else:
            get_run_logger().warning(
                "  suggest-header-mappings returned success=false: %s",
                resp.get("message", ""),
            )
            return []
    except Exception as exc:
        get_run_logger().warning("  suggest-header-mappings failed (%s) — treating as no suggestions.", exc)
        return []


# ══════════════════════════════════════════════════════════════════════════════
# Synthesise unmatched suggestions when API returns nothing
# ══════════════════════════════════════════════════════════════════════════════

def synthesise_unmatched(headers: list[str]) -> list[dict]:
    """
    When suggest-header-mappings returns an empty list (no partner_mappings rows
    configured, or all headers are genuinely unknown), build a synthetic list so
    the email and the frontend both have actionable rows to display.

    is_mandatory=True is required: without it classify_suggestions() treats every
    synthetic row as an optional/ignored field, the file lands in the
    "0 columns need attention" automated path, and it would be uploaded
    unchanged to SFTP_OUTPUT_DIR with no review.
    """
    get_run_logger().warning(
        "suggest-header-mappings returned 0 suggestions — "
        "synthesising %d unmatched rows so the ops team can assign them manually.",
        len(headers),
    )
    return [
        {
            "uploaded":     h,
            "suggested":    "",
            "output_field": "",
            "confidence":   0.0,
            "method":       "",
            "source":       "unmatched",
            "is_mandatory": True,
            "reason":       "No mapping found in partner_mappings table — please assign manually",
        }
        for h in headers
    ]


# ══════════════════════════════════════════════════════════════════════════════
# Analyse suggestions
# ══════════════════════════════════════════════════════════════════════════════

def analyse_suggestions(
    suggestions: list[dict],
    actual_headers: list[str],
) -> tuple[bool, list[str]]:
    if not suggestions:
        return False, ["No suggestions returned from the API — all headers are unmatched."]

    sug_map = {s["uploaded"]: s for s in suggestions}
    problems = []

    for h in actual_headers:
        s = sug_map.get(h)
        if s is None:
            problems.append(f"{h}  —  not returned by suggest-header-mappings")
            continue

        suggested  = s.get("suggested") or ""
        confidence = float(s.get("confidence") or 0)

        if not suggested:
            reason = s.get("reason", "no match found")
            problems.append(f"{h}  —  unmatched ({reason})")
        elif mapping_kind(s) == "ai" and not meets_ai_threshold(confidence):
            problems.append(
                f"{h}  —  {pct_str(confidence)} confidence "
                f"(suggested → '{suggested}') — below {AI_AUTO_ACCEPT_THRESHOLD_PCT}% threshold"
            )

    return len(problems) == 0, problems


# ══════════════════════════════════════════════════════════════════════════════
# Check if ALL mapped suggestions are direct matches
# ══════════════════════════════════════════════════════════════════════════════

def all_are_direct(suggestions: list[dict]) -> bool:
    # FIX: use effective_output_field so pass-through columns (suggested set,
    # output_field empty) still count as "mapped" instead of being excluded.
    mapped = [s for s in suggestions if effective_output_field(s)]
    if not mapped:
        return False
    return all(mapping_kind(s) == "direct" for s in mapped)


# ══════════════════════════════════════════════════════════════════════════════
# Fix column — local file edit (no Node API call)
# ══════════════════════════════════════════════════════════════════════════════

def fix_column(
    cfg: Config,
    file_path: str,
    sheet_name: str,
    old_column: str,
    new_column: str,
    skip_rows: int,
) -> bool:
    ext = Path(file_path).suffix.lower()

    try:
        if ext in (".xlsx", ".xls"):
            if not OPENPYXL_AVAILABLE:
                raise RuntimeError("openpyxl not installed. Run: pip install openpyxl")

            wb = openpyxl.load_workbook(file_path)

            sheets_to_process = (
                [sheet_name] if sheet_name and sheet_name in wb.sheetnames
                else wb.sheetnames
            )

            renamed = False
            for sn in sheets_to_process:
                ws = wb[sn]
                rows = list(ws.iter_rows(
                    min_row=skip_rows + 1,
                    max_row=skip_rows + 1,
                    values_only=False,
                ))
                if not rows:
                    continue
                header_row = rows[0]
                for cell in header_row:
                    if cell.value and str(cell.value).strip().lower() == old_column.strip().lower():
                        get_run_logger().info("  Renaming '%s' → '%s' in sheet '%s'", old_column, new_column, sn)
                        cell.value = new_column.strip()
                        renamed = True
                        break
                if renamed:
                    break

            if not renamed:
                get_run_logger().warning("  Column '%s' not found in file.", old_column)
                return False

            wb.save(file_path)
            get_run_logger().info("  ✅ Saved: %s", file_path)
            return True

        elif ext == ".csv":
            with open(file_path, newline="", encoding="utf-8-sig") as fh:
                lines = list(fh)

            if skip_rows >= len(lines):
                get_run_logger().warning("  skip_rows=%d exceeds file length", skip_rows)
                return False

            header_line = lines[skip_rows]
            headers = header_line.rstrip("\n").split(",")
            headers = [h.strip().strip('"') for h in headers]

            old_trimmed = old_column.strip()
            idx = next(
                (i for i, h in enumerate(headers) if h.lower() == old_trimmed.lower()),
                -1,
            )
            if idx == -1:
                get_run_logger().warning("  Column '%s' not found in CSV.", old_column)
                return False

            headers[idx] = new_column.strip()
            lines[skip_rows] = ",".join(headers) + "\n"

            with open(file_path, "w", newline="", encoding="utf-8-sig") as fh:
                fh.writelines(lines)

            get_run_logger().info("  ✅ CSV column renamed: '%s' → '%s'", old_column, new_column)
            return True

        else:
            get_run_logger().warning("  Unsupported file type: %s", ext)
            return False

    except Exception as exc:
        get_run_logger().warning("  ❌ fix_column exception: %s", exc)
        return False


# ══════════════════════════════════════════════════════════════════════════════
# Step 7b — POST /api/workflow/confirm-header-mapping
# ══════════════════════════════════════════════════════════════════════════════

def confirm_header_mapping(
    cfg: Config,
    partner_id: str,
    expected_header: str,
    output_field: str,
) -> None:
    base_url = cfg.get('node_service_base_url', fallback='https://test.bomisco.ai/api').rstrip('/')
    url = f"{base_url}/workflow/confirm-header-mapping"
    try:
        _post_json(url, {
            "clientId":       cfg.get('client_id', fallback=''),
            "partnerId":      partner_id,
            "expectedHeader": expected_header,
            "outputField":    output_field,
        }, cfg)
        get_run_logger().info("  confirm-header-mapping: '%s' → '%s'", expected_header, output_field)
    except Exception as exc:
        get_run_logger().warning("  confirm-header-mapping failed (non-fatal): %s", exc)


# ══════════════════════════════════════════════════════════════════════════════
# Frontend deep-link (for ops email only)
# ══════════════════════════════════════════════════════════════════════════════

def build_frontend_url(
    cfg: Config,
    file_path: str,
    file_name: str,
    partner_id: str,
    session_id: str,
    partner_name: str = "",
    pos_inv_flag: str = "",
    sheet_name: str = "",
    skip_rows: int = 0,
    headers: list = None,
    sheet_selection_required: bool = False,
    configured_sheet_name: str = "",
    available_sheet_names: list = None,
) -> str:
    """
    NOTE: now includes sessionId. The frontend uses this id to (a) check
    whether this review has already been completed on page load, and
    (b) tell the backend when the ops user finishes the review, so this
    Python process can stop polling AI confidence and instead just wait
    on a single, explicit "done" signal.

    CRITICAL FIX: We pass ONLY the filename (not the full path) because
    the file is always in SFTP_INPUT_DIR. The frontend/backend will
    construct the full path using SFTP_INPUT_DIR + filename.

    ★ FIX ★
    We also pass sftpInputDir / sftpOutputDir explicitly in the URL.
    Previously this script did `os.environ["SFTP_INPUT_DIR"] = ...` and
    relied on Node picking that up — but Node is a separate, already-running
    process, so it never saw those changes and silently fell back to
    whatever was in its own environment (a different directory). Passing
    the directories through the URL means the frontend can forward the
    *exact same* directories Python used to every backend call, so there's
    a single source of truth (config.ini on the Python side) instead of
    two independently-configured directories that can drift apart.

    ★ FIX ★ (sheet resolution)
    When the raw file's sheet couldn't be uniquely resolved against what's
    configured in partner_master_v2 (see resolve_sheet_name /
    SheetNotResolvedError), sheet_selection_required=True routes the ops
    user to a sheet picker instead of straight to header mapping. In that
    case sheetSelectionRequired / configuredSheetName / availableSheetNames
    are sent instead of sheetName — selectedSheetName is intentionally
    omitted; its absence alongside sheetSelectionRequired=true is the
    "nothing selected yet" signal, since an empty-string value would imply
    a selected-sheet field exists with a blank value, which isn't the
    actual state.
    """
    # ONLY pass the filename, NOT the full path
    params_dict = {
        "clientId":        cfg.get('client_id', fallback=''),
        "partnerId":       partner_id,
        "partnerName":     partner_name,
        "filePath":        file_name,
        "fileName":        file_name,
        "posInvFlag":      pos_inv_flag,
        "skipRows":        skip_rows,
        "sessionId":       session_id,
        "uploadedHeaders": ",".join(headers) if headers else "",
        "sftpInputDir":    cfg.get('input_files_location', section='sftp', fallback='').rstrip('/'),
        "sftpOutputDir":   cfg.get('output_files_location', section='sftp', fallback='').rstrip('/'),
    }

    if sheet_selection_required:
        params_dict["sheetSelectionRequired"] = "true"
        params_dict["configuredSheetName"] = configured_sheet_name
        params_dict["availableSheetNames"] = ",".join(available_sheet_names or [])
    else:
        params_dict["sheetName"] = sheet_name

    params = urllib.parse.urlencode(params_dict)
    base_url = cfg.get('frontend_base_url', fallback='https://test.bomisco.ai').rstrip('/')
    return f"{base_url}/#/gateway/process-files?{params}"


# ══════════════════════════════════════════════════════════════════════════════
# Ops email
# ══════════════════════════════════════════════════════════════════════════════

def _render_attention_row(uploaded: str, reason: str, input_field: str = "") -> str:
    """
    Renders one row in the "needs attention" table.

    Most reasons (unmatched header, low confidence, duplicate-match "Multiple
    headers matched…") describe a problem with an *uploaded* column, so they
    render as before: the uploaded column name + the reason text.

    The one different case is a missing MANDATORY field — there the problem
    isn't an uploaded column at all (there's nothing in the file to point at),
    it's an expected output field with no candidate. For that case we parse
    the field name out of the reason and show it under Expected Field instead
    of dumping the raw sentence next to a made-up "uploaded" value.
    """
    m = _MANDATORY_FIELD_RE.search(reason or "")
    if m:
        missing_field = m.group(1)
        return (
            f"<tr style='border-bottom:1px solid #ffe0b2;'>"
            f"<td style='padding:8px 14px;font-size:13px;color:#9e9e9e;font-family:monospace;'>—</td>"
            f"<td style='padding:8px 8px;font-size:16px;color:#9e9e9e;text-align:center;'>→</td>"
            f"<td style='padding:8px 14px;font-size:13px;color:#e65100;font-family:monospace;font-weight:600;'>⚠️ {missing_field}</td>"
            f"<td style='padding:8px 14px;font-size:13px;color:#555;font-family:monospace;'>{input_field}</td>"
            f"<td style='padding:8px 14px;font-size:12px;color:#bf360c;' colspan='3'>Mandatory field — no matching column in the uploaded file; please select manually</td>"
            f"</tr>"
        )
    return (
        f"<tr style='border-bottom:1px solid #ffe0b2;'>"
        f"<td style='padding:8px 14px;font-size:13px;color:#e65100;"
        f"     font-family:monospace;font-weight:600;'>⚠️ {uploaded}</td>"
        f"<td style='padding:8px 14px;font-size:12px;color:#bf360c;' colspan='6'>{reason}</td>"
        f"</tr>"
    )




def send_ops_email(
    cfg: Config,
    frontend_url: str,
    file_name: str,
    partner_id: str,
    mapped_items: list[dict],
    attention_items: list[dict],
    ignored_items: list[str],
    duplicate_groups: dict[str, list[str]],
    partner_name: str = "",
    pos_inv_flag: str = "",
    raw_file_sftp_url: str = "",
) -> None:
    if not cfg.get('ops_alert_email', section='pipeline', fallback=''):
        get_run_logger().warning("[pipeline] ops_alert_email not set in config.ini — skipping email.")
        return

    recipients = cfg.get_list('ops_alert_email', section='pipeline')
    smtp_host = cfg.get('smtp_host', section='zoho', fallback='smtp.zoho.com')
    smtp_port = cfg.get_int('smtp_port', section='zoho', fallback=465)
    smtp_user = cfg.get('email', section='zoho', fallback='')
    smtp_password = cfg.get('password', section='zoho', fallback='')
    from_email = smtp_user
    from_name = 'Bomisco Portal'

    flag_label = {"I": "Inventory", "C": "Customer / POS+INV", "P": "POS"}.get(
        pos_inv_flag.upper(), pos_inv_flag or "Unknown"
    )
    subject = (
        f"[Action Required] Header mapping review — "
        f"{file_name} | Partner {partner_id} ({partner_name})"
    )

    # No re-derivation here — mapped_items/attention_items/ignored_items/
    # duplicate_groups were already classified once in main() via
    # classify_suggestions(), so what gets rendered is guaranteed to match
    # what the send/skip decision was based on.
    mapped_rows = []
    for s in mapped_items:
        uploaded     = s.get("uploaded") or ""
        suggested    = s.get("suggested") or ""
        input_field  = s.get("input_field") or ""
        is_mandatory    = s.get("is_mandatory")
        is_mandatory_label = "Yes" if is_mandatory is True else ("No" if is_mandatory is False else "—")
        confidence   = float(s.get("confidence") or 0)
        conf_pct     = round(confidence * 100)
        kind = mapping_kind(s)
        method_badge_color = {"direct": "#1565c0", "manual": "#0277bd", "ai": "#6a1b9a"}[kind]
        method_label = MAPPING_KIND_LABELS[kind]
        mapped_rows.append(
            f"<tr style='border-bottom:1px solid #f0f0f0;'>"
            f"<td style='padding:8px 14px;font-size:13px;color:#333;font-family:monospace;'>{uploaded}</td>"
            f"<td style='padding:8px 8px;font-size:16px;color:#9e9e9e;text-align:center;'>→</td>"
            f"<td style='padding:8px 14px;font-size:13px;color:#2e7d32;font-family:monospace;font-weight:600;'>{suggested}</td>"
            f"<td style='padding:8px 14px;font-size:13px;color:#555;font-family:monospace;'>{input_field}</td>"
            f"<td style='padding:8px 14px;font-size:12px;color:#555;text-align:center;'>{is_mandatory_label}</td>"
            f"<td style='padding:8px 14px;text-align:center;'>"
            f"  <span style='background:{method_badge_color};color:#fff;font-size:11px;"
            f"         font-weight:700;padding:2px 8px;border-radius:10px;'>{method_label}</span>"
            f"</td>"
            f"<td style='padding:8px 14px;font-size:12px;color:#555;text-align:right;'>{conf_pct}%</td>"
            f"</tr>"
        )

    attention_rows = [
        _render_attention_row(
            s.get("uploaded") or "",
            s.get("reason") or "no match found",
            s.get("input_field") or "",
        )
        for s in attention_items
    ]

    ignored_chips = list(ignored_items)
    duplicate_match_groups = duplicate_groups
    duplicate_match_total = sum(len(cols) for cols in duplicate_match_groups.values())

    attention_section = ""
    if attention_rows:
        attention_section = (
            f"<p style='color:#555;font-size:12px;font-weight:700;text-transform:uppercase;"
            f"letter-spacing:0.5px;margin:0 0 6px;'>⚠️ Fields needing attention "
            f"({len(attention_rows)})</p>"
            f"<table style='width:100%;border-collapse:collapse;background:#fff8f0;"
            f"border:1px solid #ffe0b2;border-radius:8px;overflow:hidden;margin-bottom:24px;'>"
            + "".join(attention_rows) +
            f"</table>"
        )

    if mapped_rows:
        mapped_section = (
            f"<p style='color:#555;font-size:12px;font-weight:700;text-transform:uppercase;"
            f"letter-spacing:0.5px;margin:0 0 6px;'>✅ Successfully mapped "
            f"({len(mapped_rows)})</p>"
            f"<table style='width:100%;border-collapse:collapse;background:#f1f8e9;"
            f"border:1px solid #a5d6a7;border-radius:8px;overflow:hidden;margin-bottom:24px;'>"
            f"<tr style='background:#e8f5e9;'>"
            f"  <th style='padding:7px 14px;font-size:11px;color:#2e7d32;text-align:left;"
            f"      font-weight:700;text-transform:uppercase;'>Uploaded Column</th>"
            f"  <th style='padding:7px 8px;'></th>"
            f"  <th style='padding:7px 14px;font-size:11px;color:#2e7d32;text-align:left;"
            f"      font-weight:700;text-transform:uppercase;'>Expected Field</th>"
            f"  <th style='padding:7px 14px;font-size:11px;color:#2e7d32;text-align:left;"
            f"      font-weight:700;text-transform:uppercase;'>Input Field</th>"
            f"  <th style='padding:7px 14px;font-size:11px;color:#2e7d32;text-align:center;"
            f"      font-weight:700;text-transform:uppercase;'>Is Mapped</th>"
            f"  <th style='padding:7px 14px;font-size:11px;color:#2e7d32;text-align:center;"
            f"      font-weight:700;text-transform:uppercase;'>Kind</th>"
            f"  <th style='padding:7px 14px;font-size:11px;color:#2e7d32;text-align:right;"
            f"      font-weight:700;text-transform:uppercase;'>Conf</th>"
            f"</tr>"
            + "".join(mapped_rows) +
            f"</table>"
        )
    else:
        mapped_section = ""

    if ignored_chips:
        chips_html = "".join(
            f"<span style='display:inline-block;margin:3px 4px;padding:4px 12px;"
            f"background:#f5f5f5;border:1px solid #bdbdbd;border-radius:16px;"
            f"font-size:12px;color:#616161;font-family:monospace;'>{c}</span>"
            for c in ignored_chips
        )
        ignored_section = (
            f"<p style='color:#555;font-size:12px;font-weight:700;text-transform:uppercase;"
            f"letter-spacing:0.5px;margin:0 0 8px;'>Ignored fields "
            f"({len(ignored_chips)})</p>"
            f"<div style='padding:12px 14px;background:#fafafa;border:1px solid #e0e0e0;"
            f"border-radius:8px;margin-bottom:24px;line-height:2;'>"
            + chips_html +
            f"</div>"
        )
    else:
        ignored_section = ""

    summary_html = (
        f"<table style='width:100%;border-collapse:collapse;background:#e3f2fd;"
        f"border:1px solid #90caf9;border-radius:8px;margin-bottom:24px;'>"
        f"<tr>"
        f"<td style='padding:12px 18px;text-align:center;'>"
        f"  <div style='font-size:22px;font-weight:800;color:#1565c0;'>{len(mapped_rows)}</div>"
        f"  <div style='font-size:11px;color:#1565c0;font-weight:600;text-transform:uppercase;'>Mapped</div>"
        f"</td>"
        f"<td style='padding:12px 18px;text-align:center;border-left:1px solid #90caf9;'>"
        f"  <div style='font-size:22px;font-weight:800;color:#e65100;'>{len(attention_rows)}</div>"
        f"  <div style='font-size:11px;color:#e65100;font-weight:600;text-transform:uppercase;'>Need Attention</div>"
        f"</td>"
        f"<td style='padding:12px 18px;text-align:center;border-left:1px solid #90caf9;'>"
        f"  <div style='font-size:22px;font-weight:800;color:#757575;'>{len(ignored_chips)}</div>"
        f"  <div style='font-size:11px;color:#757575;font-weight:600;text-transform:uppercase;'>Ignored</div>"
        f"</td>"
        f"</tr></table>"
    )

    html_body = f"""<!DOCTYPE html>
<html><head><meta charset="UTF-8"></head>
<body style="font-family:Arial,sans-serif;background:#f4f6f8;margin:0;padding:0;">
<table width="100%" cellpadding="0" cellspacing="0"
       style="background:#f4f6f8;padding:40px 0;">
<tr><td align="center">
<table width="680" cellpadding="0" cellspacing="0"
       style="background:#fff;border-radius:12px;overflow:hidden;
              box-shadow:0 4px 24px rgba(0,0,0,0.09);">

   <tr><td style="background:linear-gradient(135deg,#1565c0,#283593);
                 padding:34px 40px;">
    <h1 style="color:#fff;margin:0;font-size:22px;">
      ⚠️ Header Mapping Review Required
    </h1>
    <p style="color:#90caf9;margin:8px 0 0;font-size:14px;">
      Bomisco Portal &nbsp;·&nbsp;
      Partner <strong style="color:#fff;">{partner_id}</strong>
      &nbsp;·&nbsp; {partner_name}
      &nbsp;·&nbsp;
      File type: <strong style="color:#fff;">{flag_label}</strong>
    </p>
   </td></tr>

   <tr><td style="padding:32px 40px;">
    <p style="color:#333;font-size:15px;line-height:1.65;margin:0 0 20px;">
      Automated header mapping for <strong>{file_name}</strong> did not reach
      the required confidence on all fields.
      Please review and fix the flagged columns in the portal,
      then re-run the script or notify the ops team.
    </p>

    {summary_html}
    {attention_section}
    {mapped_section}
    {ignored_section}

    <div style="text-align:center;margin:28px 0;">
      <a href="{frontend_url}"
         style="display:inline-block;background:#1565c0;color:#fff;
                padding:14px 44px;border-radius:8px;text-decoration:none;
                font-size:16px;font-weight:700;">
        🔧 Open Header Mapping Screen
      </a>
    </div>

    {f'''<div style="text-align:center;margin:0 0 28px;">
      <a href="{raw_file_sftp_url}"
         style="display:inline-block;background:#546e7a;color:#fff;
                padding:10px 30px;border-radius:8px;text-decoration:none;
                font-size:14px;font-weight:600;">
        📁 View Raw File on SFTP
      </a>
    </div>''' if raw_file_sftp_url else ''}

    <table style="width:100%;border-collapse:collapse;
                  background:#f8f9fa;border-radius:8px;
                  border:1px solid #e9ecef;">
      <tr><td style="padding:18px 22px;">
        <p style="margin:0 0 8px;color:#999;font-size:11px;text-transform:uppercase;">File Details</p>
        <p style="margin:3px 0;font-size:13px;color:#333;">
          <strong>File:</strong> {file_name}</p>
        <p style="margin:3px 0;font-size:13px;color:#333;">
          <strong>Partner ID:</strong> {partner_id}</p>
        <p style="margin:3px 0;font-size:13px;color:#333;">
          <strong>Partner Name:</strong> {partner_name}</p>
        <p style="margin:3px 0;font-size:13px;color:#333;">
          <strong>File Type (transaction_type_identifier):</strong> {flag_label} ({pos_inv_flag})</p>
        <p style="margin:3px 0;font-size:13px;color:#333;">
          <strong>Client ID:</strong> {cfg.get('client_id', fallback='')}</p>
        {f'<p style="margin:3px 0;font-size:13px;color:#333;"><strong>Raw file (SFTP):</strong> <a href="{raw_file_sftp_url}" style="color:#1565c0;word-break:break-all;">{raw_file_sftp_url}</a></p>' if raw_file_sftp_url else ''}
        <p style="margin:3px 0;font-size:13px;color:#333;">
          <strong>Time:</strong>
          {datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')}</p>
      </td></tr>
     </table>
   </td></tr>

   <tr><td style="background:#f4f6f8;padding:18px 40px;text-align:center;">
    <p style="color:#bbb;font-size:12px;margin:0;">
      Bomisco Portal · {datetime.utcnow().strftime('%Y-%m-%d')}
    </p>
   </td></tr>
</table>
</td></tr>
</table>
</body></html>"""

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"]    = f"{from_name} <{from_email}>"
    msg["To"]      = ", ".join(recipients)
    msg.attach(MIMEText(html_body, "html"))

    get_run_logger().info(
        "Sending email → %s via %s:%d",
        recipients, smtp_host, smtp_port,
    )
    try:
        if smtp_port == 465:
            with smtplib.SMTP_SSL(smtp_host, smtp_port) as srv:
                srv.login(smtp_user, smtp_password)
                srv.sendmail(from_email, recipients, msg.as_string())
        else:
            with smtplib.SMTP(smtp_host, smtp_port) as srv:
                srv.starttls()
                srv.login(smtp_user, smtp_password)
                srv.sendmail(from_email, recipients, msg.as_string())
        get_run_logger().info("✅ Email sent to %s", recipients)
    except Exception as exc:
        get_run_logger().error("Email failed: %s", exc)


def send_sheet_selection_email(
    cfg: Config,
    frontend_url: str,
    file_name: str,
    partner_id: str,
    partner_name: str,
    configured_sheet_name: str,
    available_sheets: list[str],
    raw_file_sftp_url: str = "",
) -> None:
    """Notify ops that a file's sheet could not be automatically resolved
    (Case 3): multiple sheets exist and none matches the configured name
    uniquely. Deliberately separate from send_ops_email — there are no
    headers or suggestions at this point, only a sheet to pick."""
    if not cfg.get('ops_alert_email', section='pipeline', fallback=''):
        get_run_logger().warning("[pipeline] ops_alert_email not set in config.ini — skipping email.")
        return

    recipients = cfg.get_list('ops_alert_email', section='pipeline')
    smtp_host = cfg.get('smtp_host', section='zoho', fallback='smtp.zoho.com')
    smtp_port = cfg.get_int('smtp_port', section='zoho', fallback=465)
    smtp_user = cfg.get('email', section='zoho', fallback='')
    smtp_password = cfg.get('password', section='zoho', fallback='')
    from_email = smtp_user
    from_name = 'Bomisco Portal'

    subject = (
        f"[Action Required] Sheet selection needed — "
        f"{file_name} | Partner {partner_id} ({partner_name})"
    )

    sheet_chips = "".join(
        f"<span style='display:inline-block;margin:3px 4px;padding:4px 12px;"
        f"background:#fff8f0;border:1px solid #ffe0b2;border-radius:16px;"
        f"font-size:12px;color:#e65100;font-family:monospace;'>{html.escape(s)}</span>"
        for s in available_sheets
    )

    html_body = f"""<!DOCTYPE html>
<html><head><meta charset="UTF-8"></head>
<body style="font-family:Arial,sans-serif;background:#f4f6f8;margin:0;padding:0;">
<table width="100%" cellpadding="0" cellspacing="0" style="background:#f4f6f8;padding:40px 0;">
<tr><td align="center">
<table width="680" cellpadding="0" cellspacing="0"
       style="background:#fff;border-radius:12px;overflow:hidden;box-shadow:0 4px 24px rgba(0,0,0,0.09);">
   <tr><td style="background:linear-gradient(135deg,#e65100,#bf360c);padding:34px 40px;">
    <h1 style="color:#fff;margin:0;font-size:22px;">⚠️ Sheet Selection Required</h1>
    <p style="color:#ffe0b2;margin:8px 0 0;font-size:14px;">
      Bomisco Portal &nbsp;·&nbsp;
      Partner <strong style="color:#fff;">{html.escape(partner_id)}</strong> &nbsp;·&nbsp; {html.escape(partner_name)}
    </p>
   </td></tr>
   <tr><td style="padding:32px 40px;">
    <p style="color:#333;font-size:15px;line-height:1.65;margin:0 0 20px;">
      <strong>{html.escape(file_name)}</strong> contains multiple sheets and the configured
      sheet name (<code>{html.escape(configured_sheet_name) or "(blank)"}</code>) did not match
      exactly one of them. Header mapping cannot proceed until a sheet is selected.
    </p>
    <p style="margin:0 0 8px;color:#999;font-size:11px;text-transform:uppercase;">
      Available sheets
    </p>
    <div style="margin-bottom:24px;">{sheet_chips}</div>
    <div style="text-align:center;margin:28px 0;">
      <a href="{frontend_url}"
         style="display:inline-block;background:#e65100;color:#fff;
                padding:14px 44px;border-radius:8px;text-decoration:none;
                font-size:16px;font-weight:700;">
        🔧 Select Sheet in Portal
      </a>
    </div>
    {f'''<div style="text-align:center;margin:0 0 28px;">
      <a href="{raw_file_sftp_url}"
         style="display:inline-block;background:#546e7a;color:#fff;
                padding:10px 30px;border-radius:8px;text-decoration:none;
                font-size:14px;font-weight:600;">
        📁 View Raw File on SFTP
      </a>
    </div>''' if raw_file_sftp_url else ''}
   </td></tr>
   <tr><td style="background:#f4f6f8;padding:18px 40px;text-align:center;">
    <p style="color:#bbb;font-size:12px;margin:0;">Bomisco Portal · {datetime.utcnow().strftime('%Y-%m-%d')}</p>
   </td></tr>
</table>
</td></tr>
</table>
</body></html>"""

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"]    = f"{from_name} <{from_email}>"
    msg["To"]      = ", ".join(recipients)
    msg.attach(MIMEText(html_body, "html"))

    get_run_logger().info("Sending sheet-selection email → %s via %s:%d", recipients, smtp_host, smtp_port)
    try:
        if smtp_port == 465:
            with smtplib.SMTP_SSL(smtp_host, smtp_port) as srv:
                srv.login(smtp_user, smtp_password)
                srv.sendmail(from_email, recipients, msg.as_string())
        else:
            with smtplib.SMTP(smtp_host, smtp_port) as srv:
                srv.starttls()
                srv.login(smtp_user, smtp_password)
                srv.sendmail(from_email, recipients, msg.as_string())
        get_run_logger().info("✅ Sheet-selection email sent to %s", recipients)
    except Exception as exc:
        get_run_logger().error("Sheet-selection email failed: %s", exc)


# ══════════════════════════════════════════════════════════════════════════════
# Main
# ══════════════════════════════════════════════════════════════════════════════

def execute(
    partner_id: str,
    file_name: str = "",
    config: str | Path = DEFAULT_CONFIG_PATH,
    skip_rows: int | None = None,
    sheet: str | None = None,
    no_email: bool = False,
    skip_poll: bool = False,
    keep_local_staging: bool = False,
) -> dict | None:
    """Run header mapping for a partner and an optional exact SFTP filename.

    ``skip_poll`` is retained for callers; the pipeline already exits after
    sending a review email and does not poll.
    """
    if not isinstance(partner_id, str) or not partner_id.strip():
        raise ValueError("partner_id must be a non-empty string")
    if not isinstance(file_name, str):
        raise TypeError("file_name must be a string")
    if skip_rows is not None and (isinstance(skip_rows, bool) or not isinstance(skip_rows, int)):
        raise TypeError("skip_rows must be an integer or None")

    partner_id = partner_id.strip()

    # ── 1. Load config ────────────────────────────────────────────────────────
    cfg = Config(config)
    client_id = cfg.get('client_id', fallback='')
    sftp_host = cfg.get('host', section='sftp', fallback='')
    sftp_input_files_dir = cfg.get('input_files_location', section='sftp', fallback='').rstrip('/')
    sftp_mapped_files_dir = cfg.get('mapped_files_location', section='sftp', fallback='').rstrip('/')
    sftp_port = cfg.get_int('port', section='sftp', fallback=22)
    if not client_id:
        get_run_logger().error("client_id not set in [default] of config.ini — aborting.")
        sys.exit(1)
    if not sftp_host or not sftp_input_files_dir or not sftp_mapped_files_dir:
        get_run_logger().error(
            "[sftp] host / input_files_location / mapped_files_location must be set in config.ini "
            "— input and output are read/written via SFTP only."
        )
        sys.exit(1)

    # Unique id for this run's review session
    session_id = str(uuid.uuid4())

    get_run_logger().info("Partner ID       : %s", partner_id)
    get_run_logger().info("Client ID        : %s", client_id)
    get_run_logger().info("Session ID       : %s", session_id)
    get_run_logger().info("SFTP host        : %s:%d", sftp_host, sftp_port)
    get_run_logger().info("SFTP input dir   : %s", sftp_input_files_dir)
    get_run_logger().info("SFTP output dir  : %s", sftp_mapped_files_dir)

    # ── Local staging folder ──────────────────────────────────────────────────
    base_work_dir = os.path.join(tempfile.gettempdir(), 'headermapping_work')
    try:
        os.makedirs(base_work_dir, exist_ok=True)
    except OSError as exc:
        get_run_logger().warning("Could not create local staging directory '%s' (%s) — falling back to OS temp dir.", base_work_dir, exc)
        base_work_dir = tempfile.gettempdir()

    run_local_dir = tempfile.mkdtemp(prefix="run_", dir=base_work_dir)
    get_run_logger().info("Local staging dir: %s", run_local_dir)

    # ── Warm up Firebase token early ──────────────────────────────────────────
    if not cfg.get_bool('firebase_auth_disabled'):
        get_run_logger().info("Obtaining Firebase ID token …")
        try:
            get_firebase_token(cfg)
        except Exception as exc:
            get_run_logger().error("Firebase auth failed: %s", exc)
            sys.exit(1)

    # ── 2. Fetch partner metadata from partner_master_v2 ──────────────────────
    pc = fetch_partner_config(cfg, partner_id)

    # Method overrides take precedence over DB values
    skip_rows  = skip_rows if skip_rows is not None else pc.skip_rows
    sheet_name = sheet if sheet is not None else pc.sheet_name

    get_run_logger().info(
        "Partner → name='%s'  txn_type='%s'  sheet='%s'  header_row_index=%d",
        pc.partner_name, pc.pos_inv_flag, sheet_name, skip_rows,
    )

    # ── 3. Find + download the raw input file via SFTP ─────────────────────────
    try:
        file_name_arg = file_name.strip()
        file_path, file_name, remote_input_path = download_input_file_via_sftp(
            run_local_dir, partner_name=pc.partner_name, target_filename=file_name_arg, config=cfg,
        )
    except (FileNotFoundError, RuntimeError) as exc:
        get_run_logger().error(str(exc))
        sys.exit(1)

    # ── 3b. (Informational only — Node does NOT read these; see note below) ───
    os.environ["SFTP_INPUT_DIR"] = sftp_input_files_dir
    os.environ["SFTP_OUTPUT_DIR"] = sftp_mapped_files_dir
    os.environ["SFTP_HOST"] = sftp_host
    os.environ["SFTP_PORT"] = str(sftp_port)
    os.environ["SFTP_USERNAME"] = cfg.get('username', section='sftp', fallback='')
    os.environ["SFTP_PASSWORD"] = cfg.get('password', section='sftp', fallback='')

    # ── 3c. Build the SFTP link for the ops email ─────────────────────────────
    raw_file_url = build_sftp_link(remote_input_path, config=cfg)

    # ── 3d. Resolve which sheet to use (Excel only; CSV/TSV have no sheets) ───
    ext = Path(file_path).suffix.lower()
    resolved_sheet_name = sheet_name  # unchanged for CSV/TSV
    if ext in (".xlsx", ".xls"):
        try:
            resolved_sheet_name, _available = resolve_sheet_name(file_path, sheet_name)
        except SheetNotResolvedError as exc:
            get_run_logger().warning(
                "Sheet could not be uniquely resolved for '%s' "
                "(configured=%r, available=%r) — routing to sheet-selection review.",
                file_name, sheet_name, exc.available_sheets,
            )

            frontend_url = build_frontend_url(
                cfg, file_path, file_name, partner_id, session_id,
                partner_name=pc.partner_name,
                pos_inv_flag=pc.pos_inv_flag,
                skip_rows=skip_rows,
                sheet_selection_required=True,
                configured_sheet_name=sheet_name,
                available_sheet_names=exc.available_sheets,
            )
            get_run_logger().info("Sheet-selection review URL:\n  %s", frontend_url)

            if not no_email:
                send_sheet_selection_email(
                    cfg, frontend_url, file_name, partner_id,
                    partner_name=pc.partner_name,
                    configured_sheet_name=sheet_name,
                    available_sheets=exc.available_sheets,
                    raw_file_sftp_url=raw_file_url,
                )
            else:
                print(f"\nSheet-selection review URL:\n  {frontend_url}\n")

            get_run_logger().info("✅ Waiting for ops to select a sheet in the portal.")
            if not keep_local_staging:
                shutil.rmtree(run_local_dir, ignore_errors=True)
            return None

    # ── 4. Read headers locally ─────────────────────────────────────────────────
    try:
        headers, used_sheet = read_headers_locally(
            file_path,
            skip_rows=skip_rows,
            target_sheet=resolved_sheet_name,
        )
    except Exception as exc:
        get_run_logger().error("Cannot read file headers: %s", exc)
        sys.exit(1)

    if not headers:
        get_run_logger().error(
            "0 headers found (sheet='%s', skip_rows=%d). "
            "Check partner_master_v2 or pass skip_rows / sheet.",
            used_sheet or sheet_name, skip_rows,
        )
        sys.exit(1)

    get_run_logger().info(
        "Columns found (%d): %s%s",
        len(headers),
        ", ".join(headers[:8]),
        " …" if len(headers) > 8 else "",
    )

    # ── 5. Call suggest-header-mappings ───────────────────────────────────────
    suggestions = call_suggest_header_mappings(cfg, partner_id, headers)

    if not suggestions:
        suggestions = synthesise_unmatched(headers)

    get_run_logger().info("─" * 100)
    get_run_logger().info(
        "%-3s %-30s  %-25s  %-25s  %-9s  %-6s  %s",
        "", "UPLOADED HEADER", "EXPECTED FIELD", "INPUT FIELD", "IS MAPPED", "CONF%", "KIND",
    )
    get_run_logger().info("─" * 100)
    sug_map = {s["uploaded"]: s for s in suggestions if s.get("uploaded")}
    for h in headers:
        s = sug_map.get(h)
        if s:
            suggested   = s.get("suggested") or "(none)"
            input_field = s.get("input_field") or "(none)"
            is_mandatory   = s.get("is_mandatory")
            is_mandatory_label = "Yes" if is_mandatory is True else ("No" if is_mandatory is False else "—")
            kind = MAPPING_KIND_LABELS[mapping_kind(s)] if suggested != "(none)" else "—"
            decision = decide_mapping(s)
            flag = {
                STATUS_AUTO_ACCEPTED:   "✅",
                STATUS_NEEDS_ATTENTION: "⚠️ ",
                STATUS_IGNORED:         "➖",
            }[decision.status]
            get_run_logger().info(
                "%s  %-30s  %-25s  %-25s  %-9s  %-6s  %s",
                flag, h, suggested, input_field, is_mandatory_label,
                pct_str(float(s.get("confidence") or 0)), kind,
            )
        else:
            get_run_logger().info("❌  %-30s  (not in response)", h)

    # FIX: is_mandatory=true expected fields with NO uploaded column at all
    # (method == 'unmatched_mandatory') have `uploaded: null` on the JS side,
    # so they never match anything in `headers` and were silently dropped
    # from this table. Surface them explicitly using expected_header /
    # input_field so every is_mandatory=true field is visible, not just the
    # ones that happened to find a candidate column.
    missing_mandatory = [
        s for s in suggestions
        if not s.get("uploaded") and s.get("method") == "unmatched_mandatory"
    ]
    if missing_mandatory:
        get_run_logger().info("─" * 100)
        get_run_logger().info("Mandatory (is_mandatory=true) fields with NO matching uploaded column:")
        for s in missing_mandatory:
            expected_field = s.get("expected_header") or s.get("suggested") or "(unknown)"
            input_field    = s.get("input_field") or "(none)"
            get_run_logger().info(
                "⚠️   %-30s  %-25s  %-25s  %-9s  %-6d  %s",
                "(none)", expected_field, input_field, "Yes", 0, "—",
            )
    get_run_logger().info("─" * 100)

# ── 6. Classify ────────────────────────────────────────────────────────────
    mapped_items, attention_items, ignored_items, duplicate_groups = classify_suggestions(suggestions)
    needs_attention_count = len(attention_items) + sum(len(v) for v in duplicate_groups.values())
    all_ok = needs_attention_count == 0

    for s in suggestions:
        log_decision(decide_mapping(s))
    file_status = file_status_for(attention_items, duplicate_groups)
    get_run_logger().info("File status: %s", file_status)

    problems = []
    for s in attention_items:
        uploaded = s.get("uploaded") or s.get("expected_header") or "(unknown)"
        problems.append(f"{uploaded}  —  {s.get('reason') or 'no match found'}")
    for reason, cols in duplicate_groups.items():
        for c in cols:
            problems.append(f"{c}  —  {reason}")

    get_run_logger().info(
        "Attention check → mapped=%d  needs_attention=%d  ignored=%d  duplicate_groups=%d",
        len(mapped_items), needs_attention_count, len(ignored_items), len(duplicate_groups),
    )

    fix_errors = []

    # ── FAST PATH: all columns are direct matches ─────────────────────────────
    if all_ok and all_are_direct(suggestions):
        get_run_logger().info("✅ All columns are direct matches — no renaming needed, uploading file as-is to SFTP_OUTPUT_DIR.")

        print(f"\n{'═' * 70}")
        print(f"  ✅  All columns matched directly — skipping email and rename.")
        print(f"  Partner     : {partner_id} — {pc.partner_name}")
        print(f"  Txn type    : {pc.pos_inv_flag}")
        print(f"  Sheet used  : {used_sheet}")
        print(f"  Header row  : {skip_rows + 1} (0-based index: {skip_rows})")
        print(f"  Input       : {remote_input_path}")
        print(f"  Uploading to SFTP_OUTPUT_DIR …")
        print(f"{'═' * 70}")

        require_auto_accepted(file_status)
        mapped_remote_path = save_to_mapped_via_sftp(file_path, file_name, partner_name=pc.partner_name, config=cfg)
        delete_input_file_via_sftp(remote_input_path, config=cfg)

        for s in suggestions:
            suggested    = s.get("suggested") or ""
            output_field = effective_output_field(s)
            if suggested and output_field:
                confirm_header_mapping(cfg, partner_id, suggested, output_field)

        print(f"\n{'=' * 70}")
        print(f"  ✅  Header mapping complete!")
        print(f"  Partner     : {partner_id} — {pc.partner_name}")
        print(f"  Txn type    : {pc.pos_inv_flag}")
        print(f"  Sheet used  : {used_sheet}")
        print(f"  Header row  : {skip_rows + 1} (0-based index: {skip_rows})")
        print(f"  Input       : {remote_input_path} (deleted)")
        print(f"  Mapped      : {mapped_remote_path}")
        print(f"  Renamed cols: 0 / {len(headers)} (all already correct)")
        print(f"  All Headers have Direct Mapping")
        print(f"{'=' * 70}\n")

        if not keep_local_staging:
            shutil.rmtree(run_local_dir, ignore_errors=True)
        return {
            "partner_id": partner_id,
            "partner_name": pc.partner_name,
            "file_name": file_name,
            "mapped_file_path": mapped_remote_path,
            "sheet_name": used_sheet,
            "skip_rows": skip_rows,
            "partner_mappings": mapped_items,
        }

    # ── AUTOMATED PATH: 0 columns need attention — process without email ──────
    if needs_attention_count == 0:
        get_run_logger().info("─" * 72)
        get_run_logger().info("Step 7 — 0 columns need attention. Applying renames automatically, no email.")
        get_run_logger().info("─" * 72)

        for s in mapped_items:
            h            = s.get("uploaded") or ""
            suggested    = s.get("suggested") or ""
            output_field = effective_output_field(s)
            kind         = mapping_kind(s)

            if not h:
                continue

            if kind == "direct" and h.strip() == suggested.strip():
                confirm_header_mapping(cfg, partner_id, suggested, output_field)
                continue

            ok = fix_column(
                cfg, file_path=file_path, sheet_name=used_sheet,
                old_column=h, new_column=suggested, skip_rows=skip_rows,
            )
            if ok:
                confirm_header_mapping(cfg, partner_id, suggested, output_field)
            else:
                fix_errors.append(h)

        require_auto_accepted(file_status)
        mapped_remote_path = save_to_mapped_via_sftp(file_path, file_name, partner_name=pc.partner_name, config=cfg)
        delete_input_file_via_sftp(remote_input_path, config=cfg)

        print(f"\n{'=' * 70}")
        print(f"  ✅  Header mapping complete! (0 columns needed attention — no email sent)")
        print(f"  Partner     : {partner_id} — {pc.partner_name}")
        print(f"  Mapped      : {mapped_remote_path}")
        print(f"  Input       : {remote_input_path} (deleted)")
        print(f"  Renamed cols: {len(mapped_items) - len(fix_errors)} / {len(mapped_items)}")
        if fix_errors:
            print(f"  ⚠️  Failed renames: {', '.join(fix_errors)}")
        print(f"{'=' * 70}\n")

        if not keep_local_staging:
            shutil.rmtree(run_local_dir, ignore_errors=True)
        if fix_errors:
            raise RuntimeError(f"Failed to rename headers: {fix_errors}")
        return {
            "partner_id": partner_id,
            "partner_name": pc.partner_name,
            "file_name": file_name,
            "mapped_file_path": mapped_remote_path,
            "sheet_name": used_sheet,
            "skip_rows": skip_rows,
            "partner_mappings": mapped_items,
        }
    else:
        # ── MANUAL REVIEW PATH (only reached when needs_attention_count > 0) ──────
        get_run_logger().warning("⚠️  %d column(s) need review:", needs_attention_count)
        for p in problems:
            get_run_logger().warning("    • %s", p)

        frontend_url = build_frontend_url(
            cfg, file_path, file_name, partner_id, session_id,
            partner_name=pc.partner_name,
            pos_inv_flag=pc.pos_inv_flag,
            sheet_name=used_sheet,
            skip_rows=skip_rows,
            headers=headers,
        )
        get_run_logger().info("Review URL:\n  %s", frontend_url)

        if not no_email:
            send_ops_email(
                cfg, frontend_url, file_name, partner_id,
                mapped_items=mapped_items,
                attention_items=attention_items,
                ignored_items=ignored_items,
                duplicate_groups=duplicate_groups,
                partner_name=pc.partner_name,
                pos_inv_flag=pc.pos_inv_flag,
                raw_file_sftp_url=raw_file_url,
            )
        else:
            get_run_logger().info(f"\nReview URL:\n  {frontend_url}\n")

        mapped_remote_path = save_to_mapping_review_via_sftp(file_path, file_name, partner_name=pc.partner_name, config=cfg)
        delete_input_file_via_sftp(remote_input_path, config=cfg)

    get_run_logger().info("ℹ️  The portal will:")
    get_run_logger().info("    1. Apply column renames")
    get_run_logger().info("    2. Upload the corrected file to SFTP_OUTPUT_DIR")
    get_run_logger().info("    3. Delete the input file from SFTP_INPUT_DIR")
    get_run_logger().info("ℹ️  This Python script does NOT need to wait or upload anything.")

    if not keep_local_staging:
        shutil.rmtree(run_local_dir, ignore_errors=True)
    # sys.exit(0)

