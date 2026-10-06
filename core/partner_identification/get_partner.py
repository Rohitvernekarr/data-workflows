"""Score workbook signals and resolve the matching partner."""
from __future__ import annotations

import re
from difflib import SequenceMatcher

if __package__ == "core.partner_identification":
    from ..utils.config_utils import Config
    from ..utils.file_utils import FileUtils
    from .partner_models import PartnerConfig, ResolutionResult
else:
    import sys
    from pathlib import Path

    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
    from utils.config_utils import Config
    from utils.file_utils import FileUtils
    from partner_identification.partner_models import PartnerConfig, ResolutionResult

from prefect import get_run_logger

config = Config()
SCORE_SENDER = 10
SCORE_SUBJECT = 40
SCORE_FILENAME = 15
SCORE_SHEET = 5
SCORE_FTI = 10
SCORE_HEADERS = 5


def _fuzzy(a: str, b: str) -> float:
    return SequenceMatcher(None, a.lower().strip(), b.lower().strip()).ratio()


def _norm(s: str) -> str:
    return re.sub(r"[^a-z0-9]", "", (s or "").lower())


def _score_partner(
    partner: PartnerConfig,
    sender: str,
    subject: str,
    filename: str,
    actual_sheet_names: list[str],
    actual_headers: list[str],
) -> tuple[int, list[str]]:
    """
    Returns (composite_score, matched_signal_names).
    Configured signal weights total 85 points.

    Sender matching uses Email_From.
    Subject matching uses Input_SubjectName (falls back to Subject_Contains).
    Filename matching uses Input_FileName (falls back to file_name / File_Name).
    Sheet matching uses InputSheetName (falls back to Sheet_Name).
    FTI matching uses File_Type_Identifier.
    """
    score, matched = 0, []

    # ── 1. Sender email (weight 40) ───────────────────────────────────────────
    # from_email holds the authoritative pipeline sender list.
    # Email_From is the raw operational column — used as fallback inside load_all_partners.
    allowed_senders = [
        e.strip().lower() for e in partner.from_email.split(";") if e.strip()
    ]
    # if(partner.partner_id == '3003'):
    #     get_run_logger().debug("Allowed senders for partner '%s': %s", partner.partner_name, allowed_senders)
    sender_lower = sender.strip().lower()
    # if(partner.partner_id == '3003'):
    #     get_run_logger().debug("Incoming sender email: %s", sender_lower)

    sender_matched = False
    if sender_lower in allowed_senders:
        # if(partner.partner_id == '3003'):
        #     get_run_logger().debug("Exact sender match for partner '%s': %s", partner.partner_name, sender_lower)
        score += SCORE_SENDER
        matched.append("sender_email")
        sender_matched = True
    else:
        # if(partner.partner_id == '3003'):
        #     get_run_logger().debug("No exact sender match for partner '%s'. Checking domain... Sender: %s", partner.partner_name, sender_lower)
        sender_domain = sender_lower.split("@")[-1] if "@" in sender_lower else ""
        if sender_domain and any(sender_domain in s for s in allowed_senders):
            score += SCORE_SENDER // 2
            matched.append("sender_domain")
            sender_matched = True

    if sender_lower and not sender_matched:
        # A supplied sender must match; SFTP input has no sender to compare.
        return score, matched

    # ── 2. Subject match (weight 25) ─────────────────────────────────────────
    # subject_contains is resolved from Input_SubjectName in load_all_partners.
    kw = partner.subject_contains.strip()
    if kw and subject.strip():
        if _norm(kw) == _norm(subject):
            score += SCORE_SUBJECT
            matched.append("subject_exact")
        elif _norm(kw) in _norm(subject) or _norm(subject) in _norm(kw):
            score += int(SCORE_SUBJECT * 0.8)
            matched.append("subject_contains")
        elif _fuzzy(subject, kw) >= 0.75:
            score += int(SCORE_SUBJECT * 0.5)
            matched.append("subject_fuzzy")

    # ── 3. Filename pattern (weight 15) ──────────────────────────────────────
    # file_name is resolved from Input_FileName in load_all_partners.
    fn_pat = partner.file_name.strip()
    if fn_pat:
        matched_fn = False

        if fn_pat.lower() in filename.lower():
            score += SCORE_FILENAME
            matched.append("filename_substring")
            matched_fn = True

        if not matched_fn:
            try:
                if re.search(fn_pat, filename, re.IGNORECASE):
                    score += SCORE_FILENAME
                    matched.append("filename_regex")
                    matched_fn = True
            except re.error:
                pass

        if not matched_fn:
            pat_words = [w for w in re.split(r"\s+", fn_pat.lower()) if len(w) > 2]
            file_lower = filename.lower()
            if pat_words:
                hits = sum(1 for w in pat_words if w in file_lower)
                ratio = hits / len(pat_words)
                if ratio >= 0.5:
                    score += int(SCORE_FILENAME * ratio)
                    matched.append(f"filename_words({hits}/{len(pat_words)})")
                    matched_fn = True

        if not matched_fn:
            sim = _fuzzy(fn_pat, filename)
            if sim >= 0.4:
                score += int(SCORE_FILENAME * sim)
                matched.append(f"filename_fuzzy({sim:.0%})")

    # ── 4. Sheet name (weight 5) ──────────────────────────────────────────────
    # sheet_name is resolved from InputSheetName in load_all_partners.
    expected_sheet = partner.sheet_name.strip()
    if expected_sheet and actual_sheet_names:
        if any(_norm(expected_sheet) == _norm(s) for s in actual_sheet_names):
            score += SCORE_SHEET
            matched.append("sheet_exact")
        elif any(
            _norm(expected_sheet) in _norm(s) or _norm(s) in _norm(expected_sheet)
            for s in actual_sheet_names
        ):
            score += int(SCORE_SHEET * 0.6)
            matched.append("sheet_contains")

    # ── 5. File_Type_Identifier column presence (weight 10) ──────────────────
    fti = partner.file_type_identifier.strip()
    if fti and actual_headers:
        if any(_norm(fti) == _norm(h) for h in actual_headers):
            score += SCORE_FTI
            matched.append(f"fti_exact:'{fti}'")
        elif any(_fuzzy(fti, h) >= config.get("fuzzy_header_cutoff", section="pipeline", datatype=float) for h in actual_headers):
            score += int(SCORE_FTI * 0.6)
            matched.append(f"fti_fuzzy:'{fti}'")
        else:
            get_run_logger().debug(
                "FTI column '%s' NOT found in headers %s for partner '%s'",
                fti,
                actual_headers,
                partner.partner_name,
            )

    # ── 6. General column headers (weight 5) ─────────────────────────────────
    if partner.input_columns and actual_headers:
        hits = sum(
            1
            for ah in actual_headers
            if any(_fuzzy(ah, ec) >= config.get("fuzzy_header_cutoff", section="pipeline", datatype=float) for ec in partner.input_columns)
        )
        ratio = hits / max(len(partner.input_columns), 1)
        if ratio >= 0.4:
            score += int(SCORE_HEADERS * ratio)
            matched.append(f"headers({hits}/{len(partner.input_columns)})")

    return score, matched


# ── Resolution ────────────────────────────────────────────────────────────────


def resolve_partner(
    sender: str,
    subject: str,
    file_reference: str,
    file_bytes: bytes,
    partners: list[PartnerConfig],
) -> tuple[list[ResolutionResult], list[str], list[str]]:
    """
    Score all partners and return the best match above the threshold.

    Returns (ResolutionResult, sheet_names, headers_at_matched_skip_rows).
    Headers are read per-partner using their own skip_rows + sheet_name so
    File_Type_Identifier is always checked at the correct row.
    """
    partner_results: list[dict] = []
    best_score, best_partner, best_matched = 0, None, []
    best_headers: list[str] = []

    # Sheet names are the same regardless of partner — extract once.
    all_sheet_names = FileUtils.sheet_names(file_bytes)
    for p in partners:
        get_run_logger().debug(
            "Partner '%s' — scoring against sender='%s', subject='%s', file='%s', sheet='%s', transaction_type='%s'",
            p.partner_name,
            sender,
            subject,
            file_reference,
            p.sheet_name,
            p.transaction_type_identifier,
        )
        _, headers = FileUtils.extract_file_info(
            file_bytes, skip_rows=p.pre_header_rows, target_sheet=p.sheet_name
        )
        s, m = _score_partner(
            p, sender, subject, file_reference, all_sheet_names, headers
        )
        get_run_logger().debug("Partner '%s'  score=%d  signals=%s", p.partner_name, s, m)
        partner_results.append(
            {"partner": p, "score": s, "signals": m, "headers": headers}
        )

        if s > best_score:
            best_score, best_partner, best_matched = s, p, m
            best_headers = headers
            # get_run_logger().debug("New best match: partner '%s' with score %d and signals %s", best_partner.partner_name, best_score, best_matched)

    get_run_logger().debug("PartnerResults")
    for r in partner_results:
        get_run_logger().debug(
            "%s",
            [
                f"Partner '{r['partner'].partner_name}': score={r['score']}, signals={r['signals']}, transaction_type={r['partner'].transaction_type_identifier} '"
            ],
        )

    # If no partner meets the threshold, return a single ResolutionResult with partner=None.
    if best_score < config.get("match_threshold", section="pipeline", datatype=float) or best_partner is None:
        get_run_logger().debug(
            "No partner matched. Best score=%d/~80 (threshold=%d). Signals=%s",
            best_score,
            config.get("match_threshold", section="pipeline", datatype=float),
            best_matched,
        )
        return (
            [
                ResolutionResult(
                    partner=None, score=best_score, matched_signals=best_matched
                )
            ],
            all_sheet_names,
            best_headers,
        )

    get_run_logger().debug("Best match: partner '%s' with score %d and signals %s",
        best_partner.partner_name, best_score, best_matched)
    grouped = [
        r
        for r in partner_results
        if r["score"] >= config.get("match_threshold", section="pipeline", datatype=float)
        and r["score"] == best_score
        # and r["partner"].file_name == best_partner.file_name
        # and r["partner"].sheet_name == best_partner.sheet_name
        # and r["partner"].transaction_type_identifier in ("P", "I", "C")
    ]

    get_run_logger().debug(
        "Groupings:"
    )

    for g in grouped:
            get_run_logger().debug(
                "Partner '%s': score=%f, signals=%s, transaction_type=%s",
                g["partner"].partner_name,
                g["score"],
                g["signals"],
                g["partner"].transaction_type_identifier,
            )
    results = [
            ResolutionResult(
                partner=r["partner"], score=r["score"], matched_signals=r["signals"]
            )
            for r in grouped
        ]
    return results, all_sheet_names, best_headers
