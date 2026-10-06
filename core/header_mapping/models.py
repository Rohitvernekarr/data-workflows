"""Small data and error types used by the header mapping pipeline."""

from dataclasses import dataclass

if __package__:
    from ..utils.partner_config_utils import PartnerConfig
else:
    from utils.partner_config_utils import PartnerConfig


class SheetNotResolvedError(Exception):
    """Raised when a workbook's sheet cannot be automatically resolved."""

    def __init__(self, available_sheets, configured_sheet_name):
        self.available_sheets = available_sheets
        self.configured_sheet_name = configured_sheet_name
        super().__init__(
            f"Could not uniquely resolve a sheet (configured: {configured_sheet_name!r}, "
            f"available: {available_sheets!r})"
        )


@dataclass(frozen=True)
class MappingDecision:
    uploaded:     str
    suggested:    str
    output_field: str
    kind:         str      # 'direct' | 'manual' | 'ai'
    confidence:   float
    is_mandatory: bool
    status:       str      # STATUS_*
    reason:       str
    duplicate:    bool = False


