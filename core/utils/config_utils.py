"""Typed access to values in a caller-selected INI file."""

import configparser
import os
from pathlib import Path
from typing import Callable


# PIPELINE_CONFIG_PATH may be absolute, or relative to the repo root (e.g. sonos/config.ini).
DEFAULT_CONFIG_PATH = Path(__file__).resolve().parents[2] / (
    os.environ.get("PIPELINE_CONFIG_PATH") or "sonos/config.ini"
)


class Config:
    """Read any INI key from any section, with optional type conversion."""

    def __init__(self, path: str | Path = DEFAULT_CONFIG_PATH):
        self.path = Path(path)
        if not self.path.is_file():
            raise FileNotFoundError(f"Config file not found: {path}")
        self.parser = configparser.ConfigParser(interpolation=None)
        self.parser.read(self.path)

    def get(
        self,
        key: str,
        section: str = "default",
        datatype: type | Callable = str,
        fallback=None,
    ):
        """Return a key as str, int, float, bool, list, or a custom converter."""
        raw = self.parser.get(section, key, fallback=None)
        if raw is None:
            return fallback
        try:
            if datatype is bool:
                return self.parser.BOOLEAN_STATES[raw.strip().lower()]
            if datatype is list:
                return [item.strip() for item in raw.split(",") if item.strip()]
            return datatype(raw)
        except (TypeError, ValueError, KeyError):
            if fallback is not None:
                return fallback
            raise

    def get_int(self, key: str, section: str = "default", fallback: int = 0) -> int:
        return self.get(key, section, int, fallback)

    def get_float(self, key: str, section: str = "default", fallback: float = 0.0) -> float:
        return self.get(key, section, float, fallback)

    def get_bool(self, key: str, section: str = "default", fallback: bool = False) -> bool:
        return self.get(key, section, bool, fallback)

    def get_list(self, key: str, section: str = "default", separator: str = ",") -> list[str]:
        raw = self.get(key, section, fallback="")
        return [item.strip() for item in raw.split(separator) if item.strip()]