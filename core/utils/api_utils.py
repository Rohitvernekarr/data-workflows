"""Authenticated JSON API calls shared by partner workflows.

Example::

    mappings = fetch_partner_mappings(cfg, resolved_output["partner_id"])

The returned JSON is preserved, including any response envelope. New endpoint
wrappers can reuse ``call_api`` with query parameters and/or a JSON body.
"""

import json
from collections.abc import Mapping
from typing import Any
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from .config_utils import Config
from .firebase_auth import auth_headers


def call_api(
    cfg: Config,
    endpoint: str,
    *,
    method: str = "GET",
    params: Mapping[str, str | int | float | bool | None] | None = None,
    payload: Any = None,
    headers: Mapping[str, str] | None = None,
    timeout: float = 30,
) -> Any:
    """Call a relative service endpoint and return decoded JSON (None for 204).

    Uses ``node_service_base_url`` and the existing Firebase configuration.
    Query values are URL-encoded; booleans become lowercase and None is omitted.
    HTTP, connection, timeout and JSON decoding errors propagate to the caller.
    """
    if not endpoint or "://" in endpoint or endpoint.startswith("//"):
        raise ValueError("endpoint must be a non-empty relative service path")
    if timeout <= 0:
        raise ValueError("timeout must be positive")

    base_url = cfg.get(
        "node_service_base_url", fallback="https://test.bomisco.ai/api",
    ).rstrip("/")
    url = f"{base_url}/{endpoint.lstrip('/')}"
    query = urlencode({
        key: str(value).lower() if isinstance(value, bool) else value
        for key, value in (params or {}).items()
        if value is not None
    })
    if query:
        url += ("&" if "?" in url else "?") + query

    request_headers = {"Accept": "application/json", **auth_headers(cfg)}
    data = None
    if payload is not None:
        data = json.dumps(payload).encode("utf-8")
        request_headers["Content-Type"] = "application/json"
    request_headers.update(headers or {})
    request = Request(url, data=data, headers=request_headers, method=method.upper())
    with urlopen(request, timeout=timeout) as response:
        if response.status == 204:
            return None
        return json.loads(response.read().decode("utf-8"))


def fetch_partner_mappings(
    cfg: Config,
    partner_id: str,
    *,
    client_id: str | int | None = None,
    key: str = "partnermap",
    partners_only: bool = False,
    show_all: bool = False,
    timeout: float = 30,
) -> Any:
    """Fetch mappings for one partner, preserving the API's response JSON.

    ``partner_id`` can come from a pipeline input/output's ``partner_id`` field.
    The client defaults to ``client_id`` in Config, or 2 when not configured.
    """
    if not isinstance(partner_id, str) or not partner_id.strip():
        raise ValueError("partner_id must be a non-empty string")
    return call_api(
        cfg,
        "partnermapping/partner-mapping",
        params={
            "clientid": cfg.get("client_id", fallback=2) if client_id is None else client_id,
            "key": key,
            "partnerId": partner_id.strip(),
            "partnersOnly": partners_only,
            "showAll": show_all,
        },
        timeout=timeout,
    )
