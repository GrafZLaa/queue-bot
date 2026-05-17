import hashlib
import hmac
import json
import time
from typing import Any
from urllib.parse import parse_qsl


class WebAppAuthError(ValueError):
    """Raised when Telegram WebApp init data cannot be trusted."""


def validate_init_data(
    init_data: str,
    bot_token: str,
    max_age_seconds: int = 86400,
) -> dict[str, Any]:
    """Validate Telegram WebApp init data and return the embedded user payload."""
    if not init_data:
        raise WebAppAuthError("Missing Telegram init data")
    if not bot_token:
        raise WebAppAuthError("BOT_TOKEN is required for WebApp verification")

    values = dict(parse_qsl(init_data, keep_blank_values=True))
    received_hash = values.pop("hash", "")
    if not received_hash:
        raise WebAppAuthError("Missing hash")

    data_check_string = "\n".join(f"{key}={values[key]}" for key in sorted(values))
    secret_key = hmac.new(
        b"WebAppData",
        bot_token.encode("utf-8"),
        hashlib.sha256,
    ).digest()
    calculated_hash = hmac.new(
        secret_key,
        data_check_string.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()

    if not hmac.compare_digest(calculated_hash, received_hash):
        raise WebAppAuthError("Invalid hash")

    auth_date = int(values.get("auth_date", "0") or 0)
    if max_age_seconds > 0 and time.time() - auth_date > max_age_seconds:
        raise WebAppAuthError("Expired init data")

    try:
        return json.loads(values.get("user", "{}"))
    except json.JSONDecodeError as exc:
        raise WebAppAuthError("Invalid user payload") from exc
