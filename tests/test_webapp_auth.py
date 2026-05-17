import hashlib
import hmac
import json
import time
import unittest
from urllib.parse import urlencode

from queue_bot.webapp_auth import WebAppAuthError, validate_init_data


def make_init_data(bot_token: str, payload: dict) -> str:
    values = {
        "auth_date": str(int(time.time())),
        "query_id": "test-query",
        "user": json.dumps(payload, separators=(",", ":")),
    }
    data_check_string = "\n".join(f"{key}={values[key]}" for key in sorted(values))
    secret_key = hmac.new(b"WebAppData", bot_token.encode(), hashlib.sha256).digest()
    values["hash"] = hmac.new(secret_key, data_check_string.encode(), hashlib.sha256).hexdigest()
    return urlencode(values)


class WebAppAuthTestCase(unittest.TestCase):
    def test_validate_init_data_returns_user(self):
        token = "123456:ABCDEF"
        init_data = make_init_data(token, {"id": 42, "first_name": "Ada"})

        user = validate_init_data(init_data, token)

        self.assertEqual(user["id"], 42)
        self.assertEqual(user["first_name"], "Ada")

    def test_validate_init_data_rejects_tampering(self):
        token = "123456:ABCDEF"
        init_data = make_init_data(token, {"id": 42}).replace("42", "43")

        with self.assertRaises(WebAppAuthError):
            validate_init_data(init_data, token)


if __name__ == "__main__":
    unittest.main()
