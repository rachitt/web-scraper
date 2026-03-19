"""
X/Twitter authentication module.

Handles the multi-step login flow using X's internal onboarding API,
manages cookie caching, and provides authenticated session headers.
"""

import json
import logging
import os
import time
from pathlib import Path

import httpx

logger = logging.getLogger(__name__)

# Public bearer token embedded in X's web app JS bundles
BEARER_TOKEN = (
    "AAAAAAAAAAAAAAAAAAAAANRILgAAAAAAnNwIzUejRCOuH5E6I8xnZz4puTs"
    "%3D1Zv7ttfk8LF81IUq16cHjhLTvJu4FA33AGWWjCpTnA"
)

BASE_URL = "https://api.x.com"
ONBOARDING_URL = f"{BASE_URL}/1.1/onboarding/task.json"
GUEST_ACTIVATE_URL = f"{BASE_URL}/1.1/guest/activate.json"

DEFAULT_COOKIE_PATH = Path("data/x_cookies.json")

USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
)


class XAuthError(Exception):
    """Raised when authentication fails."""


class XAuth:
    """Manages X/Twitter authentication via internal login flow."""

    def __init__(self, cookie_path: str | Path | None = None):
        self.cookie_path = Path(cookie_path) if cookie_path else DEFAULT_COOKIE_PATH
        self.client = httpx.Client(
            headers={
                "authorization": f"Bearer {BEARER_TOKEN}",
                "user-agent": USER_AGENT,
                "accept-language": "en-US,en;q=0.9",
                "content-type": "application/json",
            },
            follow_redirects=True,
            timeout=30.0,
        )
        self._ct0: str | None = None
        self._auth_token: str | None = None
        self._guest_token: str | None = None

    # ── Public API ──────────────────────────────────────────────

    def get_session_headers(self) -> dict[str, str]:
        """Return headers required for authenticated API calls.

        Loads cached cookies if available, otherwise performs full login.
        Re-authenticates if cookies are expired.
        """
        if not self._ct0 or not self._auth_token:
            self._load_cookies()

        if not self._ct0 or not self._auth_token:
            self.login()

        if not self._verify_session():
            logger.info("Session expired, re-authenticating...")
            self.login()

        return {
            "authorization": f"Bearer {BEARER_TOKEN}",
            "x-csrf-token": self._ct0,
            "x-twitter-auth-type": "OAuth2Session",
            "x-twitter-active-user": "yes",
            "user-agent": USER_AGENT,
            "accept-language": "en-US,en;q=0.9",
        }

    def get_cookies(self) -> dict[str, str]:
        """Return cookies dict for authenticated requests."""
        return {"ct0": self._ct0, "auth_token": self._auth_token}

    def login(self) -> None:
        """Perform the full multi-step login flow."""
        username = os.environ.get("X_USERNAME")
        password = os.environ.get("X_PASSWORD")
        if not username or not password:
            raise XAuthError("X_USERNAME and X_PASSWORD environment variables are required")

        logger.info("Starting X login flow for @%s", username)

        # Step 0: get guest token
        self._activate_guest()

        # Step 1: initiate login flow
        flow_token = self._initiate_login()

        # Step 2: JS instrumentation
        flow_token = self._js_instrumentation(flow_token)

        # Step 3: submit username
        flow_token, subtasks = self._submit_username(flow_token, username)

        # Handle alternate identifier challenge (X may ask for email/phone)
        if self._has_subtask(subtasks, "LoginEnterAlternateIdentifierSubtask"):
            logger.info("X is requesting alternate identifier verification")
            alt_id = input("X requires additional verification. Enter your email or phone: ").strip()
            flow_token, subtasks = self._submit_alternate_identifier(flow_token, alt_id)

        # Step 4: submit password
        flow_token, subtasks = self._submit_password(flow_token, password)

        # Handle 2FA if required
        if self._has_subtask(subtasks, "LoginTwoFactorAuthChallenge"):
            code = input("Enter your 2FA code: ").strip()
            flow_token, subtasks = self._submit_2fa(flow_token, code)

        # Handle account duplication check
        if self._has_subtask(subtasks, "AccountDuplicationCheck"):
            flow_token, subtasks = self._account_duplication_check(flow_token)

        # Extract tokens from cookies
        self._extract_tokens()
        self._save_cookies()
        logger.info("Login successful")

    # ── Login flow steps ────────────────────────────────────────

    def _activate_guest(self) -> None:
        resp = self.client.post(GUEST_ACTIVATE_URL)
        resp.raise_for_status()
        self._guest_token = resp.json()["guest_token"]
        self.client.headers["x-guest-token"] = self._guest_token
        logger.debug("Guest token acquired")

    def _initiate_login(self) -> str:
        payload = {
            "input_flow_data": {
                "flow_context": {
                    "debug_overrides": {},
                    "start_location": {"location": "splash_screen"},
                }
            },
            "subtask_versions": {},
        }
        resp = self.client.post(
            ONBOARDING_URL,
            params={"flow_name": "login"},
            json=payload,
        )
        resp.raise_for_status()
        data = resp.json()
        return data["flow_token"]

    def _js_instrumentation(self, flow_token: str) -> str:
        payload = {
            "flow_token": flow_token,
            "subtask_inputs": [
                {
                    "subtask_id": "LoginJsInstrumentationSubtask",
                    "js_instrumentation": {"response": "{}", "link": "next_link"},
                }
            ],
        }
        return self._post_task(payload)

    def _submit_username(self, flow_token: str, username: str) -> tuple[str, list]:
        payload = {
            "flow_token": flow_token,
            "subtask_inputs": [
                {
                    "subtask_id": "LoginEnterUserIdentifierSSO",
                    "settings_list": {
                        "setting_responses": [
                            {
                                "key": "user_identifier",
                                "response_data": {
                                    "text_data": {"result": username}
                                },
                            }
                        ],
                        "link": "next_link",
                    },
                }
            ],
        }
        return self._post_task_with_subtasks(payload)

    def _submit_alternate_identifier(self, flow_token: str, identifier: str) -> tuple[str, list]:
        payload = {
            "flow_token": flow_token,
            "subtask_inputs": [
                {
                    "subtask_id": "LoginEnterAlternateIdentifierSubtask",
                    "enter_text": {"text": identifier, "link": "next_link"},
                }
            ],
        }
        return self._post_task_with_subtasks(payload)

    def _submit_password(self, flow_token: str, password: str) -> tuple[str, list]:
        payload = {
            "flow_token": flow_token,
            "subtask_inputs": [
                {
                    "subtask_id": "LoginEnterPassword",
                    "enter_password": {"password": password, "link": "next_link"},
                }
            ],
        }
        return self._post_task_with_subtasks(payload)

    def _submit_2fa(self, flow_token: str, code: str) -> tuple[str, list]:
        payload = {
            "flow_token": flow_token,
            "subtask_inputs": [
                {
                    "subtask_id": "LoginTwoFactorAuthChallenge",
                    "enter_text": {"text": code, "link": "next_link"},
                }
            ],
        }
        return self._post_task_with_subtasks(payload)

    def _account_duplication_check(self, flow_token: str) -> tuple[str, list]:
        payload = {
            "flow_token": flow_token,
            "subtask_inputs": [
                {
                    "subtask_id": "AccountDuplicationCheck",
                    "check_logged_in_account": {
                        "link": "AccountDuplicationCheck_false"
                    },
                }
            ],
        }
        return self._post_task_with_subtasks(payload)

    # ── HTTP helpers ────────────────────────────────────────────

    def _post_task(self, payload: dict) -> str:
        resp = self.client.post(ONBOARDING_URL, json=payload)
        resp.raise_for_status()
        data = resp.json()
        self._sync_ct0_from_response(resp)
        return data["flow_token"]

    def _post_task_with_subtasks(self, payload: dict) -> tuple[str, list]:
        resp = self.client.post(ONBOARDING_URL, json=payload)
        if resp.status_code == 400:
            data = resp.json()
            errors = data.get("errors", [])
            if errors:
                raise XAuthError(f"Login step failed: {errors[0].get('message', resp.text)}")
            raise XAuthError(f"Login step failed: {resp.text}")
        resp.raise_for_status()
        data = resp.json()
        self._sync_ct0_from_response(resp)
        return data["flow_token"], data.get("subtasks", [])

    def _sync_ct0_from_response(self, resp: httpx.Response) -> None:
        """Keep ct0 in sync if X rotates it during the flow."""
        for cookie in resp.cookies.jar:
            if cookie.name == "ct0":
                self._ct0 = cookie.value
                self.client.headers["x-csrf-token"] = cookie.value

    @staticmethod
    def _has_subtask(subtasks: list, subtask_id: str) -> bool:
        return any(s.get("subtask_id") == subtask_id for s in subtasks)

    # ── Token extraction ────────────────────────────────────────

    def _extract_tokens(self) -> None:
        cookies = {c.name: c.value for c in self.client.cookies.jar}
        self._ct0 = cookies.get("ct0")
        self._auth_token = cookies.get("auth_token")
        if not self._ct0 or not self._auth_token:
            raise XAuthError(
                "Login completed but ct0/auth_token cookies not found. "
                "The account may need additional verification."
            )
        # Remove guest token header now that we're authenticated
        self.client.headers.pop("x-guest-token", None)

    # ── Session verification ────────────────────────────────────

    def _verify_session(self) -> bool:
        """Check if the current session is still valid."""
        try:
            resp = self.client.get(
                f"{BASE_URL}/1.1/account/verify_credentials.json",
                headers={
                    "x-csrf-token": self._ct0,
                    "x-twitter-auth-type": "OAuth2Session",
                    "x-twitter-active-user": "yes",
                },
                cookies={"ct0": self._ct0, "auth_token": self._auth_token},
            )
            return resp.status_code == 200
        except httpx.HTTPError:
            return False

    # ── Cookie persistence ──────────────────────────────────────

    def _save_cookies(self) -> None:
        self.cookie_path.parent.mkdir(parents=True, exist_ok=True)
        data = {
            "ct0": self._ct0,
            "auth_token": self._auth_token,
            "saved_at": time.time(),
        }
        self.cookie_path.write_text(json.dumps(data, indent=2))
        logger.debug("Cookies saved to %s", self.cookie_path)

    def _load_cookies(self) -> None:
        if not self.cookie_path.exists():
            return
        try:
            data = json.loads(self.cookie_path.read_text())
            self._ct0 = data.get("ct0")
            self._auth_token = data.get("auth_token")
            logger.debug("Cookies loaded from %s", self.cookie_path)
        except (json.JSONDecodeError, KeyError):
            logger.warning("Failed to load cached cookies, will re-authenticate")

    def close(self) -> None:
        self.client.close()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()
