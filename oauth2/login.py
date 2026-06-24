#!/usr/bin/env python3

# Copyright 2025 Elasticsearch B.V.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Authenticate to Elasticsearch/Kibana via Okta SSO using the OAuth2 device flow."""

import json
import sys
import time
from datetime import datetime, timedelta, timezone
from logging import Logger
from pathlib import Path

import requests
from bs4 import BeautifulSoup
from elastic.pipes.core import Pipe
from elastic.pipes.core.util import get_node
from typing_extensions import Annotated, Optional

_SAFE_MODE = 0o600
_DEVICE_GRANT = "urn:ietf:params:oauth:grant-type:device_code"
_TOKEN_EXPIRY_BUFFER = 60  # seconds before actual expiry to treat token as stale


def _check_permissions(path: Path, log) -> None:
    mode = path.stat().st_mode & 0o777
    if mode != _SAFE_MODE:
        log.error(f"token cache '{path}' has unsafe permissions {oct(mode)}; " f"expected {oct(_SAFE_MODE)}. Fix with: chmod 600 '{path}'")
        sys.exit(1)


def _load_cache(path: Path, log) -> Optional[dict]:
    if not path.exists():
        return None
    _check_permissions(path, log)
    with path.open() as f:
        return json.load(f)


def _save_cache(path: Path, data: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(data, indent=2))
    tmp.chmod(_SAFE_MODE)
    tmp.rename(path)


def _token_valid(cache: dict) -> bool:
    expires_at = cache.get("expires_at")
    if not expires_at:
        return False
    expiry = datetime.fromisoformat(expires_at)
    if expiry.tzinfo is None:
        expiry = expiry.replace(tzinfo=timezone.utc)
    return datetime.now(timezone.utc) < expiry - timedelta(seconds=_TOKEN_EXPIRY_BUFFER)


def _okta_device_flow(org_domain: str, client_id: str, log) -> str:
    """Run the OAuth2 device authorisation flow and return the Okta access token."""
    device_auth_url = f"https://{org_domain}/oauth2/v1/device/authorize"
    token_url = f"https://{org_domain}/oauth2/v1/token"

    log.info("initiating Okta device authorisation flow")
    resp = requests.post(
        device_auth_url,
        data={
            "client_id": client_id,
            "scope": "openid okta.sessions.manage",
        },
    )
    resp.raise_for_status()
    auth = resp.json()

    device_code = auth["device_code"]
    interval = auth.get("interval", 5)
    expires_in = auth.get("expires_in", 300)
    uri = auth.get("verification_uri_complete") or auth.get("verification_uri", "")

    print(f"\nOpen this URL to authenticate:\n  {uri}\n", file=sys.stderr)

    deadline = time.monotonic() + expires_in
    while time.monotonic() < deadline:
        time.sleep(interval)
        resp = requests.post(
            token_url,
            data={
                "grant_type": _DEVICE_GRANT,
                "device_code": device_code,
                "client_id": client_id,
            },
        )
        data = resp.json()
        if "access_token" in data:
            log.info("Okta authentication successful")
            return data["access_token"]
        error = data.get("error", "")
        if error == "authorization_pending":
            continue
        if error == "slow_down":
            interval += 5
            continue
        log.error(f"device flow error: {data.get('error_description', error)}")
        sys.exit(1)

    log.error("device flow timed out: user did not authenticate in time")
    sys.exit(1)


def _okta_session(org_domain: str, access_token: str, log) -> str:
    """Exchange an Okta access token for a session ID.

    Requires the OIDC client to have the 'okta.sessions.manage' scope.
    """
    log.info("creating Okta session from access token")
    resp = requests.post(
        f"https://{org_domain}/api/v1/sessions",
        json={},
        headers={"Authorization": f"Bearer {access_token}"},
    )
    if not resp.ok:
        log.error(f"failed to create Okta session ({resp.status_code}); " "ensure the OIDC client has the 'okta.sessions.manage' scope")
        sys.exit(1)
    return resp.json()["id"]


def _saml_assertion(org_domain: str, saml_app_id: str, session_id: str, log) -> str:
    """Trigger IdP-initiated SAML SSO and return the base64-encoded SAMLResponse."""
    sso_url = f"https://{org_domain}/app/{saml_app_id}/sso/saml"
    log.info("requesting SAML assertion from Okta")
    resp = requests.get(sso_url, cookies={"sid": session_id}, allow_redirects=True)
    if not resp.ok:
        log.error(f"SAML SSO request failed: {resp.status_code}")
        sys.exit(1)

    soup = BeautifulSoup(resp.text, "html.parser")
    tag = soup.find("input", {"name": "SAMLResponse"})
    if not tag:
        log.error("SAMLResponse not found in Okta response; the session may lack access to this application")
        sys.exit(1)

    return tag["value"]


def _es_saml_authenticate(es_url: str, saml_assertion: str, log) -> dict:
    """Exchange a SAMLResponse for Elasticsearch access + refresh tokens."""
    log.info("authenticating to Elasticsearch via SAML")
    resp = requests.post(
        f"{es_url}/_security/saml/authenticate",
        json={"content": saml_assertion, "ids": []},
    )
    if not resp.ok:
        log.error(f"Elasticsearch SAML authentication failed: {resp.status_code} {resp.text}")
        sys.exit(1)
    return resp.json()


def _es_refresh(es_url: str, refresh_token: str, log) -> Optional[dict]:
    """Attempt to refresh an Elasticsearch token. Returns new token data or None."""
    try:
        resp = requests.post(
            f"{es_url}/_security/oauth2/token",
            json={"grant_type": "refresh_token", "refresh_token": refresh_token},
        )
        if resp.ok:
            return resp.json()
        log.debug(f"ES token refresh rejected: {resp.status_code}")
        return None
    except Exception as exc:
        log.debug(f"ES token refresh error: {exc}")
        return None


@Pipe()
def main(
    log: Logger,
    stack: Annotated[
        dict,
        Pipe.State("stack", mutable=True),
        Pipe.Help("stack state — reads elasticsearch.url, writes credentials.token"),
    ],
    org_domain: Annotated[
        str,
        Pipe.Config("org-domain"),
        Pipe.Help("Okta organisation domain (e.g. elastic.okta.com)"),
    ],
    client_id: Annotated[
        str,
        Pipe.Config("client-id"),
        Pipe.Help("OIDC client ID configured for the OAuth2 device authorisation flow"),
    ],
    saml_app_id: Annotated[
        str,
        Pipe.Config("saml-app-id"),
        Pipe.Help("Okta application ID for Elasticsearch/Kibana SAML SSO"),
        Pipe.Notes("the hex ID found in idp.metadata.path, e.g. exkypx15mqqKrrm611t7"),
    ],
    token_cache: Annotated[
        str,
        Pipe.Config("token-cache"),
        Pipe.Help("path to the token cache file"),
        Pipe.Notes("default: ~/.config/elastic-pipes/tokens/<org-domain>.json"),
    ] = None,
):
    """Authenticate to Elasticsearch/Kibana via Okta SSO (OIDC device flow + SAML)."""

    es_url = get_node(stack, "elasticsearch.url")

    cache_path = (
        Path(token_cache).expanduser()
        if token_cache is not None
        else Path.home() / ".config" / "elastic-pipes" / "tokens" / f"{org_domain}.json"
    )

    # 1. Try cached token
    cache = _load_cache(cache_path, log)
    es_token = None

    if cache and _token_valid(cache):
        log.info("using cached Elasticsearch token")
        es_token = cache["access_token"]

    # 2. Try ES token refresh
    if es_token is None and cache and cache.get("refresh_token"):
        log.info("cached token expired, attempting refresh")
        refreshed = _es_refresh(es_url, cache["refresh_token"], log)
        if refreshed:
            es_token = refreshed["access_token"]
            expires_at = (datetime.now(timezone.utc) + timedelta(seconds=refreshed.get("expires_in", 1200))).isoformat()
            _save_cache(
                cache_path,
                {
                    "access_token": es_token,
                    "refresh_token": refreshed.get("refresh_token", cache.get("refresh_token")),
                    "expires_at": expires_at,
                },
            )

    # 3. Full authentication: device flow → Okta session → SAML → ES token
    if es_token is None:
        okta_token = _okta_device_flow(org_domain, client_id, log)
        session_id = _okta_session(org_domain, okta_token, log)
        assertion = _saml_assertion(org_domain, saml_app_id, session_id, log)
        es_auth = _es_saml_authenticate(es_url, assertion, log)

        es_token = es_auth["access_token"]
        expires_at = (datetime.now(timezone.utc) + timedelta(seconds=es_auth.get("expires_in", 1200))).isoformat()
        _save_cache(
            cache_path,
            {
                "access_token": es_token,
                "refresh_token": es_auth.get("refresh_token"),
                "expires_at": expires_at,
            },
        )

    # 4. Write token into stack credentials for downstream pipes
    stack.setdefault("credentials", {})["token"] = es_token


if __name__ == "__main__":
    main()
