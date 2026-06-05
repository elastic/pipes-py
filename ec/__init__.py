# Copyright 2026 Elasticsearch B.V.
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

"""Elastic Cloud context and HTTP client for pipes."""

from logging import Logger

import httpx
from elastic.pipes.core import Pipe
from typing_extensions import Annotated


def handle_response(response: httpx.Response, log: Logger) -> dict:
    """Handle HTTP response: raise for status and parse JSON, logging errors.

    Args:
        response: The HTTP response to handle
        log: Logger for debug output

    Returns:
        Parsed JSON response body

    Raises:
        httpx.HTTPStatusError: If the response status indicates an error
    """
    try:
        response.raise_for_status()
    except Exception:
        try:
            log.debug(f"error response:\n{response.json()}")
        except Exception:
            pass
        raise
    try:
        result = response.json()
        log.debug(f"result:\n{result}")
        return result
    except Exception:
        return {}


class Context(Pipe.Context):
    """Elastic Cloud API context: auth key and base URL."""

    auth_key: Annotated[
        str,
        Pipe.Config("ec-auth-key"),
        Pipe.Help("Elastic Cloud API key for authentication"),
    ]
    api_url: Annotated[
        str,
        Pipe.Config("ec-api-url"),
        Pipe.Help("Elastic Cloud API base URL"),
    ] = "https://api.elastic-cloud.com/api/v1"

    def __enter__(self):
        self.client = httpx.Client(
            base_url=self.api_url,
            headers={
                "Authorization": f"ApiKey {self.auth_key}",
                "Content-Type": "application/json",
            },
        )
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.client.close()
