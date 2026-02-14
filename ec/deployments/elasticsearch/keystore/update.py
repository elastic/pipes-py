#!/usr/bin/env python3

"""
Add items to the Elasticsearch resource keystore of a deployment.

Uses the Elastic Cloud API PATCH; omitted secrets are preserved by the API.
https://www.elastic.co/docs/api/doc/cloud/operation/operation-set-deployment-es-resource-keystore
"""

from logging import Logger

import httpx
from elastic.pipes.core import Pipe
from typing_extensions import Annotated


@Pipe()
def main(
    log: Logger,
    deployment_id: Annotated[
        str,
        Pipe.Config("deployment-id"),
        Pipe.Help("identifier for the deployment"),
    ],
    secrets: Annotated[
        dict,
        Pipe.Config("secrets"),
        Pipe.Help("keystore secrets to add: map of key names to \"{ 'value': str|object, 'as_file': bool }\""),
    ],
    ref_id: Annotated[
        str,
        Pipe.Config("ref-id"),
        Pipe.Help("Elasticsearch resource identifier (e.g. '_main')"),
    ] = "_main",
    ec_auth_key: Annotated[
        str,
        Pipe.Config("ec-auth-key"),
        Pipe.Help("Elastic Cloud API key for authentication"),
    ] = None,
    ec_api_endpoint: Annotated[
        str,
        Pipe.Config("ec-api-endpoint"),
        Pipe.Help("Elastic Cloud API base URL"),
    ] = "https://api.elastic-cloud.com/api/v1",
):
    """Add items to the Elasticsearch resource keystore (omitted secrets are preserved by the API)."""

    url = f"{ec_api_endpoint}/deployments/{deployment_id}/elasticsearch/{ref_id}/keystore"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"ApiKey {ec_auth_key}",
    }
    body = {"secrets": secrets}
    log.info(f"updating keystore for deployment={deployment_id} ref_id={ref_id}")

    response = httpx.patch(url, headers=headers, json=body)
    response.raise_for_status()
    result = response.json()
    log.debug(result)
    log.info(f"keystore updated for deployment={deployment_id} ref_id={ref_id}")
    return result


if __name__ == "__main__":
    main()
