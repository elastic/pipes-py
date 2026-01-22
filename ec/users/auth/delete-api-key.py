#!/usr/bin/env python3

from logging import Logger

import httpx
from elastic.pipes.core import Pipe
from typing_extensions import Annotated


class Ctx(Pipe.Context):
    key: Annotated[
        dict,
        Pipe.State("key", mutable=True),
        Pipe.Help("API key to delete"),
    ]
    ec_auth_key: Annotated[
        str,
        Pipe.Config("ec-auth-key"),
        Pipe.Help("Elastic Cloud API key for authentication"),
    ]
    ec_api_url: Annotated[
        str,
        Pipe.Config("ec-api-url"),
        Pipe.Help("Elastic Cloud API URL"),
    ] = "https://api.elastic-cloud.com/api/v1"


@Pipe()
def main(
    log: Logger,
    ctx: Ctx,
):
    """Delete an Elastic Cloud API key."""

    headers = {
        "Content-Type": "application/json",
        "Authorization": f"ApiKey {ctx.ec_auth_key}",
    }
    body = {
        "keys": [ctx.key["id"]],
    }

    log.info("deleting API key '{id}'".format(**ctx.key))
    with httpx.Client() as client:
        response = client.request(
            "DELETE",
            f"{ctx.ec_api_url}/users/auth/keys",
            headers=headers,
            json=body,
        )

    response.raise_for_status()


if __name__ == "__main__":
    main()
