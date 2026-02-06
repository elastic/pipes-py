#!/usr/bin/env python3

from logging import Logger

import httpx
from elastic.pipes.core import Pipe
from typing_extensions import Annotated


class Ctx(Pipe.Context):
    key: Annotated[
        dict,
        Pipe.State("key", mutable=True),
        Pipe.Help("state node destination for the created API key"),
    ]
    description: Annotated[
        str,
        Pipe.Config("description"),
        Pipe.Help("description of the API key"),
    ]
    expiration: Annotated[
        str,
        Pipe.Config("expiration"),
        Pipe.Help("expiration provided as duration (ex. '1d', '3h')"),
    ] = None
    ec_auth_key: Annotated[
        str,
        Pipe.Config("ec-auth-key"),
        Pipe.Help("Elastic Cloud API key for authentication"),
    ]
    ec_api_endpoint: Annotated[
        str,
        Pipe.Config("ec-api-endpoint"),
        Pipe.Help("Elastic Cloud API endpoint"),
    ] = "https://api.elastic-cloud.com/api/v1"


@Pipe()
def main(
    log: Logger,
    ctx: Ctx,
):
    """Create an Elastic Cloud API key."""

    headers = {
        "Content-Type": "application/json",
        "Authorization": f"ApiKey {ctx.ec_auth_key}",
    }
    body = {
        "description": ctx.description,
    }

    if ctx.expiration:
        body["expiration"] = ctx.expiration

    log.info(f"creating API key '{ctx.description}'")
    response = httpx.post(
        f"{ctx.ec_api_endpoint}/users/auth/keys",
        headers=headers,
        json=body,
    )

    response.raise_for_status()
    ctx.key = response.json()


if __name__ == "__main__":
    main()
