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
    role_assignments: Annotated[
        dict,
        Pipe.Config("role-assignments"),
        Pipe.Help("role assignments (platform, organization, deployment, project)"),
    ] = None
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

    if ctx.role_assignments:
        body["role_assignments"] = ctx.role_assignments

    log.info(f"creating API key '{ctx.description}'")
    response = httpx.post(
        f"{ctx.ec_api_url}/users/auth/keys",
        headers=headers,
        json=body,
    )

    response.raise_for_status()
    ctx.key = response.json()


if __name__ == "__main__":
    main()
