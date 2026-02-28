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

from logging import Logger

from elastic.pipes.core import Pipe
from elastic.pipes.ec import Context
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


@Pipe()
def main(
    log: Logger,
    ec: Context,
    ctx: Ctx,
):
    """Create an Elastic Cloud API key."""

    body = {
        "description": ctx.description,
    }

    if ctx.expiration:
        body["expiration"] = ctx.expiration

    if ctx.role_assignments:
        body["role_assignments"] = ctx.role_assignments

    log.info(f"creating API key '{ctx.description}'")
    response = ec.client.post("/users/auth/keys", json=body)
    result = response.json()
    log.debug(result)
    response.raise_for_status()
    ctx.key = result


if __name__ == "__main__":
    main()
