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
        Pipe.Help("API key to delete"),
    ]


@Pipe()
def main(
    log: Logger,
    ec: Context,
    ctx: Ctx,
):
    """Delete an Elastic Cloud API key."""

    body = {
        "keys": [ctx.key["id"]],
    }

    log.info("deleting API key '{id}'".format(**ctx.key))
    response = ec.client.request("DELETE", "/users/auth/keys", json=body)
    log.debug(response.json())
    response.raise_for_status()


if __name__ == "__main__":
    main()
