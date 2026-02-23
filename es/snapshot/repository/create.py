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

import json
from logging import Logger

from elastic.pipes.core import Pipe
from elastic.pipes.core.util import get_es_client
from typing_extensions import Annotated


@Pipe()
def main(
    log: Logger,
    stack: Annotated[
        dict,
        Pipe.State("stack", mutable=True),
        Pipe.Help("stack where the repository is to be created"),
    ],
    repository: Annotated[
        str,
        Pipe.Config("repository"),
        Pipe.Help("name of the snapshot repository to create"),
    ],
    type: Annotated[
        str,
        Pipe.Config("type"),
        Pipe.Help("type of the snapshot repository (e.g. gcs)"),
    ],
    settings: Annotated[
        dict,
        Pipe.Config("settings"),
        Pipe.Help("settings for the snapshot repository"),
    ],
):
    """Create a snapshot repository in the given stack."""

    body = json.dumps(
        {
            "type": type,
            "settings": settings,
        }
    )
    log.debug(f"body: {body}")

    log.info(f"creating repository: {repository}")
    sc = get_es_client(stack).snapshot
    res = sc.create_repository(name=repository, body=body)
    log.debug(res)


if __name__ == "__main__":
    main()
