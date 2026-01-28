#!/usr/bin/env python3

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
