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

import sys
import time
from logging import Logger

from elastic.pipes.core import Pipe
from elastic.pipes.core.util import batched, get_es_client
from typing_extensions import Annotated


def get_recovering_indices(es):
    indices = []
    res = es.indices.recovery()
    for index, status in res.items():
        for shard in status["shards"]:
            if shard["type"] == "SNAPSHOT" and shard["stage"] != "DONE":
                indices.append(f"{index}: {shard['index']['size']['percent']}")
    return indices


@Pipe()
def main(
    dry_run: bool,
    log: Logger,
    stack: Annotated[
        dict,
        Pipe.State("stack", mutable=True),
        Pipe.Help("state node destination of the stack info"),
    ],
    repository: Annotated[
        str,
        Pipe.Config("repository"),
        Pipe.Help("name of the snapshot repository to restore from"),
    ],
    snapshot: Annotated[
        str,
        Pipe.Config("snapshot"),
        Pipe.Help("name of the snapshot to restore"),
        Pipe.Notes("default: latest successful snapshot"),
    ] = None,
    feature_states: Annotated[
        list,
        Pipe.Config("feature-states"),
        Pipe.Help("list of feature states to restore"),
    ] = None,
    include_aliases: Annotated[
        bool,
        Pipe.Config("include-aliases"),
        Pipe.Help("whether to include aliases in the restore"),
    ] = None,
    include_global_state: Annotated[
        bool,
        Pipe.Config("include-global-state"),
        Pipe.Help("whether to include the global state in the restore"),
    ] = None,
    close_indices: Annotated[
        bool,
        Pipe.Config("close-indices"),
        Pipe.Help("whether to close indices before restoring the snapshot"),
    ] = False,
):
    """Restore a snapshot from a snapshot repository in the given stack."""

    es = get_es_client(stack).options(request_timeout=180)

    log.info("checking if any snapshot is already being restored")
    if indices := get_recovering_indices(es):
        print(
            "indices being restored from snapshot:\n  " + "\n  ".join(indices),
            file=sys.stderr,
        )
        sys.exit(1)

    log.info(f"checking repository: {repository}")
    res = es.snapshot.get_repository(name=repository)
    log.debug(res)

    if snapshot is None:
        log.info("no snapshot specified, getting the latest snapshot")
        res = es.snapshot.get(repository=repository, snapshot="_all")
        snapshots = [s for s in res["snapshots"] if s["state"] == "SUCCESS"]
        if not snapshots:
            log.error(f"no successful snapshots found in repository: {repository}")
            sys.exit(1)
        snapshots.sort(key=lambda s: s["end_time_in_millis"], reverse=True)
        snapshot = snapshots[0]["snapshot"]
        log.info(f"latest snapshot: {snapshot}")

    log.info(f"checking snapshot: {snapshot}")
    res = es.snapshot.get(repository=repository, snapshot=snapshot)
    log.debug(res)

    if dry_run:
        return

    if close_indices:
        log.info("closing indices soon overwritten by the snapshot restore")
        indices = res["snapshots"][0]["indices"]
        for i, batch in enumerate(batched(indices, 20)):
            prefix = "closing indices:\n  " if i == 0 else "  "
            log.info(prefix + "\n  ".join(batch))
            es.indices.close(
                index=",".join(batch),
                ignore_unavailable=True,
            )

    kwargs = {
        "repository": repository,
        "snapshot": snapshot,
        "wait_for_completion": False,
    }
    if feature_states is not None:
        kwargs["feature_states"] = feature_states
    if include_aliases is not None:
        kwargs["include_aliases"] = include_aliases
    if include_global_state is not None:
        kwargs["include_global_state"] = include_global_state

    log.info("restoring snapshot")
    res = es.snapshot.restore(**kwargs)
    log.debug(res)

    log.info("you can kill this application, the restore will remain in progress")
    indices = get_recovering_indices(es)
    while indices:
        print("indices being restored from snapshot:\n  " + "\n  ".join(indices))
        time.sleep(5)
        indices = get_recovering_indices(es)


if __name__ == "__main__":
    main()
