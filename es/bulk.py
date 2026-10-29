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

"""Bulk-create documents in Elasticsearch."""

import json
import os
import time
from collections.abc import Iterable, Mapping
from logging import Logger
from pathlib import Path
from typing import Optional

from elastic.pipes.core import Pipe
from elastic.pipes.core.errors import ConfigError
from elastic.pipes.es.client import get_es_client
from typing_extensions import Annotated


def write_stats(stats_file, stats):
    """Atomically publish bulk counters for an external health reporter."""
    if not stats_file:
        return
    path = Path(stats_file)
    temporary = path.with_suffix(".tmp")
    temporary.write_text(json.dumps(stats))
    os.replace(temporary, path)


def bulk_operations(documents):
    """Serialize pipe documents as Elasticsearch bulk create operations."""
    operations = []
    for document in documents:
        if not isinstance(document, Mapping):
            raise ConfigError("bulk document must be a mapping")
        try:
            index = document["_index"]
            source = document["_source"]
        except KeyError as error:
            raise ConfigError(f"bulk document is missing {error.args[0]!r}") from error
        if not isinstance(index, str) or not index:
            raise ConfigError("bulk document _index must be a non-empty string")
        if not isinstance(source, Mapping):
            raise ConfigError("bulk document _source must be a mapping")
        operations.append(json.dumps({"create": {"_index": index}}))
        operations.append(json.dumps(source))
    return "\n".join(operations)


@Pipe("elastic.pipes.es.bulk")
def main(
    log: Logger,
    stack: Annotated[
        dict,
        Pipe.State("stack", mutable=True),
        Pipe.Help("destination Elasticsearch stack configuration"),
    ],
    documents: Annotated[
        Iterable,
        Pipe.State("documents", mutable=True),
        Pipe.Help("documents shaped as {_index: destination, _source: document}"),
    ],
    stats: Annotated[
        dict,
        Pipe.State("bulk-stats", mutable=True),
        Pipe.Help("mutable bulk-ingestion counters"),
    ],
    batch_size: Annotated[
        int,
        Pipe.Config("batch-size"),
        Pipe.Help("maximum number of documents per Elasticsearch bulk request"),
    ] = 100,
    flush_interval: Annotated[
        float,
        Pipe.Config("flush-interval"),
        Pipe.Help("maximum seconds to hold a partial bulk request"),
    ] = 1.0,
    request_timeout: Annotated[
        int,
        Pipe.Config("request-timeout"),
        Pipe.Help("per-request Elasticsearch bulk timeout in seconds"),
    ] = 120,
    stats_file: Annotated[
        Optional[str],
        Pipe.Config("stats-file"),
        Pipe.Help("optional JSON file updated after each bulk request"),
    ] = None,
):
    """Create every document in the supplied stream, including data-stream writes."""

    if batch_size < 1:
        raise ConfigError("batch-size must be at least 1")
    if flush_interval <= 0:
        raise ConfigError("flush-interval must be greater than 0")
    if request_timeout < 1:
        raise ConfigError("request-timeout must be at least 1")

    stats.update({"batches": 0, "created": 0, "failed": 0})
    write_stats(stats_file, stats)
    es = get_es_client(stack).options(request_timeout=request_timeout)

    def flush(batch):
        response = es.bulk(operations=bulk_operations(batch))
        stats["batches"] += 1
        for item in response["items"]:
            result = item["create"]
            if result["status"] == 201:
                stats["created"] += 1
                continue
            stats["failed"] += 1
            error = result.get("error", {})
            reason = error.get("reason", "unknown bulk create failure")
            log.warning("bulk create failed for %s: %s", result.get("_index", "<unknown>"), reason)
        write_stats(stats_file, stats)

    batch = []
    started_at = time.monotonic()
    for document in documents:
        batch.append(document)
        if len(batch) >= batch_size or time.monotonic() - started_at >= flush_interval:
            flush(batch)
            batch = []
            started_at = time.monotonic()
    if batch:
        flush(batch)


if __name__ == "__main__":
    main()
