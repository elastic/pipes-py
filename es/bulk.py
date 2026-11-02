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
import time
from collections.abc import Iterable, Mapping
from logging import Logger

from elastic.pipes.core import Pipe
from elastic.pipes.core.errors import ConfigError
from elastic.pipes.es.client import get_es_client
from typing_extensions import Annotated


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


def bulk_create(es, log, batch):
    """Send one bulk-create request and return documents rate limited by Elasticsearch."""
    retry = []
    response = es.bulk(operations=bulk_operations(batch))
    for i, item in enumerate(response["items"]):
        result = item["create"]
        if result["status"] == 201:
            continue
        error = result.get("error", {})
        reason = error.get("reason", "unknown bulk create failure")
        if result["status"] == 429:
            log.warning("bulk create failed for %s: %s (retrying)", result.get("_index", "<unknown>"), reason)
            retry.append(batch[i])
            continue
        log.warning("bulk create failed for %s: %s", result.get("_index", "<unknown>"), reason)
    return retry


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
    batch_size: Annotated[
        int,
        Pipe.Config("batch-size"),
        Pipe.Help("maximum number of documents per Elasticsearch bulk request"),
    ] = 100,
    request_timeout: Annotated[
        int,
        Pipe.Config("request-timeout"),
        Pipe.Help("per-request Elasticsearch bulk timeout in seconds"),
    ] = 120,
    initial_backoff: Annotated[
        float,
        Pipe.Config("initial-backoff"),
        Pipe.Help("seconds to wait before the first rate-limited bulk retry"),
    ] = 1.0,
    maximum_backoff: Annotated[
        float,
        Pipe.Config("maximum-backoff"),
        Pipe.Help("maximum seconds to wait between rate-limited bulk retries"),
    ] = 60.0,
):
    """Create every document in the supplied stream, including data-stream writes."""

    if batch_size < 1:
        raise ConfigError("batch-size must be at least 1")
    if request_timeout < 1:
        raise ConfigError("request-timeout must be at least 1")
    if initial_backoff <= 0:
        raise ConfigError("initial-backoff must be greater than 0")
    if maximum_backoff < initial_backoff:
        raise ConfigError("maximum-backoff must be at least initial-backoff")

    es = get_es_client(stack).options(request_timeout=request_timeout)

    def flush(batch):
        delay = initial_backoff
        while True:
            retry = bulk_create(es, log, batch)
            if not retry:
                return
            log.warning("retrying %d rate-limited bulk creates in %g seconds", len(retry), delay)
            time.sleep(delay)
            delay = min(delay * 2, maximum_backoff)
            batch = retry

    batch = []
    for document in documents:
        batch.append(document)
        if len(batch) >= batch_size:
            flush(batch)
            batch = []
    if batch:
        flush(batch)


if __name__ == "__main__":
    main()
