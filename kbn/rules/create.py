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

"""Create Kibana Detection Engine rules."""

from __future__ import annotations

from logging import Logger
from typing import Mapping, Sequence

from elastic.pipes.core import Pipe
from elastic.pipes.core.errors import ConfigError
from elastic.pipes.kbn.client import get_kbn_client
from typing_extensions import Annotated


@Pipe("elastic.pipes.kbn.rules.create")
def main(
    log: Logger,
    stack: Annotated[
        dict,
        Pipe.State("stack", mutable=True),
        Pipe.Help("Kibana connection configuration"),
    ],
    rules: Annotated[
        Sequence[Mapping],
        Pipe.Config("rules"),
        Pipe.Help("Detection Engine rule definitions to create"),
    ],
):
    """Create Detection Engine rules, failing on the first one Kibana rejects."""
    if not rules:
        raise ConfigError("rules must not be empty")
    client = get_kbn_client(stack)
    try:
        for rule in rules:
            client.create_detection_engine_rule(dict(rule))
            log.info(
                "created Detection Engine rule %s",
                rule.get("rule_id"),
                extra={"event": {"action": "create", "outcome": "success"}, "rule": {"id": rule.get("rule_id")}},
            )
    finally:
        client.close()


if __name__ == "__main__":
    main()
