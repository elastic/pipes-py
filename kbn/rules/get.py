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

"""Retrieve Kibana Detection Engine rules into Pipes state."""

from __future__ import annotations

from collections.abc import Sequence
from logging import Logger
from typing import Optional

import requests
from elastic.pipes.core import Pipe
from elastic.pipes.core.errors import ConfigError
from elastic.pipes.kbn.client import get_kbn_client
from typing_extensions import Annotated


def selected_rules(log, client, rule_ids, rule_filter, allow_missing_rule_ids=True):
    """Retrieve requested rules or list the target catalogue through Kibana."""
    if rule_ids is None:
        return client.list_detection_engine_rules(rule_filter=rule_filter)
    if isinstance(rule_ids, str) or not isinstance(rule_ids, Sequence) or any(not isinstance(rule_id, str) for rule_id in rule_ids):
        raise ConfigError("rule-ids must be a sequence of strings")
    if rule_filter is None:
        rules = []
        for rule_id in dict.fromkeys(rule_ids):
            try:
                rules.append(client.get_detection_engine_rule_by_rule_id(rule_id))
            except requests.HTTPError as error:
                if error.response is not None and error.response.status_code == 404 and allow_missing_rule_ids:
                    log.warning(
                        "configured Detection Engine rule was not found",
                        extra={"event": {"outcome": "failure"}, "rule": {"id": rule_id}},
                    )
                    continue
                raise
        return rules

    rules_by_id = {rule["rule_id"]: rule for rule in client.list_detection_engine_rules(rule_filter=rule_filter)}
    rules = []
    for rule_id in dict.fromkeys(rule_ids):
        try:
            rules.append(rules_by_id[rule_id])
        except KeyError:
            if not allow_missing_rule_ids:
                raise ConfigError("configured Detection Engine rule was not returned by Kibana filter")
            log.warning(
                "configured Detection Engine rule was not returned by Kibana filter",
                extra={"event": {"outcome": "failure"}, "rule": {"id": rule_id}},
            )
    return rules


@Pipe("elastic.pipes.kbn.rules.get")
def main(
    log: Logger,
    state: Annotated[
        dict,
        Pipe.State(None, mutable=True),
        Pipe.Help("shared Pipes state populated with retrieved rules"),
    ],
    stack: Annotated[
        dict,
        Pipe.State("stack", mutable=True),
        Pipe.Help("Kibana connection configuration"),
    ],
    rule_ids: Annotated[
        Optional[Sequence[str]],  # noqa: UP045 - package supports Python 3.8.
        Pipe.Config("rule-ids"),
        Pipe.Help("optional stable Detection Engine rule IDs to retrieve"),
    ] = None,
    rule_filter: Annotated[
        Optional[str],  # noqa: UP045 - package supports Python 3.8.
        Pipe.Config("filter"),
        Pipe.Help("optional Kibana Detection Engine filter expression"),
    ] = None,
    allow_missing_rule_ids: Annotated[
        bool,
        Pipe.Config("allow-missing-rule-ids"),
        Pipe.Help("whether configured rule IDs absent from Kibana results may be ignored"),
    ] = True,
):
    """Retrieve Detection Engine rules into ``rules`` state."""
    client = get_kbn_client(stack)
    try:
        rules = selected_rules(log, client, rule_ids, rule_filter, allow_missing_rule_ids)
    finally:
        client.close()
    state["rules"] = rules
    log.info(
        "retrieved %d Detection Engine rules",
        len(rules),
        extra={"event": {"action": "retrieve", "outcome": "success"}},
    )
