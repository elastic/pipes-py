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

from collections.abc import Mapping, Sequence
from logging import Logger
from typing import Optional

import requests
from elastic.pipes.core import Pipe
from elastic.pipes.core.errors import ConfigError
from elastic.pipes.kbn.client import get_kbn_client
from typing_extensions import Annotated

KNOWN_FILTERS = {"enabled"}


def validated_filter(rule_filter):
    """Validate the generic rule filter configuration."""
    if rule_filter is None:
        return {}
    if not isinstance(rule_filter, Mapping):
        raise ConfigError("filter must be a mapping")
    unknown = set(rule_filter) - KNOWN_FILTERS
    if unknown:
        raise ConfigError(f"unknown rule filter: {min(unknown)}")
    enabled = rule_filter.get("enabled")
    if enabled is not None and not isinstance(enabled, bool):
        raise ConfigError("filter.enabled must be a boolean")
    return dict(rule_filter)


def selected_rules(log, client, rule_ids, rule_filter):
    """Retrieve requested rules or list the target catalogue, then filter it."""
    enabled = rule_filter.get("enabled")
    if rule_ids is None:
        rules = client.list_detection_engine_rules(enabled=enabled)
    else:
        if isinstance(rule_ids, str) or not isinstance(rule_ids, Sequence) or any(not isinstance(rule_id, str) for rule_id in rule_ids):
            raise ConfigError("rule-ids must be a sequence of strings")
        rules = []
        for rule_id in dict.fromkeys(rule_ids):
            try:
                rules.append(client.get_detection_engine_rule_by_rule_id(rule_id))
            except requests.HTTPError as error:
                if error.response is not None and error.response.status_code == 404:
                    log.warning("configured Detection Engine rule no longer exists")
                    continue
                raise
    if enabled is not None:
        rules = [rule for rule in rules if rule.get("enabled") is enabled]
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
        Mapping,  # noqa: RUF013 - Pipes needs Mapping to validate nested config nodes.
        Pipe.Config("filter"),
        Pipe.Help("optional rule filter; currently supports enabled"),
    ] = None,
):
    """Retrieve Detection Engine rules into ``rules`` state."""
    rule_filter = validated_filter(rule_filter)
    client = get_kbn_client(stack)
    try:
        rules = selected_rules(log, client, rule_ids, rule_filter)
    finally:
        client.close()
    state["rules"] = rules
    log.info("retrieved %d Detection Engine rules", len(rules))
