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

import unittest
from unittest.mock import Mock

import requests
from elastic.pipes.core.errors import ConfigError
from elastic.pipes.kbn.rules.get import selected_rules


class Client:
    def __init__(self, rules, filtered_rules=None):
        self.rules = rules
        self.filtered_rules = filtered_rules
        self.filters = []

    def get_detection_engine_rule_by_rule_id(self, rule_id):
        return self.rules[rule_id]

    def list_detection_engine_rules(self, *, rule_filter):
        self.filters.append(rule_filter)
        return list(self.rules.values()) if self.filtered_rules is None else self.filtered_rules


class MissingRuleClient:
    def get_detection_engine_rule_by_rule_id(self, rule_id):
        error = requests.HTTPError()
        error.response = Mock(status_code=404)
        raise error


class RulesGetTests(unittest.TestCase):
    def test_explicit_ids_intersect_kibana_filtered_rules(self):
        client = Client(
            {
                "enabled": {"rule_id": "enabled", "enabled": True},
                "disabled": {"rule_id": "disabled", "enabled": False},
            },
            filtered_rules=[{"rule_id": "enabled", "enabled": True}],
        )

        rules = selected_rules(
            Mock(),
            client,
            ["enabled", "disabled"],
            "alert.attributes.enabled:true",
        )

        self.assertEqual(rules, [{"rule_id": "enabled", "enabled": True}])
        self.assertEqual(client.filters, ["alert.attributes.enabled:true"])

    def test_filtered_explicit_ids_can_fail_when_not_returned(self):
        client = Client(
            {"enabled": {"rule_id": "enabled", "enabled": True}},
            filtered_rules=[{"rule_id": "enabled", "enabled": True}],
        )

        with self.assertRaisesRegex(ConfigError, "was not returned by Kibana filter"):
            selected_rules(
                Mock(),
                client,
                ["enabled", "missing"],
                "alert.attributes.enabled:true",
                allow_missing_rule_ids=False,
            )

    def test_unfiltered_explicit_ids_use_rule_lookup(self):
        rules = selected_rules(
            Mock(),
            Client({"rule": {"rule_id": "rule"}}),
            ["rule"],
            None,
        )

        self.assertEqual(rules, [{"rule_id": "rule"}])

    def test_missing_explicit_ids_are_allowed_by_default(self):
        log = Mock()

        rules = selected_rules(log, MissingRuleClient(), ["missing"], None)

        self.assertEqual(rules, [])
        log.warning.assert_called_once_with(
            "configured Detection Engine rule was not found",
            extra={"event": {"outcome": "failure"}, "rule": {"id": "missing"}},
        )

    def test_missing_explicit_ids_can_fail_the_pipe(self):
        with self.assertRaises(requests.HTTPError):
            selected_rules(Mock(), MissingRuleClient(), ["missing"], None, allow_missing_rule_ids=False)
