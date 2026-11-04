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

from elastic.pipes.kbn.rules.get import selected_rules, validated_filter


class Client:
    def __init__(self, rules):
        self.rules = rules

    def get_detection_engine_rule_by_rule_id(self, rule_id):
        return self.rules[rule_id]


class RulesGetTests(unittest.TestCase):
    def test_explicit_ids_still_exclude_disabled_rules(self):
        rules = selected_rules(
            Mock(),
            Client(
                {
                    "enabled": {"rule_id": "enabled", "enabled": True},
                    "disabled": {"rule_id": "disabled", "enabled": False},
                }
            ),
            ["enabled", "disabled"],
            {"enabled": True},
        )

        self.assertEqual(rules, [{"rule_id": "enabled", "enabled": True}])

    def test_filter_rejects_unknown_options(self):
        with self.assertRaisesRegex(Exception, "unknown rule filter: language"):
            validated_filter({"language": "eql"})
