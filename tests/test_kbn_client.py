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

from elastic.pipes.kbn.client import Kibana


class Response:
    def __init__(self, payload):
        self.payload = payload

    def raise_for_status(self):
        pass

    def json(self):
        return self.payload


class Session:
    def __init__(self, responses):
        self.responses = iter(responses)
        self.requests = []

    def get(self, url, params):
        self.requests.append((url, params))
        return Response(next(self.responses))


class ListDetectionEngineRulesTests(unittest.TestCase):
    def test_paginates_stably_and_deduplicates_rules(self):
        client = Kibana(url="https://kibana.example")
        session = Session(
            [
                {
                    "data": [{"id": "saved-a", "rule_id": "a"}, {"id": "saved-b", "rule_id": "b", "version": 1}],
                    "perPage": 2,
                    "total": 3,
                },
                {
                    "data": [{"id": "saved-b", "rule_id": "b", "version": 2}, {"id": "saved-c", "rule_id": "c"}],
                    "perPage": 2,
                    "total": 3,
                },
            ]
        )
        client.session = session

        rules = client.list_detection_engine_rules(enabled=True, per_page=2)

        self.assertEqual([rule["rule_id"] for rule in rules], ["a", "b", "c"])
        self.assertEqual(rules[1]["version"], 2)
        self.assertEqual(
            session.requests,
            [
                (
                    "https://kibana.example/api/detection_engine/rules/_find",
                    {
                        "page": 1,
                        "per_page": 2,
                        "sort_field": "created_at",
                        "sort_order": "asc",
                        "filter": "alert.attributes.enabled:true",
                    },
                ),
                (
                    "https://kibana.example/api/detection_engine/rules/_find",
                    {
                        "page": 2,
                        "per_page": 2,
                        "sort_field": "created_at",
                        "sort_order": "asc",
                        "filter": "alert.attributes.enabled:true",
                    },
                ),
            ],
        )
