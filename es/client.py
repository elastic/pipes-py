# Copyright 2025 Elasticsearch B.V.
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

"""Elasticsearch client factory."""

from elastic.pipes.core.util import get_node


def get_es_client(stack):
    """Create Elasticsearch client from configuration.

    Args:
        stack: Configuration dictionary containing elasticsearch.url and
            credentials (api-key or username/password).

    Returns:
        Configured Elasticsearch client instance.
    """
    from elasticsearch import Elasticsearch

    shell_expand = get_node(stack, "shell-expand", False)
    api_key = get_node(stack, "credentials.api-key", None, shell_expand=shell_expand)
    username = get_node(stack, "credentials.username", None, shell_expand=shell_expand)
    password = get_node(stack, "credentials.password", None, shell_expand=shell_expand)

    args = {
        "hosts": get_node(stack, "elasticsearch.url", shell_expand=shell_expand),
    }
    if api_key:
        args["api_key"] = api_key
    elif username:
        args["basic_auth"] = (username, password)
    return Elasticsearch(**args)
