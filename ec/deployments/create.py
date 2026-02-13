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

"""
Create an Elastic Cloud deployment.

https://www.elastic.co/docs/api/doc/cloud/operation/operation-create-deployment
"""

from logging import Logger

from elastic.pipes.core import Pipe
from elastic.pipes.ec import Context
from typing_extensions import Annotated


@Pipe()
def main(
    log: Logger,
    ec: Context,
    deployment: Annotated[
        dict,
        Pipe.State("deployment", mutable=True),
        Pipe.Help("state node destination to store the deployment info"),
    ],
    name: Annotated[
        str,
        Pipe.Config("name"),
        Pipe.Help("name of the deployment"),
    ],
    resources: Annotated[
        dict,
        Pipe.Config("resources"),
        Pipe.Help("deployment resources configuration"),
    ],
):
    """Create an Elastic Cloud deployment."""

    body = {
        "name": name,
        "resources": resources,
    }

    log.info(f"creating deployment: {name}")
    log.debug(f"request body:\n{body}")

    response = ec.client.post("/deployments", json=body)
    result = response.json()
    log.debug(f"result:\n{result}")
    response.raise_for_status()

    deployment_id = result.get("id")
    log.info(f"deployment created: {deployment_id}")

    deployment.clear()
    deployment.update(result)


if __name__ == "__main__":
    main()
