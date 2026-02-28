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
Delete an Elastic Cloud deployment.

https://www.elastic.co/docs/api/doc/cloud/operation/operation-shutdown-deployment
"""

from logging import Logger

from elastic.pipes.core import Pipe
from elastic.pipes.ec import Context
from typing_extensions import Annotated


@Pipe()
def main(
    log: Logger,
    ec: Context,
    deployment_id: Annotated[
        str,
        Pipe.Config("deployment-id"),
        Pipe.Help("identifier of the deployment to delete"),
    ],
):
    """Delete an Elastic Cloud deployment."""

    log.info(f"deleting deployment: {deployment_id}")
    response = ec.client.post(f"/deployments/{deployment_id}/_shutdown")
    result = response.json()
    log.debug(f"result:\n{result}")
    response.raise_for_status()

    log.info(f"deployment deleted: {deployment_id}")


if __name__ == "__main__":
    main()
