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
Get the Elasticsearch resource keystore.

https://www.elastic.co/docs/api/doc/cloud/operation/operation-get-deployment-es-resource-keystore
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
        Pipe.Help("identifier of the deployment"),
    ],
    keystore: Annotated[
        dict,
        Pipe.State("keystore", mutable=True),
        Pipe.Help("state node destination to store the keystore contents"),
    ],
    ref_id: Annotated[
        str,
        Pipe.Config("ref-id"),
        Pipe.Help("Elasticsearch resource identifier"),
    ] = "_main",
):
    """Get the Elasticsearch resource keystore."""

    log.info(f"getting keystore for deployment {deployment_id}, ref_id {ref_id}")
    response = ec.client.get(f"/deployments/{deployment_id}/elasticsearch/{ref_id}/keystore")
    result = response.json()
    log.debug(result)
    response.raise_for_status()

    keystore.clear()
    keystore.update(result)

    log.info(f"keystore retrieved for deployment {deployment_id}, ref_id {ref_id}")


if __name__ == "__main__":
    main()
