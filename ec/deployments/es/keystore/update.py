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
Add, update or remove items from the Elasticsearch resource keystore.

https://www.elastic.co/docs/api/doc/cloud/operation/operation-set-deployment-es-resource-keystore
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
    secrets: Annotated[
        dict,
        Pipe.Config("secrets"),
        Pipe.Help("map of the secrets: \"{ 'value': str|object, 'as_file': bool }\""),
        Pipe.Notes("secrets with a null value are removed from the keystore, those unspecified are preserved"),
    ],
    ref_id: Annotated[
        str,
        Pipe.Config("ref-id"),
        Pipe.Help("Elasticsearch resource identifier"),
    ] = "_main",
):
    """Add, update or remove items from the Elasticsearch resource keystore."""

    body = {"secrets": secrets}

    log.info(f"updating keystore for deployment {deployment_id}, ref_id {ref_id}")
    response = ec.client.patch(f"/deployments/{deployment_id}/elasticsearch/{ref_id}/keystore", json=body)
    result = response.json()
    log.debug(result)
    response.raise_for_status()
    log.info(f"keystore updated for deployment {deployment_id}, ref_id {ref_id}")


if __name__ == "__main__":
    main()
