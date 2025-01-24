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

"""Helper functions for the Elastic Pipes implementation."""

import sys

from .errors import Error

if sys.version_info >= (3, 12):
    from itertools import batched
else:
    from itertools import islice

    def batched(iterable, chunk_size):
        iterator = iter(iterable)
        while chunk := list(islice(iterator, chunk_size)):
            yield chunk


def get_field(dict, path, *, shell_expand=False):
    if path in (None, "", "."):
        return dict
    keys = path.split(".")
    if not all(keys):
        raise Error(f"invalid path: {path}")
    try:
        for key in keys:
            if dict is None:
                break
            dict = dict[key]
    except KeyError:
        return None

    if shell_expand:
        from .shelllib import shell_expand

        dict = shell_expand(dict)
    return dict


def set_field(dict, path, value):
    if path in (None, "", "."):
        dict.clear()
        dict.update(value)
        return
    keys = path.split(".")
    if not all(keys):
        raise Error(f"invalid path: {path}")
    for key in keys[:-1]:
        dict = dict.setdefault(key, {})
    dict[keys[-1]] = value


def serialize_yaml(file, state):
    import yaml

    try:
        # use the LibYAML C library bindings if available
        from yaml import CDumper as Dumper
    except ImportError:
        from yaml import Dumper

    yaml.dump(state, file, Dumper=Dumper)


def deserialize_yaml(file):
    import yaml

    try:
        # use the LibYAML C library bindings if available
        from yaml import CLoader as Loader
    except ImportError:
        from yaml import Loader

    return yaml.load(file, Loader=Loader)
