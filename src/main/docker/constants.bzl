# Copyright 2020 The Measurement System Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Shared constants for container build targets."""

_DEBIAN_JAVA_11_HOME = "/usr/lib/jvm/java-11-openjdk-amd64"
_DEBIAN_JAVA_11_BIN = _DEBIAN_JAVA_11_HOME + "/bin"

DEBIAN_JAVA_11 = struct(
    home = _DEBIAN_JAVA_11_HOME,
    bin = _DEBIAN_JAVA_11_BIN,
    env = {
        "JAVA_HOME": _DEBIAN_JAVA_11_HOME,
        "PATH": _DEBIAN_JAVA_11_BIN + ":$$PATH",
    },
)
