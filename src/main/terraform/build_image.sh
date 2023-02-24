# Copyright 2020 The Cross-Media Measurement Authors
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

#!/bin/bash
set -x

# Make the script file executable
chmod +x

# Clone the Git repository
git clone https://github.com/world-federation-of-advertisers/cross-media-measurement.git
cd cross-media-measurement

# Checkout to the desired commit
#git checkout 7fab61049e425bb0edd5fa2802290bf1722254e7
#cd ..

 # Build and push Docker images using Bazel

bazel query 'filter("push_kingdom", kind("container_push", //src/main/docker:all))' |
  xargs bazel build -c opt --define container_registry=gcr.io \
  --define image_repo_prefix=my-project-test-373810 --define image_tag=build-0004


bazel query 'filter("push_kingdom", kind("container_push", //src/main/docker:all))' |
  xargs -n 1 bazel run -c opt --define container_registry=gcr.io \
  --define image_repo_prefix=my-project-test-373810 --define image_tag=build-0002
