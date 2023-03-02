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
set -x && chmod +x
sudo crontab -r

cd /tmp/cross-media-measurement

mkdir -p /root/.config/gcloud/configurations
echo "[core]" > /root/.config/gcloud/configurations/config_default
echo "account = mohanraj.dharmalingam@zealsols.com" >> /root/.config/gcloud/configurations/config_default
echo "project = halo-cmm-sandbox" >>  /root/.config/gcloud/configurations/config_default

# write code to login to the account.
# Try workload identity

echo Y | sudo gcloud auth configure-docker
echo "Executing command to Push the built image to GCR"
sudo bazelisk query 'filter("push_kingdom", kind("container_push", //src/main/docker:all))' | \
  xargs -n 1 bazelisk run -c opt --define container_registry=gcr.io \
  --define image_repo_prefix=halo-cmm-sandbox --define image_tag=build-0004 > push_image.log