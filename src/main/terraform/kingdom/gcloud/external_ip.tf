# Copyright 2023 The Cross-Media Measurement Authors
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

# This is step 10a as per the document
# https://github.com/world-federation-of-advertisers/cross-media-measurement/blob/main/docs/gke/kingdom-deployment.md

resource "google_compute_address" "v2alpha_public_api_server_ip" {
  name = "v2alpha-public-api-server-ip"
  region = local.zone
}

resource "google_compute_address" "system_api_server_ip" {
  name = "system-api-server-ip"
  region = local.zone
}
