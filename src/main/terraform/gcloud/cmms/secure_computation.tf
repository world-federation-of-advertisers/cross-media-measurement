# Copyright 2025 The Cross-Media Measurement Authors
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

#TODO(@MarcoPremier): Update `configure-secure-computation-control-plane.yml` to use kingdom cluster (https://github.com/world-federation-of-advertisers/cross-media-measurement/blob/main/.github/workflows/configure-secure-computation-control-plane.yml#L98)

module "secure_computation" {
  source = "../modules/secure-computation"

  spanner_instance                          = google_spanner_instance.spanner_instance
}
