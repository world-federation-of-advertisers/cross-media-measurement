# Copyright 2023 The Cross-Media Measurement Authors
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

module "simulators_cluster" {
  source = "../modules/cluster"

  name            = local.simulators_cluster_name
  location        = local.cluster_location
  release_channel = var.cluster_release_channel

  trusted_image_signing_fingerprint = ""
}

module "simulators" {
  source = "../modules/simulators"
  for_each = toset(var.edp_simulator_names)

  simulator_name                  = each.key
  location                        = local.cluster_location
  tee_image_signature_fingerprint = var.trusted_image_signing_fingerprint
}
