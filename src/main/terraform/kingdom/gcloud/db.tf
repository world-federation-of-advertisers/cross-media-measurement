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

# This is step 1 as per the document
# https://github.com/world-federation-of-advertisers/cross-media-measurement/blob/main/docs/gke/kingdom-deployment.md

resource "google_spanner_instance" "halo_spanner_db" {
  config       = "regional-${local.zone}"
  display_name = "${local.prefix}-spanner-instance"
  num_nodes    = local.spanner_db.num_nodes
}

resource "google_spanner_database" "database" {
  instance = google_spanner_instance.halo_spanner_db.name
  name     = "${local.prefix}-spanner-database"
  version_retention_period = local.spanner_db.version_retention_period
  deletion_protection = local.spanner_db.deletion_protection
}
