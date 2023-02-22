# Copyright 2020 The Cross-Media Measurement Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

resource "google_spanner_instance" "db_instance" {
  config = "regional-${data.google_client_config.current.region}"
  name = "${var.db_info.instance_name}"
  display_name = "${var.db_info.instance_name}"
  processing_units = 100
}

resource "google_spanner_database" "db" {
  instance = google_spanner_instance.db_instance.name
  name = "${var.db_info.db_name}"
  deletion_protection = false
}
