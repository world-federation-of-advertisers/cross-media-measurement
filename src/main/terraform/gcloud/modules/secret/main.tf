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

resource "google_secret_manager_secret" "secret" {
  secret_id = var.secret_id
  replication {
      auto {}
    }
}

resource "google_secret_manager_secret_version" "secret_version" {
  secret                = google_secret_manager_secret.secret.id
  is_secret_data_base64 = var.is_binary_format ? true : false
  secret_data           = var.is_binary_format ? filebase64(var.secret_path) : file(var.secret_path)
}