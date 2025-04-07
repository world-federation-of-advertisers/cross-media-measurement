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

module "secure_computation_storage" {
  source = "../modules/storage-bucket"

  name     = var.secure_computation_storage_bucket_name
  location = local.secure_computation_storage_bucket_location
}

module "data_watcher" {
    source = "../modules/gcs-bucket-cloud-function"

    trigger_bucket_name                 = var.secure_computation_storage_bucket_name
    cloud_function_service_account_name = var.data_watcher_cloud_function_service_account_name
    cloud_function_name                 = var.data_watcher_cloud_function_name
    entry_point                         = var.data_watcher_entry_point
    cloud_function_source_bucket        = var.cloud_function_source_bucket
    cloud_function_source_object        = var.data_watcher_source_object
}