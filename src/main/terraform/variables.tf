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

variable "project" {
  type = string
  default = "halo-cmm-dev"
  description = "Project name used"
}

variable env {
  type = string
  default = "dev"
  description = "Represents the environment used."
}

variable "service_account" {
  type = string
  # TODO: Our approach is to put this in github secrets and fetch it during runtime and feed it here.
  default = "serviceAccount:service-1049178966878@compute-system.iam.gserviceaccount.com"
  description = "The Service account to be used to create these resources"
}