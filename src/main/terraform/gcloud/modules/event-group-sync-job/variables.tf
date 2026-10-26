# Copyright 2026 The Cross-Media Measurement Authors
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

variable "project" { type = string }
variable "region" { type = string }
variable "name" { type = string }
variable "terraform_service_account" { type = string }
variable "function_name" { type = string }
variable "target" {
  description = "Scheduled job request. The stock container must supply /bin/sh and curl. Remove the matching DataWatcher route before enabling."
  type = object({
    schedule       = string
    time_zone      = optional(string, "Etc/UTC")
    paused         = optional(bool, false)
    data_provider  = string
    input_blob_uri = string
    caller_image   = string
  })
  validation {
    condition     = can(regex("^dataProviders/[^/]+$", var.target.data_provider)) && can(regex("^gs://[^/]+/.+", var.target.input_blob_uri))
    error_message = "Supply a DataProvider resource name and a gs:// object URI."
  }
  validation {
    condition     = can(regex("@sha256:[0-9a-f]{64}$", var.target.caller_image))
    error_message = "Pin the stock shell-and-curl container to a SHA-256 digest."
  }
}
