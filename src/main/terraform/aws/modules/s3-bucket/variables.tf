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

variable "bucket_name" {
  description = "S3 bucket name"
  type        = string
  nullable    = false

  validation {
    condition     = length(var.bucket_name) > 2 && length(var.bucket_name) < 64 && can(regex("^[0-9A-Za-z-]+$", var.bucket_name))
    error_message = "The bucket_name must follow naming rules. Check them out at: https://docs.aws.amazon.com/AmazonS3/latest/userguide/bucketnamingrules.html."
  }
}
