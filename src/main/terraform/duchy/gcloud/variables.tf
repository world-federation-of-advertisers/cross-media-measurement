# Copyright 2020 The Cross-Media Measurement Authors
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

/*
locals {
  project = "halo"
  tags = {
    Environment     = var.env
  }
  prefix      = "${var.env}-${local.project}"
  prefix_path = "${var.env}/${local.project}"
}
*/
variable projectName {
  default = "halo"
  description = "The IAC project of WFA "
}

variable project {
  default = "halo-cmm-sandbox"
  description = "The project ID"
}

variable "path" {default  = "C:/Users/HARISH KAWALKAR/IdeaProjects/duchy/terraform/secrets"}
