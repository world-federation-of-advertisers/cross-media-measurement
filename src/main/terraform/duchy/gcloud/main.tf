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
  This is a terraform provider that will tie the terraform with the GCP account that we have.
  The JSON mentioned here should never be pushed with the code.
*/
provider "google" {
  project     = var.project
  # credentials = ${file("${var.path}")}"
}


#terraform {
# backend "gcs" {
# }
#}


