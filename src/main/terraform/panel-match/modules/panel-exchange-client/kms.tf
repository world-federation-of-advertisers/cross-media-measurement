# Copyright 2022 The Cross-Media Measurement Authors
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

resource "aws_kms_key" "k8s_key" {
  description             = "key for ocmm"
  deletion_window_in_days = 7
}

resource "aws_kms_alias" "k8s_key_alias" {
  name          = "alias/${var.kms_alias_name}"
  target_key_id = aws_kms_key.k8s_key.key_id
}
