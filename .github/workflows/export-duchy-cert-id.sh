#!/usr/bin/env bash
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


declare duchy_cert_id
case "$DUCHY_NAME" in
  aggregator)
    duchy_cert_id="$AGGREGATOR_DUCHY_CERT_ID"
    ;;
  worker1)
    duchy_cert_id="$WORKER1_DUCHY_CERT_ID"
    ;;
  worker2)
    duchy_cert_id="$WORKER2_DUCHY_CERT_ID"
    ;;
  *)
    echo "Unexpected Duchy name $DUCHY_NAME" >&2
    exit 1
    ;;
esac

echo "DUCHY_CERT_ID=${duchy_cert_id}" >> "$GITHUB_ENV"
