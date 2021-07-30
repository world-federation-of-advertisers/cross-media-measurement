// Copyright 2020 The Cross-Media Measurement Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common

class KingdomInternalException(val code: Code) : Exception() {
  enum class Code {
    /** MeasurementConsumer resource queried could not be found. */
    MEASUREMENT_CONSUMER_NOT_FOUND,

    /** DataProvider resource queried could not be found. */
    DATA_PROVIDER_NOT_FOUND,

    /** DUCHY resource queried could not be found. */
    DUCHY_NOT_FOUND,

    /** Certificate with the same subject key identifier (SKID) already exists. */
    CERT_SUBJECT_KEY_ID_ALREADY_EXISTS,
  }
}
