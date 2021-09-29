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

class KingdomInternalException : Exception {
  val code: Code

  constructor(code: Code) : super() {
    this.code = code
  }

  constructor(code: Code, buildMessage: () -> String) : super(buildMessage()) {
    this.code = code
  }

  enum class Code {
    /** MeasurementConsumer resource queried could not be found. */
    MEASUREMENT_CONSUMER_NOT_FOUND,

    /** DataProvider resource queried could not be found. */
    DATA_PROVIDER_NOT_FOUND,

    /** Duchy resource queried could not be found. */
    DUCHY_NOT_FOUND,

    /** Measurement resource queried could not be found. */
    MEASUREMENT_NOT_FOUND,

    /** Measurement is in an illegal state for the operation. */
    MEASUREMENT_STATE_ILLEGAL,

    /** Certificate with the same subject key identifier (SKID) already exists. */
    CERT_SUBJECT_KEY_ID_ALREADY_EXISTS,

    /** Certificate resource queried could not be found. */
    CERTIFICATE_NOT_FOUND,

    /** Computation Participant should have been in another state. */
    COMPUTATION_PARTICIPANT_STATE_ILLEGAL,

    /** Computation Participant resource queried could not be found. */
    COMPUTATION_PARTICIPANT_NOT_FOUND,

    /** Requisition entity could not be found. */
    REQUISITION_NOT_FOUND,

    /** Requisition is in an illegal state for the operation. */
    REQUISITION_STATE_ILLEGAL,

    /** Account resource queried could not be found. */
    ACCOUNT_NOT_FOUND,

    /** UsernameIdentity with the same username already exists. */
    USERNAME_ALREADY_EXISTS,

    /** Account has already been activated. */
    ACCOUNT_ALREADY_ACTIVATED,

    /** Account has not been activated yet. */
    ACCOUNT_NOT_ACTIVATED,

    /** Account does not own the measurement consumer. */
    ACCOUNT_NOT_OWNER,
  }
}
