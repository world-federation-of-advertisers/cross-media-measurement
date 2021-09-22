// Copyright 2021 The Cross-Media Measurement Authors
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

package org.wfanet.panelmatch.client.privatemembership.testing

import com.google.privatemembership.batch.ParametersKt.cryptoParameters
import java.lang.Long.parseUnsignedLong

/**
 * Example test crypto parameters for Private Membership.
 *
 * The security of these parameters may be calculated using the code found at:
 * https://bitbucket.org/malb/lwe-estimator/src/master/
 */
val PRIVATE_MEMBERSHIP_CRYPTO_PARAMETERS = cryptoParameters {
  requestModulus += parseUnsignedLong("18446744073708380161")
  requestModulus += parseUnsignedLong("137438953471")
  responseModulus += parseUnsignedLong("2056193")
  logDegree = 12
  logT = 1
  variance = 8
  levelsOfRecursion = 2
  logCompressionFactor = 4
  logDecompositionModulus = 10
}
