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

package org.wfanet.panelmatch.client.privatemembership

import java.io.Serializable

/**
 * Provides a computationally symmetric form of private information query result decryption.
 * Computationally symmetric forms of private information retrieval do not allow the querier to be
 * able to access information for queries outside of their scope while still preventing the database
 * from knowing what was queried.
 */
interface SymmetricPrivateMembershipCryptor : Serializable {

  /** Generates a public and private key for query compression and expansion */
  fun generatePrivateMembershipKeys(request: GenerateKeysRequest): GenerateKeysResponse

  /** Decrypts a set of encrypted queries */
  fun decryptQueryResults(request: SymmetricDecryptQueriesRequest): SymmetricDecryptQueriesResponse
}
