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
 * Provides oblivious query compression encryption and decryption for use in private information
 * retrieval
 */
interface PrivateMembershipCryptor : Serializable {

  /** Generates a public and private key for query compression and expansion */
  fun generateKeys(request: GenerateKeysRequest): GenerateKeysResponse

  /** decrypts a set of encrypted queries */
  fun decryptQueryResults(request: DecryptQueriesRequest): DecryptQueriesResponse

  /** encrypts a set of unencrypted queries */
  fun encryptQueries(request: EncryptQueriesRequest): EncryptQueriesResponse
}
