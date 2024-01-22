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

import com.google.protobuf.ByteString
import java.io.Serializable
import org.wfanet.panelmatch.common.crypto.AsymmetricKeyPair

/**
 * Provides oblivious query compression encryption and decryption for use in private information
 * retrieval
 */
interface PrivateMembershipCryptor : Serializable {

  /** Generates a public and private key for query compression and expansion */
  fun generateKeys(): AsymmetricKeyPair

  /** encrypts a set of unencrypted queries */
  fun encryptQueries(
    unencryptedQueries: Iterable<UnencryptedQuery>,
    keys: AsymmetricKeyPair,
  ): ByteString
}
