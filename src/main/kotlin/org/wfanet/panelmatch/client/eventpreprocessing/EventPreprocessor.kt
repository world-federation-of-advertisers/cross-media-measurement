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

package org.wfanet.panelmatch.client.eventpreprocessing

import java.io.Serializable

/**
 * Implements several encryption schemes to encrypt event data and an identifier. A deterministic
 * commutative encryption scheme is used to encrypt the identifier, which is hashed using a Sha256
 * method. The encrypted identifier is also used in an HKDF to create an AES key, which is used for
 * an AES encryption/decryption of event data.
 */
interface EventPreprocessor : Serializable {
  /** Preprocesses each of the events in [request] as described above */
  fun preprocess(request: PreprocessEventsRequest): PreprocessEventsResponse
}
