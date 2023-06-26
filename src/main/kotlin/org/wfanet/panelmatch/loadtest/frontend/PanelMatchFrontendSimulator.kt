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

package org.wfanet.panelmatch.loadtest.frontend

import org.wfanet.measurement.common.crypto.PrivateKeyHandle
import org.wfanet.measurement.common.crypto.SigningKeyHandle

data class PanelMatchConsumerData(
  // The MC's public API resource name
  val name: String,
  /** The MC's consent signaling signing key. */
  val signingKey: SigningKeyHandle,
  /** The MC's encryption private key. */
  val encryptionKey: PrivateKeyHandle,
  /** An API key for the MC. */
  val apiAuthenticationKey: String
)

class PanelMatchFrontendSimulator {
}
