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

package org.wfanet.panelmatch.common.certificates

import java.security.PrivateKey
import java.security.cert.X509Certificate

/** Creates X509Certificates signed by a protected root private key. */
interface CertificateAuthority {
  data class Context(
    val commonName: String,
    val organization: String,
    val dnsName: String,
    val validDays: Int,
  )

  /**
   * Creates a [PrivateKey] and corresponding [X509Certificate] signed by a root private key and
   * verifiable by the shared root public key.
   *
   * TODO(@efoxepstein): use SigningKeyHandle instead of a PrivateKey directly.
   */
  suspend fun generateX509CertificateAndPrivateKey(): Pair<X509Certificate, PrivateKey>
}
