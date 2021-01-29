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

package org.wfanet.measurement.common.crypto

import java.io.File
import java.security.PrivateKey
import java.security.cert.X509Certificate

/** Certificates and associated private key for digital signatures. */
data class SigningCerts(
  /** [X509Certificate] for verifying digital signatures made with [privateKey]. */
  val certificate: X509Certificate,
  /** [PrivateKey] matching the public key in [certificate]. */
  val privateKey: PrivateKey,
  val trustedCertificates: Collection<X509Certificate>
) {
  companion object {
    fun fromPemFiles(
      certificateFile: File,
      privateKeyFile: File,
      trustedCertCollectionFile: File
    ): SigningCerts {
      val certificate = readCertificate(certificateFile)
      val keyAlgorithm = certificate.publicKey.algorithm

      return SigningCerts(
        certificate,
        readPrivateKey(privateKeyFile, keyAlgorithm),
        readCertificateCollection(trustedCertCollectionFile)
      )
    }
  }
}
