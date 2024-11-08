// Copyright 2024 The Cross-Media Measurement Authors
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

import com.google.common.truth.Truth.assertThat
import java.io.StringReader
import java.security.KeyPair
import org.bouncycastle.asn1.x500.X500Name
import org.bouncycastle.openssl.PEMParser
import org.bouncycastle.pkcs.PKCS10CertificationRequest
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.common.crypto.SignatureAlgorithm
import org.wfanet.measurement.common.crypto.readCertificate
import org.wfanet.measurement.common.crypto.readPrivateKey
import org.wfanet.measurement.common.crypto.testing.TestData
import org.wfanet.panelmatch.common.certificates.CertificateSigningRequests.generateCsrFromKeyPair

@RunWith(JUnit4::class)
class CertificateSigningRequestsTest {

  @Test
  fun generateCsrFromKeyPairReturnsExpectedCsr() {
    val publicKey = readCertificate(TestData.FIXED_CA_CERT_PEM_FILE).publicKey
    val privateKey = readPrivateKey(TestData.FIXED_CA_KEY_FILE, "EC")

    val pemCsr =
      generateCsrFromKeyPair(
        keyPair = KeyPair(publicKey, privateKey),
        organization = "Example",
        commonName = "example.com",
        signatureAlgorithm = SignatureAlgorithm.ECDSA_WITH_SHA256,
      )

    val pkcs10Csr = pkcs10CsrFromPemCsr(pemCsr)
    assertThat(pkcs10Csr.subject).isEqualTo(X500Name("O=Example, CN=example.com"))
    assertThat(pkcs10Csr.subjectPublicKeyInfo.encoded).isEqualTo(publicKey.encoded)
    assertThat(pkcs10Csr.signatureAlgorithm.algorithm.id)
      .isEqualTo(SignatureAlgorithm.ECDSA_WITH_SHA256.oid)
  }

  companion object {
    private fun pkcs10CsrFromPemCsr(pemCsr: String): PKCS10CertificationRequest {
      val pemParser = PEMParser(StringReader(pemCsr))
      return pemParser.readObject() as PKCS10CertificationRequest
    }
  }
}
