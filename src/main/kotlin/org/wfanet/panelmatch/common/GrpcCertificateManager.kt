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

package org.wfanet.panelmatch.common

import com.google.protobuf.ByteString
import java.security.KeyFactory
import java.security.PrivateKey
import java.security.cert.X509Certificate
import java.security.spec.PKCS8EncodedKeySpec
import java.util.concurrent.ConcurrentHashMap
import org.wfanet.measurement.api.v2alpha.CertificatesGrpcKt
import org.wfanet.measurement.api.v2alpha.ExchangeKey
import org.wfanet.measurement.api.v2alpha.getCertificateRequest
import org.wfanet.measurement.common.crypto.jceProvider
import org.wfanet.measurement.common.crypto.readCertificate

// TODO: Rename (and maybe move the package) to indicate dependency on the v2alpha API.
/**
 * Interface to grab and validate certificates. Provides validated X509 certificates for an exchange
 * to use to validate .
 */
class GrpcCertificateManager(
  /** Connection to the APIs certificate service used to grab certs registered with the Kingdom */
  private val certificateService: CertificatesGrpcKt.CertificatesCoroutineStub,
  private val rootCerts: SecretMap<String, X509Certificate>,
  private val privateKeys: SecretMap<String, ByteString>,
  private val algorithm: String
) : CertificateManager {

  private val cache = ConcurrentHashMap<Pair<String, String>, X509Certificate>()

  private fun verifyCertificate(
    certificate: X509Certificate,
    certOwnerName: String
  ): X509Certificate {
    val rootCert = rootCerts[certOwnerName]
    certificate.verify(rootCert.publicKey, jceProvider)
    return certificate
  }

  override suspend fun getCertificate(
    exchangeKey: ExchangeKey,
    certOwnerName: String,
    certResourceName: String
  ): X509Certificate {
    return cache.getOrPut((certOwnerName to exchangeKey.toName())) {
      val response =
        certificateService.getCertificate(getCertificateRequest { name = certResourceName })
      val x509 = readCertificate(response.x509Der)

      verifyCertificate(x509, certOwnerName)
    }
  }

  override fun getExchangePrivateKey(exchangeKey: ExchangeKey): PrivateKey {
    return KeyFactory.getInstance(algorithm, jceProvider)
      .generatePrivate(PKCS8EncodedKeySpec(privateKeys[exchangeKey.toName()].toByteArray()))
  }

  override suspend fun getPartnerRootCertificate(partnerName: String): X509Certificate {
    return rootCerts[partnerName]
  }
}
