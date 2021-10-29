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

import java.security.KeyFactory
import java.security.PrivateKey
import java.security.cert.X509Certificate
import java.security.spec.PKCS8EncodedKeySpec
import java.util.concurrent.ConcurrentHashMap
import org.wfanet.measurement.api.v2alpha.CertificatesGrpcKt.CertificatesCoroutineStub
import org.wfanet.measurement.api.v2alpha.getCertificateRequest
import org.wfanet.measurement.common.crypto.jceProvider
import org.wfanet.measurement.common.crypto.readCertificate
import org.wfanet.panelmatch.common.ExchangeDateKey
import org.wfanet.panelmatch.common.secrets.SecretMap

/** [CertificateManager] that loads [X509Certificate]s from [certificateService]. */
class V2AlphaCertificateManager(
  private val certificateService: CertificatesCoroutineStub,
  private val rootCerts: SecretMap,
  private val privateKeys: SecretMap,
  private val algorithm: String
) : CertificateManager {

  private val cache = ConcurrentHashMap<Pair<String, String>, X509Certificate>()

  private suspend fun verifyCertificate(
    certificate: X509Certificate,
    certOwnerName: String
  ): X509Certificate {
    val rootCert = getRootCertificate(certOwnerName)
    certificate.verify(rootCert.publicKey, jceProvider)
    return certificate
  }

  override suspend fun getCertificate(
    exchange: ExchangeDateKey,
    certOwnerName: String,
    certResourceName: String
  ): X509Certificate {
    return cache.getOrPut(certOwnerName to exchange.path) {
      val request = getCertificateRequest { name = certResourceName }
      val response = certificateService.getCertificate(request)
      val x509 = readCertificate(response.x509Der)
      verifyCertificate(x509, certOwnerName)
    }
  }

  override suspend fun getExchangePrivateKey(exchange: ExchangeDateKey): PrivateKey {
    val keyBytes = requireNotNull(privateKeys.get(exchange.path))
    val keyFactory = KeyFactory.getInstance(algorithm, jceProvider)
    return keyFactory.generatePrivate(PKCS8EncodedKeySpec(keyBytes.toByteArray()))
  }

  override suspend fun getPartnerRootCertificate(partnerName: String): X509Certificate {
    return getRootCertificate(partnerName)
  }

  private suspend fun getRootCertificate(ownerName: String): X509Certificate {
    val certBytes =
      requireNotNull(rootCerts.get(ownerName)) { "Missing root certificate for $ownerName" }
    return readCertificate(certBytes)
  }
}
