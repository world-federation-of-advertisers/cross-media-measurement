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

import com.google.protobuf.ByteString
import com.google.protobuf.kotlin.toByteString
import java.security.KeyFactory
import java.security.PrivateKey
import java.security.cert.X509Certificate
import java.security.spec.PKCS8EncodedKeySpec
import java.util.Optional
import java.util.concurrent.ConcurrentHashMap
import org.wfanet.measurement.api.v2alpha.CertificateKey
import org.wfanet.measurement.api.v2alpha.CertificatesGrpcKt.CertificatesCoroutineStub
import org.wfanet.measurement.api.v2alpha.certificate
import org.wfanet.measurement.api.v2alpha.createCertificateRequest
import org.wfanet.measurement.api.v2alpha.getCertificateRequest
import org.wfanet.measurement.common.crypto.jceProvider
import org.wfanet.measurement.common.crypto.readCertificate
import org.wfanet.panelmatch.common.ExchangeDateKey
import org.wfanet.panelmatch.common.certificates.CertificateManager.KeyPair
import org.wfanet.panelmatch.common.loggerFor
import org.wfanet.panelmatch.common.secrets.MutableSecretMap
import org.wfanet.panelmatch.common.secrets.SecretMap

/**
 * [CertificateManager] that loads [X509Certificate]s from [certificateService].
 *
 * [certificateAuthority] should be a private CA that's capable of signing with the party's root
 * private key. This abstraction is important because this private key is extraordinarily sensitive
 * and should be locked down.
 */
class V2AlphaCertificateManager(
  private val certificateService: CertificatesCoroutineStub,
  private val rootCerts: SecretMap,
  private val privateKeys: MutableSecretMap,
  private val algorithm: String,
  private val certificateAuthority: CertificateAuthority,
  private val localName: String,
  private val fallbackPrivateKeyBlobKey: String? = null,
) : CertificateManager {

  private val x509CertCache = ConcurrentHashMap<String, X509Certificate>()
  private val rootCertsCache = ConcurrentHashMap<String, X509Certificate>()
  private val signingKeysCache = ConcurrentHashMap<String, Optional<SigningKeys>>()

  override suspend fun getCertificate(
    exchange: ExchangeDateKey,
    certName: String,
  ): X509Certificate {
    val certificateKey =
      requireNotNull(CertificateKey.fromName(certName)) {
        "Certificate name must be a valid resource name, but was: $certName"
      }
    return x509CertCache.getOrPut(certName) {
      // TODO: handle revoked certificates.
      val request = getCertificateRequest { name = certName }
      val response = certificateService.getCertificate(request)
      val x509 = readCertificate(response.x509Der)
      verifyCertificate(x509, certificateKey.parentKey.toName())
    }
  }

  override suspend fun getExchangePrivateKey(exchange: ExchangeDateKey): PrivateKey {
    val signingKeys = requireNotNull(getSigningKeys(exchange.path)) { "Missing keys for $exchange" }
    return parsePrivateKey(signingKeys.privateKey)
  }

  override suspend fun getExchangeKeyPair(exchange: ExchangeDateKey): KeyPair {
    val keyFromPrimaryPath = getSigningKeys(exchange.path)
    val signingKeys =
      keyFromPrimaryPath ?: checkNotNull(getSigningKeys(fallbackPrivateKeyBlobKey!!))
    val x509Certificate = getCertificate(exchange, signingKeys.certName)
    val privateKey = parsePrivateKey(signingKeys.privateKey)
    return KeyPair(x509Certificate, privateKey, signingKeys.certName)
  }

  override suspend fun getPartnerRootCertificate(partnerName: String): X509Certificate {
    return getRootCertificate(partnerName)
  }

  override suspend fun createForExchange(exchange: ExchangeDateKey): String {
    val existingKeys = getSigningKeys(exchange.path)
    if (existingKeys != null) {
      return existingKeys.certName
    }

    val (x509, privateKey) = certificateAuthority.generateX509CertificateAndPrivateKey()

    val request = createCertificateRequest {
      parent = localName
      certificate = certificate { x509Der = x509.encoded.toByteString() }
    }
    val certificate = certificateService.createCertificate(request)
    val certResourceName = certificate.name

    val signingKeys = signingKeys {
      this.certName = certResourceName
      this.privateKey = privateKey.encoded.toByteString()
    }

    logger.fine { "Writing private key to SecretMap" }
    privateKeys.put(exchange.path, signingKeys.toByteString())
    logger.fine { "Finish writing private key to SecretMap" }
    x509CertCache[certResourceName] = x509
    signingKeysCache[exchange.path] = Optional.of(signingKeys)
    logger.fine { "Returning certResourceName: $certResourceName" }

    return certResourceName
  }

  private suspend fun getSigningKeys(name: String): SigningKeys? {
    val keys =
      signingKeysCache.getOrPut(name) {
        val bytes = privateKeys.get(name) ?: return@getOrPut Optional.empty()

        @Suppress("BlockingMethodInNonBlockingContext") // This is in-memory.
        Optional.of(SigningKeys.parseFrom(bytes))
      }

    return keys.orElse(null)
  }

  private fun parsePrivateKey(bytes: ByteString): PrivateKey {
    val keyFactory = KeyFactory.getInstance(algorithm, jceProvider)
    return keyFactory.generatePrivate(PKCS8EncodedKeySpec(bytes.toByteArray()))
  }

  private suspend fun verifyCertificate(
    certificate: X509Certificate,
    ownerName: String,
  ): X509Certificate {
    val rootCert = getRootCertificate(ownerName)
    certificate.verify(rootCert.publicKey, jceProvider)
    return certificate
  }

  private suspend fun getRootCertificate(ownerName: String): X509Certificate {
    return rootCertsCache.getOrPut(ownerName) {
      val certBytes =
        requireNotNull(rootCerts.get(ownerName)) { "Missing root certificate for $ownerName" }
      readCertificate(certBytes)
    }
  }

  companion object {
    private val logger by loggerFor()
  }
}
