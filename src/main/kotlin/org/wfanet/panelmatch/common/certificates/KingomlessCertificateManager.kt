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

import com.google.protobuf.kotlin.toByteString
import java.security.KeyFactory
import java.security.PrivateKey
import java.security.cert.X509Certificate
import java.security.spec.PKCS8EncodedKeySpec
import java.time.Clock
import java.util.concurrent.ConcurrentHashMap
import org.wfanet.measurement.common.crypto.jceProvider
import org.wfanet.measurement.common.crypto.readCertificate
import org.wfanet.measurement.storage.StorageClient
import org.wfanet.panelmatch.client.common.Identity
import org.wfanet.panelmatch.client.internal.Certificate
import org.wfanet.panelmatch.client.internal.ExchangeWorkflow.Party
import org.wfanet.panelmatch.client.internal.certificate
import org.wfanet.panelmatch.common.ExchangeDateKey
import org.wfanet.panelmatch.common.certificates.CertificateManager.KeyPair
import org.wfanet.panelmatch.common.secrets.MutableSecretMap
import org.wfanet.panelmatch.common.secrets.SecretMap
import org.wfanet.panelmatch.common.storage.toByteString

/** [CertificateManager] that maintains [Certificate]s in shared storage. */
class KingdomlessCertificateManager(
  private val identity: Identity,
  private val rootCerts: SecretMap,
  private val privateKeys: MutableSecretMap,
  private val algorithm: String,
  private val certificateAuthority: CertificateAuthority,
  private val clock: Clock,
  private val fallbackPrivateKeyBlobKey: String? = null,
  private val getSharedStorage: (ExchangeDateKey) -> StorageClient,
) : CertificateManager {

  private val x509CertCache = ConcurrentHashMap<String, X509Certificate>()
  private val rootCertsCache = ConcurrentHashMap<String, X509Certificate>()
  private val signingKeysCache = ConcurrentHashMap<String, SigningKeys>()

  override suspend fun getCertificate(
    exchange: ExchangeDateKey,
    certName: String,
  ): X509Certificate {
    return x509CertCache.getOrPut(certName) {
      val sharedStorage = getSharedStorage(exchange)
      val certificate =
        requireNotNull(sharedStorage.readCertificate(certName)) {
          "Certificate not found: $certName"
        }
      val x509 = readCertificate(certificate.x509Der)
      verifyCertificate(x509, certificate.ownerId)
    }
  }

  override suspend fun getPartnerRootCertificate(partnerName: String): X509Certificate {
    return getRootCertificate(partnerName)
  }

  override suspend fun getExchangePrivateKey(exchange: ExchangeDateKey): PrivateKey {
    val signingKeys =
      requireNotNull(getSigningKeys(exchange.path)) { "Missing keys for exchange: $exchange" }
    return signingKeys.parsePrivateKey()
  }

  override suspend fun getExchangeKeyPair(exchange: ExchangeDateKey): KeyPair {
    val keyFromPrimaryPath = getSigningKeys(exchange.path)
    val signingKeys =
      keyFromPrimaryPath ?: checkNotNull(getSigningKeys(fallbackPrivateKeyBlobKey!!))
    val x509Certificate = getCertificate(exchange, signingKeys.certResourceName)
    val privateKey = signingKeys.parsePrivateKey()
    return KeyPair(x509Certificate, privateKey, signingKeys.certResourceName)
  }

  override suspend fun createForExchange(exchange: ExchangeDateKey): String {
    val existingKeys = getSigningKeys(exchange.path)
    if (existingKeys != null) {
      return existingKeys.certResourceName
    }

    val sharedStorage = getSharedStorage(exchange)
    val (x509, privateKey) = certificateAuthority.generateX509CertificateAndPrivateKey()
    val certificate = certificate {
      ownerId = identity.id
      x509Der = x509.encoded.toByteString()
    }
    val certPrefix =
      when (identity.party) {
        Party.DATA_PROVIDER -> DATA_PROVIDER_CERT_PREFIX
        Party.MODEL_PROVIDER -> MODEL_PROVIDER_CERT_PREFIX
        Party.PARTY_UNSPECIFIED,
        Party.UNRECOGNIZED -> error("Unsupported party: ${identity.party}")
      }
    val certName = "$certPrefix-${clock.instant().epochSecond}"
    sharedStorage.writeCertificate(certName, certificate)

    val signingKeys = signingKeys {
      certResourceName = certName
      this.privateKey = privateKey.encoded.toByteString()
    }
    privateKeys.put(exchange.path, signingKeys.toByteString())
    x509CertCache[certName] = x509
    signingKeysCache[exchange.path] = signingKeys

    return certName
  }

  private suspend fun verifyCertificate(
    certificate: X509Certificate,
    ownerId: String,
  ): X509Certificate {
    val rootCert = getRootCertificate(ownerId)
    certificate.verify(rootCert.publicKey, jceProvider)
    return certificate
  }

  private suspend fun getRootCertificate(ownerId: String): X509Certificate {
    return rootCertsCache.getOrPut(ownerId) {
      val certBytes =
        requireNotNull(rootCerts.get(ownerId)) { "Missing root certificate for $ownerId" }
      readCertificate(certBytes)
    }
  }

  private suspend fun getSigningKeys(name: String): SigningKeys? {
    if (signingKeysCache.containsKey(name)) {
      return signingKeysCache.getValue(name)
    }
    val serializedSigningKeys = privateKeys.get(name) ?: return null
    return signingKeysCache.getOrPut(name) { SigningKeys.parseFrom(serializedSigningKeys) }
  }

  private fun SigningKeys.parsePrivateKey(): PrivateKey {
    val keyFactory = KeyFactory.getInstance(algorithm, jceProvider)
    return keyFactory.generatePrivate(PKCS8EncodedKeySpec(privateKey.toByteArray()))
  }

  private suspend fun StorageClient.readCertificate(certName: String): Certificate? {
    val blob = getBlob("$BLOB_KEY_PREFIX/$certName") ?: return null
    return Certificate.parseFrom(blob.toByteString())
  }

  private suspend fun StorageClient.writeCertificate(certName: String, cert: Certificate) {
    writeBlob("$BLOB_KEY_PREFIX/$certName", cert.toByteString())
  }

  companion object {
    private const val BLOB_KEY_PREFIX = "certificates"
    private const val DATA_PROVIDER_CERT_PREFIX = "edp-certificate"
    private const val MODEL_PROVIDER_CERT_PREFIX = "mp-certificate"
  }
}
