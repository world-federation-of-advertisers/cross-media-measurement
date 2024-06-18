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

package org.wfanet.panelmatch.common.certificates.testing

import java.security.PrivateKey
import java.security.cert.X509Certificate
import org.wfanet.measurement.common.crypto.readCertificate
import org.wfanet.measurement.common.crypto.readPrivateKey
import org.wfanet.measurement.common.crypto.testing.TestData
import org.wfanet.panelmatch.common.ExchangeDateKey
import org.wfanet.panelmatch.common.certificates.CertificateManager
import org.wfanet.panelmatch.common.certificates.CertificateManager.KeyPair

private const val KEY_ALGORITHM = "EC"

object TestCertificateManager : CertificateManager {
  override suspend fun getCertificate(
    exchange: ExchangeDateKey,
    certName: String,
  ): X509Certificate {
    return CERTIFICATE
  }

  override suspend fun getPartnerRootCertificate(partnerName: String): X509Certificate {
    return CERTIFICATE
  }

  override suspend fun getExchangePrivateKey(exchange: ExchangeDateKey): PrivateKey {
    return PRIVATE_KEY
  }

  override suspend fun getExchangeKeyPair(exchange: ExchangeDateKey): KeyPair {
    return KeyPair(CERTIFICATE, PRIVATE_KEY, RESOURCE_NAME)
  }

  override suspend fun createForExchange(exchange: ExchangeDateKey): String {
    return RESOURCE_NAME
  }

  const val RESOURCE_NAME = "some-resource-name"
  val CERTIFICATE: X509Certificate by lazy { readCertificate(TestData.FIXED_SERVER_CERT_PEM_FILE) }
  val PRIVATE_KEY: PrivateKey by lazy {
    readPrivateKey(TestData.FIXED_SERVER_KEY_FILE, KEY_ALGORITHM)
  }
}
