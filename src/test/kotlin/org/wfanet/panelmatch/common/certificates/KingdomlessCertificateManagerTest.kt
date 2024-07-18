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
import com.google.protobuf.ByteString
import com.google.protobuf.kotlin.toByteString
import java.time.LocalDate
import java.util.UUID
import kotlin.test.assertFailsWith
import kotlinx.coroutines.flow.Flow
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.mockito.Mockito.never
import org.mockito.Mockito.verify
import org.mockito.kotlin.any
import org.mockito.kotlin.spy
import org.mockito.kotlin.times
import org.wfanet.measurement.common.crypto.readCertificate
import org.wfanet.measurement.common.crypto.readPrivateKey
import org.wfanet.measurement.common.crypto.testing.TestData
import org.wfanet.measurement.storage.StorageClient
import org.wfanet.measurement.storage.testing.InMemoryStorageClient
import org.wfanet.panelmatch.client.common.Identity
import org.wfanet.panelmatch.client.internal.ExchangeWorkflow.Party
import org.wfanet.panelmatch.client.internal.ExchangeWorkflowKt.exchangeIdentifiers
import org.wfanet.panelmatch.client.internal.certificate
import org.wfanet.panelmatch.client.internal.exchangeWorkflow
import org.wfanet.panelmatch.common.ExchangeDateKey
import org.wfanet.panelmatch.common.certificates.KingdomlessCertificateManager.CertificateKey
import org.wfanet.panelmatch.common.certificates.testing.TestCertificateAuthority
import org.wfanet.panelmatch.common.secrets.testing.TestMutableSecretMap
import org.wfanet.panelmatch.common.secrets.testing.TestSecretMap
import org.wfanet.panelmatch.common.storage.PrefixedStorageClient
import org.wfanet.panelmatch.common.testing.runBlockingTest

@RunWith(JUnit4::class)
class KingdomlessCertificateManagerTest {

  private val validExchangeWorkflows =
    TestSecretMap(RECURRING_EXCHANGE_ID to WORKFLOW.toByteString())
  private val rootCerts = TestSecretMap()
  private val privateKeys = TestMutableSecretMap()
  private val rootSharedStorage =
    spy<StorageClient>(MockableForwardingStorageClient(InMemoryStorageClient()))

  private val certificateManager: KingdomlessCertificateManager =
    KingdomlessCertificateManager(
      identity = IDENTITY,
      validExchangeWorkflows = validExchangeWorkflows,
      rootCerts = rootCerts,
      privateKeys = privateKeys,
      algorithm = PRIVATE_KEY.algorithm,
      certificateAuthority = TestCertificateAuthority,
    ) {
      buildSharedStorage(it)
    }

  @Test
  fun getCertificateThrowsWhenOwnerIdIsNotParticipatingInExchange() = runBlockingTest {
    val exception =
      assertFailsWith<IllegalArgumentException> {
        certificateManager.getCertificate(EXCHANGE_DATE_KEY, "some-other-party-id:some-uuid")
      }
    assertThat(exception)
      .hasMessageThat()
      .matches("Certificate owner must be one of .+ but got: some-other-party-id")
  }

  @Test
  fun getCertificateReturnsCertificate() = runBlockingTest {
    prepareRootCertificate(MODEL_PROVIDER_ID)
    prepareSharedStorageCertificate(EXCHANGE_DATE_KEY)

    assertThat(certificateManager.getCertificate(EXCHANGE_DATE_KEY, CERT_NAME))
      .isEqualTo(CERTIFICATE)
  }

  @Test
  fun getCertificateReturnsCachedCertificate() = runBlockingTest {
    prepareRootCertificate(MODEL_PROVIDER_ID)
    prepareSharedStorageCertificate(EXCHANGE_DATE_KEY)

    val first = certificateManager.getCertificate(EXCHANGE_DATE_KEY, CERT_NAME)
    val second = certificateManager.getCertificate(EXCHANGE_DATE_KEY, CERT_NAME)

    assertThat(second).isEqualTo(first)
    verify(rootSharedStorage, times(1)).getBlob(any())
  }

  @Test
  fun getCertificateUsesCertNameAsCacheKey() = runBlockingTest {
    val otherCertName = "$DATA_PROVIDER_ID:${UUID.randomUUID()}"
    prepareRootCertificate(DATA_PROVIDER_ID)
    prepareRootCertificate(MODEL_PROVIDER_ID)
    prepareSharedStorageCertificate(EXCHANGE_DATE_KEY, CERT_NAME)
    prepareSharedStorageCertificate(EXCHANGE_DATE_KEY, otherCertName)

    certificateManager.getCertificate(EXCHANGE_DATE_KEY, CERT_NAME)
    certificateManager.getCertificate(EXCHANGE_DATE_KEY, otherCertName)

    verify(rootSharedStorage, times(2)).getBlob(any())
  }

  @Test
  fun getExchangePrivateKeyReturnsPrivateKey() = runBlockingTest {
    privateKeys.underlyingMap[EXCHANGE_DATE_KEY.path] =
      signingKeys {
          certName = CERT_NAME
          privateKey = PRIVATE_KEY.encoded.toByteString()
        }
        .toByteString()

    assertThat(certificateManager.getExchangePrivateKey(EXCHANGE_DATE_KEY)).isEqualTo(PRIVATE_KEY)
  }

  @Test
  fun getPartnerRootCertificateReturnsRootCertificate() = runBlockingTest {
    prepareRootCertificate(DATA_PROVIDER_ID)

    assertThat(certificateManager.getPartnerRootCertificate(DATA_PROVIDER_ID))
      .isEqualTo(ROOT_CERTIFICATE)
  }

  @Test
  fun getPartnerRootCertificateThrowsForMissingPartner() = runBlockingTest {
    val exception =
      assertFailsWith<IllegalArgumentException> {
        certificateManager.getPartnerRootCertificate(DATA_PROVIDER_ID)
      }
    assertThat(exception)
      .hasMessageThat()
      .contains("Missing root certificate for $DATA_PROVIDER_ID")
  }

  @Test
  fun createForExchangeCreatesCertificate() = runBlockingTest {
    prepareRootCertificate(MODEL_PROVIDER_ID)

    val certName = certificateManager.createForExchange(EXCHANGE_DATE_KEY)
    val certKey = CertificateKey.fromCertName(certName)
    assertThat(certKey?.ownerId).isEqualTo(MODEL_PROVIDER_ID)

    val privateKey = certificateManager.getExchangePrivateKey(EXCHANGE_DATE_KEY)
    assertThat(privateKey).isEqualTo(PRIVATE_KEY)

    val x509 = certificateManager.getCertificate(EXCHANGE_DATE_KEY, certName)
    assertThat(x509).isEqualTo(CERTIFICATE)
  }

  @Test
  fun createForExchangeReturnsExistingCertOnCacheHit() = runBlockingTest {
    privateKeys.underlyingMap[EXCHANGE_DATE_KEY.path] =
      signingKeys {
          certName = CERT_NAME
          privateKey = PRIVATE_KEY.encoded.toByteString()
        }
        .toByteString()

    val certName = certificateManager.createForExchange(EXCHANGE_DATE_KEY)

    assertThat(certName).isEqualTo(CERT_NAME)
    verify(rootSharedStorage, never()).writeBlob(any<String>(), any<Flow<ByteString>>())
    verify(rootSharedStorage, never()).getBlob(any())
  }

  @Test
  fun certificateKeyFromCertNameReturnsNullForInvalidName() {
    assertThat(CertificateKey.fromCertName(DATA_PROVIDER_ID)).isNull()
  }

  @Test
  fun certificateKeyFromCertNameReturnsCorrectKey() {
    val certKey = CertificateKey(ownerId = DATA_PROVIDER_ID, uuid = UUID.randomUUID().toString())

    val parsedCertKey = CertificateKey.fromCertName(certKey.certName)

    assertThat(parsedCertKey).isEqualTo(certKey)
  }

  private fun prepareRootCertificate(ownerId: String) {
    rootCerts.underlyingMap[ownerId] = ROOT_CERTIFICATE.encoded.toByteString()
  }

  private suspend fun prepareSharedStorageCertificate(
    exchangeDateKey: ExchangeDateKey,
    certName: String = CERT_NAME,
  ) {
    val sharedStorage = buildSharedStorage(exchangeDateKey)
    sharedStorage.writeBlob("certificates/$certName", STORED_CERTIFICATE.toByteString())
  }

  private fun buildSharedStorage(exchangeDateKey: ExchangeDateKey): StorageClient {
    val (recurringExchangeId, date) = exchangeDateKey
    return PrefixedStorageClient(
      delegate = rootSharedStorage,
      prefix = "recurringExchanges/$recurringExchangeId/exchanges/$date",
    )
  }

  private open class MockableForwardingStorageClient(delegate: StorageClient) :
    StorageClient by delegate

  companion object {

    private const val RECURRING_EXCHANGE_ID = "some-recurring-exchange-id"
    private const val DATA_PROVIDER_ID = "some-data-provider-id"
    private const val MODEL_PROVIDER_ID = "some-model-provider-id"

    private val WORKFLOW = exchangeWorkflow {
      exchangeIdentifiers = exchangeIdentifiers {
        dataProviderId = DATA_PROVIDER_ID
        modelProviderId = MODEL_PROVIDER_ID
      }
    }

    private val ROOT_CERTIFICATE by lazy { readCertificate(TestData.FIXED_CA_CERT_PEM_FILE) }
    private val CERTIFICATE by lazy { readCertificate(TestData.FIXED_SERVER_CERT_PEM_FILE) }
    private val PRIVATE_KEY by lazy {
      readPrivateKey(TestData.FIXED_SERVER_KEY_FILE, CERTIFICATE.publicKey.algorithm)
    }

    private val STORED_CERTIFICATE by lazy {
      certificate { x509Der = CERTIFICATE.encoded.toByteString() }
    }

    private val DATE = LocalDate.parse("2024-07-01")
    private val EXCHANGE_DATE_KEY = ExchangeDateKey(RECURRING_EXCHANGE_ID, DATE)
    private val IDENTITY = Identity(id = MODEL_PROVIDER_ID, party = Party.MODEL_PROVIDER)
    private val CERT_NAME = "$MODEL_PROVIDER_ID:${UUID.randomUUID()}"
  }
}
