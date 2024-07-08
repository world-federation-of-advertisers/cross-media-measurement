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

import com.google.common.truth.Truth.assertThat
import com.google.common.truth.extensions.proto.ProtoTruth.assertThat
import com.google.protobuf.kotlin.toByteString
import java.time.LocalDate
import kotlin.test.assertFailsWith
import kotlinx.coroutines.runBlocking
import org.junit.Rule
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.mockito.kotlin.any
import org.mockito.kotlin.argumentCaptor
import org.mockito.kotlin.never
import org.mockito.kotlin.times
import org.mockito.kotlin.verify
import org.mockito.kotlin.verifyBlocking
import org.wfanet.measurement.api.v2alpha.CertificatesGrpcKt.CertificatesCoroutineImplBase
import org.wfanet.measurement.api.v2alpha.CertificatesGrpcKt.CertificatesCoroutineStub
import org.wfanet.measurement.api.v2alpha.CreateCertificateRequest
import org.wfanet.measurement.api.v2alpha.GetCertificateRequest
import org.wfanet.measurement.api.v2alpha.certificate
import org.wfanet.measurement.api.v2alpha.createCertificateRequest
import org.wfanet.measurement.api.v2alpha.getCertificateRequest
import org.wfanet.measurement.common.crypto.readCertificate
import org.wfanet.measurement.common.crypto.readPrivateKey
import org.wfanet.measurement.common.crypto.testing.TestData
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.common.grpc.testing.mockService
import org.wfanet.panelmatch.common.ExchangeDateKey
import org.wfanet.panelmatch.common.certificates.testing.TestCertificateAuthority
import org.wfanet.panelmatch.common.secrets.testing.TestMutableSecretMap
import org.wfanet.panelmatch.common.secrets.testing.TestSecretMap
import org.wfanet.panelmatch.common.testing.runBlockingTest

private const val LOCAL_NAME = "dataProviders/someDataProviderId"
private const val PARTNER_NAME = "modelProviders/someModelProviderId"

private val ROOT_CERTIFICATE by lazy { readCertificate(TestData.FIXED_CA_CERT_PEM_FILE) }
private val CERTIFICATE by lazy { readCertificate(TestData.FIXED_SERVER_CERT_PEM_FILE) }
private val PRIVATE_KEY by lazy {
  readPrivateKey(TestData.FIXED_SERVER_KEY_FILE, CERTIFICATE.publicKey.algorithm)
}

private val DATE = LocalDate.now()
private const val RECURRING_EXCHANGE_ID = "some-recurring-exchange-id"
private val EXCHANGE_DATE_KEY = ExchangeDateKey(RECURRING_EXCHANGE_ID, DATE)
private const val RESOURCE_NAME = "$LOCAL_NAME/certificates/someCertificateId"

private val API_CERTIFICATE = certificate {
  name = RESOURCE_NAME
  x509Der = CERTIFICATE.encoded.toByteString()
}

@RunWith(JUnit4::class)
class V2AlphaCertificateManagerTest {
  private val certificatesService: CertificatesCoroutineImplBase = mockService {
    onBlocking { getCertificate(any()) }.thenReturn(API_CERTIFICATE)
    onBlocking { createCertificate(any()) }.thenReturn(API_CERTIFICATE)
  }

  @get:Rule val grpcTestServerRule = GrpcTestServerRule { addService(certificatesService) }

  private val rootCerts = TestSecretMap()
  private val privateKeys = TestMutableSecretMap()

  private val certificateManager: CertificateManager by lazy {
    V2AlphaCertificateManager(
      CertificatesCoroutineStub(grpcTestServerRule.channel),
      rootCerts,
      privateKeys,
      PRIVATE_KEY.algorithm,
      TestCertificateAuthority,
      LOCAL_NAME,
    )
  }

  private fun setRootCertificate(partyName: String) {
    rootCerts.underlyingMap[partyName] = ROOT_CERTIFICATE.encoded.toByteString()
  }

  @Test
  fun getCertificate() = runBlockingTest {
    setRootCertificate(LOCAL_NAME)

    assertThat(certificateManager.getCertificate(EXCHANGE_DATE_KEY, RESOURCE_NAME))
      .isEqualTo(CERTIFICATE)

    // Call again to ensure that caching works. There should only be one API call.
    assertThat(certificateManager.getCertificate(EXCHANGE_DATE_KEY, RESOURCE_NAME))
      .isEqualTo(CERTIFICATE)

    verifyBlocking(certificatesService, times(1)) {
      argumentCaptor<GetCertificateRequest> {
        getCertificate(capture())
        assertThat(allValues).containsExactly(getCertificateRequest { name = RESOURCE_NAME })
      }
    }
  }

  @Test
  fun getCertificateUsesResourceNameAsCacheKey() = runBlockingTest {
    setRootCertificate(LOCAL_NAME)

    assertThat(certificateManager.getCertificate(EXCHANGE_DATE_KEY, RESOURCE_NAME))
      .isEqualTo(CERTIFICATE)

    val otherResourceName = "$LOCAL_NAME/certificates/anotherCertificateId"
    assertThat(certificateManager.getCertificate(EXCHANGE_DATE_KEY, otherResourceName))
      .isEqualTo(CERTIFICATE)

    verify(certificatesService, times(2)).getCertificate(any())
  }

  @Test
  fun getExchangePrivateKey() = runBlockingTest {
    privateKeys.underlyingMap[EXCHANGE_DATE_KEY.path] =
      signingKeys {
          certName = RESOURCE_NAME
          privateKey = PRIVATE_KEY.encoded.toByteString()
        }
        .toByteString()
    assertThat(certificateManager.getExchangePrivateKey(EXCHANGE_DATE_KEY)).isEqualTo(PRIVATE_KEY)
  }

  @Test
  fun getPartnerRootCertificate() = runBlockingTest {
    setRootCertificate(PARTNER_NAME)

    assertThat(certificateManager.getPartnerRootCertificate(PARTNER_NAME))
      .isEqualTo(ROOT_CERTIFICATE)
  }

  @Test
  fun getPartnerRootCertificateForMissingPartner() = runBlockingTest {
    assertFailsWith<IllegalArgumentException> {
      certificateManager.getPartnerRootCertificate(PARTNER_NAME)
    }
  }

  @Test
  fun createForExchange() = runBlockingTest {
    setRootCertificate(LOCAL_NAME)

    assertThat(certificateManager.createForExchange(EXCHANGE_DATE_KEY)).isEqualTo(RESOURCE_NAME)
    val privateKey = certificateManager.getExchangePrivateKey(EXCHANGE_DATE_KEY)
    assertThat(privateKey).isEqualTo(PRIVATE_KEY)

    val x509 = certificateManager.getCertificate(EXCHANGE_DATE_KEY, RESOURCE_NAME)
    assertThat(x509).isEqualTo(CERTIFICATE)

    verifyBlocking(certificatesService) {
      argumentCaptor<CreateCertificateRequest> {
        createCertificate(capture())
        assertThat(allValues)
          .containsExactly(
            createCertificateRequest {
              parent = LOCAL_NAME
              certificate = certificate { x509Der = x509.encoded.toByteString() }
            }
          )
      }
    }

    verify(certificatesService, times(0)).getCertificate(any())
  }

  @Test
  fun createForExchangeCacheHit() {
    privateKeys.underlyingMap[EXCHANGE_DATE_KEY.path] =
      signingKeys {
          certName = RESOURCE_NAME
          privateKey = PRIVATE_KEY.encoded.toByteString()
        }
        .toByteString()

    val response = runBlocking { certificateManager.createForExchange(EXCHANGE_DATE_KEY) }
    assertThat(response).isEqualTo(RESOURCE_NAME)

    verifyBlocking(certificatesService, never()) { createCertificate(any()) }
    verifyBlocking(certificatesService, never()) { getCertificate(any()) }
  }
}
