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

package org.wfanet.measurement.kingdom.service.api.v2alpha

import com.google.common.truth.Truth.assertThat
import com.google.common.truth.extensions.proto.ProtoTruth.assertThat
import com.google.protobuf.ByteString
import com.google.protobuf.kotlin.toByteString
import io.grpc.Status
import io.grpc.StatusRuntimeException
import java.security.cert.X509Certificate
import kotlin.test.assertFailsWith
import kotlinx.coroutines.runBlocking
import org.junit.Before
import org.junit.Rule
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.mockito.kotlin.any
import org.wfanet.measurement.api.Version
import org.wfanet.measurement.api.v2alpha.dataProvider
import org.wfanet.measurement.api.v2alpha.getDataProviderRequest
import org.wfanet.measurement.api.v2alpha.signedData
import org.wfanet.measurement.api.v2alpha.testing.makeDataProvider
import org.wfanet.measurement.api.v2alpha.withDataProviderPrincipal
import org.wfanet.measurement.api.v2alpha.withMeasurementConsumerPrincipal
import org.wfanet.measurement.api.v2alpha.withModelProviderPrincipal
import org.wfanet.measurement.common.crypto.readCertificate
import org.wfanet.measurement.common.crypto.subjectKeyIdentifier
import org.wfanet.measurement.common.crypto.testing.TestData
import org.wfanet.measurement.common.crypto.tink.loadPublicKey
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.common.grpc.testing.mockService
import org.wfanet.measurement.common.testing.verifyProtoArgument
import org.wfanet.measurement.common.toProtoTime
import org.wfanet.measurement.consent.client.common.toEncryptionPublicKey
import org.wfanet.measurement.internal.kingdom.CertificateKt
import org.wfanet.measurement.internal.kingdom.DataProvider as InternalDataProvider
import org.wfanet.measurement.internal.kingdom.DataProviderKt.details
import org.wfanet.measurement.internal.kingdom.DataProvidersGrpcKt.DataProvidersCoroutineImplBase as InternalDataProvidersService
import org.wfanet.measurement.internal.kingdom.DataProvidersGrpcKt.DataProvidersCoroutineStub as InternalDataProvidersClient
import org.wfanet.measurement.internal.kingdom.certificate as internalCertificate
import org.wfanet.measurement.internal.kingdom.dataProvider as internalDataProvider
import org.wfanet.measurement.internal.kingdom.getDataProviderRequest as internalGetDataProviderRequest

private const val DATA_PROVIDER_ID = 123L
private const val DATA_PROVIDER_ID_2 = 124L
private const val CERTIFICATE_ID = 456L

private val DATA_PROVIDER_NAME = makeDataProvider(DATA_PROVIDER_ID)
private val DATA_PROVIDER_NAME_2 = makeDataProvider(DATA_PROVIDER_ID_2)
private val CERTIFICATE_NAME = "$DATA_PROVIDER_NAME/certificates/AAAAAAAAAcg"
private const val MEASUREMENT_CONSUMER_NAME = "measurementConsumers/AAAAAAAAAHs"
private const val MODEL_PROVIDER_NAME = "modelProviders/AAAAAAAAAHs"

@RunWith(JUnit4::class)
class DataProvidersServiceTest {
  private val internalServiceMock: InternalDataProvidersService =
    mockService() { onBlocking { getDataProvider(any()) }.thenReturn(INTERNAL_DATA_PROVIDER) }

  @get:Rule val grpcTestServerRule = GrpcTestServerRule { addService(internalServiceMock) }

  private lateinit var service: DataProvidersService

  @Before
  fun initService() {
    service = DataProvidersService(InternalDataProvidersClient(grpcTestServerRule.channel))
  }

  @Test
  fun `get with edp caller returns resource`() {
    val dataProvider =
      withDataProviderPrincipal(DATA_PROVIDER_NAME) {
        runBlocking {
          service.getDataProvider(getDataProviderRequest { name = DATA_PROVIDER_NAME })
        }
      }

    val expectedDataProvider = dataProvider {
      name = DATA_PROVIDER_NAME
      certificate = CERTIFICATE_NAME
      certificateDer = SERVER_CERTIFICATE_DER
      publicKey = SIGNED_PUBLIC_KEY
    }
    assertThat(dataProvider).isEqualTo(expectedDataProvider)
    verifyProtoArgument(internalServiceMock, InternalDataProvidersService::getDataProvider)
      .isEqualTo(internalGetDataProviderRequest { externalDataProviderId = DATA_PROVIDER_ID })
  }

  @Test
  fun `get with mc caller returns resource`() {
    val dataProvider =
      withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME) {
        runBlocking {
          service.getDataProvider(getDataProviderRequest { name = DATA_PROVIDER_NAME })
        }
      }

    val expectedDataProvider = dataProvider {
      name = DATA_PROVIDER_NAME
      certificate = CERTIFICATE_NAME
      certificateDer = SERVER_CERTIFICATE_DER
      publicKey = SIGNED_PUBLIC_KEY
    }
    assertThat(dataProvider).isEqualTo(expectedDataProvider)
    verifyProtoArgument(internalServiceMock, InternalDataProvidersService::getDataProvider)
      .isEqualTo(internalGetDataProviderRequest { externalDataProviderId = DATA_PROVIDER_ID })
  }

  @Test
  fun `get throws UNAUTHENTICATED when no principal found`() {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        runBlocking {
          service.getDataProvider(getDataProviderRequest { name = DATA_PROVIDER_NAME })
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.UNAUTHENTICATED)
  }

  @Test
  fun `get throws PERMISSION_DENIED when edp caller doesn't match`() {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withDataProviderPrincipal(DATA_PROVIDER_NAME_2) {
          runBlocking {
            service.getDataProvider(getDataProviderRequest { name = DATA_PROVIDER_NAME })
          }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.PERMISSION_DENIED)
  }

  @Test
  fun `get throws PERMISSION_DENIED when principal with no authorization found`() {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withModelProviderPrincipal(MODEL_PROVIDER_NAME) {
          runBlocking {
            service.getDataProvider(getDataProviderRequest { name = DATA_PROVIDER_NAME })
          }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.PERMISSION_DENIED)
  }

  @Test
  fun `get throws INVALID_ARGUMENT when name is missing`() {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withDataProviderPrincipal(DATA_PROVIDER_NAME) {
          runBlocking { service.getDataProvider(getDataProviderRequest {}) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `get throws INVALID_ARGUMENT when name is invalid`() {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withDataProviderPrincipal(DATA_PROVIDER_NAME) {
          runBlocking { service.getDataProvider(getDataProviderRequest { name = "foo" }) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  companion object {
    private val serverCertificate: X509Certificate =
      readCertificate(TestData.FIXED_SERVER_CERT_PEM_FILE)
    private val SERVER_CERTIFICATE_DER = serverCertificate.encoded.toByteString()

    private val ENCRYPTION_PUBLIC_KEY =
      loadPublicKey(TestData.FIXED_ENCRYPTION_PUBLIC_KEYSET).toEncryptionPublicKey()
    private val SIGNED_PUBLIC_KEY = signedData {
      data = ENCRYPTION_PUBLIC_KEY.toByteString()
      signature = ByteString.copyFromUtf8("Fake signature of public key")
    }

    private val INTERNAL_DATA_PROVIDER: InternalDataProvider = internalDataProvider {
      externalDataProviderId = DATA_PROVIDER_ID
      details = details {
        apiVersion = Version.V2_ALPHA.string
        publicKey = SIGNED_PUBLIC_KEY.data
        publicKeySignature = SIGNED_PUBLIC_KEY.signature
      }
      certificate = internalCertificate {
        externalDataProviderId = DATA_PROVIDER_ID
        externalCertificateId = CERTIFICATE_ID
        subjectKeyIdentifier = serverCertificate.subjectKeyIdentifier!!
        notValidBefore = serverCertificate.notBefore.toInstant().toProtoTime()
        notValidAfter = serverCertificate.notAfter.toInstant().toProtoTime()
        details = CertificateKt.details { x509Der = SERVER_CERTIFICATE_DER }
      }
    }
  }
}
