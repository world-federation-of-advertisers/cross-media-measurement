/*
 * Copyright 2023 The Cross-Media Measurement Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.wfanet.measurement.reporting.service.api.v2alpha

import com.google.common.truth.Truth.assertThat
import com.google.common.truth.extensions.proto.ProtoTruth.assertThat
import com.google.protobuf.ByteString
import com.google.protobuf.kotlin.toByteString
import io.grpc.ServerInterceptors
import io.grpc.Status
import io.grpc.StatusException
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
import org.mockito.kotlin.whenever
import org.wfanet.measurement.api.ApiKeyConstants
import org.wfanet.measurement.api.v2alpha.DataProviderKey
import org.wfanet.measurement.api.v2alpha.DataProvidersGrpcKt
import org.wfanet.measurement.api.v2alpha.DataProvidersGrpcKt.DataProvidersCoroutineImplBase
import org.wfanet.measurement.api.v2alpha.DataProvidersGrpcKt.DataProvidersCoroutineStub
import org.wfanet.measurement.api.v2alpha.DuchyKey
import org.wfanet.measurement.api.v2alpha.MeasurementConsumerKey
import org.wfanet.measurement.api.v2alpha.dataProvider
import org.wfanet.measurement.api.v2alpha.getDataProviderRequest
import org.wfanet.measurement.api.v2alpha.setMessage
import org.wfanet.measurement.api.v2alpha.signedMessage
import org.wfanet.measurement.api.v2alpha.withDataProviderPrincipal
import org.wfanet.measurement.common.crypto.readCertificate
import org.wfanet.measurement.common.crypto.testing.TestData
import org.wfanet.measurement.common.crypto.tink.loadPublicKey
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.common.grpc.testing.mockService
import org.wfanet.measurement.common.pack
import org.wfanet.measurement.common.testing.HeaderCapturingInterceptor
import org.wfanet.measurement.common.testing.verifyProtoArgument
import org.wfanet.measurement.config.reporting.measurementConsumerConfig
import org.wfanet.measurement.consent.client.common.toEncryptionPublicKey

@RunWith(JUnit4::class)
class DataProvidersServiceTest {
  private val publicKingdomDataProvidersMock: DataProvidersCoroutineImplBase = mockService {
    onBlocking { getDataProvider(any()) }.thenReturn(DATA_PROVIDER)
  }

  private val headerCapturingInterceptor = HeaderCapturingInterceptor()

  @get:Rule
  val grpcTestServerRule = GrpcTestServerRule {
    addService(
      ServerInterceptors.intercept(publicKingdomDataProvidersMock, headerCapturingInterceptor)
    )
  }

  private lateinit var service: DataProvidersService

  @Before
  fun initService() {
    service = DataProvidersService(DataProvidersCoroutineStub(grpcTestServerRule.channel))
  }

  @Test
  fun `getDataProvider returns dataProvider`() = runBlocking {
    val response =
      withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME, CONFIG) {
        runBlocking {
          service.getDataProvider(getDataProviderRequest { name = DATA_PROVIDER_NAME })
        }
      }

    assertThat(response).isEqualTo(DATA_PROVIDER)

    assertThat(
        headerCapturingInterceptor
          .captured(DataProvidersGrpcKt.getDataProviderMethod)
          .single()
          .get(ApiKeyConstants.API_AUTHENTICATION_KEY_METADATA_KEY)
      )
      .isEqualTo(API_AUTHENTICATION_KEY)

    verifyProtoArgument(
        publicKingdomDataProvidersMock,
        DataProvidersCoroutineImplBase::getDataProvider
      )
      .isEqualTo(getDataProviderRequest { name = DATA_PROVIDER_NAME })
  }

  @Test
  fun `getDataProvider throws UNAUTHENTICATED when principal isn't reporting`() {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withDataProviderPrincipal(DATA_PROVIDER_NAME) {
          runBlocking {
            service.getDataProvider(getDataProviderRequest { name = DATA_PROVIDER_NAME })
          }
        }
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.UNAUTHENTICATED)
    assertThat(exception.message).contains("No ReportingPrincipal")
  }

  @Test
  fun `getDataProvider throws UNAUTHENTICATED when principal is missing`() {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        runBlocking {
          service.getDataProvider(getDataProviderRequest { name = DATA_PROVIDER_NAME })
        }
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.UNAUTHENTICATED)
    assertThat(exception.message).contains("No ReportingPrincipal")
  }

  @Test
  fun `getDataProvider throws NOT_FOUND when kingdom returns not found`() {
    runBlocking {
      whenever(publicKingdomDataProvidersMock.getDataProvider(any()))
        .thenThrow(Status.NOT_FOUND.asRuntimeException())
    }

    val exception =
      assertFailsWith<StatusException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME, CONFIG) {
          runBlocking {
            service.getDataProvider(getDataProviderRequest { name = DATA_PROVIDER_NAME })
          }
        }
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
  }

  companion object {
    private const val API_AUTHENTICATION_KEY = "nR5QPN7ptx"
    private val CONFIG = measurementConsumerConfig { apiKey = API_AUTHENTICATION_KEY }
    private const val MEASUREMENT_CONSUMER_ID = "1234"
    private val MEASUREMENT_CONSUMER_NAME = MeasurementConsumerKey(MEASUREMENT_CONSUMER_ID).toName()

    private const val DATA_PROVIDER_ID = "1235"
    private val DATA_PROVIDER_NAME = DataProviderKey(DATA_PROVIDER_ID).toName()

    private val CERTIFICATE_NAME = "${DATA_PROVIDER_NAME}/certificates/AAAAAAAAAcg"
    private const val EXTERNAL_DUCHY_ID = "worker1"
    private val DUCHY_KEY = DuchyKey(EXTERNAL_DUCHY_ID)
    private val DUCHY_NAME = DUCHY_KEY.toName()
    private val DUCHY_NAMES = listOf(DUCHY_NAME)

    private val serverCertificate: X509Certificate =
      readCertificate(TestData.FIXED_SERVER_CERT_PEM_FILE)
    private val SERVER_CERTIFICATE_DER = serverCertificate.encoded.toByteString()

    private val ENCRYPTION_PUBLIC_KEY =
      loadPublicKey(TestData.FIXED_ENCRYPTION_PUBLIC_KEYSET).toEncryptionPublicKey()
    private val SIGNED_PUBLIC_KEY = signedMessage {
      setMessage(ENCRYPTION_PUBLIC_KEY.pack())
      signature = ByteString.copyFromUtf8("Fake signature of public key")
      signatureAlgorithmOid = "2.9999"
    }

    private val DATA_PROVIDER = dataProvider {
      name = DATA_PROVIDER_NAME
      certificate = CERTIFICATE_NAME
      certificateDer = SERVER_CERTIFICATE_DER
      publicKey = SIGNED_PUBLIC_KEY
      requiredDuchies += DUCHY_NAMES
    }
  }
}
