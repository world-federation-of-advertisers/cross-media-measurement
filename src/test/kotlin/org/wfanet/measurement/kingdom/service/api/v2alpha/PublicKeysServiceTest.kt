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
import io.grpc.Status
import io.grpc.StatusRuntimeException
import kotlin.test.assertFailsWith
import kotlinx.coroutines.runBlocking
import org.junit.Before
import org.junit.Rule
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.mockito.kotlin.any
import org.wfanet.measurement.api.Version
import org.wfanet.measurement.api.v2alpha.DataProviderCertificateKey
import org.wfanet.measurement.api.v2alpha.MeasurementConsumerCertificateKey
import org.wfanet.measurement.api.v2alpha.PublicKey
import org.wfanet.measurement.api.v2alpha.copy
import org.wfanet.measurement.api.v2alpha.publicKey
import org.wfanet.measurement.api.v2alpha.signedData
import org.wfanet.measurement.api.v2alpha.updatePublicKeyRequest
import org.wfanet.measurement.api.v2alpha.withDataProviderPrincipal
import org.wfanet.measurement.api.v2alpha.withMeasurementConsumerPrincipal
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.common.grpc.testing.mockService
import org.wfanet.measurement.common.identity.apiIdToExternalId
import org.wfanet.measurement.common.testing.verifyProtoArgument
import org.wfanet.measurement.internal.kingdom.PublicKeysGrpcKt
import org.wfanet.measurement.internal.kingdom.UpdatePublicKeyResponse
import org.wfanet.measurement.internal.kingdom.updatePublicKeyRequest as internalUpdatePublicKeyRequest

private const val DATA_PROVIDERS_NAME = "dataProviders/AAAAAAAAAHs"
private const val DATA_PROVIDERS_NAME_2 = "dataProviders/BBBBBBBBBHs"
private const val DATA_PROVIDERS_PUBLIC_KEY_NAME = "$DATA_PROVIDERS_NAME/publicKey"
private const val DATA_PROVIDERS_PUBLIC_KEY_NAME_2 = "$DATA_PROVIDERS_NAME_2/publicKey"
private const val DATA_PROVIDERS_CERTIFICATE_NAME = "$DATA_PROVIDERS_NAME/certificates/AAAAAAAAAHs"
private const val DATA_PROVIDERS_CERTIFICATE_NAME_2 =
  "$DATA_PROVIDERS_NAME_2/certificates/AAAAAAAAAHs"
private const val MEASUREMENT_CONSUMER_NAME = "measurementConsumers/AAAAAAAAAHs"
private const val MEASUREMENT_CONSUMER_PUBLIC_KEY_NAME = "$MEASUREMENT_CONSUMER_NAME/publicKey"
private const val MEASUREMENT_CONSUMER_PUBLIC_KEY_NAME_2 =
  "measurementConsumers/BBBBBBBBBHs/publicKey"
private const val MEASUREMENT_CONSUMER_CERTIFICATE_NAME =
  "$MEASUREMENT_CONSUMER_NAME/certificates/AAAAAAAAAHs"

@RunWith(JUnit4::class)
class PublicKeysServiceTest {
  private val internalPublicKeysMock: PublicKeysGrpcKt.PublicKeysCoroutineImplBase =
    mockService() {
      onBlocking { updatePublicKey(any()) }.thenReturn(UpdatePublicKeyResponse.getDefaultInstance())
    }

  @get:Rule val grpcTestServerRule = GrpcTestServerRule { addService(internalPublicKeysMock) }

  private lateinit var service: PublicKeysService

  @Before
  fun initService() {
    service =
      PublicKeysService(PublicKeysGrpcKt.PublicKeysCoroutineStub(grpcTestServerRule.channel))
  }

  @Test
  fun `updatePublicKey returns the same public key when caller is data provider`() {
    val request = updatePublicKeyRequest { publicKey = PUBLIC_KEY }

    val result =
      withDataProviderPrincipal(DATA_PROVIDERS_NAME) {
        runBlocking { service.updatePublicKey(request) }
      }

    val expected = PUBLIC_KEY

    verifyProtoArgument(
        internalPublicKeysMock,
        PublicKeysGrpcKt.PublicKeysCoroutineImplBase::updatePublicKey
      )
      .isEqualTo(
        internalUpdatePublicKeyRequest {
          val certificateKey = DataProviderCertificateKey.fromName(PUBLIC_KEY.certificate)!!
          externalDataProviderId = apiIdToExternalId(certificateKey.dataProviderId)
          externalCertificateId = apiIdToExternalId(certificateKey.certificateId)
          apiVersion = Version.V2_ALPHA.toString()
          publicKey = PUBLIC_KEY.publicKey.data
          publicKeySignature = PUBLIC_KEY.publicKey.signature
        }
      )

    assertThat(result).ignoringRepeatedFieldOrder().isEqualTo(expected)
  }

  @Test
  fun `updatePublicKey returns the same public key when caller is measurement consumer`() {
    val request = updatePublicKeyRequest {
      publicKey =
        PUBLIC_KEY.copy {
          name = MEASUREMENT_CONSUMER_PUBLIC_KEY_NAME
          certificate = MEASUREMENT_CONSUMER_CERTIFICATE_NAME
        }
    }

    val result =
      withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME) {
        runBlocking { service.updatePublicKey(request) }
      }

    val expected =
      PUBLIC_KEY.copy {
        name = MEASUREMENT_CONSUMER_PUBLIC_KEY_NAME
        certificate = MEASUREMENT_CONSUMER_CERTIFICATE_NAME
      }

    verifyProtoArgument(
        internalPublicKeysMock,
        PublicKeysGrpcKt.PublicKeysCoroutineImplBase::updatePublicKey
      )
      .isEqualTo(
        internalUpdatePublicKeyRequest {
          val certificateKey =
            MeasurementConsumerCertificateKey.fromName(request.publicKey.certificate)!!
          externalMeasurementConsumerId = apiIdToExternalId(certificateKey.measurementConsumerId)
          externalCertificateId = apiIdToExternalId(certificateKey.certificateId)
          apiVersion = Version.V2_ALPHA.toString()
          publicKey = PUBLIC_KEY.publicKey.data
          publicKeySignature = PUBLIC_KEY.publicKey.signature
        }
      )

    assertThat(result).ignoringRepeatedFieldOrder().isEqualTo(expected)
  }

  @Test
  fun `updatePublicKey throws UNAUTHENTICATED when principal is missing`() {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        runBlocking {
          service.updatePublicKey(
            updatePublicKeyRequest { publicKey = PUBLIC_KEY.copy { clearName() } }
          )
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.UNAUTHENTICATED)
  }

  @Test
  fun `updatePublicKey throws PERMISSION_DENIED when edp caller doesn't match name parent type`() {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withDataProviderPrincipal(DATA_PROVIDERS_NAME) {
          runBlocking {
            service.updatePublicKey(
              updatePublicKeyRequest {
                publicKey = PUBLIC_KEY.copy { name = MEASUREMENT_CONSUMER_PUBLIC_KEY_NAME }
              }
            )
          }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.PERMISSION_DENIED)
  }

  @Test
  fun `updatePublicKey throws PERMISSION_DENIED when mc caller doesn't match name parent type`() {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME) {
          runBlocking { service.updatePublicKey(updatePublicKeyRequest { publicKey = PUBLIC_KEY }) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.PERMISSION_DENIED)
  }

  @Test
  fun `updatePublicKey throws PERMISSION_DENIED when edp caller doesn't match name parent id`() {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withDataProviderPrincipal(DATA_PROVIDERS_NAME) {
          runBlocking {
            service.updatePublicKey(
              updatePublicKeyRequest {
                publicKey = PUBLIC_KEY.copy { name = DATA_PROVIDERS_PUBLIC_KEY_NAME_2 }
              }
            )
          }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.PERMISSION_DENIED)
  }

  @Test
  fun `updatePublicKey throws PERMISSION_DENIED when mc caller doesn't match name parent id`() {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME) {
          runBlocking {
            service.updatePublicKey(
              updatePublicKeyRequest {
                publicKey = PUBLIC_KEY.copy { name = MEASUREMENT_CONSUMER_PUBLIC_KEY_NAME_2 }
              }
            )
          }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.PERMISSION_DENIED)
  }

  @Test
  fun `updatePublicKey throws INVALID_ARGUMENT when resource name is missing`() {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withDataProviderPrincipal(DATA_PROVIDERS_NAME) {
          runBlocking {
            service.updatePublicKey(
              updatePublicKeyRequest { publicKey = PUBLIC_KEY.copy { clearName() } }
            )
          }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `updatePublicKey throws INVALID_ARGUMENT when EncryptionPublicKey is missing`() {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withDataProviderPrincipal(DATA_PROVIDERS_NAME) {
          runBlocking {
            service.updatePublicKey(
              updatePublicKeyRequest { publicKey = PUBLIC_KEY.copy { clearPublicKey() } }
            )
          }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `updatePublicKey throws INVALID_ARGUMENT when certificate is missing`() {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withDataProviderPrincipal(DATA_PROVIDERS_NAME) {
          runBlocking {
            service.updatePublicKey(
              updatePublicKeyRequest { publicKey = PUBLIC_KEY.copy { clearCertificate() } }
            )
          }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `updatePublicKey throws INVALID_ARGUMENT when name and certificate have diff parent types`() {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withDataProviderPrincipal(DATA_PROVIDERS_NAME) {
          runBlocking {
            service.updatePublicKey(
              updatePublicKeyRequest {
                publicKey =
                  PUBLIC_KEY.copy {
                    name = DATA_PROVIDERS_PUBLIC_KEY_NAME
                    certificate = MEASUREMENT_CONSUMER_CERTIFICATE_NAME
                  }
              }
            )
          }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `updatePublicKey throws INVALID_ARGUMENT when name and certificate have diff parent ids`() {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withDataProviderPrincipal(DATA_PROVIDERS_NAME) {
          runBlocking {
            service.updatePublicKey(
              updatePublicKeyRequest {
                publicKey =
                  PUBLIC_KEY.copy {
                    name = DATA_PROVIDERS_PUBLIC_KEY_NAME
                    certificate = DATA_PROVIDERS_CERTIFICATE_NAME_2
                  }
              }
            )
          }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }
}

private val PUBLIC_KEY: PublicKey = publicKey {
  name = DATA_PROVIDERS_PUBLIC_KEY_NAME
  publicKey = signedData {
    data = ByteString.copyFromUtf8("1")
    signature = ByteString.copyFromUtf8("1")
  }
  certificate = DATA_PROVIDERS_CERTIFICATE_NAME
}
