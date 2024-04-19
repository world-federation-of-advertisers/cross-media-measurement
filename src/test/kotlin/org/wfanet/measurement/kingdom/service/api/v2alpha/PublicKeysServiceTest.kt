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
import org.mockito.kotlin.stub
import org.wfanet.measurement.api.Version
import org.wfanet.measurement.api.v2alpha.DataProviderCertificateKey
import org.wfanet.measurement.api.v2alpha.DataProviderKey
import org.wfanet.measurement.api.v2alpha.MeasurementConsumerCertificateKey
import org.wfanet.measurement.api.v2alpha.MeasurementConsumerKey
import org.wfanet.measurement.api.v2alpha.PublicKey
import org.wfanet.measurement.api.v2alpha.copy
import org.wfanet.measurement.api.v2alpha.encryptionPublicKey
import org.wfanet.measurement.api.v2alpha.publicKey
import org.wfanet.measurement.api.v2alpha.signedMessage
import org.wfanet.measurement.api.v2alpha.updatePublicKeyRequest
import org.wfanet.measurement.api.v2alpha.withDataProviderPrincipal
import org.wfanet.measurement.api.v2alpha.withMeasurementConsumerPrincipal
import org.wfanet.measurement.common.grpc.errorInfo
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.common.grpc.testing.mockService
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.common.identity.apiIdToExternalId
import org.wfanet.measurement.common.identity.externalIdToApiId
import org.wfanet.measurement.common.pack
import org.wfanet.measurement.common.testing.verifyProtoArgument
import org.wfanet.measurement.internal.kingdom.PublicKeysGrpcKt
import org.wfanet.measurement.internal.kingdom.UpdatePublicKeyResponse
import org.wfanet.measurement.internal.kingdom.updatePublicKeyRequest as internalUpdatePublicKeyRequest
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.DataProviderNotFoundException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.MeasurementConsumerCertificateNotFoundException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.MeasurementConsumerNotFoundException

private const val EXTERNAL_DATA_PROVIDER_ID = 123L
private const val EXTERNAL_DATA_PROVIDER_ID_2 = 456L
private val DATA_PROVIDERS_NAME =
  DataProviderKey(externalIdToApiId(EXTERNAL_DATA_PROVIDER_ID)).toName()
private val DATA_PROVIDERS_NAME_2 =
  DataProviderKey(externalIdToApiId(EXTERNAL_DATA_PROVIDER_ID_2)).toName()
private val DATA_PROVIDERS_PUBLIC_KEY_NAME = "$DATA_PROVIDERS_NAME/publicKey"
private val DATA_PROVIDERS_PUBLIC_KEY_NAME_2 = "$DATA_PROVIDERS_NAME_2/publicKey"
private val DATA_PROVIDERS_CERTIFICATE_NAME = "$DATA_PROVIDERS_NAME/certificates/AAAAAAAAAHs"
private val DATA_PROVIDERS_CERTIFICATE_NAME_2 = "$DATA_PROVIDERS_NAME_2/certificates/AAAAAAAAAHs"
private const val EXTERNAL_MEASUREMENT_CONSUMER_ID = 123L
private val MEASUREMENT_CONSUMER_NAME =
  MeasurementConsumerKey(externalIdToApiId(EXTERNAL_MEASUREMENT_CONSUMER_ID)).toName()
private val MEASUREMENT_CONSUMER_PUBLIC_KEY_NAME = "$MEASUREMENT_CONSUMER_NAME/publicKey"
private const val MEASUREMENT_CONSUMER_PUBLIC_KEY_NAME_2 =
  "measurementConsumers/BBBBBBBBBHs/publicKey"
private val MEASUREMENT_CONSUMER_CERTIFICATE_NAME =
  "$MEASUREMENT_CONSUMER_NAME/certificates/AAAAAAAAAHs"

@RunWith(JUnit4::class)
class PublicKeysServiceTest {
  private val internalPublicKeysMock: PublicKeysGrpcKt.PublicKeysCoroutineImplBase = mockService {
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
  fun `updatePublicKey returns updated public key when caller is data provider`() {
    val request = updatePublicKeyRequest { publicKey = DATA_PROVIDER_PUBLIC_KEY }

    val result =
      withDataProviderPrincipal(DATA_PROVIDERS_NAME) {
        runBlocking { service.updatePublicKey(request) }
      }

    verifyProtoArgument(
        internalPublicKeysMock,
        PublicKeysGrpcKt.PublicKeysCoroutineImplBase::updatePublicKey,
      )
      .isEqualTo(
        internalUpdatePublicKeyRequest {
          val certificateKey =
            DataProviderCertificateKey.fromName(DATA_PROVIDER_PUBLIC_KEY.certificate)!!
          externalDataProviderId = apiIdToExternalId(certificateKey.dataProviderId)
          externalCertificateId = apiIdToExternalId(certificateKey.certificateId)
          apiVersion = Version.V2_ALPHA.toString()
          publicKey = DATA_PROVIDER_PUBLIC_KEY.publicKey.message.value
          publicKeySignature = DATA_PROVIDER_PUBLIC_KEY.publicKey.signature
          publicKeySignatureAlgorithmOid = DATA_PROVIDER_PUBLIC_KEY.publicKey.signatureAlgorithmOid
        }
      )
    assertThat(result).isEqualTo(DATA_PROVIDER_PUBLIC_KEY)
  }

  @Test
  fun `updatePublicKey returns updated public key when caller is measurement consumer`() {
    val request = updatePublicKeyRequest { publicKey = MEASUREMENT_CONSUMER_PUBLIC_KEY }

    val response =
      withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME) {
        runBlocking { service.updatePublicKey(request) }
      }

    verifyProtoArgument(
        internalPublicKeysMock,
        PublicKeysGrpcKt.PublicKeysCoroutineImplBase::updatePublicKey,
      )
      .isEqualTo(
        internalUpdatePublicKeyRequest {
          val certificateKey =
            MeasurementConsumerCertificateKey.fromName(request.publicKey.certificate)!!
          externalMeasurementConsumerId = apiIdToExternalId(certificateKey.measurementConsumerId)
          externalCertificateId = apiIdToExternalId(certificateKey.certificateId)
          apiVersion = Version.V2_ALPHA.toString()
          publicKey = DATA_PROVIDER_PUBLIC_KEY.publicKey.message.value
          publicKeySignature = DATA_PROVIDER_PUBLIC_KEY.publicKey.signature
          publicKeySignatureAlgorithmOid = DATA_PROVIDER_PUBLIC_KEY.publicKey.signatureAlgorithmOid
        }
      )
    assertThat(response).isEqualTo(MEASUREMENT_CONSUMER_PUBLIC_KEY)
  }

  @Test
  fun `updatePublicKey throws UNAUTHENTICATED when principal is missing`() {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        runBlocking {
          service.updatePublicKey(updatePublicKeyRequest { publicKey = DATA_PROVIDER_PUBLIC_KEY })
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
                publicKey =
                  DATA_PROVIDER_PUBLIC_KEY.copy { name = MEASUREMENT_CONSUMER_PUBLIC_KEY_NAME }
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
          runBlocking {
            service.updatePublicKey(updatePublicKeyRequest { publicKey = DATA_PROVIDER_PUBLIC_KEY })
          }
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
                publicKey =
                  DATA_PROVIDER_PUBLIC_KEY.copy { name = DATA_PROVIDERS_PUBLIC_KEY_NAME_2 }
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
                publicKey =
                  DATA_PROVIDER_PUBLIC_KEY.copy { name = MEASUREMENT_CONSUMER_PUBLIC_KEY_NAME_2 }
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
              updatePublicKeyRequest { publicKey = DATA_PROVIDER_PUBLIC_KEY.copy { clearName() } }
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
              updatePublicKeyRequest {
                publicKey = DATA_PROVIDER_PUBLIC_KEY.copy { clearPublicKey() }
              }
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
              updatePublicKeyRequest {
                publicKey = DATA_PROVIDER_PUBLIC_KEY.copy { clearCertificate() }
              }
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
                  DATA_PROVIDER_PUBLIC_KEY.copy {
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
                  DATA_PROVIDER_PUBLIC_KEY.copy {
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

  @Test
  fun `updatePublicKey throws FAILED_PRECONDITION with measurement consumer certificate name when certificate is not found`() {
    internalPublicKeysMock.stub {
      onBlocking { updatePublicKey(any()) }
        .thenThrow(
          MeasurementConsumerCertificateNotFoundException(
              ExternalId(EXTERNAL_MEASUREMENT_CONSUMER_ID),
              ExternalId(
                apiIdToExternalId(
                  MeasurementConsumerCertificateKey.fromName(
                      MEASUREMENT_CONSUMER_PUBLIC_KEY.certificate
                    )!!
                    .certificateId
                )
              ),
            )
            .asStatusRuntimeException(Status.Code.FAILED_PRECONDITION, "Certificate not found.")
        )
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME) {
          runBlocking {
            service.updatePublicKey(
              updatePublicKeyRequest { publicKey = MEASUREMENT_CONSUMER_PUBLIC_KEY }
            )
          }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.FAILED_PRECONDITION)
    assertThat(exception.errorInfo?.metadataMap)
      .containsEntry("measurementConsumerCertificate", MEASUREMENT_CONSUMER_CERTIFICATE_NAME)
  }

  @Test
  fun `updatePublicKey throws NOT_FOUND with data provider name when data provider not found`() {
    internalPublicKeysMock.stub {
      onBlocking { updatePublicKey(any()) }
        .thenThrow(
          DataProviderNotFoundException(ExternalId(EXTERNAL_DATA_PROVIDER_ID))
            .asStatusRuntimeException(Status.Code.NOT_FOUND, "DataProvider not found.")
        )
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withDataProviderPrincipal(DATA_PROVIDERS_NAME) {
          runBlocking {
            service.updatePublicKey(updatePublicKeyRequest { publicKey = DATA_PROVIDER_PUBLIC_KEY })
          }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
    assertThat(exception.errorInfo?.metadataMap).containsEntry("dataProvider", DATA_PROVIDERS_NAME)
  }

  @Test
  fun `updatePublicKey throws NOT_FOUND with measurement consumer name when mmeasurement consumer not found`() {
    internalPublicKeysMock.stub {
      onBlocking { updatePublicKey(any()) }
        .thenThrow(
          MeasurementConsumerNotFoundException(ExternalId(EXTERNAL_MEASUREMENT_CONSUMER_ID))
            .asStatusRuntimeException(Status.Code.NOT_FOUND, "MeasurementConsumer not found.")
        )
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME) {
          runBlocking {
            service.updatePublicKey(
              updatePublicKeyRequest { publicKey = MEASUREMENT_CONSUMER_PUBLIC_KEY }
            )
          }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
    assertThat(exception.errorInfo?.metadataMap)
      .containsEntry("measurementConsumer", MEASUREMENT_CONSUMER_NAME)
  }

  companion object {
    private val DATA_PROVIDER_PUBLIC_KEY: PublicKey = publicKey {
      name = DATA_PROVIDERS_PUBLIC_KEY_NAME
      publicKey = signedMessage {
        message = encryptionPublicKey { data = ByteString.copyFromUtf8("1") }.pack()
        signature = ByteString.copyFromUtf8("1")
        signatureAlgorithmOid = "2.9999"
      }
      certificate = DATA_PROVIDERS_CERTIFICATE_NAME
    }

    private val MEASUREMENT_CONSUMER_PUBLIC_KEY =
      DATA_PROVIDER_PUBLIC_KEY.copy {
        name = MEASUREMENT_CONSUMER_PUBLIC_KEY_NAME
        certificate = MEASUREMENT_CONSUMER_CERTIFICATE_NAME
      }
  }
}
