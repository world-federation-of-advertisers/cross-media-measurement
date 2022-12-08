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
import org.wfanet.measurement.api.v2alpha.GetMeasurementConsumerRequest
import org.wfanet.measurement.api.v2alpha.MeasurementConsumer
import org.wfanet.measurement.api.v2alpha.MeasurementConsumerCertificateKey
import org.wfanet.measurement.api.v2alpha.addMeasurementConsumerOwnerRequest
import org.wfanet.measurement.api.v2alpha.copy
import org.wfanet.measurement.api.v2alpha.createMeasurementConsumerRequest
import org.wfanet.measurement.api.v2alpha.getMeasurementConsumerRequest
import org.wfanet.measurement.api.v2alpha.measurementConsumer
import org.wfanet.measurement.api.v2alpha.removeMeasurementConsumerOwnerRequest
import org.wfanet.measurement.api.v2alpha.signedData
import org.wfanet.measurement.api.v2alpha.testing.makeDataProvider
import org.wfanet.measurement.api.v2alpha.withDataProviderPrincipal
import org.wfanet.measurement.api.v2alpha.withMeasurementConsumerPrincipal
import org.wfanet.measurement.api.v2alpha.withModelProviderPrincipal
import org.wfanet.measurement.common.crypto.hashSha256
import org.wfanet.measurement.common.crypto.readCertificate
import org.wfanet.measurement.common.crypto.subjectKeyIdentifier
import org.wfanet.measurement.common.crypto.testing.TestData
import org.wfanet.measurement.common.crypto.tink.loadPublicKey
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.common.grpc.testing.mockService
import org.wfanet.measurement.common.identity.apiIdToExternalId
import org.wfanet.measurement.common.identity.externalIdToApiId
import org.wfanet.measurement.common.testing.verifyProtoArgument
import org.wfanet.measurement.common.toProtoTime
import org.wfanet.measurement.consent.client.common.toEncryptionPublicKey
import org.wfanet.measurement.internal.kingdom.Account
import org.wfanet.measurement.internal.kingdom.CertificateKt
import org.wfanet.measurement.internal.kingdom.MeasurementConsumer as InternalMeasurementConsumer
import org.wfanet.measurement.internal.kingdom.MeasurementConsumerKt.details
import org.wfanet.measurement.internal.kingdom.MeasurementConsumersGrpcKt.MeasurementConsumersCoroutineImplBase as InternalMeasurementConsumersService
import org.wfanet.measurement.internal.kingdom.MeasurementConsumersGrpcKt.MeasurementConsumersCoroutineStub as InternalMeasurementConsumersClient
import org.wfanet.measurement.internal.kingdom.account
import org.wfanet.measurement.internal.kingdom.addMeasurementConsumerOwnerRequest as internalAddMeasurementConsumerOwnerRequest
import org.wfanet.measurement.internal.kingdom.certificate
import org.wfanet.measurement.internal.kingdom.copy
import org.wfanet.measurement.internal.kingdom.createMeasurementConsumerRequest as internalCreateMeasurementConsumerRequest
import org.wfanet.measurement.internal.kingdom.getMeasurementConsumerRequest as internalGetMeasurementConsumerRequest
import org.wfanet.measurement.internal.kingdom.measurementConsumer as internalMeasurementConsumer
import org.wfanet.measurement.internal.kingdom.removeMeasurementConsumerOwnerRequest as internalRemoveMeasurementConsumerOwnerRequest

private const val MEASUREMENT_CONSUMER_ID = 123L
private const val ACCOUNT_ID = 123L
private const val CERTIFICATE_ID = 456L
private const val MEASUREMENT_CONSUMER_NAME = "measurementConsumers/AAAAAAAAAHs"
private const val MEASUREMENT_CONSUMER_NAME_2 = "measurementConsumers/BBBBBBBBBHs"
private const val ACCOUNT_NAME = "accounts/AAAAAAAAAHs"
private const val CERTIFICATE_NAME = "$MEASUREMENT_CONSUMER_NAME/certificates/AAAAAAAAAcg"
private const val MEASUREMENT_CONSUMER_CREATION_TOKEN = "MTIzNDU2NzM"

private val DATA_PROVIDER_NAME = makeDataProvider(123L)
private const val MODEL_PROVIDER_NAME = "modelProviders/AAAAAAAAAHs"

@RunWith(JUnit4::class)
class MeasurementConsumersServiceTest {
  private val internalServiceMock: InternalMeasurementConsumersService =
    mockService() {
      onBlocking { createMeasurementConsumer(any()) }.thenReturn(INTERNAL_MEASUREMENT_CONSUMER)
      onBlocking { getMeasurementConsumer(any()) }.thenReturn(INTERNAL_MEASUREMENT_CONSUMER)
      onBlocking { addMeasurementConsumerOwner(any()) }.thenReturn(INTERNAL_MEASUREMENT_CONSUMER)
      onBlocking { removeMeasurementConsumerOwner(any()) }.thenReturn(INTERNAL_MEASUREMENT_CONSUMER)
    }

  @get:Rule val grpcTestServerRule = GrpcTestServerRule { addService(internalServiceMock) }

  private lateinit var service: MeasurementConsumersService

  @Before
  fun initService() {
    service =
      MeasurementConsumersService(InternalMeasurementConsumersClient(grpcTestServerRule.channel))
  }

  @Test
  fun `create fills created resource names`() {
    val request = createMeasurementConsumerRequest {
      measurementConsumer = measurementConsumer {
        certificateDer = SERVER_CERTIFICATE_DER
        publicKey = SIGNED_PUBLIC_KEY
        measurementConsumerCreationToken = MEASUREMENT_CONSUMER_CREATION_TOKEN
      }
    }

    val createdMeasurementConsumer =
      withAccount(ACTIVATED_INTERNAL_ACCOUNT) {
        runBlocking { service.createMeasurementConsumer(request) }
      }

    val expectedMeasurementConsumer =
      request.measurementConsumer.copy {
        name = MEASUREMENT_CONSUMER_NAME
        certificate = CERTIFICATE_NAME
        clearMeasurementConsumerCreationToken()
      }
    assertThat(createdMeasurementConsumer).isEqualTo(expectedMeasurementConsumer)
    verifyProtoArgument(
        internalServiceMock,
        InternalMeasurementConsumersService::createMeasurementConsumer
      )
      .isEqualTo(
        internalCreateMeasurementConsumerRequest {
          measurementConsumer =
            INTERNAL_MEASUREMENT_CONSUMER.copy {
              clearExternalMeasurementConsumerId()
              certificate =
                certificate.copy {
                  clearExternalMeasurementConsumerId()
                  clearExternalCertificateId()
                }
            }
          externalAccountId = ACCOUNT_ID
          measurementConsumerCreationTokenHash =
            hashSha256(apiIdToExternalId(MEASUREMENT_CONSUMER_CREATION_TOKEN))
        }
      )
  }

  @Test
  fun `create throws UNAUTHENTICATED when account principal is missing`() {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        runBlocking { service.createMeasurementConsumer(createMeasurementConsumerRequest {}) }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.UNAUTHENTICATED)
  }

  @Test
  fun `create throws INVALID_ARGUMENT when certificate DER is missing`() {
    val request = createMeasurementConsumerRequest {
      measurementConsumer = measurementConsumer {
        publicKey = SIGNED_PUBLIC_KEY
        measurementConsumerCreationToken = MEASUREMENT_CONSUMER_CREATION_TOKEN
      }
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withAccount(ACTIVATED_INTERNAL_ACCOUNT) {
          runBlocking { service.createMeasurementConsumer(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `create throws INVALID_ARGUMENT when creation token is missing`() {
    val request = createMeasurementConsumerRequest {
      measurementConsumer = measurementConsumer {
        certificateDer = SERVER_CERTIFICATE_DER
        publicKey = SIGNED_PUBLIC_KEY
      }
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withAccount(ACTIVATED_INTERNAL_ACCOUNT) {
          runBlocking { service.createMeasurementConsumer(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `create throws INVALID_ARGUMENT when public key is missing`() {
    val request = createMeasurementConsumerRequest {
      measurementConsumer = measurementConsumer {
        certificateDer = SERVER_CERTIFICATE_DER
        measurementConsumerCreationToken = MEASUREMENT_CONSUMER_CREATION_TOKEN
      }
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withAccount(ACTIVATED_INTERNAL_ACCOUNT) {
          runBlocking { service.createMeasurementConsumer(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `get returns resource when mc caller is found`() {
    val measurementConsumer =
      withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME) {
        runBlocking {
          service.getMeasurementConsumer(
            getMeasurementConsumerRequest { name = MEASUREMENT_CONSUMER_NAME }
          )
        }
      }

    val expectedMeasurementConsumer = measurementConsumer {
      name = MEASUREMENT_CONSUMER_NAME
      certificate = CERTIFICATE_NAME
      certificateDer = SERVER_CERTIFICATE_DER
      publicKey = SIGNED_PUBLIC_KEY
    }
    assertThat(measurementConsumer).isEqualTo(expectedMeasurementConsumer)

    verifyProtoArgument(
        internalServiceMock,
        InternalMeasurementConsumersService::getMeasurementConsumer
      )
      .isEqualTo(
        internalGetMeasurementConsumerRequest {
          externalMeasurementConsumerId = MEASUREMENT_CONSUMER_ID
        }
      )
  }

  @Test
  fun `get returns resource when edp caller is found`() {
    val measurementConsumer =
      withDataProviderPrincipal(DATA_PROVIDER_NAME) {
        runBlocking {
          service.getMeasurementConsumer(
            getMeasurementConsumerRequest { name = MEASUREMENT_CONSUMER_NAME }
          )
        }
      }

    val expectedMeasurementConsumer = measurementConsumer {
      name = MEASUREMENT_CONSUMER_NAME
      certificate = CERTIFICATE_NAME
      certificateDer = SERVER_CERTIFICATE_DER
      publicKey = SIGNED_PUBLIC_KEY
    }
    assertThat(measurementConsumer).isEqualTo(expectedMeasurementConsumer)

    verifyProtoArgument(
        internalServiceMock,
        InternalMeasurementConsumersService::getMeasurementConsumer
      )
      .isEqualTo(
        internalGetMeasurementConsumerRequest {
          externalMeasurementConsumerId = MEASUREMENT_CONSUMER_ID
        }
      )
  }

  @Test
  fun `get throws INVALID_ARGUMENT when name is missing`() {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME) {
          runBlocking {
            service.getMeasurementConsumer(GetMeasurementConsumerRequest.getDefaultInstance())
          }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `get throws INVALID_ARGUMENT when name is invalid`() {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME) {
          runBlocking {
            service.getMeasurementConsumer(getMeasurementConsumerRequest { name = "foo" })
          }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `get throws PERMISSION_DENIED when mc caller doesn't match`() {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME_2) {
          runBlocking {
            service.getMeasurementConsumer(
              getMeasurementConsumerRequest { name = MEASUREMENT_CONSUMER_NAME }
            )
          }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.PERMISSION_DENIED)
  }

  @Test
  fun `get throws PERMISSION_DENIED when principal without authorization found`() {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withModelProviderPrincipal(MODEL_PROVIDER_NAME) {
          runBlocking {
            service.getMeasurementConsumer(
              getMeasurementConsumerRequest { name = MEASUREMENT_CONSUMER_NAME }
            )
          }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.PERMISSION_DENIED)
  }

  @Test
  fun `get throws UNAUTHENTICATED when no principal is found`() {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        runBlocking {
          service.getMeasurementConsumer(
            getMeasurementConsumerRequest { name = MEASUREMENT_CONSUMER_NAME }
          )
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.UNAUTHENTICATED)
  }

  @Test
  fun `addMeasurementConsumerOwner throws UNAUTHENTICATED when account principal is missing`() {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        runBlocking { service.addMeasurementConsumerOwner(addMeasurementConsumerOwnerRequest {}) }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.UNAUTHENTICATED)
  }

  @Test
  fun `addMeasurementConsumerOwner throws INVALID_ARGUMENT when name is invalid`() {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withAccount(ACTIVATED_INTERNAL_ACCOUNT) {
          runBlocking {
            service.addMeasurementConsumerOwner(
              addMeasurementConsumerOwnerRequest {
                name = "foo"
                account = ACCOUNT_NAME
              }
            )
          }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `addMeasurementConsumerOwner throws PERMISSION_DENIED when account doesnt own mc`() {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withAccount(
          ACTIVATED_INTERNAL_ACCOUNT.copy { externalOwnedMeasurementConsumerIds.clear() }
        ) {
          runBlocking {
            service.addMeasurementConsumerOwner(
              addMeasurementConsumerOwnerRequest {
                name = MEASUREMENT_CONSUMER_NAME
                account = ACCOUNT_NAME
              }
            )
          }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.PERMISSION_DENIED)
  }

  @Test
  fun `addMeasurementConsumerOwner throws INVALID_ARGUMENT when account is invalid`() {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withAccount(ACTIVATED_INTERNAL_ACCOUNT) {
          runBlocking {
            service.addMeasurementConsumerOwner(
              addMeasurementConsumerOwnerRequest {
                name = MEASUREMENT_CONSUMER_NAME
                account = "foo"
              }
            )
          }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `addMeasurementConsumerOwner adds new owner and returns measurement consumer`() {
    val result =
      withAccount(ACTIVATED_INTERNAL_ACCOUNT) {
        runBlocking {
          service.addMeasurementConsumerOwner(
            addMeasurementConsumerOwnerRequest {
              name = MEASUREMENT_CONSUMER_NAME
              account = ACCOUNT_NAME
            }
          )
        }
      }
    assertThat(result).isEqualTo(MEASUREMENT_CONSUMER)

    verifyProtoArgument(
        internalServiceMock,
        InternalMeasurementConsumersService::addMeasurementConsumerOwner
      )
      .isEqualTo(
        internalAddMeasurementConsumerOwnerRequest {
          externalMeasurementConsumerId = MEASUREMENT_CONSUMER_ID
          externalAccountId = ACCOUNT_ID
        }
      )
  }

  @Test
  fun `removeMeasurementConsumerOwner throws UNAUTHENTICATED when account principal is missing`() {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        runBlocking {
          service.removeMeasurementConsumerOwner(removeMeasurementConsumerOwnerRequest {})
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.UNAUTHENTICATED)
  }

  @Test
  fun `removeMeasurementConsumerOwner throws INVALID_ARGUMENT when name is invalid`() {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withAccount(ACTIVATED_INTERNAL_ACCOUNT) {
          runBlocking {
            service.removeMeasurementConsumerOwner(
              removeMeasurementConsumerOwnerRequest {
                name = "foo"
                account = ACCOUNT_NAME
              }
            )
          }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `removeMeasurementConsumerOwner throws PERMISSION_DENIED when account doesnt own mc`() {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withAccount(
          ACTIVATED_INTERNAL_ACCOUNT.copy { externalOwnedMeasurementConsumerIds.clear() }
        ) {
          runBlocking {
            service.removeMeasurementConsumerOwner(
              removeMeasurementConsumerOwnerRequest {
                name = MEASUREMENT_CONSUMER_NAME
                account = ACCOUNT_NAME
              }
            )
          }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.PERMISSION_DENIED)
  }

  @Test
  fun `removeMeasurementConsumerOwner throws INVALID_ARGUMENT when account is invalid`() {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withAccount(ACTIVATED_INTERNAL_ACCOUNT) {
          runBlocking {
            service.removeMeasurementConsumerOwner(
              removeMeasurementConsumerOwnerRequest {
                name = MEASUREMENT_CONSUMER_NAME
                account = "foo"
              }
            )
          }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `removeMeasurementConsumerOwner removes owner and returns measurement consumer`() {
    val result =
      withAccount(ACTIVATED_INTERNAL_ACCOUNT) {
        runBlocking {
          service.removeMeasurementConsumerOwner(
            removeMeasurementConsumerOwnerRequest {
              name = MEASUREMENT_CONSUMER_NAME
              account = ACCOUNT_NAME
            }
          )
        }
      }
    assertThat(result).isEqualTo(MEASUREMENT_CONSUMER)

    verifyProtoArgument(
        internalServiceMock,
        InternalMeasurementConsumersService::removeMeasurementConsumerOwner
      )
      .isEqualTo(
        internalRemoveMeasurementConsumerOwnerRequest {
          externalMeasurementConsumerId = MEASUREMENT_CONSUMER_ID
          externalAccountId = ACCOUNT_ID
        }
      )
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

    private val INTERNAL_MEASUREMENT_CONSUMER: InternalMeasurementConsumer =
      internalMeasurementConsumer {
        externalMeasurementConsumerId = MEASUREMENT_CONSUMER_ID
        details = details {
          apiVersion = Version.V2_ALPHA.string
          publicKey = SIGNED_PUBLIC_KEY.data
          publicKeySignature = SIGNED_PUBLIC_KEY.signature
        }
        certificate = certificate {
          externalMeasurementConsumerId = MEASUREMENT_CONSUMER_ID
          externalCertificateId = CERTIFICATE_ID
          subjectKeyIdentifier = serverCertificate.subjectKeyIdentifier!!
          notValidBefore = serverCertificate.notBefore.toInstant().toProtoTime()
          notValidAfter = serverCertificate.notAfter.toInstant().toProtoTime()
          details = CertificateKt.details { x509Der = SERVER_CERTIFICATE_DER }
        }
      }

    private val MEASUREMENT_CONSUMER: MeasurementConsumer = measurementConsumer {
      val measurementConsumerId: String = externalIdToApiId(MEASUREMENT_CONSUMER_ID)
      val certificateId: String = externalIdToApiId(CERTIFICATE_ID)

      name = MEASUREMENT_CONSUMER_NAME
      certificate = MeasurementConsumerCertificateKey(measurementConsumerId, certificateId).toName()
      certificateDer = INTERNAL_MEASUREMENT_CONSUMER.certificate.details.x509Der
      publicKey = signedData {
        data = INTERNAL_MEASUREMENT_CONSUMER.details.publicKey
        signature = INTERNAL_MEASUREMENT_CONSUMER.details.publicKeySignature
      }
    }

    private val ACTIVATED_INTERNAL_ACCOUNT: Account = account {
      externalAccountId = ACCOUNT_ID
      activationState = Account.ActivationState.ACTIVATED
      externalOwnedMeasurementConsumerId = MEASUREMENT_CONSUMER_ID
      externalOwnedMeasurementConsumerIds += MEASUREMENT_CONSUMER_ID
    }
  }
}
