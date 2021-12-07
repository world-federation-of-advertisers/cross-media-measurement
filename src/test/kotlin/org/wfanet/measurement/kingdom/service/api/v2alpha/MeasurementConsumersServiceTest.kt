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
import java.nio.file.Path
import java.nio.file.Paths
import java.security.PrivateKey
import java.security.cert.X509Certificate
import kotlin.test.assertFailsWith
import kotlinx.coroutines.runBlocking
import org.junit.Before
import org.junit.Rule
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.mockito.kotlin.UseConstructor
import org.mockito.kotlin.any
import org.mockito.kotlin.mock
import org.wfanet.measurement.api.Version
import org.wfanet.measurement.api.v2alpha.EncryptionPublicKey
import org.wfanet.measurement.api.v2alpha.GetMeasurementConsumerRequest
import org.wfanet.measurement.api.v2alpha.MeasurementConsumer
import org.wfanet.measurement.api.v2alpha.MeasurementConsumerCertificateKey
import org.wfanet.measurement.api.v2alpha.addMeasurementConsumerOwnerRequest
import org.wfanet.measurement.api.v2alpha.copy
import org.wfanet.measurement.api.v2alpha.createMeasurementConsumerRequest
import org.wfanet.measurement.api.v2alpha.encryptionPublicKey
import org.wfanet.measurement.api.v2alpha.getMeasurementConsumerRequest
import org.wfanet.measurement.api.v2alpha.measurementConsumer
import org.wfanet.measurement.api.v2alpha.removeMeasurementConsumerOwnerRequest
import org.wfanet.measurement.api.v2alpha.signedData
import org.wfanet.measurement.common.crypto.readCertificate
import org.wfanet.measurement.common.crypto.readPrivateKey
import org.wfanet.measurement.common.crypto.subjectKeyIdentifier
import org.wfanet.measurement.common.getRuntimePath
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.common.identity.externalIdToApiId
import org.wfanet.measurement.common.testing.verifyProtoArgument
import org.wfanet.measurement.common.toProtoTime
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
import org.wfanet.measurement.internal.kingdom.getMeasurementConsumerRequest as internalGetMeasurementConsumerRequest
import org.wfanet.measurement.internal.kingdom.measurementConsumer as internalMeasurementConsumer
import org.wfanet.measurement.internal.kingdom.removeMeasurementConsumerOwnerRequest as internalRemoveMeasurementConsumerOwnerRequest

/**
 * Path to `testdata` directory containing certs and keys.
 *
 * TODO(@SanjayVas): Reference these files from the properties defined in common-jvm v0.2.0+.
 */
private val TESTDATA_DIR =
  Paths.get(
    "wfa_common_jvm",
    "src",
    "main",
    "kotlin",
    "org",
    "wfanet",
    "measurement",
    "common",
    "crypto",
    "testing",
    "testdata"
  )
private const val MEASUREMENT_CONSUMER_ID = 123L
private const val ACCOUNT_ID = 123L
private const val CERTIFICATE_ID = 456L
private const val MEASUREMENT_CONSUMER_NAME = "measurementConsumers/AAAAAAAAAHs"
private const val ACCOUNT_NAME = "accounts/AAAAAAAAAHs"
private const val CERTIFICATE_NAME = "$MEASUREMENT_CONSUMER_NAME/certificates/AAAAAAAAAcg"
private const val MEASUREMENT_CONSUMER_CREATION_TOKEN = 12345673L

@RunWith(JUnit4::class)
class MeasurementConsumersServiceTest {
  private val internalServiceMock: InternalMeasurementConsumersService =
    mock(useConstructor = UseConstructor.parameterless()) {
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
      measurementConsumer =
        measurementConsumer {
          certificateDer = SERVER_CERTIFICATE_DER
          publicKey = SIGNED_PUBLIC_KEY
        }
    }

    val createdMeasurementConsumer = runBlocking { service.createMeasurementConsumer(request) }

    val expectedMeasurementConsumer =
      request.measurementConsumer.copy {
        name = MEASUREMENT_CONSUMER_NAME
        certificate = CERTIFICATE_NAME
      }
    assertThat(createdMeasurementConsumer).isEqualTo(expectedMeasurementConsumer)
    verifyProtoArgument(
        internalServiceMock,
        InternalMeasurementConsumersService::createMeasurementConsumer
      )
      .isEqualTo(
        INTERNAL_MEASUREMENT_CONSUMER.copy {
          clearExternalMeasurementConsumerId()
          certificate =
            certificate.copy {
              clearExternalMeasurementConsumerId()
              clearExternalCertificateId()
            }
        }
      )
  }

  @Test
  fun `create throws INVALID_ARGUMENT when certificate DER is missing`() {
    val request = createMeasurementConsumerRequest {
      measurementConsumer = measurementConsumer { publicKey = SIGNED_PUBLIC_KEY }
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        runBlocking { service.createMeasurementConsumer(request) }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.status.description).isEqualTo("certificate_der is not specified")
  }

  @Test
  fun `create throws INVALID_ARGUMENT when public key is missing`() {
    val request = createMeasurementConsumerRequest {
      measurementConsumer = measurementConsumer { certificateDer = SERVER_CERTIFICATE_DER }
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        runBlocking { service.createMeasurementConsumer(request) }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.status.description).isEqualTo("public_key.data is missing")
  }

  @Test
  fun `get returns resource`() {
    val measurementConsumer = runBlocking {
      service.getMeasurementConsumer(
        getMeasurementConsumerRequest { name = MEASUREMENT_CONSUMER_NAME }
      )
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
        runBlocking {
          service.getMeasurementConsumer(GetMeasurementConsumerRequest.getDefaultInstance())
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.status.description).isEqualTo("Resource name unspecified or invalid")
  }

  @Test
  fun `get throws INVALID_ARGUMENT when name is invalid`() {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        runBlocking {
          service.getMeasurementConsumer(getMeasurementConsumerRequest { name = "foo" })
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.status.description).isEqualTo("Resource name unspecified or invalid")
  }

  @Test
  fun `addMeasurementConsumerOwner throws UNAUTHENTICATED when account principal is missing`() {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        runBlocking { service.addMeasurementConsumerOwner(addMeasurementConsumerOwnerRequest {}) }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.UNAUTHENTICATED)
    assertThat(exception.status.description).isEqualTo("Account credentials are invalid or missing")
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
    assertThat(exception.status.description).isEqualTo("Resource name unspecified or invalid")
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
    assertThat(exception.status.description).isEqualTo("Account doesn't own MeasurementConsumer")
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
    assertThat(exception.status.description).isEqualTo("Account unspecified or invalid")
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
    assertThat(exception.status.description).isEqualTo("Account credentials are invalid or missing")
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
    assertThat(exception.status.description).isEqualTo("Resource name unspecified or invalid")
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
    assertThat(exception.status.description).isEqualTo("Account doesn't own MeasurementConsumer")
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
    assertThat(exception.status.description).isEqualTo("Account unspecified or invalid")
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
    private val serverCertificate: X509Certificate
    init {
      val pemPath: Path = checkNotNull(getRuntimePath(TESTDATA_DIR.resolve("server.pem")))
      serverCertificate = readCertificate(pemPath.toFile())
    }
    private val SERVER_CERTIFICATE_DER = ByteString.copyFrom(serverCertificate.encoded)

    private val serverPrivateKey: PrivateKey
    init {
      val keyPath: Path = checkNotNull(getRuntimePath(TESTDATA_DIR.resolve("server.key")))
      serverPrivateKey = readPrivateKey(keyPath.toFile(), serverCertificate.publicKey.algorithm)
    }

    private val PUBLIC_KEY_DER: ByteString
    init {
      val publicKeyPath: Path = checkNotNull(getRuntimePath(TESTDATA_DIR.resolve("ec-public.der")))
      PUBLIC_KEY_DER = publicKeyPath.toFile().inputStream().use { ByteString.readFrom(it) }
    }

    private val PUBLIC_KEY = encryptionPublicKey {
      format = EncryptionPublicKey.Format.TINK_KEYSET
      data = PUBLIC_KEY_DER
    }

    private val SIGNED_PUBLIC_KEY = signedData {
      data = PUBLIC_KEY.toByteString()
      signature = ByteString.copyFromUtf8("Fake signature of public key")
    }

    private val INTERNAL_MEASUREMENT_CONSUMER: InternalMeasurementConsumer =
        internalMeasurementConsumer {
      externalMeasurementConsumerId = MEASUREMENT_CONSUMER_ID
      details =
        details {
          apiVersion = Version.V2_ALPHA.string
          publicKey = SIGNED_PUBLIC_KEY.data
          publicKeySignature = SIGNED_PUBLIC_KEY.signature
        }
      certificate =
        certificate {
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
      publicKey =
        signedData {
          data = INTERNAL_MEASUREMENT_CONSUMER.details.publicKey
          signature = INTERNAL_MEASUREMENT_CONSUMER.details.publicKeySignature
        }
    }

    private val ACTIVATED_INTERNAL_ACCOUNT: Account = account {
      externalAccountId = ACCOUNT_ID
      activationState = Account.ActivationState.ACTIVATED
      externalOwnedMeasurementConsumerId = MEASUREMENT_CONSUMER_ID
      measurementConsumerCreationToken = MEASUREMENT_CONSUMER_CREATION_TOKEN
      externalOwnedMeasurementConsumerIds += MEASUREMENT_CONSUMER_ID
    }
  }
}
