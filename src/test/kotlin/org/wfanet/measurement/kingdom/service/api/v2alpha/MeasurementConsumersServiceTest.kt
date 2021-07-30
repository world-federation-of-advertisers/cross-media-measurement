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
import org.wfanet.measurement.api.v2alpha.CreateMeasurementConsumerRequest
import org.wfanet.measurement.api.v2alpha.EncryptionPublicKey
import org.wfanet.measurement.api.v2alpha.GetMeasurementConsumerRequest
import org.wfanet.measurement.api.v2alpha.MeasurementConsumer
import org.wfanet.measurement.api.v2alpha.SignedData
import org.wfanet.measurement.common.crypto.readCertificate
import org.wfanet.measurement.common.crypto.readPrivateKey
import org.wfanet.measurement.common.crypto.subjectKeyIdentifier
import org.wfanet.measurement.common.getRuntimePath
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.common.testing.verifyProtoArgument
import org.wfanet.measurement.common.toProtoTime
import org.wfanet.measurement.internal.kingdom.MeasurementConsumer as InternalMeasurementConsumer
import org.wfanet.measurement.internal.kingdom.MeasurementConsumersGrpcKt.MeasurementConsumersCoroutineImplBase as InternalMeasurementConsumersService
import org.wfanet.measurement.internal.kingdom.MeasurementConsumersGrpcKt.MeasurementConsumersCoroutineStub as InternalMeasurementConsumersClient

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
private const val CERTIFICATE_ID = 456L
private const val MEASUREMENT_CONSUMER_NAME = "measurementConsumers/AAAAAAAAAHs"
private const val CERTIFICATE_NAME = "$MEASUREMENT_CONSUMER_NAME/certificates/AAAAAAAAAcg"

@RunWith(JUnit4::class)
class MeasurementConsumersServiceTest {
  private val internalServiceMock: InternalMeasurementConsumersService =
    mock(useConstructor = UseConstructor.parameterless()) {
      onBlocking { createMeasurementConsumer(any()) }.thenReturn(INTERNAL_MEASUREMENT_CONSUMER)
      onBlocking { getMeasurementConsumer(any()) }.thenReturn(INTERNAL_MEASUREMENT_CONSUMER)
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
    val request = buildCreateMeasurementConsumerRequest {
      measurementConsumerBuilder.apply {
        preferredCertificateDer = SERVER_CERTIFICATE_DER
        publicKey = SIGNED_PUBLIC_KEY
      }
    }

    val createdMeasurementConsumer = runBlocking { service.createMeasurementConsumer(request) }

    val expectedMeasurementConsumer =
      request.measurementConsumer.rebuild {
        name = MEASUREMENT_CONSUMER_NAME
        preferredCertificate = CERTIFICATE_NAME
      }
    assertThat(createdMeasurementConsumer).isEqualTo(expectedMeasurementConsumer)
    verifyProtoArgument(
        internalServiceMock,
        InternalMeasurementConsumersService::createMeasurementConsumer
      )
      .isEqualTo(
        INTERNAL_MEASUREMENT_CONSUMER.rebuild {
          clearExternalMeasurementConsumerId()
          clearExternalPublicKeyCertificateId()
          preferredCertificate {
            clearExternalMeasurementConsumerId()
            clearExternalCertificateId()
          }
        }
      )
  }

  @Test
  fun `create throws INVALID_ARGUMENT when preferred certificate DER is missing`() {
    val request = buildCreateMeasurementConsumerRequest {
      measurementConsumerBuilder.apply { publicKey = SIGNED_PUBLIC_KEY }
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        runBlocking { service.createMeasurementConsumer(request) }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.status.description).isEqualTo("preferred_certificate_der is not specified")
  }

  @Test
  fun `create throws INVALID_ARGUMENT when public key is missing`() {
    val request = buildCreateMeasurementConsumerRequest {
      measurementConsumerBuilder.apply { preferredCertificateDer = SERVER_CERTIFICATE_DER }
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        runBlocking { service.createMeasurementConsumer(request) }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.status.description).isEqualTo("public_key is not fully specified")
  }

  @Test
  fun `get returns resource`() {
    val measurementConsumer = runBlocking {
      service.getMeasurementConsumer(
        buildGetMeasurementConsumerRequest { name = MEASUREMENT_CONSUMER_NAME }
      )
    }

    val expectedMeasurementConsumer = buildMeasurementConsumer {
      name = MEASUREMENT_CONSUMER_NAME
      preferredCertificate = CERTIFICATE_NAME
      preferredCertificateDer = SERVER_CERTIFICATE_DER
      publicKey = SIGNED_PUBLIC_KEY
    }
    assertThat(measurementConsumer).isEqualTo(expectedMeasurementConsumer)
    verifyProtoArgument(
        internalServiceMock,
        InternalMeasurementConsumersService::getMeasurementConsumer
      )
      .isEqualTo(
        buildInternalGetMeasurementConsumerRequest {
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
          service.getMeasurementConsumer(buildGetMeasurementConsumerRequest { name = "foo" })
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.status.description).isEqualTo("Resource name unspecified or invalid")
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

    private val PUBLIC_KEY = buildEncryptionPublicKey {
      type = EncryptionPublicKey.Type.EC_P256
      publicKeyInfo = PUBLIC_KEY_DER
    }

    private val SIGNED_PUBLIC_KEY = buildSignedData {
      data = PUBLIC_KEY.toByteString()
      signature = ByteString.copyFromUtf8("Fake signature of public key")
    }

    private val INTERNAL_MEASUREMENT_CONSUMER: InternalMeasurementConsumer =
        buildInternalMeasurementConsumer {
      externalMeasurementConsumerId = MEASUREMENT_CONSUMER_ID
      externalPublicKeyCertificateId = CERTIFICATE_ID
      details {
        apiVersion = Version.V2_ALPHA.string
        publicKey = SIGNED_PUBLIC_KEY.data
        publicKeySignature = SIGNED_PUBLIC_KEY.signature
      }
      preferredCertificate {
        externalMeasurementConsumerId = MEASUREMENT_CONSUMER_ID
        externalCertificateId = CERTIFICATE_ID
        subjectKeyIdentifier = serverCertificate.subjectKeyIdentifier
        notValidBefore = serverCertificate.notBefore.toInstant().toProtoTime()
        notValidAfter = serverCertificate.notAfter.toInstant().toProtoTime()
        detailsBuilder.x509Der = SERVER_CERTIFICATE_DER
      }
    }
  }
}

private inline fun buildCreateMeasurementConsumerRequest(
  fill: (@Builder CreateMeasurementConsumerRequest.Builder).() -> Unit
) = CreateMeasurementConsumerRequest.newBuilder().apply(fill).build()

private inline fun buildGetMeasurementConsumerRequest(
  fill: (@Builder GetMeasurementConsumerRequest.Builder).() -> Unit
) = GetMeasurementConsumerRequest.newBuilder().apply(fill).build()

private inline fun buildEncryptionPublicKey(
  fill: (@Builder EncryptionPublicKey.Builder).() -> Unit
) = EncryptionPublicKey.newBuilder().apply(fill).build()

private inline fun buildSignedData(fill: (@Builder SignedData.Builder).() -> Unit) =
  SignedData.newBuilder().apply(fill).build()

private inline fun MeasurementConsumer.rebuild(
  fill: (@Builder MeasurementConsumer.Builder).() -> Unit
) = toBuilder().apply(fill).build()

private inline fun InternalMeasurementConsumer.rebuild(
  fill: (@Builder InternalMeasurementConsumer.Builder).() -> Unit
) = toBuilder().apply(fill).build()
