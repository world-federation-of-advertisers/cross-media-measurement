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
import org.wfanet.measurement.api.v2alpha.CreateDataProviderRequest
import org.wfanet.measurement.api.v2alpha.DataProvider
import org.wfanet.measurement.api.v2alpha.EncryptionPublicKey
import org.wfanet.measurement.api.v2alpha.GetDataProviderRequest
import org.wfanet.measurement.api.v2alpha.SignedData
import org.wfanet.measurement.common.crypto.readCertificate
import org.wfanet.measurement.common.crypto.readPrivateKey
import org.wfanet.measurement.common.crypto.subjectKeyIdentifier
import org.wfanet.measurement.common.getRuntimePath
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.common.testing.verifyProtoArgument
import org.wfanet.measurement.common.toProtoTime
import org.wfanet.measurement.internal.kingdom.DataProvider as InternalDataProvider
import org.wfanet.measurement.internal.kingdom.DataProvidersGrpcKt.DataProvidersCoroutineImplBase as InternalDataProvidersService
import org.wfanet.measurement.internal.kingdom.DataProvidersGrpcKt.DataProvidersCoroutineStub as InternalDataProvidersClient

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
private const val DATA_PROVIDER_ID = 123L
private const val CERTIFICATE_ID = 456L
private const val DATA_PROVIDER_NAME = "dataProviders/AAAAAAAAAHs"
private const val CERTIFICATE_NAME = "$DATA_PROVIDER_NAME/certificates/AAAAAAAAAcg"

@RunWith(JUnit4::class)
class DataProvidersServiceTest {
  private val internalServiceMock: InternalDataProvidersService =
    mock(useConstructor = UseConstructor.parameterless()) {
      onBlocking { createDataProvider(any()) }.thenReturn(INTERNAL_DATA_PROVIDER)
      onBlocking { getDataProvider(any()) }.thenReturn(INTERNAL_DATA_PROVIDER)
    }

  @get:Rule val grpcTestServerRule = GrpcTestServerRule { addService(internalServiceMock) }

  private lateinit var service: DataProvidersService

  @Before
  fun initService() {
    service = DataProvidersService(InternalDataProvidersClient(grpcTestServerRule.channel))
  }

  @Test
  fun `create fills created resource names`() {
    val request = buildCreateDataProviderRequest {
      dataProviderBuilder.apply {
        certificateDer = SERVER_CERTIFICATE_DER
        publicKey = SIGNED_PUBLIC_KEY
      }
    }

    val createdDataProvider = runBlocking { service.createDataProvider(request) }

    val expectedDataProvider =
      request.dataProvider.rebuild {
        name = DATA_PROVIDER_NAME
        certificate = CERTIFICATE_NAME
      }
    assertThat(createdDataProvider).isEqualTo(expectedDataProvider)
    verifyProtoArgument(internalServiceMock, InternalDataProvidersService::createDataProvider)
      .isEqualTo(
        INTERNAL_DATA_PROVIDER.rebuild {
          clearExternalDataProviderId()
          clearExternalPublicKeyCertificateId()
          preferredCertificate {
            clearExternalDataProviderId()
            clearExternalCertificateId()
          }
        }
      )
  }

  @Test
  fun `create throws INVALID_ARGUMENT when preferred certificate DER is missing`() {
    val request = buildCreateDataProviderRequest {
      dataProviderBuilder.apply { publicKey = SIGNED_PUBLIC_KEY }
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        runBlocking { service.createDataProvider(request) }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.status.description).isEqualTo("preferred_certificate_der is not specified")
  }

  @Test
  fun `create throws INVALID_ARGUMENT when public key is missing`() {
    val request = buildCreateDataProviderRequest {
      dataProviderBuilder.apply { certificateDer = SERVER_CERTIFICATE_DER }
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        runBlocking { service.createDataProvider(request) }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.status.description).isEqualTo("public_key is not fully specified")
  }

  @Test
  fun `get returns resource`() {
    val dataProvider = runBlocking {
      service.getDataProvider(buildGetDataProviderRequest { name = DATA_PROVIDER_NAME })
    }

    val expectedDataProvider = buildDataProvider {
      name = DATA_PROVIDER_NAME
      certificate = CERTIFICATE_NAME
      certificateDer = SERVER_CERTIFICATE_DER
      publicKey = SIGNED_PUBLIC_KEY
    }
    assertThat(dataProvider).isEqualTo(expectedDataProvider)
    verifyProtoArgument(internalServiceMock, InternalDataProvidersService::getDataProvider)
      .isEqualTo(buildInternalGetDataProviderRequest { externalDataProviderId = DATA_PROVIDER_ID })
  }

  @Test
  fun `get throws INVALID_ARGUMENT when name is missing`() {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        runBlocking { service.getDataProvider(GetDataProviderRequest.getDefaultInstance()) }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.status.description).isEqualTo("Resource name unspecified or invalid")
  }

  @Test
  fun `get throws INVALID_ARGUMENT when name is invalid`() {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        runBlocking { service.getDataProvider(buildGetDataProviderRequest { name = "foo" }) }
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

    private val INTERNAL_DATA_PROVIDER: InternalDataProvider = buildInternalDataProvider {
      externalDataProviderId = DATA_PROVIDER_ID
      externalPublicKeyCertificateId = CERTIFICATE_ID
      details {
        apiVersion = Version.V2_ALPHA.string
        publicKey = SIGNED_PUBLIC_KEY.data
        publicKeySignature = SIGNED_PUBLIC_KEY.signature
      }
      preferredCertificate {
        externalDataProviderId = DATA_PROVIDER_ID
        externalCertificateId = CERTIFICATE_ID
        subjectKeyIdentifier = serverCertificate.subjectKeyIdentifier
        notValidBefore = serverCertificate.notBefore.toInstant().toProtoTime()
        notValidAfter = serverCertificate.notAfter.toInstant().toProtoTime()
        detailsBuilder.x509Der = SERVER_CERTIFICATE_DER
      }
    }
  }
}

private inline fun buildCreateDataProviderRequest(
  fill: (@Builder CreateDataProviderRequest.Builder).() -> Unit
) = CreateDataProviderRequest.newBuilder().apply(fill).build()

private inline fun buildGetDataProviderRequest(
  fill: (@Builder GetDataProviderRequest.Builder).() -> Unit
) = GetDataProviderRequest.newBuilder().apply(fill).build()

private inline fun buildEncryptionPublicKey(
  fill: (@Builder EncryptionPublicKey.Builder).() -> Unit
) = EncryptionPublicKey.newBuilder().apply(fill).build()

private inline fun buildSignedData(fill: (@Builder SignedData.Builder).() -> Unit) =
  SignedData.newBuilder().apply(fill).build()

private inline fun DataProvider.rebuild(fill: (@Builder DataProvider.Builder).() -> Unit) =
  toBuilder().apply(fill).build()

private inline fun InternalDataProvider.rebuild(
  fill: (@Builder InternalDataProvider.Builder).() -> Unit
) = toBuilder().apply(fill).build()
