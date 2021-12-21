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

package org.wfanet.measurement.loadtest.resourcesetup

import com.google.protobuf.kotlin.toByteString
import kotlinx.coroutines.runBlocking
import org.junit.Rule
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.mockito.kotlin.UseConstructor
import org.mockito.kotlin.mock
import org.wfanet.measurement.api.v2alpha.CertificatesGrpcKt.CertificatesCoroutineImplBase
import org.wfanet.measurement.api.v2alpha.CertificatesGrpcKt.CertificatesCoroutineStub
import org.wfanet.measurement.api.v2alpha.CreateDataProviderRequest
import org.wfanet.measurement.api.v2alpha.CreateMeasurementConsumerRequest
import org.wfanet.measurement.api.v2alpha.DataProvidersGrpcKt.DataProvidersCoroutineImplBase
import org.wfanet.measurement.api.v2alpha.DataProvidersGrpcKt.DataProvidersCoroutineStub
import org.wfanet.measurement.api.v2alpha.MeasurementConsumersGrpcKt.MeasurementConsumersCoroutineImplBase
import org.wfanet.measurement.api.v2alpha.MeasurementConsumersGrpcKt.MeasurementConsumersCoroutineStub
import org.wfanet.measurement.common.crypto.testing.FIXED_ENCRYPTION_PUBLIC_KEYSET as ENCRYPTION_PUBLIC_KEYSET
import org.wfanet.measurement.common.crypto.testing.FIXED_SERVER_CERT_DER_FILE as CONSENT_SIGNAL_CERTIFICATE_DER_FILE
import org.wfanet.measurement.common.crypto.testing.FIXED_SERVER_KEY_DER_FILE as CONSENT_SIGNAL_PRIVATE_KEY_DER_FILE
import org.wfanet.measurement.common.crypto.testing.loadSigningKey
import org.wfanet.measurement.common.crypto.tink.testing.loadPublicKey
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.common.testing.verifyProtoArgument
import org.wfanet.measurement.consent.client.common.toEncryptionPublicKey

private const val RUN_ID = "run id"
private const val EDP_DISPLAY_NAME = "edp"
private const val MC_DISPLAY_NAME = "mc"
private const val MEASUREMENT_CONSUMER_CREATION_TOKEN = "MTIzNDU2NzM"
private const val ID_TOKEN = "token"

@RunWith(JUnit4::class)
class ResourceSetupImplTest {

  private val mockDataProvidersService: DataProvidersCoroutineImplBase =
    mock(useConstructor = UseConstructor.parameterless())
  private val mockMeasurementConsumersService: MeasurementConsumersCoroutineImplBase =
    mock(useConstructor = UseConstructor.parameterless())
  private val mockCertificatesService: CertificatesCoroutineImplBase =
    mock(useConstructor = UseConstructor.parameterless())

  @get:Rule
  val grpcTestServerRule = GrpcTestServerRule {
    addService(mockDataProvidersService)
    addService(mockMeasurementConsumersService)
    addService(mockCertificatesService)
  }

  private val dataProvidersStub: DataProvidersCoroutineStub by lazy {
    DataProvidersCoroutineStub(grpcTestServerRule.channel)
  }
  private val measurementConsumersStub: MeasurementConsumersCoroutineStub by lazy {
    MeasurementConsumersCoroutineStub(grpcTestServerRule.channel)
  }
  private val certificatesStub: CertificatesCoroutineStub by lazy {
    CertificatesCoroutineStub(grpcTestServerRule.channel)
  }

  private var resourceSetup: ResourceSetup =
    ResourceSetup(dataProvidersStub, certificatesStub, measurementConsumersStub, RUN_ID)

  @Test
  fun `create data provider successfully`() = runBlocking {
    val dataProviderContent =
      EntityContent(
        displayName = EDP_DISPLAY_NAME,
        signingKey =
          loadSigningKey(CONSENT_SIGNAL_CERTIFICATE_DER_FILE, CONSENT_SIGNAL_PRIVATE_KEY_DER_FILE),
        encryptionPublicKey = loadPublicKey(ENCRYPTION_PUBLIC_KEYSET).toEncryptionPublicKey()
      )

    resourceSetup.createDataProvider(dataProviderContent)

    verifyProtoArgument(
        mockDataProvidersService,
        DataProvidersCoroutineImplBase::createDataProvider
      )
      .comparingExpectedFieldsOnly()
      .isEqualTo(
        CreateDataProviderRequest.newBuilder()
          .apply {
            dataProviderBuilder.apply {
              certificateDer = dataProviderContent.signingKey.certificate.encoded.toByteString()
              publicKeyBuilder.apply {
                data = dataProviderContent.encryptionPublicKey.toByteString()
              }
              displayName = EDP_DISPLAY_NAME
            }
          }
          .build()
      )

    // TODO: call the client library to verifyPublicKey
  }

  @Test
  fun `create measurement consumer successfully`() = runBlocking {
    val measurementConsumerContent =
      EntityContent(
        displayName = MC_DISPLAY_NAME,
        signingKey =
          loadSigningKey(CONSENT_SIGNAL_CERTIFICATE_DER_FILE, CONSENT_SIGNAL_PRIVATE_KEY_DER_FILE),
        encryptionPublicKey = loadPublicKey(ENCRYPTION_PUBLIC_KEYSET).toEncryptionPublicKey()
      )

    resourceSetup.createMeasurementConsumer(
      measurementConsumerContent,
      MEASUREMENT_CONSUMER_CREATION_TOKEN,
      ID_TOKEN
    )

    verifyProtoArgument(
        mockMeasurementConsumersService,
        MeasurementConsumersCoroutineImplBase::createMeasurementConsumer
      )
      .comparingExpectedFieldsOnly()
      .isEqualTo(
        CreateMeasurementConsumerRequest.newBuilder()
          .apply {
            measurementConsumerBuilder.apply {
              certificateDer =
                measurementConsumerContent.signingKey.certificate.encoded.toByteString()
              publicKeyBuilder.apply {
                data = measurementConsumerContent.encryptionPublicKey.toByteString()
              }
              displayName = MC_DISPLAY_NAME
            }
          }
          .build()
      )

    // TODO: verify signature
  }
}
