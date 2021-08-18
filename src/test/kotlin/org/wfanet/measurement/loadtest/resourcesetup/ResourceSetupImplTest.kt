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

import com.google.common.truth.extensions.proto.ProtoTruth.assertThat
import java.security.cert.X509Certificate
import kotlin.test.assertTrue
import kotlinx.coroutines.runBlocking
import org.junit.BeforeClass
import org.junit.Rule
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.mockito.kotlin.UseConstructor
import org.mockito.kotlin.any
import org.mockito.kotlin.mock
import org.mockito.kotlin.whenever
import org.wfanet.measurement.api.v2alpha.CreateDataProviderRequest
import org.wfanet.measurement.api.v2alpha.CreateMeasurementConsumerRequest
import org.wfanet.measurement.api.v2alpha.DataProvidersGrpcKt.DataProvidersCoroutineImplBase
import org.wfanet.measurement.api.v2alpha.DataProvidersGrpcKt.DataProvidersCoroutineStub
import org.wfanet.measurement.api.v2alpha.EncryptionPublicKey
import org.wfanet.measurement.api.v2alpha.MeasurementConsumersGrpcKt.MeasurementConsumersCoroutineImplBase
import org.wfanet.measurement.api.v2alpha.MeasurementConsumersGrpcKt.MeasurementConsumersCoroutineStub
import org.wfanet.measurement.common.crypto.readCertificate
import org.wfanet.measurement.common.crypto.testing.FIXED_ENCRYPTION_PUBLIC_KEY_DER_FILE
import org.wfanet.measurement.common.crypto.testing.FIXED_SERVER_CERT_DER_FILE
import org.wfanet.measurement.common.crypto.testing.FIXED_SERVER_KEY_DER_FILE
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.common.toByteString
import org.wfanet.measurement.consent.crypto.keystore.testing.InMemoryKeyStore
import org.wfanet.measurement.consent.crypto.verifySignature

private const val RUN_ID = "run id"
private const val EDP_DISPLAY_NAME = "edp"
private const val MC_DISPLAY_NAME = "mc"
private val CONSENT_SIGNAL_PRIVATE_KEY_DER = FIXED_SERVER_KEY_DER_FILE.readBytes().toByteString()
private val CONSENT_SIGNAL_CERTIFICATE_DER = FIXED_SERVER_CERT_DER_FILE.readBytes().toByteString()
private val ENCRYPTION_PUBLIC_KEY_DER =
  FIXED_ENCRYPTION_PUBLIC_KEY_DER_FILE.readBytes().toByteString()
private val CONSENT_SIGNAL_X509: X509Certificate = readCertificate(CONSENT_SIGNAL_CERTIFICATE_DER)

private var KEY_STORE = InMemoryKeyStore()

@RunWith(JUnit4::class)
class ResourceSetupImplTest {

  private val mockDataProvidersService: DataProvidersCoroutineImplBase =
    mock(useConstructor = UseConstructor.parameterless())
  private val mockMeasurementConsumersService: MeasurementConsumersCoroutineImplBase =
    mock(useConstructor = UseConstructor.parameterless())

  @get:Rule
  val grpcTestServerRule = GrpcTestServerRule {
    addService(mockDataProvidersService)
    addService(mockMeasurementConsumersService)
  }

  private val dataProvidersStub: DataProvidersCoroutineStub by lazy {
    DataProvidersCoroutineStub(grpcTestServerRule.channel)
  }
  private val measurementConsumersStub: MeasurementConsumersCoroutineStub by lazy {
    MeasurementConsumersCoroutineStub(grpcTestServerRule.channel)
  }

  private var resourceSetup: ResourceSetup =
    ResourceSetup(KEY_STORE, dataProvidersStub, measurementConsumersStub, RUN_ID)

  @Test
  fun `create data provider successfully`() = runBlocking {
    val dataProviderContent =
      EntityContent(
        displayName = EDP_DISPLAY_NAME,
        consentSignalPrivateKeyDer = CONSENT_SIGNAL_PRIVATE_KEY_DER,
        consentSignalCertificateDer = CONSENT_SIGNAL_CERTIFICATE_DER,
        encryptionPublicKeyDer = ENCRYPTION_PUBLIC_KEY_DER
      )

    var apiRequest = CreateDataProviderRequest.getDefaultInstance()
    whenever(mockDataProvidersService.createDataProvider(any())).thenAnswer {
      apiRequest = it.getArgument(0)
      apiRequest.dataProvider
    }

    resourceSetup.createDataProvider(dataProviderContent)

    assertThat(apiRequest)
      .comparingExpectedFieldsOnly()
      .isEqualTo(
        CreateDataProviderRequest.newBuilder()
          .apply {
            dataProviderBuilder.apply {
              certificateDer = FIXED_SERVER_CERT_DER_FILE.readBytes().toByteString()
              publicKeyBuilder.apply {
                data =
                  EncryptionPublicKey.newBuilder()
                    .apply {
                      type = EncryptionPublicKey.Type.EC_P256
                      publicKeyInfo =
                        FIXED_ENCRYPTION_PUBLIC_KEY_DER_FILE.readBytes().toByteString()
                    }
                    .build()
                    .toByteString()
              }
              displayName = EDP_DISPLAY_NAME
            }
          }
          .build()
      )

    // TODO: call the client library to verifyPublicKey
    assertTrue(CONSENT_SIGNAL_X509.verifySignature(apiRequest.dataProvider.publicKey))
  }

  @Test
  fun `create measurement consumer successfully`() = runBlocking {
    val measurementConsumerContent =
      EntityContent(
        displayName = MC_DISPLAY_NAME,
        consentSignalPrivateKeyDer = CONSENT_SIGNAL_PRIVATE_KEY_DER,
        consentSignalCertificateDer = CONSENT_SIGNAL_CERTIFICATE_DER,
        encryptionPublicKeyDer = ENCRYPTION_PUBLIC_KEY_DER
      )

    var apiRequest = CreateMeasurementConsumerRequest.getDefaultInstance()
    whenever(mockMeasurementConsumersService.createMeasurementConsumer(any())).thenAnswer {
      apiRequest = it.getArgument(0)
      apiRequest.measurementConsumer
    }

    resourceSetup.createMeasurementConsumer(measurementConsumerContent)

    assertThat(apiRequest)
      .comparingExpectedFieldsOnly()
      .isEqualTo(
        CreateMeasurementConsumerRequest.newBuilder()
          .apply {
            measurementConsumerBuilder.apply {
              certificateDer = FIXED_SERVER_CERT_DER_FILE.readBytes().toByteString()
              publicKeyBuilder.apply {
                data =
                  EncryptionPublicKey.newBuilder()
                    .apply {
                      type = EncryptionPublicKey.Type.EC_P256
                      publicKeyInfo =
                        FIXED_ENCRYPTION_PUBLIC_KEY_DER_FILE.readBytes().toByteString()
                    }
                    .build()
                    .toByteString()
              }
              displayName = MC_DISPLAY_NAME
            }
          }
          .build()
      )

    assertTrue(CONSENT_SIGNAL_X509.verifySignature(apiRequest.measurementConsumer.publicKey))
  }

  companion object {
    @JvmStatic
    @BeforeClass
    fun writePrivateKeysToKeyStore() =
      runBlocking<Unit> {
        // Uses the same private key for EDP and MC in the test.
        KEY_STORE.storePrivateKeyDer(EDP_DISPLAY_NAME, CONSENT_SIGNAL_PRIVATE_KEY_DER)
        KEY_STORE.storePrivateKeyDer(MC_DISPLAY_NAME, CONSENT_SIGNAL_PRIVATE_KEY_DER)
      }
  }
}
