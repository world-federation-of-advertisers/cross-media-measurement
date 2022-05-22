// Copyright 2022 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.kingdom.service.api.v2alpha.tools

import com.google.common.truth.Truth.assertThat
import com.google.common.truth.extensions.proto.ProtoTruth.assertThat
import com.google.protobuf.timestamp
import io.grpc.ServerServiceDefinition
import io.netty.handler.ssl.ClientAuth
import java.io.File
import java.nio.file.Path
import java.nio.file.Paths
import kotlinx.coroutines.runBlocking
import org.junit.BeforeClass
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.mockito.kotlin.any
import org.mockito.kotlin.verify
import org.wfanet.measurement.api.v2alpha.CreateMeasurementRequest
import org.wfanet.measurement.api.v2alpha.DataProvidersGrpcKt.DataProvidersCoroutineImplBase
import org.wfanet.measurement.api.v2alpha.Measurement
import org.wfanet.measurement.api.v2alpha.MeasurementConsumersGrpcKt.MeasurementConsumersCoroutineImplBase
import org.wfanet.measurement.api.v2alpha.MeasurementSpec
import org.wfanet.measurement.api.v2alpha.MeasurementSpecKt
import org.wfanet.measurement.api.v2alpha.MeasurementSpecKt.reachAndFrequency
import org.wfanet.measurement.api.v2alpha.MeasurementsGrpcKt.MeasurementsCoroutineImplBase
import org.wfanet.measurement.api.v2alpha.RequisitionSpec
import org.wfanet.measurement.api.v2alpha.RequisitionSpecKt.EventGroupEntryKt.value as eventGroupEntryValue
import org.wfanet.measurement.api.v2alpha.RequisitionSpecKt.eventFilter
import org.wfanet.measurement.api.v2alpha.RequisitionSpecKt.eventGroupEntry
import org.wfanet.measurement.api.v2alpha.dataProvider
import org.wfanet.measurement.api.v2alpha.differentialPrivacyParams
import org.wfanet.measurement.api.v2alpha.measurementConsumer
import org.wfanet.measurement.api.v2alpha.measurementSpec
import org.wfanet.measurement.api.v2alpha.requisitionSpec
import org.wfanet.measurement.api.v2alpha.signedData
import org.wfanet.measurement.api.v2alpha.timeInterval
import org.wfanet.measurement.common.crypto.SigningCerts
import org.wfanet.measurement.common.crypto.readCertificate
import org.wfanet.measurement.common.crypto.tink.testing.loadPrivateKey
import org.wfanet.measurement.common.getRuntimePath
import org.wfanet.measurement.common.grpc.CommonServer
import org.wfanet.measurement.common.grpc.testing.mockService
import org.wfanet.measurement.common.readByteString
import org.wfanet.measurement.common.testing.captureFirst
import org.wfanet.measurement.consent.client.dataprovider.decryptRequisitionSpec
import org.wfanet.measurement.consent.client.dataprovider.verifyMeasurementSpec
import org.wfanet.measurement.consent.client.dataprovider.verifyRequisitionSpec
import picocli.CommandLine

private const val HOST = "localhost"
private const val PORT = 15788
private val SECRETS_DIR: Path =
  getRuntimePath(
    Paths.get(
      "wfa_measurement_system",
      "src",
      "main",
      "k8s",
      "testing",
      "secretfiles",
    )
  )!!

private const val MEASUREMENT_CONSUMER_NAME = "measurementConsumers/1"
private const val MEASUREMENT_CONSUMER_CERTIFICATE_NAME = "measurementConsumers/1/certificates/1"
private val MEASUREMENT_CONSUMER_CERTIFICATE_DER =
  SECRETS_DIR.resolve("mc_cs_cert.der").toFile().readByteString()
private val MEASUREMENT_CONSUMER_CERTIFICATE = readCertificate(MEASUREMENT_CONSUMER_CERTIFICATE_DER)
private val MEASUREMENT_PUBLIC_KEY =
  SECRETS_DIR.resolve("mc_enc_public.tink").toFile().readByteString()

private const val DATA_PROVIDER_NAME = "dataProviders/1"
private const val DATA_PROVIDER_CERTIFICATE_NAME = "dataProviders/1/certificates/1"
private val DATA_PROVIDER_PUBLIC_KEY =
  SECRETS_DIR.resolve("edp1_enc_public.tink").toFile().readByteString()
private val DATA_PROVIDER_PRIVATE_KEY_HANDLE =
  loadPrivateKey(SECRETS_DIR.resolve("edp1_enc_private.tink").toFile())

private val MEASUREMENT_CONSUMER = measurementConsumer {
  name = MEASUREMENT_CONSUMER_NAME
  certificateDer = MEASUREMENT_CONSUMER_CERTIFICATE_DER
  certificate = MEASUREMENT_CONSUMER_CERTIFICATE_NAME
  publicKey = signedData { data = MEASUREMENT_PUBLIC_KEY }
}

private val DATA_PROVIDER = dataProvider {
  name = DATA_PROVIDER_NAME
  certificate = DATA_PROVIDER_CERTIFICATE_NAME
  publicKey = signedData { data = DATA_PROVIDER_PUBLIC_KEY }
}

private val MEASUREMENT = Measurement.getDefaultInstance()

@RunWith(JUnit4::class)
class SimpleReportTest {
  companion object {
    private val measurementConsumersServiceMock: MeasurementConsumersCoroutineImplBase =
      mockService() {
        onBlocking { getMeasurementConsumer(any()) }.thenReturn(MEASUREMENT_CONSUMER)
      }
    private val dataProvidersServiceMock: DataProvidersCoroutineImplBase =
      mockService() { onBlocking { getDataProvider(any()) }.thenReturn(DATA_PROVIDER) }
    private val measurementsServiceMock: MeasurementsCoroutineImplBase =
      mockService() { onBlocking { createMeasurement(any()) }.thenReturn(MEASUREMENT) }

    @BeforeClass
    @JvmStatic
    fun initServer() {
      val services: List<ServerServiceDefinition> =
        listOf(
          measurementConsumersServiceMock.bindService(),
          dataProvidersServiceMock.bindService(),
          measurementsServiceMock.bindService(),
        )

      val serverCerts =
        SigningCerts.fromPemFiles(
          certificateFile = File("$SECRETS_DIR/kingdom_tls.pem"),
          privateKeyFile = File("$SECRETS_DIR/kingdom_tls.key"),
          trustedCertCollectionFile = File("$SECRETS_DIR/mc_root.pem")
        )

      CommonServer.fromParameters(
          PORT,
          true,
          serverCerts,
          ClientAuth.REQUIRE,
          "kingdom-test",
          services
        )
        .start()
    }
  }

  @Test
  fun `Create command call public api with valid Measurement`() {
    val input =
      """
      --tls-cert-file=$SECRETS_DIR/mc_tls.pem
      --tls-key-file=$SECRETS_DIR/mc_tls.key
      --cert-collection-file=$SECRETS_DIR/kingdom_root.pem
      --api-target=$HOST:$PORT
      --api-cert-host=localhost
      create
      --measurement-consumer-name=measurementConsumers/777
      --private-key-der-file=$SECRETS_DIR/mc_cs_private.der
      --measurement-ref-id=9999
      --data-provider-name=dataProviders/1
      --event-group-name=dataProviders/1/eventGroups/1 --event-filter-expression=abcd
      --event-filter-start-time=100 --event-filter-end-time=200
      --event-group-name=dataProviders/1/eventGroups/2 --event-filter-expression=efgh
      --event-filter-start-time=300 --event-filter-end-time=400
      --data-provider-name=dataProviders/2
      --event-group-name=dataProviders/2/eventGroups/1 --event-filter-expression=ijk
      --event-filter-start-time=400 --event-filter-end-time=500
      """.trimIndent()

    val args = input.split(" ", "\n").toTypedArray()
    CommandLine(SimpleReport()).execute(*args)

    val request =
      captureFirst<CreateMeasurementRequest> {
        runBlocking { verify(measurementsServiceMock).createMeasurement(capture()) }
      }
    val measurement = request.measurement
    // measurementSpec matches
    val measurementSpec = MeasurementSpec.parseFrom(measurement.measurementSpec.data)
    assertThat(
        verifyMeasurementSpec(
          measurement.measurementSpec.signature,
          measurementSpec,
          MEASUREMENT_CONSUMER_CERTIFICATE
        )
      )
      .isTrue()
    val nonceHashes = measurement.dataProvidersList.map { it.value.nonceHash }
    assertThat(measurementSpec)
      .isEqualTo(
        measurementSpec {
          measurementPublicKey = MEASUREMENT_PUBLIC_KEY
          this.nonceHashes.addAll(nonceHashes)
          reachAndFrequency = reachAndFrequency {
            reachPrivacyParams = differentialPrivacyParams {
              epsilon = 1.0
              delta = 1.0
            }
            frequencyPrivacyParams = differentialPrivacyParams {
              epsilon = 1.0
              delta = 1.0
            }
            vidSamplingInterval =
              MeasurementSpecKt.vidSamplingInterval {
                start = 0.0f
                width = 1.0f
              }
          }
        }
      )
    assertThat(measurement.measurementReferenceId).isEqualTo("9999")
    // dataProvider1 matches
    val dataProviderEntry1 = measurement.dataProvidersList[0]
    assertThat(dataProviderEntry1.key).isEqualTo("dataProviders/1")
    val signedRequisitionSpec1 =
      decryptRequisitionSpec(
        dataProviderEntry1.value.encryptedRequisitionSpec,
        DATA_PROVIDER_PRIVATE_KEY_HANDLE
      )
    val requisitionSpec1 = RequisitionSpec.parseFrom(signedRequisitionSpec1.data)
    assertThat(
        verifyRequisitionSpec(
          signedRequisitionSpec1.signature,
          requisitionSpec1,
          measurementSpec,
          MEASUREMENT_CONSUMER_CERTIFICATE
        )
      )
      .isTrue()
    assertThat(requisitionSpec1)
      .comparingExpectedFieldsOnly()
      .isEqualTo(
        requisitionSpec {
          measurementPublicKey = MEASUREMENT_PUBLIC_KEY
          eventGroups.add(
            eventGroupEntry {
              key = "dataProviders/1/eventGroups/1"
              value = eventGroupEntryValue {
                collectionInterval = timeInterval {
                  startTime = timestamp { seconds = 100 }
                  endTime = timestamp { seconds = 200 }
                }
                filter = eventFilter { expression = "abcd" }
              }
            }
          )
          eventGroups.add(
            eventGroupEntry {
              key = "dataProviders/1/eventGroups/2"
              value = eventGroupEntryValue {
                collectionInterval = timeInterval {
                  startTime = timestamp { seconds = 300 }
                  endTime = timestamp { seconds = 400 }
                }
                filter = eventFilter { expression = "efgh" }
              }
            }
          )
        }
      )
    // dataProvider2 matches
    val dataProviderEntry2 = measurement.dataProvidersList[1]
    assertThat(dataProviderEntry2.key).isEqualTo("dataProviders/2")
    val signedRequisitionSpec2 =
      decryptRequisitionSpec(
        dataProviderEntry2.value.encryptedRequisitionSpec,
        DATA_PROVIDER_PRIVATE_KEY_HANDLE
      )
    val requisitionSpec2 = RequisitionSpec.parseFrom(signedRequisitionSpec2.data)
    assertThat(
        verifyRequisitionSpec(
          signedRequisitionSpec2.signature,
          requisitionSpec2,
          measurementSpec,
          MEASUREMENT_CONSUMER_CERTIFICATE
        )
      )
      .isTrue()
    assertThat(requisitionSpec2)
      .comparingExpectedFieldsOnly()
      .isEqualTo(
        requisitionSpec {
          measurementPublicKey = MEASUREMENT_PUBLIC_KEY
          eventGroups.add(
            eventGroupEntry {
              key = "dataProviders/2/eventGroups/1"
              value = eventGroupEntryValue {
                collectionInterval = timeInterval {
                  startTime = timestamp { seconds = 400 }
                  endTime = timestamp { seconds = 500 }
                }
                filter = eventFilter { expression = "ijk" }
              }
            }
          )
        }
      )
  }
}
