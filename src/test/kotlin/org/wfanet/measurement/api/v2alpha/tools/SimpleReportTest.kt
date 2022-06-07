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

package org.wfanet.measurement.api.v2alpha.tools

import com.google.common.truth.Truth.assertThat
import com.google.common.truth.extensions.proto.ProtoTruth.assertThat
import com.google.protobuf.ByteString
import com.google.protobuf.Timestamp
import com.google.protobuf.duration as protoDuration
import com.google.protobuf.timestamp
import io.grpc.ServerServiceDefinition
import io.netty.handler.ssl.ClientAuth
import java.nio.file.Path
import java.nio.file.Paths
import java.time.OffsetDateTime
import java.util.concurrent.TimeUnit.SECONDS
import kotlinx.coroutines.runBlocking
import org.junit.After
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.mockito.kotlin.any
import org.mockito.kotlin.verify
import org.wfanet.measurement.api.v2alpha.CertificatesGrpcKt
import org.wfanet.measurement.api.v2alpha.CreateMeasurementRequest
import org.wfanet.measurement.api.v2alpha.DataProvidersGrpcKt.DataProvidersCoroutineImplBase
import org.wfanet.measurement.api.v2alpha.EncryptionPublicKey
import org.wfanet.measurement.api.v2alpha.GetMeasurementRequest
import org.wfanet.measurement.api.v2alpha.ListMeasurementsRequest
import org.wfanet.measurement.api.v2alpha.Measurement
import org.wfanet.measurement.api.v2alpha.MeasurementConsumersGrpcKt.MeasurementConsumersCoroutineImplBase
import org.wfanet.measurement.api.v2alpha.MeasurementKt
import org.wfanet.measurement.api.v2alpha.MeasurementKt.ResultKt
import org.wfanet.measurement.api.v2alpha.MeasurementKt.failure
import org.wfanet.measurement.api.v2alpha.MeasurementKt.result
import org.wfanet.measurement.api.v2alpha.MeasurementKt.resultPair
import org.wfanet.measurement.api.v2alpha.MeasurementSpec
import org.wfanet.measurement.api.v2alpha.MeasurementSpecKt
import org.wfanet.measurement.api.v2alpha.MeasurementSpecKt.duration
import org.wfanet.measurement.api.v2alpha.MeasurementSpecKt.impression
import org.wfanet.measurement.api.v2alpha.MeasurementSpecKt.reachAndFrequency
import org.wfanet.measurement.api.v2alpha.MeasurementsGrpcKt.MeasurementsCoroutineImplBase
import org.wfanet.measurement.api.v2alpha.RequisitionSpec
import org.wfanet.measurement.api.v2alpha.RequisitionSpecKt.EventGroupEntryKt.value as eventGroupEntryValue
import org.wfanet.measurement.api.v2alpha.RequisitionSpecKt.eventFilter
import org.wfanet.measurement.api.v2alpha.RequisitionSpecKt.eventGroupEntry
import org.wfanet.measurement.api.v2alpha.certificate
import org.wfanet.measurement.api.v2alpha.dataProvider
import org.wfanet.measurement.api.v2alpha.differentialPrivacyParams
import org.wfanet.measurement.api.v2alpha.encryptionPublicKey
import org.wfanet.measurement.api.v2alpha.getMeasurementRequest
import org.wfanet.measurement.api.v2alpha.listMeasurementsRequest
import org.wfanet.measurement.api.v2alpha.listMeasurementsResponse
import org.wfanet.measurement.api.v2alpha.measurement
import org.wfanet.measurement.api.v2alpha.measurementConsumer
import org.wfanet.measurement.api.v2alpha.measurementSpec
import org.wfanet.measurement.api.v2alpha.requisitionSpec
import org.wfanet.measurement.api.v2alpha.signedData
import org.wfanet.measurement.api.v2alpha.timeInterval
import org.wfanet.measurement.common.crypto.SigningCerts
import org.wfanet.measurement.common.crypto.SigningKeyHandle
import org.wfanet.measurement.common.crypto.readCertificate
import org.wfanet.measurement.common.crypto.readPrivateKey
import org.wfanet.measurement.common.crypto.tink.testing.loadPrivateKey
import org.wfanet.measurement.common.getRuntimePath
import org.wfanet.measurement.common.grpc.CommonServer
import org.wfanet.measurement.common.grpc.testing.mockService
import org.wfanet.measurement.common.readByteString
import org.wfanet.measurement.common.testing.captureFirst
import org.wfanet.measurement.consent.client.dataprovider.decryptRequisitionSpec
import org.wfanet.measurement.consent.client.dataprovider.verifyMeasurementSpec
import org.wfanet.measurement.consent.client.dataprovider.verifyRequisitionSpec
import org.wfanet.measurement.consent.client.duchy.encryptResult
import org.wfanet.measurement.consent.client.duchy.signResult
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

private const val TIME_STRING_1 = "2022-05-22T01:00:00.000Z"
private const val TIME_STRING_2 = "2022-05-24T05:00:00.000Z"
private const val TIME_STRING_3 = "2022-05-22T01:22:32.122Z"
private const val TIME_STRING_4 = "2022-05-23T03:14:55.876Z"
private const val TIME_STRING_5 = "2022-04-22T01:19:42.567Z"
private const val TIME_STRING_6 = "2022-05-22T01:56:12.483Z"

private val AGGREGATOR_CERTIFICATE_DER =
  SECRETS_DIR.resolve("aggregator_cs_cert.der").toFile().readByteString()
private val AGGREGATOR_PRIVATE_KEY_DER =
  SECRETS_DIR.resolve("aggregator_cs_private.der").toFile().readByteString()
private val AGGREGATOR_SIGNING_KEY: SigningKeyHandle by lazy {
  val consentSignal509Cert = readCertificate(AGGREGATOR_CERTIFICATE_DER)
  SigningKeyHandle(
    consentSignal509Cert,
    readPrivateKey(AGGREGATOR_PRIVATE_KEY_DER, consentSignal509Cert.publicKey.algorithm)
  )
}
private val AGGREGATOR_CERTIFICATE = certificate { x509Der = AGGREGATOR_CERTIFICATE_DER }

private const val MEASUREMENT_NAME = "$MEASUREMENT_CONSUMER_NAME/measurements/100"
private val MEASUREMENT = measurement { name = MEASUREMENT_NAME }
private val SUCCEEDED_MEASUREMENT = measurement {
  name = MEASUREMENT_NAME
  state = Measurement.State.SUCCEEDED

  val measurementPublicKey = encryptionPublicKey {
    format = EncryptionPublicKey.Format.TINK_KEYSET
    data = MEASUREMENT_PUBLIC_KEY
  }
  results += resultPair {
    val result = result { reach = MeasurementKt.ResultKt.reach { value = 4096 } }
    encryptedResult = getEncryptedResult(result, measurementPublicKey)
    certificate = DATA_PROVIDER_CERTIFICATE_NAME
  }
  results += resultPair {
    val result = result {
      frequency =
        MeasurementKt.ResultKt.frequency {
          relativeFrequencyDistribution.put(1, 1.0 / 6)
          relativeFrequencyDistribution.put(2, 3.0 / 6)
          relativeFrequencyDistribution.put(3, 2.0 / 6)
        }
    }
    encryptedResult = getEncryptedResult(result, measurementPublicKey)
    certificate = DATA_PROVIDER_CERTIFICATE_NAME
  }
  results += resultPair {
    val result = result { impression = ResultKt.impression { value = 4096 } }
    encryptedResult = getEncryptedResult(result, measurementPublicKey)
    certificate = DATA_PROVIDER_CERTIFICATE_NAME
  }
  results += resultPair {
    val result = result {
      watchDuration =
        ResultKt.watchDuration {
          value = protoDuration {
            seconds = 100
            nanos = 99
          }
        }
    }
    encryptedResult = getEncryptedResult(result, measurementPublicKey)
    certificate = DATA_PROVIDER_CERTIFICATE_NAME
  }
}

private val LIST_MEASUREMENT_RESPONSE = listMeasurementsResponse {
  measurement += measurement {
    name = "$MEASUREMENT_CONSUMER_NAME/measurements/101"
    state = Measurement.State.AWAITING_REQUISITION_FULFILLMENT
  }
  measurement += measurement {
    name = "$MEASUREMENT_CONSUMER_NAME/measurements/102"
    state = Measurement.State.SUCCEEDED
  }
  measurement += measurement {
    name = "$MEASUREMENT_CONSUMER_NAME/measurements/102"
    state = Measurement.State.FAILED
    failure = failure {
      reason = Measurement.Failure.Reason.REQUISITION_REFUSED
      message = "Privacy budget exceeded."
    }
  }
}

private fun getEncryptedResult(
  result: Measurement.Result,
  publicKey: EncryptionPublicKey
): ByteString {
  val signedResult = signResult(result, AGGREGATOR_SIGNING_KEY)
  return encryptResult(signedResult, publicKey)
}

@RunWith(JUnit4::class)
class SimpleReportTest {
  private val measurementConsumersServiceMock: MeasurementConsumersCoroutineImplBase =
    mockService() { onBlocking { getMeasurementConsumer(any()) }.thenReturn(MEASUREMENT_CONSUMER) }
  private val dataProvidersServiceMock: DataProvidersCoroutineImplBase =
    mockService() { onBlocking { getDataProvider(any()) }.thenReturn(DATA_PROVIDER) }
  private val measurementsServiceMock: MeasurementsCoroutineImplBase =
    mockService() {
      onBlocking { createMeasurement(any()) }.thenReturn(MEASUREMENT)
      onBlocking { listMeasurements(any()) }.thenReturn(LIST_MEASUREMENT_RESPONSE)
      onBlocking { getMeasurement(any()) }.thenReturn(SUCCEEDED_MEASUREMENT)
    }
  private val certificatesServiceMock: CertificatesGrpcKt.CertificatesCoroutineImplBase =
    mockService() { onBlocking { getCertificate(any()) }.thenReturn(AGGREGATOR_CERTIFICATE) }

  private lateinit var server: CommonServer
  @Before
  fun initServer() {
    val services: List<ServerServiceDefinition> =
      listOf(
        measurementConsumersServiceMock.bindService(),
        dataProvidersServiceMock.bindService(),
        measurementsServiceMock.bindService(),
        certificatesServiceMock.bindService(),
      )

    val serverCerts =
      SigningCerts.fromPemFiles(
        certificateFile = SECRETS_DIR.resolve("kingdom_tls.pem").toFile(),
        privateKeyFile = SECRETS_DIR.resolve("kingdom_tls.key").toFile(),
        trustedCertCollectionFile = SECRETS_DIR.resolve("mc_root.pem").toFile(),
      )

    server =
      CommonServer.fromParameters(
        PORT,
        true,
        serverCerts,
        ClientAuth.REQUIRE,
        "kingdom-test",
        services
      )
    server.start()
  }

  @After
  fun shutdownServer() {
    server.server.shutdown()
    server.server.awaitTermination(1, SECONDS)
  }

  private fun String.toTimestamp(): Timestamp {
    val instant = OffsetDateTime.parse(this).toInstant()
    return timestamp {
      seconds = instant.epochSecond
      nanos = instant.nano
    }
  }

  @Test
  fun `Create command call public api with valid Measurement`() {
    val args =
      arrayOf(
        "--tls-cert-file=$SECRETS_DIR/mc_tls.pem",
        "--tls-key-file=$SECRETS_DIR/mc_tls.key",
        "--cert-collection-file=$SECRETS_DIR/kingdom_root.pem",
        "--kingdom-public-api-target=$HOST:$PORT",
        "create",
        "--impression",
        "--impression-privacy-epsilon=0.015",
        "--impression-privacy-delta=0.0",
        "--max-frequency=1000",
        "--measurement-consumer=measurementConsumers/777",
        "--private-key-der-file=$SECRETS_DIR/mc_cs_private.der",
        "--measurement-ref-id=9999",
        "--data-provider=dataProviders/1",
        "--event-group=dataProviders/1/eventGroups/1",
        "--event-filter=abcd",
        "--event-start-time=$TIME_STRING_1",
        "--event-end-time=$TIME_STRING_2",
        "--event-group=dataProviders/1/eventGroups/2",
        "--event-start-time=$TIME_STRING_3",
        "--event-end-time=$TIME_STRING_4",
        "--data-provider=dataProviders/2",
        "--event-group=dataProviders/2/eventGroups/1",
        "--event-filter=ijk",
        "--event-start-time=$TIME_STRING_5",
        "--event-end-time=$TIME_STRING_6",
      )
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
      .comparingExpectedFieldsOnly()
      .isEqualTo(
        measurementSpec {
          measurementPublicKey = MEASUREMENT_PUBLIC_KEY
          this.nonceHashes += nonceHashes
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
          eventGroups += eventGroupEntry {
            key = "dataProviders/1/eventGroups/1"
            value = eventGroupEntryValue {
              collectionInterval = timeInterval {
                startTime = TIME_STRING_1.toTimestamp()
                endTime = TIME_STRING_2.toTimestamp()
              }
              filter = eventFilter { expression = "abcd" }
            }
          }
          eventGroups += eventGroupEntry {
            key = "dataProviders/1/eventGroups/2"
            value = eventGroupEntryValue {
              collectionInterval = timeInterval {
                startTime = TIME_STRING_3.toTimestamp()
                endTime = TIME_STRING_4.toTimestamp()
              }
            }
          }
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
          eventGroups += eventGroupEntry {
            key = "dataProviders/2/eventGroups/1"
            value = eventGroupEntryValue {
              collectionInterval = timeInterval {
                startTime = TIME_STRING_5.toTimestamp()
                endTime = TIME_STRING_6.toTimestamp()
              }
              filter = eventFilter { expression = "ijk" }
            }
          }
        }
      )
  }

  @Test
  fun `Create command call public api with correct REACH_AND_FREQUENCY params`() {
    val args =
      arrayOf(
        "--tls-cert-file=$SECRETS_DIR/mc_tls.pem",
        "--tls-key-file=$SECRETS_DIR/mc_tls.key",
        "--cert-collection-file=$SECRETS_DIR/kingdom_root.pem",
        "--kingdom-public-api-target=$HOST:$PORT",
        "create",
        "--measurement-consumer=measurementConsumers/777",
        "--reach-and-frequency",
        "--reach-privacy-epsilon=0.015",
        "--reach-privacy-delta=0.0",
        "--frequency-privacy-epsilon=0.02",
        "--frequency-privacy-delta=0.0",
        "--vid-sampling-start=0.1",
        "--vid-sampling-width=0.2",
        "--private-key-der-file=$SECRETS_DIR/mc_cs_private.der",
        "--data-provider=dataProviders/1",
        "--event-group=dataProviders/1/eventGroups/1",
        "--event-filter=abcd",
        "--event-start-time=$TIME_STRING_1",
        "--event-end-time=$TIME_STRING_2",
      )
    CommandLine(SimpleReport()).execute(*args)

    val request =
      captureFirst<CreateMeasurementRequest> {
        runBlocking { verify(measurementsServiceMock).createMeasurement(capture()) }
      }

    val measurement = request.measurement
    val measurementSpec = MeasurementSpec.parseFrom(measurement.measurementSpec.data)
    assertThat(measurementSpec)
      .comparingExpectedFieldsOnly()
      .isEqualTo(
        measurementSpec {
          reachAndFrequency = reachAndFrequency {
            reachPrivacyParams = differentialPrivacyParams {
              epsilon = 0.015
              delta = 0.0
            }
            frequencyPrivacyParams = differentialPrivacyParams {
              epsilon = 0.02
              delta = 0.0
            }
            vidSamplingInterval =
              MeasurementSpecKt.vidSamplingInterval {
                start = 0.1f
                width = 0.2f
              }
          }
        }
      )
  }

  @Test
  fun `Create command call public api with correct IMPRESSION params`() {
    val args =
      arrayOf(
        "--tls-cert-file=$SECRETS_DIR/mc_tls.pem",
        "--tls-key-file=$SECRETS_DIR/mc_tls.key",
        "--cert-collection-file=$SECRETS_DIR/kingdom_root.pem",
        "--kingdom-public-api-target=$HOST:$PORT",
        "create",
        "--measurement-consumer=measurementConsumers/777",
        "--impression",
        "--impression-privacy-epsilon=0.015",
        "--impression-privacy-delta=0.0",
        "--max-frequency=1000",
        "--private-key-der-file=$SECRETS_DIR/mc_cs_private.der",
        "--data-provider=dataProviders/1",
        "--event-group=dataProviders/1/eventGroups/1",
        "--event-filter=abcd",
        "--event-start-time=$TIME_STRING_1",
        "--event-end-time=$TIME_STRING_2"
      )
    CommandLine(SimpleReport()).execute(*args)

    val request =
      captureFirst<CreateMeasurementRequest> {
        runBlocking { verify(measurementsServiceMock).createMeasurement(capture()) }
      }

    val measurement = request.measurement
    val measurementSpec = MeasurementSpec.parseFrom(measurement.measurementSpec.data)
    assertThat(measurementSpec)
      .comparingExpectedFieldsOnly()
      .isEqualTo(
        measurementSpec {
          impression = impression {
            privacyParams = differentialPrivacyParams {
              epsilon = 0.015
              delta = 0.0
            }
            maximumFrequencyPerUser = 1000
          }
        }
      )
  }

  @Test
  fun `Create command call public api with correct DURATION params`() {
    val args =
      arrayOf(
        "--tls-cert-file=$SECRETS_DIR/mc_tls.pem",
        "--tls-key-file=$SECRETS_DIR/mc_tls.key",
        "--cert-collection-file=$SECRETS_DIR/kingdom_root.pem",
        "--kingdom-public-api-target=$HOST:$PORT",
        "create",
        "--measurement-consumer=measurementConsumers/777",
        "--duration",
        "--duration-privacy-epsilon=0.015",
        "--duration-privacy-delta=0.0",
        "--max-duration=1000",
        "--private-key-der-file=$SECRETS_DIR/mc_cs_private.der",
        "--data-provider=dataProviders/1",
        "--event-group=dataProviders/1/eventGroups/1",
        "--event-filter=abcd",
        "--event-start-time=$TIME_STRING_1",
        "--event-end-time=$TIME_STRING_2",
      )
    CommandLine(SimpleReport()).execute(*args)

    val request =
      captureFirst<CreateMeasurementRequest> {
        runBlocking { verify(measurementsServiceMock).createMeasurement(capture()) }
      }

    val measurement = request.measurement
    val measurementSpec = MeasurementSpec.parseFrom(measurement.measurementSpec.data)
    assertThat(measurementSpec)
      .comparingExpectedFieldsOnly()
      .isEqualTo(
        measurementSpec {
          duration = duration {
            privacyParams = differentialPrivacyParams {
              epsilon = 0.015
              delta = 0.0
            }
            maximumWatchDurationPerUser = 1000
          }
        }
      )
  }

  @Test
  fun `List command call public api with correct request`() {
    val args =
      arrayOf(
        "--tls-cert-file=$SECRETS_DIR/mc_tls.pem",
        "--tls-key-file=$SECRETS_DIR/mc_tls.key",
        "--cert-collection-file=$SECRETS_DIR/kingdom_root.pem",
        "--kingdom-public-api-target=$HOST:$PORT",
        "list",
        "--measurement-consumer=$MEASUREMENT_CONSUMER_NAME"
      )
    CommandLine(SimpleReport()).execute(*args)

    val request =
      captureFirst<ListMeasurementsRequest> {
        runBlocking { verify(measurementsServiceMock).listMeasurements(capture()) }
      }
    assertThat(request)
      .comparingExpectedFieldsOnly()
      .isEqualTo(listMeasurementsRequest { parent = MEASUREMENT_CONSUMER_NAME })
  }

  @Test
  fun `Get command call public api with correct request`() {
    val args =
      arrayOf(
        "--tls-cert-file=$SECRETS_DIR/mc_tls.pem",
        "--tls-key-file=$SECRETS_DIR/mc_tls.key",
        "--cert-collection-file=$SECRETS_DIR/kingdom_root.pem",
        "--kingdom-public-api-target=$HOST:$PORT",
        "get",
        "--measurement=$MEASUREMENT_NAME",
        "--encryption-private-key-file=$SECRETS_DIR/mc_enc_private.tink",
      )
    CommandLine(SimpleReport()).execute(*args)

    val request =
      captureFirst<GetMeasurementRequest> {
        runBlocking { verify(measurementsServiceMock).getMeasurement(capture()) }
      }
    assertThat(request)
      .comparingExpectedFieldsOnly()
      .isEqualTo(getMeasurementRequest { name = MEASUREMENT_NAME })
  }
}
