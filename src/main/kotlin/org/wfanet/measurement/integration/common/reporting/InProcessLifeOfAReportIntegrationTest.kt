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

package org.wfanet.measurement.integration.common.reporting

import com.google.common.truth.Truth.assertThat
import com.google.protobuf.ByteString
import com.google.protobuf.DescriptorProtos
import com.google.protobuf.duration
import com.google.protobuf.kotlin.toByteString
import com.google.protobuf.timestamp
import io.grpc.Status
import java.io.File
import java.nio.file.Paths
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import org.junit.Rule
import org.junit.Test
import org.junit.rules.TestRule
import org.mockito.kotlin.any
import org.wfanet.measurement.api.v2alpha.Certificate
import org.wfanet.measurement.api.v2alpha.CertificatesGrpcKt
import org.wfanet.measurement.api.v2alpha.DataProvidersGrpcKt
import org.wfanet.measurement.api.v2alpha.EncryptionPublicKey
import org.wfanet.measurement.api.v2alpha.EventGroup
import org.wfanet.measurement.api.v2alpha.EventGroupKt
import org.wfanet.measurement.api.v2alpha.EventGroupMetadataDescriptor
import org.wfanet.measurement.api.v2alpha.EventGroupMetadataDescriptorsGrpcKt
import org.wfanet.measurement.api.v2alpha.EventGroupsGrpcKt
import org.wfanet.measurement.api.v2alpha.GetMeasurementRequest
import org.wfanet.measurement.api.v2alpha.Measurement
import org.wfanet.measurement.api.v2alpha.MeasurementConsumer
import org.wfanet.measurement.api.v2alpha.MeasurementConsumersGrpcKt
import org.wfanet.measurement.api.v2alpha.MeasurementKt
import org.wfanet.measurement.api.v2alpha.MeasurementSpecKt
import org.wfanet.measurement.api.v2alpha.MeasurementsGrpcKt
import org.wfanet.measurement.api.v2alpha.SignedData
import org.wfanet.measurement.api.v2alpha.batchGetEventGroupMetadataDescriptorsResponse
import org.wfanet.measurement.api.v2alpha.certificate
import org.wfanet.measurement.api.v2alpha.copy
import org.wfanet.measurement.api.v2alpha.dataProvider
import org.wfanet.measurement.api.v2alpha.differentialPrivacyParams
import org.wfanet.measurement.api.v2alpha.eventGroup
import org.wfanet.measurement.api.v2alpha.eventGroupMetadataDescriptor
import org.wfanet.measurement.api.v2alpha.listEventGroupsResponse
import org.wfanet.measurement.api.v2alpha.measurement
import org.wfanet.measurement.api.v2alpha.measurementConsumer
import org.wfanet.measurement.api.v2alpha.measurementSpec
import org.wfanet.measurement.common.HexString
import org.wfanet.measurement.common.crypto.SigningKeyHandle
import org.wfanet.measurement.common.crypto.readCertificate
import org.wfanet.measurement.common.crypto.testing.loadSigningKey
import org.wfanet.measurement.common.crypto.tink.loadPublicKey
import org.wfanet.measurement.common.getRuntimePath
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.common.grpc.testing.mockService
import org.wfanet.measurement.common.readByteString
import org.wfanet.measurement.common.testing.chainRulesSequentially
import org.wfanet.measurement.config.reporting.EncryptionKeyPairConfig
import org.wfanet.measurement.config.reporting.EncryptionKeyPairConfigKt.keyPair
import org.wfanet.measurement.config.reporting.EncryptionKeyPairConfigKt.principalKeyPairs
import org.wfanet.measurement.config.reporting.encryptionKeyPairConfig
import org.wfanet.measurement.config.reporting.measurementConsumerConfig
import org.wfanet.measurement.consent.client.common.toEncryptionPublicKey
import org.wfanet.measurement.consent.client.common.toPublicKeyHandle
import org.wfanet.measurement.consent.client.duchy.encryptResult
import org.wfanet.measurement.consent.client.duchy.signResult
import org.wfanet.measurement.consent.client.measurementconsumer.signEncryptionPublicKey
import org.wfanet.measurement.consent.client.measurementconsumer.signMeasurementSpec
import org.wfanet.measurement.integration.common.reporting.identity.withPrincipalName
import org.wfanet.measurement.reporting.deploy.common.server.ReportingDataServer
import org.wfanet.measurement.reporting.v1alpha.EventGroupsGrpcKt.EventGroupsCoroutineStub
import org.wfanet.measurement.reporting.v1alpha.ListEventGroupsResponse
import org.wfanet.measurement.reporting.v1alpha.ListReportingSetsResponse
import org.wfanet.measurement.reporting.v1alpha.ListReportsResponse
import org.wfanet.measurement.reporting.v1alpha.Metric
import org.wfanet.measurement.reporting.v1alpha.MetricKt
import org.wfanet.measurement.reporting.v1alpha.Report
import org.wfanet.measurement.reporting.v1alpha.ReportKt
import org.wfanet.measurement.reporting.v1alpha.ReportingSet
import org.wfanet.measurement.reporting.v1alpha.ReportingSetsGrpcKt.ReportingSetsCoroutineStub
import org.wfanet.measurement.reporting.v1alpha.ReportsGrpcKt.ReportsCoroutineStub
import org.wfanet.measurement.reporting.v1alpha.createReportRequest
import org.wfanet.measurement.reporting.v1alpha.createReportingSetRequest
import org.wfanet.measurement.reporting.v1alpha.getReportRequest
import org.wfanet.measurement.reporting.v1alpha.listEventGroupsRequest
import org.wfanet.measurement.reporting.v1alpha.listReportingSetsRequest
import org.wfanet.measurement.reporting.v1alpha.listReportsRequest
import org.wfanet.measurement.reporting.v1alpha.metric
import org.wfanet.measurement.reporting.v1alpha.periodicTimeInterval
import org.wfanet.measurement.reporting.v1alpha.report
import org.wfanet.measurement.reporting.v1alpha.reportingSet

/**
 * Test that everything is wired up properly.
 *
 * This is abstract so that different implementations of dependencies can all run the same tests
 * easily.
 */
abstract class InProcessLifeOfAReportIntegrationTest {
  abstract val reportingServerDataServices: ReportingDataServer.Services

  private val publicKingdomCertificatesMock: CertificatesGrpcKt.CertificatesCoroutineImplBase =
    mockService {
      onBlocking { getCertificate(any()) }.thenReturn(CERTIFICATE)
    }
  private val publicKingdomDataProvidersMock: DataProvidersGrpcKt.DataProvidersCoroutineImplBase =
    mockService {
      onBlocking { getDataProvider(any()) }.thenReturn(DATA_PROVIDER)
    }
  private val publicKingdomEventGroupsMock: EventGroupsGrpcKt.EventGroupsCoroutineImplBase =
    mockService {
      onBlocking { listEventGroups(any()) }
        .thenReturn(listEventGroupsResponse { eventGroups += EVENT_GROUP })
    }
  private val publicKingdomEventGroupMetadataDescriptorsMock:
    EventGroupMetadataDescriptorsGrpcKt.EventGroupMetadataDescriptorsCoroutineImplBase =
    mockService {
      onBlocking { batchGetEventGroupMetadataDescriptors(any()) }
        .thenReturn(
          batchGetEventGroupMetadataDescriptorsResponse {
            eventGroupMetadataDescriptors += EVENT_GROUP_METADATA_DESCRIPTOR
          }
        )
    }
  private val publicKingdomMeasurementConsumersMock:
    MeasurementConsumersGrpcKt.MeasurementConsumersCoroutineImplBase =
    mockService {
      onBlocking { getMeasurementConsumer(any()) }.thenReturn(MEASUREMENT_CONSUMER)
    }
  private val publicKingdomMeasurementsMock: MeasurementsGrpcKt.MeasurementsCoroutineImplBase =
    mockService {
      onBlocking { createMeasurement(any()) }
        .apply {
          var chain = this
          MEASUREMENT_NAME_SET.forEach { chain = chain.thenReturn(MEASUREMENT.copy { name = it }) }
        }

      onBlocking { getMeasurement(any()) }
        .thenAnswer {
          val name = it.getArgument(0, GetMeasurementRequest::class.java).name
          if (MEASUREMENT_NAME_SET.contains(name)) {
            MEASUREMENT.copy { this.name = name }
          } else {
            throw Status.NOT_FOUND.asRuntimeException()
          }
        }
    }

  private val publicKingdomServer = GrpcTestServerRule {
    addService(publicKingdomCertificatesMock)
    addService(publicKingdomDataProvidersMock)
    addService(publicKingdomEventGroupsMock)
    addService(publicKingdomEventGroupMetadataDescriptorsMock)
    addService(publicKingdomMeasurementConsumersMock)
    addService(publicKingdomMeasurementsMock)
  }

  private val encryptionKeyPairConfig: EncryptionKeyPairConfig = encryptionKeyPairConfig {
    principalKeyPairs += principalKeyPairs {
      principal = MEASUREMENT_CONSUMER_NAME
      keyPairs += keyPair {
        publicKeyFile = "mc_enc_public.tink"
        privateKeyFile = "mc_enc_private.tink"
      }
    }
  }

  private val measurementConsumerConfig = measurementConsumerConfig {
    apiKey = API_KEY
    signingCertificateName = MEASUREMENT_CONSUMER_CERTIFICATE_NAME
    signingPrivateKeyPath = MC_SIGNING_PRIVATE_KEY_PATH
  }

  private val reportingServer: InProcessReportingServer by lazy {
    InProcessReportingServer(
      reportingServerDataServices = reportingServerDataServices,
      publicKingdomServer.channel,
      encryptionKeyPairConfig,
      SECRETS_DIR,
      measurementConsumerConfig,
      verboseGrpcLogging = false,
    )
  }

  @get:Rule
  val ruleChain: TestRule by lazy { chainRulesSequentially(publicKingdomServer, reportingServer) }

  private val publicEventGroupsClient by lazy {
    EventGroupsCoroutineStub(reportingServer.publicApiChannel)
  }
  private val publicReportingSetsClient by lazy {
    ReportingSetsCoroutineStub(reportingServer.publicApiChannel)
  }
  private val publicReportsClient by lazy { ReportsCoroutineStub(reportingServer.publicApiChannel) }

  @Test
  fun `create Report and get the expected result successfully`() = runBlocking {
    createReportingSet("1", MEASUREMENT_CONSUMER_NAME)
    createReportingSet("2", MEASUREMENT_CONSUMER_NAME)
    createReportingSet("3", MEASUREMENT_CONSUMER_NAME)

    val createdReport = createReport("1234", MEASUREMENT_CONSUMER_NAME)
    val reports = listReports(MEASUREMENT_CONSUMER_NAME)
    assertThat(reports.reportsList).hasSize(1)
    val reportResult = getReportResult(createdReport)
    assertThat(reportResult).isEqualTo(200.0)
  }

  @Test
  fun `create multiple Reports concurrently successfully`() = runBlocking {
    createReportingSet("1", MEASUREMENT_CONSUMER_NAME)
    createReportingSet("2", MEASUREMENT_CONSUMER_NAME)
    createReportingSet("3", MEASUREMENT_CONSUMER_NAME)
    for (i in 1..5) {
      launch { createReport("$i", MEASUREMENT_CONSUMER_NAME) }
    }
  }

  private suspend fun getReportResult(report: Report): Double {
    val completedReport = getReport(report.name, report.measurementConsumer)
    var sum = 0.0
    completedReport.result.scalarTable.columnsList.forEach { column ->
      column.setOperationsList.forEach { sum += it }
    }
    return sum
  }

  private suspend fun listEventGroups(measurementConsumerName: String): ListEventGroupsResponse {
    return publicEventGroupsClient
      .withPrincipalName(measurementConsumerName)
      .listEventGroups(
        listEventGroupsRequest { parent = "$measurementConsumerName/dataProviders/-" }
      )
  }

  suspend fun createReportingSet(runId: String, measurementConsumerName: String): ReportingSet {
    val eventGroupsList = listEventGroups(measurementConsumerName).eventGroupsList
    return publicReportingSetsClient
      .withPrincipalName(measurementConsumerName)
      .createReportingSet(
        createReportingSetRequest {
          parent = measurementConsumerName
          reportingSet = reportingSet {
            displayName = "reporting-set-$runId"
            eventGroups += eventGroupsList.map { it.name }
          }
        }
      )
  }

  private suspend fun listReportingSets(
    measurementConsumerName: String
  ): ListReportingSetsResponse {
    return publicReportingSetsClient
      .withPrincipalName(measurementConsumerName)
      .listReportingSets(listReportingSetsRequest { parent = measurementConsumerName })
  }

  private suspend fun createReport(runId: String, measurementConsumerName: String): Report {
    val eventGroupsList = listEventGroups(measurementConsumerName).eventGroupsList
    val reportingSets = listReportingSets(measurementConsumerName).reportingSetsList
    assertThat(reportingSets.size).isAtLeast(3)
    return publicReportsClient
      .withPrincipalName(measurementConsumerName)
      .createReport(
        createReportRequest {
          parent = measurementConsumerName
          report = report {
            measurementConsumer = measurementConsumerName
            reportIdempotencyKey = runId
            eventGroupUniverse =
              ReportKt.eventGroupUniverse {
                eventGroupsList.forEach {
                  eventGroupEntries +=
                    ReportKt.EventGroupUniverseKt.eventGroupEntry { key = it.name }
                }
              }
            periodicTimeInterval = periodicTimeInterval {
              startTime = timestamp { seconds = 100 }
              increment = duration { seconds = 5 }
              intervalCount = 2
            }
            metrics += metric {
              impressionCount = MetricKt.impressionCountParams { maximumFrequencyPerUser = 5 }
              setOperations +=
                MetricKt.namedSetOperation {
                  uniqueName = "set-operation"
                  setOperation =
                    MetricKt.setOperation {
                      type = Metric.SetOperation.Type.UNION
                      lhs =
                        MetricKt.SetOperationKt.operand {
                          operation =
                            MetricKt.setOperation {
                              type = Metric.SetOperation.Type.UNION
                              lhs =
                                MetricKt.SetOperationKt.operand {
                                  reportingSet = reportingSets[0].name
                                }
                              rhs =
                                MetricKt.SetOperationKt.operand {
                                  reportingSet = reportingSets[1].name
                                }
                            }
                        }
                      rhs = MetricKt.SetOperationKt.operand { reportingSet = reportingSets[2].name }
                    }
                }
            }
          }
        }
      )
  }

  private suspend fun getReport(reportName: String, measurementConsumerName: String): Report {
    return publicReportsClient
      .withPrincipalName(measurementConsumerName)
      .getReport(getReportRequest { name = reportName })
  }

  private suspend fun listReports(measurementConsumerName: String): ListReportsResponse {
    return publicReportsClient
      .withPrincipalName(measurementConsumerName)
      .listReports(listReportsRequest { parent = measurementConsumerName })
  }

  companion object {
    private val SECRETS_DIR: File =
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
        .toFile()

    private val MC_CERTIFICATE_DER: ByteString =
      readCertificate(SECRETS_DIR.resolve("mc_cs_cert.der").readByteString()).encoded.toByteString()
    private val MC_SIGNING_KEY_HANDLE: SigningKeyHandle =
      loadSigningKey(
        SECRETS_DIR.resolve("mc_cs_cert.der"),
        SECRETS_DIR.resolve("mc_cs_private.der")
      )
    private val MC_ENCRYPTION_PUBLIC_KEY: EncryptionPublicKey =
      loadPublicKey(SECRETS_DIR.resolve("mc_enc_public.tink")).toEncryptionPublicKey()
    private const val MC_SIGNING_PRIVATE_KEY_PATH = "mc_cs_private.der"
    private const val API_KEY = "AAAAAAAAAHs"
    const val MEASUREMENT_CONSUMER_NAME = "measurementConsumers/AAAAAAAAAHs"
    private const val MEASUREMENT_CONSUMER_CERTIFICATE_NAME =
      "$MEASUREMENT_CONSUMER_NAME/certificates/AAAAAAAAAHs"

    private const val DATA_PROVIDER_NAME = "dataProviders/AAAAAAAAAHs"
    private const val DATA_PROVIDER_CERTIFICATE_NAME =
      "$DATA_PROVIDER_NAME/certificates/AAAAAAAAAHs"
    private val EDP_CERTIFICATE_DER: ByteString =
      readCertificate(SECRETS_DIR.resolve("edp1_cs_cert.der").readByteString())
        .encoded
        .toByteString()
    private val EDP_SIGNING_KEY_HANDLE: SigningKeyHandle =
      loadSigningKey(
        SECRETS_DIR.resolve("edp1_cs_cert.der"),
        SECRETS_DIR.resolve("edp1_cs_private.der")
      )

    private val CERTIFICATE: Certificate = certificate {
      name = DATA_PROVIDER_CERTIFICATE_NAME
      x509Der = EDP_CERTIFICATE_DER
    }

    private val DATA_PROVIDER = dataProvider {
      name = DATA_PROVIDER_NAME
      certificate = DATA_PROVIDER_CERTIFICATE_NAME
      certificateDer = EDP_CERTIFICATE_DER
      publicKey =
        signEncryptionPublicKey(
          loadPublicKey(SECRETS_DIR.resolve("edp1_enc_public.tink")).toEncryptionPublicKey(),
          EDP_SIGNING_KEY_HANDLE
        )
    }

    private val MEASUREMENT_CONSUMER: MeasurementConsumer = measurementConsumer {
      name = MEASUREMENT_CONSUMER_NAME
      certificate = MEASUREMENT_CONSUMER_CERTIFICATE_NAME
      certificateDer = MC_CERTIFICATE_DER
      publicKey = signEncryptionPublicKey(MC_ENCRYPTION_PUBLIC_KEY, MC_SIGNING_KEY_HANDLE)
    }

    private val DATA_PROVIDER_NONCE_HASH: ByteString =
      HexString("97F76220FEB39EE6F262B1F0C8D40F221285EEDE105748AE98F7DC241198D69F").bytes

    private val MEASUREMENT_SPEC = measurementSpec {
      measurementPublicKey = MEASUREMENT_CONSUMER.publicKey.data
      vidSamplingInterval = MeasurementSpecKt.vidSamplingInterval { width = 1.0f }
      impression =
        MeasurementSpecKt.impression {
          privacyParams = differentialPrivacyParams {
            epsilon = 1.0
            delta = 1.0
          }
          maximumFrequencyPerUser = 5
        }
      nonceHashes += DATA_PROVIDER_NONCE_HASH
    }

    private val MEASUREMENT_NAME_SET =
      setOf(
        "$MEASUREMENT_CONSUMER_NAME/measurements/AAAAAAAAAHs",
        "$MEASUREMENT_CONSUMER_NAME/measurements/BBBBBBBBBHs",
        "$MEASUREMENT_CONSUMER_NAME/measurements/CCCCCCCCCHs",
        "$MEASUREMENT_CONSUMER_NAME/measurements/DDDDDDDDDHs",
        "$MEASUREMENT_CONSUMER_NAME/measurements/EEEEEEEEEHs",
        "$MEASUREMENT_CONSUMER_NAME/measurements/FFFFFFFFFHs",
        "$MEASUREMENT_CONSUMER_NAME/measurements/GGGGGGGGGHs",
        "$MEASUREMENT_CONSUMER_NAME/measurements/HHHHHHHHHHs",
        "$MEASUREMENT_CONSUMER_NAME/measurements/IIIIIIIIIHs",
        "$MEASUREMENT_CONSUMER_NAME/measurements/JJJJJJJJJHs",
      )

    private val result =
      MeasurementKt.result { impression = MeasurementKt.ResultKt.impression { value = 100 } }
    private val signedResult: SignedData = signResult(result, EDP_SIGNING_KEY_HANDLE)
    private val ENCRYPTED_RESULT: ByteString =
      encryptResult(
        signedResult,
        EncryptionPublicKey.parseFrom(MEASUREMENT_SPEC.measurementPublicKey)
      )

    private val MEASUREMENT = measurement {
      name = MEASUREMENT_NAME_SET.first()
      measurementConsumerCertificate = MEASUREMENT_CONSUMER_CERTIFICATE_NAME
      measurementSpec = signMeasurementSpec(MEASUREMENT_SPEC, MC_SIGNING_KEY_HANDLE)
      dataProviders +=
        MeasurementKt.dataProviderEntry {
          key = DATA_PROVIDER_NAME
          value =
            MeasurementKt.DataProviderEntryKt.value {
              dataProviderCertificate = DATA_PROVIDER_CERTIFICATE_NAME
              dataProviderPublicKey = DATA_PROVIDER.publicKey
              encryptedRequisitionSpec = ByteString.copyFromUtf8("Fake encrypted requisition spec")
              nonceHash = DATA_PROVIDER_NONCE_HASH
            }
        }
      measurementReferenceId = "ref_id"
      results +=
        MeasurementKt.resultPair {
          certificate = DATA_PROVIDER_CERTIFICATE_NAME
          encryptedResult = ENCRYPTED_RESULT
        }
      state = Measurement.State.SUCCEEDED
    }

    private const val EVENT_GROUP_NAME = "$DATA_PROVIDER_NAME/eventGroups/AAAAAAAAAHs"
    private val VID_MODEL_LINES = listOf("model1", "model2")
    private val EVENT_TEMPLATE_TYPES = listOf("type1", "type2")
    private val EVENT_TEMPLATES =
      EVENT_TEMPLATE_TYPES.map { type -> EventGroupKt.eventTemplate { this.type = type } }

    private val EVENT_GROUP: EventGroup = eventGroup {
      name = EVENT_GROUP_NAME
      measurementConsumer = MEASUREMENT_CONSUMER_NAME
      measurementConsumerCertificate = MEASUREMENT_CONSUMER_CERTIFICATE_NAME
      eventGroupReferenceId = "aaa"
      measurementConsumerPublicKey = MEASUREMENT_CONSUMER.publicKey
      vidModelLines.addAll(VID_MODEL_LINES)
      eventTemplates.addAll(EVENT_TEMPLATES)
      encryptedMetadata =
        MC_ENCRYPTION_PUBLIC_KEY.toPublicKeyHandle()
          .hybridEncrypt(EventGroup.Metadata.getDefaultInstance().toByteString())
    }

    private const val EVENT_GROUP_METADATA_DESCRIPTOR_NAME =
      "$DATA_PROVIDER_NAME/eventGroupMetadataDescriptors/AAAAAAAAAHs"
    private val FILE_DESCRIPTOR_SET = DescriptorProtos.FileDescriptorSet.getDefaultInstance()

    private val EVENT_GROUP_METADATA_DESCRIPTOR: EventGroupMetadataDescriptor =
      eventGroupMetadataDescriptor {
        name = EVENT_GROUP_METADATA_DESCRIPTOR_NAME
        descriptorSet = FILE_DESCRIPTOR_SET
      }
  }
}
