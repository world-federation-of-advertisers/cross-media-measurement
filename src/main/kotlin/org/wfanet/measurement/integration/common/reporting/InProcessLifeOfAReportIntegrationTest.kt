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
import com.google.common.truth.extensions.proto.ProtoTruth.assertThat
import com.google.protobuf.ByteString
import com.google.protobuf.DescriptorProtos
import com.google.protobuf.duration
import com.google.protobuf.kotlin.toByteString
import com.google.protobuf.timestamp
import java.io.File
import java.nio.file.Paths
import java.time.Clock
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
import org.wfanet.measurement.api.v2alpha.MeasurementConsumer
import org.wfanet.measurement.api.v2alpha.MeasurementConsumersGrpcKt
import org.wfanet.measurement.api.v2alpha.MeasurementsGrpcKt
import org.wfanet.measurement.api.v2alpha.batchGetEventGroupMetadataDescriptorsResponse
import org.wfanet.measurement.api.v2alpha.certificate
import org.wfanet.measurement.api.v2alpha.dataProvider
import org.wfanet.measurement.api.v2alpha.eventGroup
import org.wfanet.measurement.api.v2alpha.eventGroupMetadataDescriptor
import org.wfanet.measurement.api.v2alpha.listEventGroupsResponse
import org.wfanet.measurement.api.v2alpha.measurementConsumer
import org.wfanet.measurement.common.crypto.SigningKeyHandle
import org.wfanet.measurement.common.crypto.readCertificate
import org.wfanet.measurement.common.crypto.readCertificateCollection
import org.wfanet.measurement.common.crypto.subjectKeyIdentifier
import org.wfanet.measurement.common.crypto.testing.loadSigningKey
import org.wfanet.measurement.common.crypto.tink.loadPublicKey
import org.wfanet.measurement.common.getRuntimePath
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.common.grpc.testing.mockService
import org.wfanet.measurement.common.identity.RandomIdGenerator
import org.wfanet.measurement.common.readByteString
import org.wfanet.measurement.common.testing.chainRulesSequentially
import org.wfanet.measurement.config.reporting.EncryptionKeyPairConfig
import org.wfanet.measurement.config.reporting.EncryptionKeyPairConfigKt.keyPair
import org.wfanet.measurement.config.reporting.EncryptionKeyPairConfigKt.principalKeyPairs
import org.wfanet.measurement.config.reporting.encryptionKeyPairConfig
import org.wfanet.measurement.config.reporting.measurementConsumerConfig
import org.wfanet.measurement.consent.client.common.toEncryptionPublicKey
import org.wfanet.measurement.consent.client.common.toPublicKeyHandle
import org.wfanet.measurement.consent.client.measurementconsumer.signEncryptionPublicKey
import org.wfanet.measurement.integration.common.kingdom.service.api.v2alpha.FakeMeasurementsService
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
import org.wfanet.measurement.reporting.v1alpha.copy
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

private const val NUM_SET_OPERATIONS = 50

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
  private val publicFakeKingdomMeasurementsService:
    MeasurementsGrpcKt.MeasurementsCoroutineImplBase =
    FakeMeasurementsService(
      RandomIdGenerator(Clock.systemUTC()),
      EDP_SIGNING_KEY_HANDLE,
      DATA_PROVIDER_CERTIFICATE_NAME
    )

  private val publicKingdomServer = GrpcTestServerRule {
    addService(publicKingdomCertificatesMock)
    addService(publicKingdomDataProvidersMock)
    addService(publicKingdomEventGroupsMock)
    addService(publicKingdomEventGroupMetadataDescriptorsMock)
    addService(publicKingdomMeasurementConsumersMock)
    addService(publicFakeKingdomMeasurementsService)
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
      reportingServerDataServices,
      publicKingdomServer.channel,
      encryptionKeyPairConfig,
      SECRETS_DIR,
      measurementConsumerConfig,
      TRUSTED_CERTIFICATES,
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
  fun `create Report and get the result successfully`() = runBlocking {
    createReportingSet("1", MEASUREMENT_CONSUMER_NAME)
    createReportingSet("2", MEASUREMENT_CONSUMER_NAME)
    createReportingSet("3", MEASUREMENT_CONSUMER_NAME)

    val createdReport = createReport("1234", MEASUREMENT_CONSUMER_NAME)
    val reports = listReports(MEASUREMENT_CONSUMER_NAME)
    assertThat(reports.reportsList).hasSize(1)
    val completedReport = getReport(createdReport.name, createdReport.measurementConsumer)
    assertThat(assertThat(completedReport.state).isEqualTo(Report.State.SUCCEEDED))
    val reportResult = computeReportResult(completedReport)
    // each measurement has a result of 100.0 and there are two time intervals
    assertThat(reportResult).isEqualTo(200.0 * NUM_SET_OPERATIONS)
  }

  @Test
  fun `create multiple Reports concurrently successfully`() = runBlocking {
    createReportingSet("1", MEASUREMENT_CONSUMER_NAME)
    createReportingSet("2", MEASUREMENT_CONSUMER_NAME)
    createReportingSet("3", MEASUREMENT_CONSUMER_NAME)
    launch { createReport("5", MEASUREMENT_CONSUMER_NAME, true) }
    for (i in 1..4) {
      launch { createReport("$i", MEASUREMENT_CONSUMER_NAME) }
    }
  }

  private fun computeReportResult(completedReport: Report): Double {
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

  private suspend fun createReportingSet(
    runId: String,
    measurementConsumerName: String
  ): ReportingSet {
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

  private suspend fun createReport(
    runId: String,
    measurementConsumerName: String,
    cumulative: Boolean = false
  ): Report {
    val eventGroupsList = listEventGroups(measurementConsumerName).eventGroupsList
    val reportingSets = listReportingSets(measurementConsumerName).reportingSetsList
    assertThat(reportingSets.size).isAtLeast(3)
    val createReportRequest = createReportRequest {
      parent = measurementConsumerName
      report = report {
        measurementConsumer = measurementConsumerName
        reportIdempotencyKey = runId
        eventGroupUniverse =
          ReportKt.eventGroupUniverse {
            eventGroupsList.forEach {
              eventGroupEntries += ReportKt.EventGroupUniverseKt.eventGroupEntry { key = it.name }
            }
          }
        periodicTimeInterval = periodicTimeInterval {
          startTime = timestamp { seconds = 100 }
          increment = duration { seconds = 5 }
          intervalCount = 2
        }
        metrics += metric {
          this.cumulative = cumulative
          impressionCount = MetricKt.impressionCountParams { maximumFrequencyPerUser = 5 }
          val setOperation =
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
                            MetricKt.SetOperationKt.operand { reportingSet = reportingSets[0].name }
                          rhs =
                            MetricKt.SetOperationKt.operand { reportingSet = reportingSets[1].name }
                        }
                    }
                  rhs = MetricKt.SetOperationKt.operand { reportingSet = reportingSets[2].name }
                }
            }

          for (i in 1..NUM_SET_OPERATIONS) {
            setOperations += setOperation.copy { uniqueName = "$uniqueName-$i" }
          }
        }
      }
    }

    val report =
      publicReportsClient
        .withPrincipalName(measurementConsumerName)
        .createReport(createReportRequest)

    // Verify concurrent operations process the metrics without skipping set operations.
    assertThat(report.metricsList)
      .ignoringRepeatedFieldOrder()
      .containsExactlyElementsIn(createReportRequest.report.metricsList)
    return report
  }

  private suspend fun getReport(reportName: String, principalName: String): Report {
    return publicReportsClient
      .withPrincipalName(principalName)
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

    private val TRUSTED_CERTIFICATES =
      readCertificateCollection(SECRETS_DIR.resolve("all_root_certs.pem")).associateBy {
        it.subjectKeyIdentifier!!
      }

    private val MC_CERTIFICATE_DER: ByteString =
      SECRETS_DIR.resolve("mc_cs_cert.der").readByteString()
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
