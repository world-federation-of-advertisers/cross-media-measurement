/*
 * Copyright 2023 The Cross-Media Measurement Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.wfanet.measurement.integration.common.reporting.v2

import com.google.common.truth.Truth.assertThat
import com.google.common.truth.extensions.proto.ProtoTruth.assertThat
import com.google.protobuf.timestamp
import com.google.protobuf.util.Durations
import com.google.protobuf.util.Timestamps
import com.google.type.Interval
import com.google.type.interval
import java.io.File
import java.nio.file.Paths
import java.time.LocalDate
import kotlinx.coroutines.Deferred
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import org.junit.After
import org.junit.BeforeClass
import org.junit.Rule
import org.junit.Test
import org.junit.rules.TestRule
import org.junit.runners.model.Statement
import org.wfanet.measurement.api.v2alpha.DataProviderCertificateKey
import org.wfanet.measurement.api.v2alpha.DataProvidersGrpcKt.DataProvidersCoroutineStub
import org.wfanet.measurement.api.v2alpha.EventGroupKt as CmmsEventGroupKt
import org.wfanet.measurement.api.v2alpha.EventGroupMetadataDescriptorsGrpcKt.EventGroupMetadataDescriptorsCoroutineStub
import org.wfanet.measurement.api.v2alpha.Measurement
import org.wfanet.measurement.api.v2alpha.MeasurementConsumersGrpcKt.MeasurementConsumersCoroutineStub
import org.wfanet.measurement.api.v2alpha.MeasurementKt
import org.wfanet.measurement.api.v2alpha.RequisitionSpecKt
import org.wfanet.measurement.api.v2alpha.batchGetEventGroupMetadataDescriptorsRequest
import org.wfanet.measurement.api.v2alpha.eventGroup as cmmsEventGroup
import org.wfanet.measurement.api.v2alpha.event_templates.testing.Person
import org.wfanet.measurement.api.v2alpha.getDataProviderRequest
import org.wfanet.measurement.api.v2alpha.getMeasurementConsumerRequest
import org.wfanet.measurement.api.v2alpha.testing.MeasurementResultSubject.Companion.assertThat
import org.wfanet.measurement.api.withAuthenticationKey
import org.wfanet.measurement.common.OpenEndTimeRange
import org.wfanet.measurement.common.crypto.readCertificateCollection
import org.wfanet.measurement.common.crypto.subjectKeyIdentifier
import org.wfanet.measurement.common.getRuntimePath
import org.wfanet.measurement.common.testing.ProviderRule
import org.wfanet.measurement.common.testing.chainRulesSequentially
import org.wfanet.measurement.common.toInterval
import org.wfanet.measurement.common.toProtoTime
import org.wfanet.measurement.config.reporting.EncryptionKeyPairConfig
import org.wfanet.measurement.config.reporting.EncryptionKeyPairConfigKt.keyPair
import org.wfanet.measurement.config.reporting.EncryptionKeyPairConfigKt.principalKeyPairs
import org.wfanet.measurement.config.reporting.MeasurementConsumerConfig
import org.wfanet.measurement.config.reporting.encryptionKeyPairConfig
import org.wfanet.measurement.config.reporting.measurementConsumerConfig
import org.wfanet.measurement.consent.client.dataprovider.encryptMetadata
import org.wfanet.measurement.integration.common.InProcessCmmsComponents
import org.wfanet.measurement.integration.common.InProcessDuchy
import org.wfanet.measurement.integration.common.SyntheticGenerationSpecs
import org.wfanet.measurement.integration.common.reporting.v2.identity.withPrincipalName
import org.wfanet.measurement.kingdom.deploy.common.service.DataServices
import org.wfanet.measurement.loadtest.config.VidSampling
import org.wfanet.measurement.loadtest.dataprovider.EventQuery
import org.wfanet.measurement.loadtest.dataprovider.MeasurementResults
import org.wfanet.measurement.loadtest.dataprovider.SyntheticGeneratorEventQuery
import org.wfanet.measurement.loadtest.measurementconsumer.MetadataSyntheticGeneratorEventQuery
import org.wfanet.measurement.reporting.deploy.v2.common.server.InternalReportingServer
import org.wfanet.measurement.reporting.service.api.v2alpha.withDefaults
import org.wfanet.measurement.reporting.v2alpha.EventGroup
import org.wfanet.measurement.reporting.v2alpha.EventGroupsGrpcKt.EventGroupsCoroutineStub
import org.wfanet.measurement.reporting.v2alpha.Metric
import org.wfanet.measurement.reporting.v2alpha.MetricSpec.VidSamplingInterval
import org.wfanet.measurement.reporting.v2alpha.MetricSpecKt
import org.wfanet.measurement.reporting.v2alpha.MetricsGrpcKt.MetricsCoroutineStub
import org.wfanet.measurement.reporting.v2alpha.Report
import org.wfanet.measurement.reporting.v2alpha.ReportKt
import org.wfanet.measurement.reporting.v2alpha.ReportingSet
import org.wfanet.measurement.reporting.v2alpha.ReportingSetKt
import org.wfanet.measurement.reporting.v2alpha.ReportingSetsGrpcKt.ReportingSetsCoroutineStub
import org.wfanet.measurement.reporting.v2alpha.ReportsGrpcKt.ReportsCoroutineStub
import org.wfanet.measurement.reporting.v2alpha.createMetricRequest
import org.wfanet.measurement.reporting.v2alpha.createReportRequest
import org.wfanet.measurement.reporting.v2alpha.createReportingSetRequest
import org.wfanet.measurement.reporting.v2alpha.getMetricRequest
import org.wfanet.measurement.reporting.v2alpha.getReportRequest
import org.wfanet.measurement.reporting.v2alpha.listEventGroupsRequest
import org.wfanet.measurement.reporting.v2alpha.listMetricsRequest
import org.wfanet.measurement.reporting.v2alpha.listReportingSetsRequest
import org.wfanet.measurement.reporting.v2alpha.listReportsRequest
import org.wfanet.measurement.reporting.v2alpha.metric
import org.wfanet.measurement.reporting.v2alpha.metricSpec
import org.wfanet.measurement.reporting.v2alpha.periodicTimeInterval
import org.wfanet.measurement.reporting.v2alpha.report
import org.wfanet.measurement.reporting.v2alpha.reportingSet
import org.wfanet.measurement.reporting.v2alpha.timeIntervals
import org.wfanet.measurement.system.v1alpha.ComputationLogEntriesGrpcKt

/**
 * Test that everything is wired up properly.
 *
 * This is abstract so that different implementations of dependencies can all run the same tests
 * easily.
 */
abstract class InProcessLifeOfAReportIntegrationTest(
  kingdomDataServicesRule: ProviderRule<DataServices>,
  duchyDependenciesRule:
    ProviderRule<
      (
        String, ComputationLogEntriesGrpcKt.ComputationLogEntriesCoroutineStub
      ) -> InProcessDuchy.DuchyDependencies
    >
) {
  private val inProcessCmmsComponents: InProcessCmmsComponents =
    InProcessCmmsComponents(kingdomDataServicesRule, duchyDependenciesRule)

  private val inProcessCmmsComponentsStartup = TestRule { base, _ ->
    object : Statement() {
      override fun evaluate() {
        inProcessCmmsComponents.startDaemons()
        base.evaluate()
      }
    }
  }

  abstract val internalReportingServerServices: InternalReportingServer.Services

  private val reportingServer: InProcessReportingServer by lazy {
    val encryptionKeyPairConfigGenerator: () -> EncryptionKeyPairConfig = {
      val measurementConsumerData = inProcessCmmsComponents.getMeasurementConsumerData()

      encryptionKeyPairConfig {
        principalKeyPairs += principalKeyPairs {
          principal = measurementConsumerData.name
          keyPairs += keyPair {
            publicKeyFile = "mc_enc_public.tink"
            privateKeyFile = "mc_enc_private.tink"
          }
        }
      }
    }

    val measurementConsumerConfigGenerator: suspend () -> MeasurementConsumerConfig = {
      val measurementConsumerData = inProcessCmmsComponents.getMeasurementConsumerData()

      val measurementConsumer =
        publicKingdomMeasurementConsumersClient
          .withAuthenticationKey(measurementConsumerData.apiAuthenticationKey)
          .getMeasurementConsumer(
            getMeasurementConsumerRequest { name = measurementConsumerData.name }
          )

      measurementConsumerConfig {
        apiKey = measurementConsumerData.apiAuthenticationKey
        signingCertificateName = measurementConsumer.certificate
        signingPrivateKeyPath = MC_SIGNING_PRIVATE_KEY_PATH
      }
    }

    InProcessReportingServer(
      internalReportingServerServices,
      { inProcessCmmsComponents.kingdom.publicApiChannel },
      encryptionKeyPairConfigGenerator,
      SECRETS_DIR,
      measurementConsumerConfigGenerator,
      TRUSTED_CERTIFICATES,
      verboseGrpcLogging = false,
    )
  }

  @get:Rule
  val ruleChain: TestRule by lazy {
    chainRulesSequentially(inProcessCmmsComponents, inProcessCmmsComponentsStartup, reportingServer)
  }

  private val publicKingdomMeasurementConsumersClient by lazy {
    MeasurementConsumersCoroutineStub(inProcessCmmsComponents.kingdom.publicApiChannel)
  }

  private val publicDataProvidersClient by lazy {
    DataProvidersCoroutineStub(reportingServer.publicApiChannel)
  }

  private val publicEventGroupMetadataDescriptorsClient by lazy {
    EventGroupMetadataDescriptorsCoroutineStub(reportingServer.publicApiChannel)
  }

  private val publicEventGroupsClient by lazy {
    EventGroupsCoroutineStub(reportingServer.publicApiChannel)
  }

  private val publicMetricsClient by lazy { MetricsCoroutineStub(reportingServer.publicApiChannel) }

  private val publicReportsClient by lazy { ReportsCoroutineStub(reportingServer.publicApiChannel) }

  private val publicReportingSetsClient by lazy {
    ReportingSetsCoroutineStub(reportingServer.publicApiChannel)
  }

  @After
  fun stopEdpSimulators() {
    inProcessCmmsComponents.stopEdpSimulators()
  }

  @After
  fun stopDuchyDaemons() {
    inProcessCmmsComponents.stopDuchyDaemons()
  }

  @Test
  fun `report with union reach across 2 edps has the expected result`() = runBlocking {
    val measurementConsumerData = inProcessCmmsComponents.getMeasurementConsumerData()
    val eventGroups = listEventGroups()

    val primitiveReportingSet = reportingSet {
      displayName = "primitive"
      filter = "person.age_group == ${Person.AgeGroup.YEARS_18_TO_34_VALUE}"
      primitive = ReportingSetKt.primitive { cmmsEventGroups += eventGroups[0].cmmsEventGroup }
    }

    val createdPrimitiveReportingSet =
      publicReportingSetsClient
        .withPrincipalName(measurementConsumerData.name)
        .createReportingSet(
          createReportingSetRequest {
            parent = measurementConsumerData.name
            reportingSet = primitiveReportingSet
            reportingSetId = "abc"
          }
        )

    val primitiveReportingSet2 = reportingSet {
      displayName = "primitive"
      filter = "person.age_group == ${Person.AgeGroup.YEARS_55_PLUS_VALUE}"
      primitive = ReportingSetKt.primitive { cmmsEventGroups += eventGroups[1].cmmsEventGroup }
    }

    val createdPrimitiveReportingSet2 =
      publicReportingSetsClient
        .withPrincipalName(measurementConsumerData.name)
        .createReportingSet(
          createReportingSetRequest {
            parent = measurementConsumerData.name
            reportingSet = primitiveReportingSet2
            reportingSetId = "abc2"
          }
        )

    val compositeReportingSet = reportingSet {
      displayName = "composite"
      composite =
        ReportingSetKt.composite {
          expression =
            ReportingSetKt.setExpression {
              operation = ReportingSet.SetExpression.Operation.UNION
              lhs =
                ReportingSetKt.SetExpressionKt.operand {
                  reportingSet = createdPrimitiveReportingSet.name
                }
              rhs =
                ReportingSetKt.SetExpressionKt.operand {
                  reportingSet = createdPrimitiveReportingSet2.name
                }
            }
        }
    }

    val createdCompositeReportingSet =
      publicReportingSetsClient
        .withPrincipalName(measurementConsumerData.name)
        .createReportingSet(
          createReportingSetRequest {
            parent = measurementConsumerData.name
            reportingSet = compositeReportingSet
            reportingSetId = "def"
          }
        )

    val report = report {
      reportingMetricEntries +=
        ReportKt.reportingMetricEntry {
          key = createdCompositeReportingSet.name
          value =
            ReportKt.reportingMetricCalculationSpec {
              metricCalculationSpecs +=
                ReportKt.metricCalculationSpec {
                  displayName = "union reach"
                  metricSpecs +=
                    metricSpec {
                        reach = MetricSpecKt.reachParams { privacyParams = DP_PARAMS }
                        vidSamplingInterval = VID_SAMPLING_INTERVAL
                      }
                      .withDefaults(reportingServer.metricSpecConfig)
                }
            }
        }
      timeIntervals = timeIntervals { timeIntervals += EVENT_RANGE.toInterval() }
    }

    val createdReport =
      publicReportsClient
        .withPrincipalName(measurementConsumerData.name)
        .createReport(
          createReportRequest {
            parent = measurementConsumerData.name
            this.report = report
            reportId = "report"
          }
        )

    val retrievedReport = pollForCompletedReport(measurementConsumerData.name, createdReport.name)
    assertThat(retrievedReport.state).isEqualTo(Report.State.SUCCEEDED)

    val vids =
      SYNTHETIC_EVENT_QUERY.getUserVirtualIds(
        eventGroups[0],
        "(${primitiveReportingSet.filter}) || (${primitiveReportingSet2.filter})",
        EVENT_RANGE.toInterval()
      )
    val sampledVids =
      vids.calculateSampledVids(
        report.reportingMetricEntriesList[0]
          .value
          .metricCalculationSpecsList[0]
          .metricSpecsList[0]
          .vidSamplingInterval
      )
    val expectedResult = calculateExpectedReachMeasurementResult(sampledVids)

    val actualResult =
      MeasurementKt.result {
        reach =
          MeasurementKt.ResultKt.reach {
            value =
              retrievedReport.metricCalculationResultsList[0]
                .resultAttributesList[0]
                .metricResult
                .reach
                .value
          }
      }
    // TODO(@tristanvuong2021): Assert using variance
    assertThat(actualResult).reachValue().isWithinPercent(10.0).of(expectedResult.reach.value)
  }

  @Test
  fun `report with unique reach has the expected result`() = runBlocking {
    val measurementConsumerData = inProcessCmmsComponents.getMeasurementConsumerData()
    val eventGroups = listEventGroups()
    val eventGroup = eventGroups[0]

    val primitiveReportingSet = reportingSet {
      displayName = "primitive"
      filter = "person.age_group == ${Person.AgeGroup.YEARS_18_TO_34_VALUE}"
      primitive = ReportingSetKt.primitive { cmmsEventGroups += eventGroup.cmmsEventGroup }
    }

    val createdPrimitiveReportingSet =
      publicReportingSetsClient
        .withPrincipalName(measurementConsumerData.name)
        .createReportingSet(
          createReportingSetRequest {
            parent = measurementConsumerData.name
            reportingSet = primitiveReportingSet
            reportingSetId = "abc"
          }
        )

    val primitiveReportingSet2 = reportingSet {
      displayName = "primitive"
      filter = "person.gender == ${Person.Gender.MALE_VALUE}"
      primitive = ReportingSetKt.primitive { cmmsEventGroups += eventGroup.cmmsEventGroup }
    }

    val createdPrimitiveReportingSet2 =
      publicReportingSetsClient
        .withPrincipalName(measurementConsumerData.name)
        .createReportingSet(
          createReportingSetRequest {
            parent = measurementConsumerData.name
            reportingSet = primitiveReportingSet2
            reportingSetId = "abc2"
          }
        )

    val compositeReportingSet = reportingSet {
      displayName = "composite"
      composite =
        ReportingSetKt.composite {
          expression =
            ReportingSetKt.setExpression {
              operation = ReportingSet.SetExpression.Operation.DIFFERENCE
              lhs =
                ReportingSetKt.SetExpressionKt.operand {
                  expression =
                    ReportingSetKt.setExpression {
                      operation = ReportingSet.SetExpression.Operation.UNION
                      lhs =
                        ReportingSetKt.SetExpressionKt.operand {
                          reportingSet = createdPrimitiveReportingSet.name
                        }
                      rhs =
                        ReportingSetKt.SetExpressionKt.operand {
                          reportingSet = createdPrimitiveReportingSet2.name
                        }
                    }
                }
              rhs =
                ReportingSetKt.SetExpressionKt.operand {
                  reportingSet = createdPrimitiveReportingSet2.name
                }
            }
        }
    }

    val createdCompositeReportingSet =
      publicReportingSetsClient
        .withPrincipalName(measurementConsumerData.name)
        .createReportingSet(
          createReportingSetRequest {
            parent = measurementConsumerData.name
            reportingSet = compositeReportingSet
            reportingSetId = "def"
          }
        )

    val report = report {
      reportingMetricEntries +=
        ReportKt.reportingMetricEntry {
          key = createdCompositeReportingSet.name
          value =
            ReportKt.reportingMetricCalculationSpec {
              metricCalculationSpecs +=
                ReportKt.metricCalculationSpec {
                  displayName = "unique reach"
                  metricSpecs +=
                    metricSpec {
                        reach = MetricSpecKt.reachParams { privacyParams = DP_PARAMS }
                        vidSamplingInterval = VID_SAMPLING_INTERVAL
                      }
                      .withDefaults(reportingServer.metricSpecConfig)
                }
            }
        }
      timeIntervals = timeIntervals { timeIntervals += EVENT_RANGE.toInterval() }
    }

    val createdReport =
      publicReportsClient
        .withPrincipalName(measurementConsumerData.name)
        .createReport(
          createReportRequest {
            parent = measurementConsumerData.name
            this.report = report
            reportId = "report"
          }
        )

    val retrievedReport = pollForCompletedReport(measurementConsumerData.name, createdReport.name)
    assertThat(retrievedReport.state).isEqualTo(Report.State.SUCCEEDED)

    val vidsLhs =
      SYNTHETIC_EVENT_QUERY.getUserVirtualIds(
        eventGroup,
        "(${primitiveReportingSet.filter}) || (${primitiveReportingSet2.filter})",
        EVENT_RANGE.toInterval()
      )
    val sampledVidsLhs =
      vidsLhs.calculateSampledVids(
        report.reportingMetricEntriesList[0]
          .value
          .metricCalculationSpecsList[0]
          .metricSpecsList[0]
          .vidSamplingInterval
      )
    val expectedResultLhs = calculateExpectedReachMeasurementResult(sampledVidsLhs)

    val vidsRhs =
      SYNTHETIC_EVENT_QUERY.getUserVirtualIds(
        eventGroup,
        primitiveReportingSet2.filter,
        EVENT_RANGE.toInterval()
      )
    val sampledVidsRhs =
      vidsRhs.calculateSampledVids(
        report.reportingMetricEntriesList[0]
          .value
          .metricCalculationSpecsList[0]
          .metricSpecsList[0]
          .vidSamplingInterval
      )
    val expectedResultRhs = calculateExpectedReachMeasurementResult(sampledVidsRhs)

    val actualResult =
      MeasurementKt.result {
        reach =
          MeasurementKt.ResultKt.reach {
            value =
              retrievedReport.metricCalculationResultsList[0]
                .resultAttributesList[0]
                .metricResult
                .reach
                .value
          }
      }
    // TODO(@tristanvuong2021): Assert using variance
    assertThat(actualResult)
      .reachValue()
      .isWithinPercent(0.5)
      .of(expectedResultLhs.reach.value - expectedResultRhs.reach.value)
  }

  @Test
  fun `report with intersection reach has the expected result`() = runBlocking {
    val measurementConsumerData = inProcessCmmsComponents.getMeasurementConsumerData()
    val eventGroups = listEventGroups()
    val eventGroup = eventGroups[0]

    val primitiveReportingSet = reportingSet {
      displayName = "primitive"
      filter = "person.age_group == ${Person.AgeGroup.YEARS_18_TO_34_VALUE}"
      primitive = ReportingSetKt.primitive { cmmsEventGroups += eventGroup.cmmsEventGroup }
    }

    val createdPrimitiveReportingSet =
      publicReportingSetsClient
        .withPrincipalName(measurementConsumerData.name)
        .createReportingSet(
          createReportingSetRequest {
            parent = measurementConsumerData.name
            reportingSet = primitiveReportingSet
            reportingSetId = "abc"
          }
        )

    val primitiveReportingSet2 = reportingSet {
      displayName = "primitive"
      filter = "person.gender == ${Person.Gender.FEMALE_VALUE}"
      primitive = ReportingSetKt.primitive { cmmsEventGroups += eventGroup.cmmsEventGroup }
    }

    val createdPrimitiveReportingSet2 =
      publicReportingSetsClient
        .withPrincipalName(measurementConsumerData.name)
        .createReportingSet(
          createReportingSetRequest {
            parent = measurementConsumerData.name
            reportingSet = primitiveReportingSet2
            reportingSetId = "abc2"
          }
        )

    val compositeReportingSet = reportingSet {
      displayName = "composite"
      composite =
        ReportingSetKt.composite {
          expression =
            ReportingSetKt.setExpression {
              operation = ReportingSet.SetExpression.Operation.INTERSECTION
              lhs =
                ReportingSetKt.SetExpressionKt.operand {
                  reportingSet = createdPrimitiveReportingSet.name
                }
              rhs =
                ReportingSetKt.SetExpressionKt.operand {
                  reportingSet = createdPrimitiveReportingSet2.name
                }
            }
        }
    }

    val createdCompositeReportingSet =
      publicReportingSetsClient
        .withPrincipalName(measurementConsumerData.name)
        .createReportingSet(
          createReportingSetRequest {
            parent = measurementConsumerData.name
            reportingSet = compositeReportingSet
            reportingSetId = "def"
          }
        )

    val report = report {
      reportingMetricEntries +=
        ReportKt.reportingMetricEntry {
          key = createdCompositeReportingSet.name
          value =
            ReportKt.reportingMetricCalculationSpec {
              metricCalculationSpecs +=
                ReportKt.metricCalculationSpec {
                  displayName = "intersection reach"
                  metricSpecs +=
                    metricSpec {
                        reach = MetricSpecKt.reachParams { privacyParams = DP_PARAMS }
                        vidSamplingInterval = VID_SAMPLING_INTERVAL
                      }
                      .withDefaults(reportingServer.metricSpecConfig)
                }
            }
        }
      timeIntervals = timeIntervals { timeIntervals += EVENT_RANGE.toInterval() }
    }

    val createdReport =
      publicReportsClient
        .withPrincipalName(measurementConsumerData.name)
        .createReport(
          createReportRequest {
            parent = measurementConsumerData.name
            this.report = report
            reportId = "report"
          }
        )

    val retrievedReport = pollForCompletedReport(measurementConsumerData.name, createdReport.name)
    assertThat(retrievedReport.state).isEqualTo(Report.State.SUCCEEDED)

    val vids =
      SYNTHETIC_EVENT_QUERY.getUserVirtualIds(
        eventGroup,
        "(${primitiveReportingSet.filter}) && (${primitiveReportingSet2.filter})",
        EVENT_RANGE.toInterval()
      )
    val sampledVids =
      vids.calculateSampledVids(
        report.reportingMetricEntriesList[0]
          .value
          .metricCalculationSpecsList[0]
          .metricSpecsList[0]
          .vidSamplingInterval
      )
    val expectedResult = calculateExpectedReachMeasurementResult(sampledVids)

    val actualResult =
      MeasurementKt.result {
        reach =
          MeasurementKt.ResultKt.reach {
            value =
              retrievedReport.metricCalculationResultsList[0]
                .resultAttributesList[0]
                .metricResult
                .reach
                .value
          }
      }
    // TODO(@tristanvuong2021): Assert using variance
    assertThat(actualResult).reachValue().isWithinPercent(0.5).of(expectedResult.reach.value)
  }

  @Test
  fun `report with 2 reporting metric entries has the expected result`() = runBlocking {
    val measurementConsumerData = inProcessCmmsComponents.getMeasurementConsumerData()
    val eventGroups = listEventGroups()
    val eventGroup = eventGroups[0]

    val primitiveReportingSet = reportingSet {
      displayName = "primitive"
      filter = "person.age_group == ${Person.AgeGroup.YEARS_18_TO_34_VALUE}"
      primitive = ReportingSetKt.primitive { cmmsEventGroups += eventGroup.cmmsEventGroup }
    }

    val createdPrimitiveReportingSet =
      publicReportingSetsClient
        .withPrincipalName(measurementConsumerData.name)
        .createReportingSet(
          createReportingSetRequest {
            parent = measurementConsumerData.name
            reportingSet = primitiveReportingSet
            reportingSetId = "abc"
          }
        )

    val report = report {
      reportingMetricEntries +=
        ReportKt.reportingMetricEntry {
          key = createdPrimitiveReportingSet.name
          value =
            ReportKt.reportingMetricCalculationSpec {
              metricCalculationSpecs +=
                ReportKt.metricCalculationSpec {
                  displayName = "union reach"
                  metricSpecs +=
                    metricSpec {
                        reach = MetricSpecKt.reachParams { privacyParams = DP_PARAMS }
                        vidSamplingInterval = VID_SAMPLING_INTERVAL
                      }
                      .withDefaults(reportingServer.metricSpecConfig)
                }
            }
        }
      reportingMetricEntries +=
        ReportKt.reportingMetricEntry {
          key = createdPrimitiveReportingSet.name
          value =
            ReportKt.reportingMetricCalculationSpec {
              metricCalculationSpecs +=
                ReportKt.metricCalculationSpec {
                  displayName = "union reach"
                  metricSpecs +=
                    metricSpec {
                        reach = MetricSpecKt.reachParams { privacyParams = DP_PARAMS }
                        vidSamplingInterval = VID_SAMPLING_INTERVAL
                      }
                      .withDefaults(reportingServer.metricSpecConfig)
                }
            }
        }
      timeIntervals = timeIntervals { timeIntervals += EVENT_RANGE.toInterval() }
    }

    val createdReport =
      publicReportsClient
        .withPrincipalName(measurementConsumerData.name)
        .createReport(
          createReportRequest {
            parent = measurementConsumerData.name
            this.report = report
            reportId = "report"
          }
        )

    val retrievedReport = pollForCompletedReport(measurementConsumerData.name, createdReport.name)
    assertThat(retrievedReport.state).isEqualTo(Report.State.SUCCEEDED)

    val vids =
      SYNTHETIC_EVENT_QUERY.getUserVirtualIds(
        eventGroup,
        primitiveReportingSet.filter,
        EVENT_RANGE.toInterval()
      )
    val sampledVids =
      vids.calculateSampledVids(
        report.reportingMetricEntriesList[0]
          .value
          .metricCalculationSpecsList[0]
          .metricSpecsList[0]
          .vidSamplingInterval
      )
    val expectedResult = calculateExpectedReachMeasurementResult(sampledVids)

    for (resultAttribute in retrievedReport.metricCalculationResultsList[0].resultAttributesList) {
      val actualResult =
        MeasurementKt.result {
          reach = MeasurementKt.ResultKt.reach { value = resultAttribute.metricResult.reach.value }
        }
      // TODO(@tristanvuong2021): Assert using variance
      assertThat(actualResult).reachValue().isWithinPercent(0.5).of(expectedResult.reach.value)
    }
  }

  @Test
  fun `report across two time intervals has the expected result`() = runBlocking {
    val measurementConsumerData = inProcessCmmsComponents.getMeasurementConsumerData()
    val eventGroups = listEventGroups()
    val eventGroup = eventGroups[0]

    val primitiveReportingSet = reportingSet {
      displayName = "primitive"
      filter = "person.age_group == ${Person.AgeGroup.YEARS_18_TO_34_VALUE}"
      primitive = ReportingSetKt.primitive { cmmsEventGroups += eventGroup.cmmsEventGroup }
    }

    val createdPrimitiveReportingSet =
      publicReportingSetsClient
        .withPrincipalName(measurementConsumerData.name)
        .createReportingSet(
          createReportingSetRequest {
            parent = measurementConsumerData.name
            reportingSet = primitiveReportingSet
            reportingSetId = "abc"
          }
        )

    val eventRange2 =
      OpenEndTimeRange.fromClosedDateRange(LocalDate.of(2021, 3, 18)..LocalDate.of(2021, 3, 19))

    val report = report {
      reportingMetricEntries +=
        ReportKt.reportingMetricEntry {
          key = createdPrimitiveReportingSet.name
          value =
            ReportKt.reportingMetricCalculationSpec {
              metricCalculationSpecs +=
                ReportKt.metricCalculationSpec {
                  displayName = "union reach"
                  metricSpecs +=
                    metricSpec {
                        reach = MetricSpecKt.reachParams { privacyParams = DP_PARAMS }
                        vidSamplingInterval = VID_SAMPLING_INTERVAL
                      }
                      .withDefaults(reportingServer.metricSpecConfig)
                }
            }
        }
      timeIntervals = timeIntervals {
        timeIntervals += EVENT_RANGE.toInterval()
        timeIntervals += eventRange2.toInterval()
      }
    }

    val createdReport =
      publicReportsClient
        .withPrincipalName(measurementConsumerData.name)
        .createReport(
          createReportRequest {
            parent = measurementConsumerData.name
            this.report = report
            reportId = "report"
          }
        )

    val retrievedReport = pollForCompletedReport(measurementConsumerData.name, createdReport.name)
    assertThat(retrievedReport.state).isEqualTo(Report.State.SUCCEEDED)

    val vids =
      SYNTHETIC_EVENT_QUERY.getUserVirtualIds(
        eventGroup,
        primitiveReportingSet.filter,
        EVENT_RANGE.toInterval()
      )
    val sampledVids =
      vids.calculateSampledVids(
        report.reportingMetricEntriesList[0]
          .value
          .metricCalculationSpecsList[0]
          .metricSpecsList[0]
          .vidSamplingInterval
      )
    val expectedResult = calculateExpectedReachMeasurementResult(sampledVids)

    for (resultAttribute in retrievedReport.metricCalculationResultsList[0].resultAttributesList) {
      val actualResult =
        MeasurementKt.result {
          reach = MeasurementKt.ResultKt.reach { value = resultAttribute.metricResult.reach.value }
        }
      // TODO(@tristanvuong2021): Assert using variance
      if (
        Timestamps.compare(
          resultAttribute.timeInterval.startTime,
          EVENT_RANGE.toInterval().startTime
        ) == 0
      ) {
        assertThat(actualResult).reachValue().isWithinPercent(0.5).of(expectedResult.reach.value)
      } else {
        assertThat(actualResult).reachValue().isWithinPercent(500.0).of(1)
      }
    }
  }

  @Test
  fun `report with periodic time interval has the expected result`() = runBlocking {
    val measurementConsumerData = inProcessCmmsComponents.getMeasurementConsumerData()
    val eventGroups = listEventGroups()
    val eventGroup = eventGroups[0]

    val primitiveReportingSet = reportingSet {
      displayName = "primitive"
      filter = "person.age_group == ${Person.AgeGroup.YEARS_18_TO_34_VALUE}"
      primitive = ReportingSetKt.primitive { cmmsEventGroups += eventGroup.cmmsEventGroup }
    }

    val createdPrimitiveReportingSet =
      publicReportingSetsClient
        .withPrincipalName(measurementConsumerData.name)
        .createReportingSet(
          createReportingSetRequest {
            parent = measurementConsumerData.name
            reportingSet = primitiveReportingSet
            reportingSetId = "abc"
          }
        )

    val report = report {
      reportingMetricEntries +=
        ReportKt.reportingMetricEntry {
          key = createdPrimitiveReportingSet.name
          value =
            ReportKt.reportingMetricCalculationSpec {
              metricCalculationSpecs +=
                ReportKt.metricCalculationSpec {
                  displayName = "union reach"
                  metricSpecs +=
                    metricSpec {
                        reach = MetricSpecKt.reachParams { privacyParams = DP_PARAMS }
                        vidSamplingInterval = VID_SAMPLING_INTERVAL
                      }
                      .withDefaults(reportingServer.metricSpecConfig)
                }
            }
        }
      periodicTimeInterval = periodicTimeInterval {
        startTime = EVENT_RANGE.start.toProtoTime()
        increment = Durations.fromDays(1L)
        intervalCount = 2
      }
    }

    val createdReport =
      publicReportsClient
        .withPrincipalName(measurementConsumerData.name)
        .createReport(
          createReportRequest {
            parent = measurementConsumerData.name
            this.report = report
            reportId = "report"
          }
        )

    val retrievedReport = pollForCompletedReport(measurementConsumerData.name, createdReport.name)
    assertThat(retrievedReport.state).isEqualTo(Report.State.SUCCEEDED)

    val vids =
      SYNTHETIC_EVENT_QUERY.getUserVirtualIds(
        eventGroup,
        primitiveReportingSet.filter,
        EVENT_RANGE.toInterval()
      )
    val sampledVids =
      vids.calculateSampledVids(
        report.reportingMetricEntriesList[0]
          .value
          .metricCalculationSpecsList[0]
          .metricSpecsList[0]
          .vidSamplingInterval
      )
    val expectedResult = calculateExpectedReachMeasurementResult(sampledVids)

    for (resultAttribute in retrievedReport.metricCalculationResultsList[0].resultAttributesList) {
      val actualResult =
        MeasurementKt.result {
          reach = MeasurementKt.ResultKt.reach { value = resultAttribute.metricResult.reach.value }
        }
      // TODO(@tristanvuong2021): Assert using variance
      if (
        Timestamps.compare(
          resultAttribute.timeInterval.startTime,
          EVENT_RANGE.toInterval().startTime
        ) == 0
      ) {
        assertThat(actualResult).reachValue().isWithinPercent(0.5).of(expectedResult.reach.value)
      } else {
        assertThat(actualResult).reachValue().isWithinPercent(500.0).of(1)
      }
    }
  }

  @Test
  fun `report with cumulative has the expected result`() = runBlocking {
    val measurementConsumerData = inProcessCmmsComponents.getMeasurementConsumerData()
    val eventGroups = listEventGroups()
    val eventGroup = eventGroups[0]

    val primitiveReportingSet = reportingSet {
      displayName = "primitive"
      filter = "person.age_group == ${Person.AgeGroup.YEARS_18_TO_34_VALUE}"
      primitive = ReportingSetKt.primitive { cmmsEventGroups += eventGroup.cmmsEventGroup }
    }

    val createdPrimitiveReportingSet =
      publicReportingSetsClient
        .withPrincipalName(measurementConsumerData.name)
        .createReportingSet(
          createReportingSetRequest {
            parent = measurementConsumerData.name
            reportingSet = primitiveReportingSet
            reportingSetId = "abc"
          }
        )

    val report = report {
      reportingMetricEntries +=
        ReportKt.reportingMetricEntry {
          key = createdPrimitiveReportingSet.name
          value =
            ReportKt.reportingMetricCalculationSpec {
              metricCalculationSpecs +=
                ReportKt.metricCalculationSpec {
                  displayName = "union reach"
                  metricSpecs +=
                    metricSpec {
                        reach = MetricSpecKt.reachParams { privacyParams = DP_PARAMS }
                        vidSamplingInterval = VID_SAMPLING_INTERVAL
                      }
                      .withDefaults(reportingServer.metricSpecConfig)
                  cumulative = true
                }
            }
        }
      periodicTimeInterval = periodicTimeInterval {
        startTime = EVENT_RANGE.start.toProtoTime()
        increment = Durations.fromDays(1L)
        intervalCount = 2
      }
    }

    val createdReport =
      publicReportsClient
        .withPrincipalName(measurementConsumerData.name)
        .createReport(
          createReportRequest {
            parent = measurementConsumerData.name
            this.report = report
            reportId = "report"
          }
        )

    val retrievedReport = pollForCompletedReport(measurementConsumerData.name, createdReport.name)
    assertThat(retrievedReport.state).isEqualTo(Report.State.SUCCEEDED)

    for (resultAttribute in retrievedReport.metricCalculationResultsList[0].resultAttributesList) {
      val vids =
        SYNTHETIC_EVENT_QUERY.getUserVirtualIds(
          eventGroup,
          primitiveReportingSet.filter,
          resultAttribute.timeInterval
        )
      val sampledVids =
        vids.calculateSampledVids(
          report.reportingMetricEntriesList[0]
            .value
            .metricCalculationSpecsList[0]
            .metricSpecsList[0]
            .vidSamplingInterval
        )
      val expectedResult = calculateExpectedReachMeasurementResult(sampledVids)

      val actualResult =
        MeasurementKt.result {
          reach = MeasurementKt.ResultKt.reach { value = resultAttribute.metricResult.reach.value }
        }
      // TODO(@tristanvuong2021): Assert using variance
      assertThat(actualResult).reachValue().isWithinPercent(0.5).of(expectedResult.reach.value)
    }
  }

  @Test
  fun `report with group by has the expected result`() = runBlocking {
    val measurementConsumerData = inProcessCmmsComponents.getMeasurementConsumerData()
    val eventGroups = listEventGroups()
    val eventGroup = eventGroups[0]

    val primitiveReportingSet = reportingSet {
      displayName = "primitive"
      primitive = ReportingSetKt.primitive { cmmsEventGroups += eventGroup.cmmsEventGroup }
    }

    val createdPrimitiveReportingSet =
      publicReportingSetsClient
        .withPrincipalName(measurementConsumerData.name)
        .createReportingSet(
          createReportingSetRequest {
            parent = measurementConsumerData.name
            reportingSet = primitiveReportingSet
            reportingSetId = "abc"
          }
        )

    val grouping1Predicate1 = "person.age_group == ${Person.AgeGroup.YEARS_35_TO_54_VALUE}"
    val grouping1Predicate2 = "person.age_group == ${Person.AgeGroup.YEARS_18_TO_34_VALUE}"
    val grouping2Predicate1 = "person.gender == ${Person.Gender.FEMALE_VALUE}"
    val grouping2Predicate2 = "person.gender == ${Person.Gender.MALE_VALUE}"

    val report = report {
      reportingMetricEntries +=
        ReportKt.reportingMetricEntry {
          key = createdPrimitiveReportingSet.name
          value =
            ReportKt.reportingMetricCalculationSpec {
              metricCalculationSpecs +=
                ReportKt.metricCalculationSpec {
                  displayName = "union reach"
                  metricSpecs +=
                    metricSpec {
                        reach = MetricSpecKt.reachParams { privacyParams = DP_PARAMS }
                        vidSamplingInterval = VID_SAMPLING_INTERVAL
                      }
                      .withDefaults(reportingServer.metricSpecConfig)
                  groupings +=
                    ReportKt.grouping {
                      predicates += grouping1Predicate1
                      predicates += grouping1Predicate2
                    }
                  groupings +=
                    ReportKt.grouping {
                      predicates += grouping2Predicate1
                      predicates += grouping2Predicate2
                    }
                }
            }
        }
      timeIntervals = timeIntervals { timeIntervals += EVENT_RANGE.toInterval() }
    }

    val createdReport =
      publicReportsClient
        .withPrincipalName(measurementConsumerData.name)
        .createReport(
          createReportRequest {
            parent = measurementConsumerData.name
            this.report = report
            reportId = "report"
          }
        )

    val retrievedReport = pollForCompletedReport(measurementConsumerData.name, createdReport.name)
    assertThat(retrievedReport.state).isEqualTo(Report.State.SUCCEEDED)

    val vidSamplingInterval =
      report.reportingMetricEntriesList[0]
        .value
        .metricCalculationSpecsList[0]
        .metricSpecsList[0]
        .vidSamplingInterval

    for (resultAttribute in retrievedReport.metricCalculationResultsList[0].resultAttributesList) {
      val actualResult =
        MeasurementKt.result {
          reach = MeasurementKt.ResultKt.reach { value = resultAttribute.metricResult.reach.value }
        }

      // TODO(@tristanvuong2021): Assert using variance
      if (resultAttribute.groupingPredicatesList.contains(grouping1Predicate1)) {
        if (resultAttribute.groupingPredicatesList.contains(grouping2Predicate1)) {
          val vids =
            SYNTHETIC_EVENT_QUERY.getUserVirtualIds(
              eventGroup,
              "$grouping1Predicate1 && $grouping2Predicate1",
              EVENT_RANGE.toInterval()
            )
          val sampledVids = vids.calculateSampledVids(vidSamplingInterval)
          val expectedResult = calculateExpectedReachMeasurementResult(sampledVids)

          assertThat(actualResult).reachValue().isWithinPercent(0.5).of(expectedResult.reach.value)
        } else {
          val vids =
            SYNTHETIC_EVENT_QUERY.getUserVirtualIds(
              eventGroup,
              "$grouping1Predicate1 && $grouping2Predicate2",
              EVENT_RANGE.toInterval()
            )
          val sampledVids = vids.calculateSampledVids(vidSamplingInterval)
          val expectedResult = calculateExpectedReachMeasurementResult(sampledVids)

          assertThat(actualResult).reachValue().isWithinPercent(0.5).of(expectedResult.reach.value)
        }
      } else {
        if (resultAttribute.groupingPredicatesList.contains(grouping2Predicate1)) {
          val vids =
            SYNTHETIC_EVENT_QUERY.getUserVirtualIds(
              eventGroup,
              "$grouping1Predicate2 && $grouping2Predicate1",
              EVENT_RANGE.toInterval()
            )
          val sampledVids = vids.calculateSampledVids(vidSamplingInterval)
          val expectedResult = calculateExpectedReachMeasurementResult(sampledVids)

          assertThat(actualResult).reachValue().isWithinPercent(0.5).of(expectedResult.reach.value)
        } else {
          val vids =
            SYNTHETIC_EVENT_QUERY.getUserVirtualIds(
              eventGroup,
              "$grouping1Predicate2 && $grouping2Predicate2",
              EVENT_RANGE.toInterval()
            )
          val sampledVids = vids.calculateSampledVids(vidSamplingInterval)
          val expectedResult = calculateExpectedReachMeasurementResult(sampledVids)

          assertThat(actualResult).reachValue().isWithinPercent(0.5).of(expectedResult.reach.value)
        }
      }
    }
  }

  @Test
  fun `creating 25 reports at once succeeds`() = runBlocking {
    val numReports = 25
    val measurementConsumerData = inProcessCmmsComponents.getMeasurementConsumerData()
    val eventGroups = listEventGroups()

    val primitiveReportingSet = reportingSet {
      displayName = "primitive"
      filter = "person.age_group == ${Person.AgeGroup.YEARS_18_TO_34_VALUE}"
      primitive = ReportingSetKt.primitive { cmmsEventGroups += eventGroups[0].cmmsEventGroup }
    }

    val createdPrimitiveReportingSet =
      publicReportingSetsClient
        .withPrincipalName(measurementConsumerData.name)
        .createReportingSet(
          createReportingSetRequest {
            parent = measurementConsumerData.name
            reportingSet = primitiveReportingSet
            reportingSetId = "abc"
          }
        )

    val report = report {
      reportingMetricEntries +=
        ReportKt.reportingMetricEntry {
          key = createdPrimitiveReportingSet.name
          value =
            ReportKt.reportingMetricCalculationSpec {
              metricCalculationSpecs +=
                ReportKt.metricCalculationSpec {
                  displayName = "load test"
                  metricSpecs +=
                    metricSpec {
                        reach =
                          MetricSpecKt.reachParams {
                            privacyParams = MetricSpecKt.differentialPrivacyParams {}
                          }
                      }
                      .withDefaults(reportingServer.metricSpecConfig)
                }
            }
        }
      timeIntervals = timeIntervals {
        timeIntervals += interval {
          startTime = timestamp { seconds = 100 }
          endTime = timestamp { seconds = 200 }
        }
      }
    }

    val deferred: MutableList<Deferred<Report>> = mutableListOf()
    repeat(numReports) {
      deferred.add(
        async {
          publicReportsClient
            .withPrincipalName(measurementConsumerData.name)
            .createReport(
              createReportRequest {
                parent = measurementConsumerData.name
                this.report = report
                reportId = "report$it"
              }
            )
        }
      )
    }

    deferred.awaitAll()
    val retrievedReports =
      publicReportsClient
        .withPrincipalName(measurementConsumerData.name)
        .listReports(
          listReportsRequest {
            parent = measurementConsumerData.name
            pageSize = numReports
          }
        )
        .reportsList

    assertThat(retrievedReports).hasSize(numReports)
    retrievedReports.forEach {
      assertThat(it)
        .ignoringFields(
          Report.NAME_FIELD_NUMBER,
          Report.STATE_FIELD_NUMBER,
          Report.CREATE_TIME_FIELD_NUMBER,
          Report.METRIC_CALCULATION_RESULTS_FIELD_NUMBER
        )
        .isEqualTo(report)
    }
  }

  @Test
  fun `reach metric result has the expected result`() = runBlocking {
    val measurementConsumerData = inProcessCmmsComponents.getMeasurementConsumerData()
    val eventGroups = listEventGroups()
    val eventGroup = eventGroups[0]

    val primitiveReportingSet = reportingSet {
      displayName = "primitive"
      filter = "person.age_group == ${Person.AgeGroup.YEARS_18_TO_34_VALUE}"
      primitive = ReportingSetKt.primitive { cmmsEventGroups += eventGroup.cmmsEventGroup }
    }

    val createdPrimitiveReportingSet =
      publicReportingSetsClient
        .withPrincipalName(measurementConsumerData.name)
        .createReportingSet(
          createReportingSetRequest {
            parent = measurementConsumerData.name
            reportingSet = primitiveReportingSet
            reportingSetId = "abc"
          }
        )

    val metric = metric {
      reportingSet = createdPrimitiveReportingSet.name
      timeInterval = EVENT_RANGE.toInterval()
      metricSpec =
        metricSpec {
            reach = MetricSpecKt.reachParams { privacyParams = DP_PARAMS }
            vidSamplingInterval = VID_SAMPLING_INTERVAL
          }
          .withDefaults(reportingServer.metricSpecConfig)
    }

    val createdMetric =
      publicMetricsClient
        .withPrincipalName(measurementConsumerData.name)
        .createMetric(
          createMetricRequest {
            parent = measurementConsumerData.name
            this.metric = metric
            metricId = "abc"
          }
        )

    val retrievedMetric = pollForCompletedMetric(measurementConsumerData.name, createdMetric.name)
    assertThat(retrievedMetric.state).isEqualTo(Metric.State.SUCCEEDED)

    val vids =
      SYNTHETIC_EVENT_QUERY.getUserVirtualIds(
        eventGroup,
        primitiveReportingSet.filter,
        EVENT_RANGE.toInterval()
      )
    val sampledVids = vids.calculateSampledVids(metric.metricSpec.vidSamplingInterval)
    val expectedResult = calculateExpectedReachMeasurementResult(sampledVids)

    val actualResult =
      MeasurementKt.result {
        reach = MeasurementKt.ResultKt.reach { value = retrievedMetric.result.reach.value }
      }
    // TODO(@tristanvuong2021): Assert using variance
    assertThat(actualResult).reachValue().isWithinPercent(0.5).of(expectedResult.reach.value)
  }

  @Test
  fun `frequency histogram metric has the expected result`() = runBlocking {
    val measurementConsumerData = inProcessCmmsComponents.getMeasurementConsumerData()
    val eventGroups = listEventGroups()
    val eventGroup = eventGroups[0]

    val primitiveReportingSet = reportingSet {
      displayName = "primitive"
      filter = "person.age_group == ${Person.AgeGroup.YEARS_18_TO_34_VALUE}"
      primitive = ReportingSetKt.primitive { cmmsEventGroups += eventGroup.cmmsEventGroup }
    }

    val createdPrimitiveReportingSet =
      publicReportingSetsClient
        .withPrincipalName(measurementConsumerData.name)
        .createReportingSet(
          createReportingSetRequest {
            parent = measurementConsumerData.name
            reportingSet = primitiveReportingSet
            reportingSetId = "abc"
          }
        )

    val metric = metric {
      reportingSet = createdPrimitiveReportingSet.name
      timeInterval = EVENT_RANGE.toInterval()
      metricSpec =
        metricSpec {
            frequencyHistogram =
              MetricSpecKt.frequencyHistogramParams {
                reachPrivacyParams = DP_PARAMS
                frequencyPrivacyParams = DP_PARAMS
                maximumFrequency = 5
              }
            vidSamplingInterval = VID_SAMPLING_INTERVAL
          }
          .withDefaults(reportingServer.metricSpecConfig)
    }

    val createdMetric =
      publicMetricsClient
        .withPrincipalName(measurementConsumerData.name)
        .createMetric(
          createMetricRequest {
            parent = measurementConsumerData.name
            this.metric = metric
            metricId = "abc"
          }
        )

    val retrievedMetric = pollForCompletedMetric(measurementConsumerData.name, createdMetric.name)
    assertThat(retrievedMetric.state).isEqualTo(Metric.State.SUCCEEDED)

    val vids =
      SYNTHETIC_EVENT_QUERY.getUserVirtualIds(
        eventGroup,
        primitiveReportingSet.filter,
        EVENT_RANGE.toInterval()
      )
    val sampledVids = vids.calculateSampledVids(metric.metricSpec.vidSamplingInterval)
    val expectedResult =
      calculateExpectedReachAndFrequencyMeasurementResult(
        sampledVids,
        metric.metricSpec.frequencyHistogram.maximumFrequency
      )

    val reach =
      retrievedMetric.result.frequencyHistogram.binsList.sumOf { bin -> bin.binResult.value }
    val actualResult =
      MeasurementKt.result {
        frequency =
          MeasurementKt.ResultKt.frequency {
            relativeFrequencyDistribution.putAll(
              retrievedMetric.result.frequencyHistogram.binsList.associate {
                Pair(it.label.toLong(), it.binResult.value / reach)
              }
            )
          }
      }
    // TODO(@tristanvuong2021): Assert using variance
    assertThat(actualResult)
      .frequencyDistribution()
      .isWithin(0.01)
      .of(expectedResult.frequency.relativeFrequencyDistributionMap)
  }

  @Test
  fun `impression count metric has the expected result`() = runBlocking {
    val measurementConsumerData = inProcessCmmsComponents.getMeasurementConsumerData()
    val eventGroups = listEventGroups()
    val eventGroup = eventGroups[0]

    val primitiveReportingSet = reportingSet {
      displayName = "primitive"
      filter = "person.age_group == ${Person.AgeGroup.YEARS_18_TO_34_VALUE}"
      primitive = ReportingSetKt.primitive { cmmsEventGroups += eventGroup.cmmsEventGroup }
    }

    val createdPrimitiveReportingSet =
      publicReportingSetsClient
        .withPrincipalName(measurementConsumerData.name)
        .createReportingSet(
          createReportingSetRequest {
            parent = measurementConsumerData.name
            reportingSet = primitiveReportingSet
            reportingSetId = "abc"
          }
        )

    val metric = metric {
      reportingSet = createdPrimitiveReportingSet.name
      timeInterval = EVENT_RANGE.toInterval()
      metricSpec =
        metricSpec {
            impressionCount = MetricSpecKt.impressionCountParams { privacyParams = DP_PARAMS }
            vidSamplingInterval = VID_SAMPLING_INTERVAL
          }
          .withDefaults(reportingServer.metricSpecConfig)
    }

    val createdMetric =
      publicMetricsClient
        .withPrincipalName(measurementConsumerData.name)
        .createMetric(
          createMetricRequest {
            parent = measurementConsumerData.name
            this.metric = metric
            metricId = "abc"
          }
        )

    val retrievedMetric = pollForCompletedMetric(measurementConsumerData.name, createdMetric.name)
    assertThat(retrievedMetric.state).isEqualTo(Metric.State.SUCCEEDED)

    // TODO(@tristanvuong2021): calculate expected result and compare
  }

  @Test
  fun `watch duration metric has the expected result`() = runBlocking {
    val measurementConsumerData = inProcessCmmsComponents.getMeasurementConsumerData()
    val eventGroups = listEventGroups()
    val eventGroup = eventGroups[0]

    val primitiveReportingSet = reportingSet {
      displayName = "primitive"
      filter = "person.age_group == ${Person.AgeGroup.YEARS_18_TO_34_VALUE}"
      primitive = ReportingSetKt.primitive { cmmsEventGroups += eventGroup.cmmsEventGroup }
    }

    val createdPrimitiveReportingSet =
      publicReportingSetsClient
        .withPrincipalName(measurementConsumerData.name)
        .createReportingSet(
          createReportingSetRequest {
            parent = measurementConsumerData.name
            reportingSet = primitiveReportingSet
            reportingSetId = "abc"
          }
        )

    val metric = metric {
      reportingSet = createdPrimitiveReportingSet.name
      timeInterval = EVENT_RANGE.toInterval()
      metricSpec =
        metricSpec {
            watchDuration = MetricSpecKt.watchDurationParams { privacyParams = DP_PARAMS }
            vidSamplingInterval = VID_SAMPLING_INTERVAL
          }
          .withDefaults(reportingServer.metricSpecConfig)
    }

    val createdMetric =
      publicMetricsClient
        .withPrincipalName(measurementConsumerData.name)
        .createMetric(
          createMetricRequest {
            parent = measurementConsumerData.name
            this.metric = metric
            metricId = "abc"
          }
        )

    val retrievedMetric = pollForCompletedMetric(measurementConsumerData.name, createdMetric.name)
    assertThat(retrievedMetric.state).isEqualTo(Metric.State.SUCCEEDED)

    // TODO(@tristanvuong2021): Calculate watch duration using synthetic spec.
  }

  @Test
  fun `reach metric with filter has the expected result`() = runBlocking {
    val measurementConsumerData = inProcessCmmsComponents.getMeasurementConsumerData()
    val eventGroups = listEventGroups()
    val eventGroup = eventGroups[0]

    val primitiveReportingSet = reportingSet {
      displayName = "primitive"
      filter = "person.age_group == ${Person.AgeGroup.YEARS_18_TO_34_VALUE}"
      primitive = ReportingSetKt.primitive { cmmsEventGroups += eventGroup.cmmsEventGroup }
    }

    val createdPrimitiveReportingSet =
      publicReportingSetsClient
        .withPrincipalName(measurementConsumerData.name)
        .createReportingSet(
          createReportingSetRequest {
            parent = measurementConsumerData.name
            reportingSet = primitiveReportingSet
            reportingSetId = "abc"
          }
        )

    val metric = metric {
      reportingSet = createdPrimitiveReportingSet.name
      timeInterval = EVENT_RANGE.toInterval()
      metricSpec =
        metricSpec {
            reach = MetricSpecKt.reachParams { privacyParams = DP_PARAMS }
            vidSamplingInterval = VID_SAMPLING_INTERVAL
          }
          .withDefaults(reportingServer.metricSpecConfig)
      filters += "person.gender == ${Person.Gender.MALE_VALUE}"
    }

    val createdMetric =
      publicMetricsClient
        .withPrincipalName(measurementConsumerData.name)
        .createMetric(
          createMetricRequest {
            parent = measurementConsumerData.name
            this.metric = metric
            metricId = "abc"
          }
        )

    val retrievedMetric = pollForCompletedMetric(measurementConsumerData.name, createdMetric.name)
    assertThat(retrievedMetric.state).isEqualTo(Metric.State.SUCCEEDED)

    val filters = metric.filtersList.toMutableList()
    filters.add(primitiveReportingSet.filter)
    val vids =
      SYNTHETIC_EVENT_QUERY.getUserVirtualIds(
        eventGroup,
        "(${metric.filtersList[0]}) && (${primitiveReportingSet.filter})",
        EVENT_RANGE.toInterval()
      )
    val sampledVids = vids.calculateSampledVids(metric.metricSpec.vidSamplingInterval)
    val expectedResult = calculateExpectedReachMeasurementResult(sampledVids)

    val actualResult =
      MeasurementKt.result {
        reach = MeasurementKt.ResultKt.reach { value = retrievedMetric.result.reach.value }
      }
    // TODO(@tristanvuong2021): Assert using variance
    assertThat(actualResult).reachValue().isWithinPercent(0.5).of(expectedResult.reach.value)
  }

  @Test
  fun `creating 25 metrics at once succeeds`() = runBlocking {
    val numMetrics = 25
    val measurementConsumerData = inProcessCmmsComponents.getMeasurementConsumerData()
    val eventGroups = listEventGroups()

    val primitiveReportingSet = reportingSet {
      displayName = "primitive"
      filter = "person.age_group == ${Person.AgeGroup.YEARS_18_TO_34_VALUE}"
      primitive = ReportingSetKt.primitive { cmmsEventGroups += eventGroups[0].cmmsEventGroup }
    }

    val createdPrimitiveReportingSet =
      publicReportingSetsClient
        .withPrincipalName(measurementConsumerData.name)
        .createReportingSet(
          createReportingSetRequest {
            parent = measurementConsumerData.name
            reportingSet = primitiveReportingSet
            reportingSetId = "abc"
          }
        )

    val metric = metric {
      reportingSet = createdPrimitiveReportingSet.name
      timeInterval = interval {
        startTime = timestamp { seconds = 100 }
        endTime = timestamp { seconds = 200 }
      }
      metricSpec =
        metricSpec {
            reach =
              MetricSpecKt.reachParams { privacyParams = MetricSpecKt.differentialPrivacyParams {} }
          }
          .withDefaults(reportingServer.metricSpecConfig)
    }

    val deferred: MutableList<Deferred<Metric>> = mutableListOf()
    repeat(numMetrics) {
      deferred.add(
        async {
          publicMetricsClient
            .withPrincipalName(measurementConsumerData.name)
            .createMetric(
              createMetricRequest {
                parent = measurementConsumerData.name
                this.metric = metric
                metricId = "abc$it"
              }
            )
        }
      )
    }

    deferred.awaitAll()
    val retrievedMetrics =
      publicMetricsClient
        .withPrincipalName(measurementConsumerData.name)
        .listMetrics(
          listMetricsRequest {
            parent = measurementConsumerData.name
            pageSize = numMetrics
          }
        )
        .metricsList

    assertThat(retrievedMetrics).hasSize(numMetrics)
    retrievedMetrics.forEach {
      assertThat(it)
        .ignoringFields(
          Metric.NAME_FIELD_NUMBER,
          Metric.STATE_FIELD_NUMBER,
          Metric.CREATE_TIME_FIELD_NUMBER,
          Metric.RESULT_FIELD_NUMBER
        )
        .isEqualTo(metric)
    }
  }

  @Test
  fun `creating 25 reporting sets at once succeeds`() = runBlocking {
    val numReportingSets = 25
    val measurementConsumerData = inProcessCmmsComponents.getMeasurementConsumerData()
    val eventGroups = listEventGroups()

    val primitiveReportingSet = reportingSet {
      displayName = "primitive"
      filter = "person.age_group == ${Person.AgeGroup.YEARS_18_TO_34_VALUE}"
      primitive = ReportingSetKt.primitive { cmmsEventGroups += eventGroups[0].cmmsEventGroup }
    }

    val deferred: MutableList<Deferred<ReportingSet>> = mutableListOf()
    repeat(numReportingSets) {
      deferred.add(
        async {
          publicReportingSetsClient
            .withPrincipalName(measurementConsumerData.name)
            .createReportingSet(
              createReportingSetRequest {
                parent = measurementConsumerData.name
                reportingSet = primitiveReportingSet
                reportingSetId = "abc$it"
              }
            )
        }
      )
    }

    deferred.awaitAll()
    val retrievedPrimitiveReportingSets =
      publicReportingSetsClient
        .withPrincipalName(measurementConsumerData.name)
        .listReportingSets(
          listReportingSetsRequest {
            parent = measurementConsumerData.name
            pageSize = numReportingSets
          }
        )
        .reportingSetsList

    assertThat(retrievedPrimitiveReportingSets).hasSize(numReportingSets)
    retrievedPrimitiveReportingSets.forEach {
      assertThat(it).ignoringFields(ReportingSet.NAME_FIELD_NUMBER).isEqualTo(primitiveReportingSet)
    }
  }

  @Test
  fun `retrieving data provider succeeds`() = runBlocking {
    val eventGroups = listEventGroups()
    val dataProviderName = eventGroups[0].cmmsDataProvider

    val measurementConsumerData = inProcessCmmsComponents.getMeasurementConsumerData()
    val dataProvider =
      publicDataProvidersClient
        .withPrincipalName(measurementConsumerData.name)
        .getDataProvider(getDataProviderRequest { name = dataProviderName })

    assertThat(DataProviderCertificateKey.fromName(dataProvider.certificate)).isNotNull()
  }

  @Test
  fun `retrieving metadata descriptors for event groups succeeds`() = runBlocking {
    val eventGroups = listEventGroups()

    val descriptorNames = eventGroups.map { it.metadata.eventGroupMetadataDescriptor }

    val measurementConsumerData = inProcessCmmsComponents.getMeasurementConsumerData()
    val descriptors =
      publicEventGroupMetadataDescriptorsClient
        .withPrincipalName(measurementConsumerData.name)
        .batchGetEventGroupMetadataDescriptors(
          batchGetEventGroupMetadataDescriptorsRequest { names += descriptorNames }
        )
        .eventGroupMetadataDescriptorsList

    assertThat(descriptors).hasSize(descriptorNames.size)

    val retrievedDescriptorNames = mutableSetOf<String>()
    for (descriptor in descriptors) {
      retrievedDescriptorNames.add(descriptor.name)
    }

    for (eventGroup in eventGroups) {
      assertThat(
          retrievedDescriptorNames.contains(eventGroup.metadata.eventGroupMetadataDescriptor)
        )
        .isTrue()
    }
  }

  private suspend fun listEventGroups(): List<EventGroup> {
    val measurementConsumerData = inProcessCmmsComponents.getMeasurementConsumerData()

    return publicEventGroupsClient
      .withPrincipalName(measurementConsumerData.name)
      .listEventGroups(
        listEventGroupsRequest {
          parent = measurementConsumerData.name
          pageSize = 1000
        }
      )
      .eventGroupsList
  }

  private suspend fun pollForCompletedReport(
    measurementConsumerName: String,
    reportName: String
  ): Report {
    while (true) {
      val retrievedReport =
        publicReportsClient
          .withPrincipalName(measurementConsumerName)
          .getReport(getReportRequest { name = reportName })

      @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA") // Proto enum fields are never null.
      when (retrievedReport.state) {
        Report.State.SUCCEEDED,
        Report.State.FAILED -> return retrievedReport
        Report.State.RUNNING,
        Report.State.UNRECOGNIZED,
        Report.State.STATE_UNSPECIFIED -> delay(5000)
      }
    }
  }

  private suspend fun pollForCompletedMetric(
    measurementConsumerName: String,
    metricName: String
  ): Metric {
    while (true) {
      val retrievedMetric =
        publicMetricsClient
          .withPrincipalName(measurementConsumerName)
          .getMetric(getMetricRequest { name = metricName })

      @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA") // Proto enum fields are never null.
      when (retrievedMetric.state) {
        Metric.State.SUCCEEDED,
        Metric.State.FAILED -> return retrievedMetric
        Metric.State.RUNNING,
        Metric.State.UNRECOGNIZED,
        Metric.State.STATE_UNSPECIFIED -> delay(5000)
      }
    }
  }

  private fun calculateExpectedReachMeasurementResult(
    sampledVids: Sequence<Long>
  ): Measurement.Result {
    val reach = MeasurementResults.computeReach(sampledVids.asIterable())
    return MeasurementKt.result {
      this.reach = MeasurementKt.ResultKt.reach { value = reach.toLong() }
    }
  }

  private fun calculateExpectedReachAndFrequencyMeasurementResult(
    sampledVids: Sequence<Long>,
    maxFrequency: Int
  ): Measurement.Result {
    val reachAndFrequency =
      MeasurementResults.computeReachAndFrequency(sampledVids.asIterable(), maxFrequency)
    return MeasurementKt.result {
      reach = MeasurementKt.ResultKt.reach { value = reachAndFrequency.reach.toLong() }
      frequency =
        MeasurementKt.ResultKt.frequency {
          relativeFrequencyDistribution.putAll(
            reachAndFrequency.relativeFrequencyDistribution.mapKeys { it.key.toLong() }
          )
        }
    }
  }

  private fun SyntheticGeneratorEventQuery.getUserVirtualIds(
    eventGroup: EventGroup,
    filter: String,
    collectionInterval: Interval
  ): Sequence<Long> {
    val cmmsMetadata =
      CmmsEventGroupKt.metadata {
        eventGroupMetadataDescriptor = eventGroup.metadata.eventGroupMetadataDescriptor
        metadata = eventGroup.metadata.metadata
      }
    val encryptedCmmsMetadata =
      encryptMetadata(cmmsMetadata, InProcessCmmsComponents.MC_ENTITY_CONTENT.encryptionPublicKey)
    val cmmsEventGroup = cmmsEventGroup { encryptedMetadata = encryptedCmmsMetadata }

    val eventFilter = RequisitionSpecKt.eventFilter { expression = filter }

    return this.getUserVirtualIds(
      EventQuery.EventGroupSpec(
        cmmsEventGroup,
        RequisitionSpecKt.EventGroupEntryKt.value {
          this.collectionInterval = collectionInterval
          this.filter = eventFilter
        }
      )
    )
  }

  private fun Sequence<Long>.calculateSampledVids(
    vidSamplingInterval: VidSamplingInterval
  ): Sequence<Long> {
    return this.filter { vid ->
      VidSampling.sampler.vidIsInSamplingBucket(
        vid,
        vidSamplingInterval.start,
        vidSamplingInterval.width
      )
    }
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

    private const val MC_SIGNING_PRIVATE_KEY_PATH = "mc_cs_private.der"

    private val SYNTHETIC_EVENT_QUERY =
      MetadataSyntheticGeneratorEventQuery(
        SyntheticGenerationSpecs.POPULATION_SPEC,
        InProcessCmmsComponents.MC_ENCRYPTION_PRIVATE_KEY
      )

    private val EVENT_RANGE =
      OpenEndTimeRange.fromClosedDateRange(LocalDate.of(2021, 3, 15)..LocalDate.of(2021, 3, 17))

    private val DP_PARAMS =
      MetricSpecKt.differentialPrivacyParams {
        epsilon = 1.0
        delta = 1.0
      }

    private val VID_SAMPLING_INTERVAL =
      MetricSpecKt.vidSamplingInterval {
        start = 0.0f
        width = 1.0f
      }

    @BeforeClass
    @JvmStatic
    fun initConfig() {
      InProcessCmmsComponents.initConfig()
    }
  }
}
