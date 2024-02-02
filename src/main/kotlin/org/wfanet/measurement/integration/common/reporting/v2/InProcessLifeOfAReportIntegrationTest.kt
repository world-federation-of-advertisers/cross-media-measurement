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
import com.google.type.Interval
import com.google.type.date
import com.google.type.dateTime
import com.google.type.interval
import com.google.type.timeZone
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
import org.wfanet.measurement.loadtest.common.sampleVids
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
import org.wfanet.measurement.reporting.v2alpha.MetricCalculationSpec
import org.wfanet.measurement.reporting.v2alpha.MetricCalculationSpecKt
import org.wfanet.measurement.reporting.v2alpha.MetricCalculationSpecsGrpcKt.MetricCalculationSpecsCoroutineStub
import org.wfanet.measurement.reporting.v2alpha.MetricSpec.VidSamplingInterval
import org.wfanet.measurement.reporting.v2alpha.MetricSpecKt
import org.wfanet.measurement.reporting.v2alpha.MetricsGrpcKt.MetricsCoroutineStub
import org.wfanet.measurement.reporting.v2alpha.Report
import org.wfanet.measurement.reporting.v2alpha.ReportKt
import org.wfanet.measurement.reporting.v2alpha.ReportingSet
import org.wfanet.measurement.reporting.v2alpha.ReportingSetKt
import org.wfanet.measurement.reporting.v2alpha.ReportingSetsGrpcKt.ReportingSetsCoroutineStub
import org.wfanet.measurement.reporting.v2alpha.ReportsGrpcKt.ReportsCoroutineStub
import org.wfanet.measurement.reporting.v2alpha.createMetricCalculationSpecRequest
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
import org.wfanet.measurement.reporting.v2alpha.metricCalculationSpec
import org.wfanet.measurement.reporting.v2alpha.metricSpec
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
        String, ComputationLogEntriesGrpcKt.ComputationLogEntriesCoroutineStub,
      ) -> InProcessDuchy.DuchyDependencies
    >,
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

  private val publicMetricCalculationSpecsClient by lazy {
    MetricCalculationSpecsCoroutineStub(reportingServer.publicApiChannel)
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

    val eventGroupEntries: List<Pair<EventGroup, String>> =
      listOf(
        eventGroups[0] to "person.age_group == ${Person.AgeGroup.YEARS_18_TO_34_VALUE}",
        eventGroups[1] to "person.age_group == ${Person.AgeGroup.YEARS_55_PLUS_VALUE}",
      )

    val createdPrimitiveReportingSets: List<ReportingSet> =
      createPrimitiveReportingSets(eventGroupEntries, measurementConsumerData.name)

    val compositeReportingSet = reportingSet {
      displayName = "composite"
      composite =
        ReportingSetKt.composite {
          expression =
            ReportingSetKt.setExpression {
              operation = ReportingSet.SetExpression.Operation.UNION
              lhs =
                ReportingSetKt.SetExpressionKt.operand {
                  reportingSet = createdPrimitiveReportingSets[0].name
                }
              rhs =
                ReportingSetKt.SetExpressionKt.operand {
                  reportingSet = createdPrimitiveReportingSets[1].name
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

    val createdMetricCalculationSpec =
      publicMetricCalculationSpecsClient
        .withPrincipalName(measurementConsumerData.name)
        .createMetricCalculationSpec(
          createMetricCalculationSpecRequest {
            parent = measurementConsumerData.name
            metricCalculationSpec = metricCalculationSpec {
              displayName = "union reach"
              metricSpecs +=
                metricSpec {
                    reach = MetricSpecKt.reachParams { privacyParams = DP_PARAMS }
                    vidSamplingInterval = VID_SAMPLING_INTERVAL
                  }
                  .withDefaults(reportingServer.metricSpecConfig)
            }
            metricCalculationSpecId = "fed"
          }
        )

    val report = report {
      reportingMetricEntries +=
        ReportKt.reportingMetricEntry {
          key = createdCompositeReportingSet.name
          value =
            ReportKt.reportingMetricCalculationSpec {
              metricCalculationSpecs += createdMetricCalculationSpec.name
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

    val eventGroupSpecs: Iterable<EventQuery.EventGroupSpec> =
      eventGroupEntries.map { (eventGroup, filter) ->
        buildEventGroupSpec(eventGroup, filter, EVENT_RANGE.toInterval())
      }
    val sampledVids =
      sampleVids(
        eventGroupSpecs,
        createdMetricCalculationSpec.metricSpecsList.single().vidSamplingInterval,
      )
    val expectedResult = calculateExpectedReachMeasurementResult(sampledVids)

    val reachResult =
      retrievedReport.metricCalculationResultsList
        .single()
        .resultAttributesList
        .single()
        .metricResult
        .reach
    val actualResult =
      MeasurementKt.result { reach = MeasurementKt.ResultKt.reach { value = reachResult.value } }
    val tolerance = computeErrorMargin(reachResult.univariateStatistics.standardDeviation)

    assertThat(actualResult).reachValue().isWithin(tolerance).of(expectedResult.reach.value)
  }

  @Test
  fun `report with unique reach has the expected result`() = runBlocking {
    val measurementConsumerData = inProcessCmmsComponents.getMeasurementConsumerData()
    val eventGroups = listEventGroups()
    val eventGroup = eventGroups.first()
    val eventGroupEntries: List<Pair<EventGroup, String>> =
      listOf(
        eventGroup to "person.age_group == ${Person.AgeGroup.YEARS_18_TO_34_VALUE}",
        eventGroup to "person.age_group == ${Person.Gender.MALE_VALUE}",
      )
    val createdPrimitiveReportingSets: List<ReportingSet> =
      createPrimitiveReportingSets(eventGroupEntries, measurementConsumerData.name)

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
                          reportingSet = createdPrimitiveReportingSets[0].name
                        }
                      rhs =
                        ReportingSetKt.SetExpressionKt.operand {
                          reportingSet = createdPrimitiveReportingSets[1].name
                        }
                    }
                }
              rhs =
                ReportingSetKt.SetExpressionKt.operand {
                  reportingSet = createdPrimitiveReportingSets[1].name
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

    val createdMetricCalculationSpec =
      publicMetricCalculationSpecsClient
        .withPrincipalName(measurementConsumerData.name)
        .createMetricCalculationSpec(
          createMetricCalculationSpecRequest {
            parent = measurementConsumerData.name
            metricCalculationSpec = metricCalculationSpec {
              displayName = "unique reach"
              metricSpecs +=
                metricSpec {
                    reach = MetricSpecKt.reachParams { privacyParams = DP_PARAMS }
                    vidSamplingInterval = VID_SAMPLING_INTERVAL
                  }
                  .withDefaults(reportingServer.metricSpecConfig)
            }
            metricCalculationSpecId = "fed"
          }
        )

    val report = report {
      reportingMetricEntries +=
        ReportKt.reportingMetricEntry {
          key = createdCompositeReportingSet.name
          value =
            ReportKt.reportingMetricCalculationSpec {
              metricCalculationSpecs += createdMetricCalculationSpec.name
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

    val equivalentFilter =
      "person.age_group == ${Person.AgeGroup.YEARS_18_TO_34_VALUE} && " +
        "person.age_group == ${Person.Gender.FEMALE_VALUE}"
    val eventGroupSpecs: Iterable<EventQuery.EventGroupSpec> =
      listOf(buildEventGroupSpec(eventGroup, equivalentFilter, EVENT_RANGE.toInterval()))
    val sampledVids =
      sampleVids(
        eventGroupSpecs,
        createdMetricCalculationSpec.metricSpecsList.single().vidSamplingInterval,
      )
    val expectedResult = calculateExpectedReachMeasurementResult(sampledVids)

    val reachResult =
      retrievedReport.metricCalculationResultsList
        .single()
        .resultAttributesList
        .single()
        .metricResult
        .reach
    val actualResult =
      MeasurementKt.result { reach = MeasurementKt.ResultKt.reach { value = reachResult.value } }
    val tolerance = computeErrorMargin(reachResult.univariateStatistics.standardDeviation)
    assertThat(actualResult).reachValue().isWithin(tolerance).of(expectedResult.reach.value)
  }

  @Test
  fun `report with intersection reach has the expected result`() = runBlocking {
    val measurementConsumerData = inProcessCmmsComponents.getMeasurementConsumerData()
    val eventGroups = listEventGroups()
    val eventGroup = eventGroups.first()
    val eventGroupEntries: List<Pair<EventGroup, String>> =
      listOf(
        eventGroup to "person.age_group == ${Person.AgeGroup.YEARS_18_TO_34_VALUE}",
        eventGroup to "person.age_group == ${Person.Gender.FEMALE_VALUE}",
      )
    val createdPrimitiveReportingSets: List<ReportingSet> =
      createPrimitiveReportingSets(eventGroupEntries, measurementConsumerData.name)

    val compositeReportingSet = reportingSet {
      displayName = "composite"
      composite =
        ReportingSetKt.composite {
          expression =
            ReportingSetKt.setExpression {
              operation = ReportingSet.SetExpression.Operation.INTERSECTION
              lhs =
                ReportingSetKt.SetExpressionKt.operand {
                  reportingSet = createdPrimitiveReportingSets[0].name
                }
              rhs =
                ReportingSetKt.SetExpressionKt.operand {
                  reportingSet = createdPrimitiveReportingSets[1].name
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

    val createdMetricCalculationSpec =
      publicMetricCalculationSpecsClient
        .withPrincipalName(measurementConsumerData.name)
        .createMetricCalculationSpec(
          createMetricCalculationSpecRequest {
            parent = measurementConsumerData.name
            metricCalculationSpec = metricCalculationSpec {
              displayName = "intersection reach"
              metricSpecs +=
                metricSpec {
                    reach = MetricSpecKt.reachParams { privacyParams = DP_PARAMS }
                    vidSamplingInterval = VID_SAMPLING_INTERVAL
                  }
                  .withDefaults(reportingServer.metricSpecConfig)
            }
            metricCalculationSpecId = "fed"
          }
        )

    val report = report {
      reportingMetricEntries +=
        ReportKt.reportingMetricEntry {
          key = createdCompositeReportingSet.name
          value =
            ReportKt.reportingMetricCalculationSpec {
              metricCalculationSpecs += createdMetricCalculationSpec.name
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

    val equivalentFilter =
      "(${createdPrimitiveReportingSets[0].filter}) && (${createdPrimitiveReportingSets[1].filter})"
    val eventGroupSpecs: Iterable<EventQuery.EventGroupSpec> =
      listOf(buildEventGroupSpec(eventGroup, equivalentFilter, EVENT_RANGE.toInterval()))
    val sampledVids =
      sampleVids(
        eventGroupSpecs,
        createdMetricCalculationSpec.metricSpecsList.single().vidSamplingInterval,
      )
    val expectedResult = calculateExpectedReachMeasurementResult(sampledVids)

    val reachResult =
      retrievedReport.metricCalculationResultsList
        .single()
        .resultAttributesList
        .single()
        .metricResult
        .reach
    val actualResult =
      MeasurementKt.result { reach = MeasurementKt.ResultKt.reach { value = reachResult.value } }
    val tolerance = computeErrorMargin(reachResult.univariateStatistics.standardDeviation)
    assertThat(actualResult).reachValue().isWithin(tolerance).of(expectedResult.reach.value)
  }

  @Test
  fun `report with 2 reporting metric entries has the expected result`() = runBlocking {
    val measurementConsumerData = inProcessCmmsComponents.getMeasurementConsumerData()
    val eventGroups = listEventGroups()
    val eventGroup = eventGroups.first()
    val eventGroupEntries: List<Pair<EventGroup, String>> =
      listOf(eventGroup to "person.age_group == ${Person.AgeGroup.YEARS_18_TO_34_VALUE}")
    val createdPrimitiveReportingSet: ReportingSet =
      createPrimitiveReportingSets(eventGroupEntries, measurementConsumerData.name).single()

    val createdMetricCalculationSpec =
      publicMetricCalculationSpecsClient
        .withPrincipalName(measurementConsumerData.name)
        .createMetricCalculationSpec(
          createMetricCalculationSpecRequest {
            parent = measurementConsumerData.name
            metricCalculationSpec = metricCalculationSpec {
              displayName = "union reach"
              metricSpecs +=
                metricSpec {
                    reach = MetricSpecKt.reachParams { privacyParams = DP_PARAMS }
                    vidSamplingInterval = VID_SAMPLING_INTERVAL
                  }
                  .withDefaults(reportingServer.metricSpecConfig)
            }
            metricCalculationSpecId = "fed"
          }
        )

    val report = report {
      reportingMetricEntries +=
        ReportKt.reportingMetricEntry {
          key = createdPrimitiveReportingSet.name
          value =
            ReportKt.reportingMetricCalculationSpec {
              metricCalculationSpecs += createdMetricCalculationSpec.name
            }
        }
      reportingMetricEntries +=
        ReportKt.reportingMetricEntry {
          key = createdPrimitiveReportingSet.name
          value =
            ReportKt.reportingMetricCalculationSpec {
              metricCalculationSpecs += createdMetricCalculationSpec.name
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

    val eventGroupSpecs: Iterable<EventQuery.EventGroupSpec> =
      eventGroupEntries.map { (eventGroup, filter) ->
        buildEventGroupSpec(eventGroup, filter, EVENT_RANGE.toInterval())
      }
    val sampledVids =
      sampleVids(
        eventGroupSpecs,
        createdMetricCalculationSpec.metricSpecsList.single().vidSamplingInterval,
      )
    val expectedResult = calculateExpectedReachMeasurementResult(sampledVids)

    for (resultAttribute in
      retrievedReport.metricCalculationResultsList.single().resultAttributesList) {
      val reachResult = resultAttribute.metricResult.reach
      val actualResult =
        MeasurementKt.result { reach = MeasurementKt.ResultKt.reach { value = reachResult.value } }
      val tolerance = computeErrorMargin(reachResult.univariateStatistics.standardDeviation)
      assertThat(actualResult).reachValue().isWithin(tolerance).of(expectedResult.reach.value)
    }
  }

  @Test
  fun `report across two time intervals has the expected result`() = runBlocking {
    val measurementConsumerData = inProcessCmmsComponents.getMeasurementConsumerData()
    val eventGroups = listEventGroups()
    val eventGroup = eventGroups.first()
    val eventGroupEntries: List<Pair<EventGroup, String>> =
      listOf(eventGroup to "person.age_group == ${Person.AgeGroup.YEARS_18_TO_34_VALUE}")
    val createdPrimitiveReportingSet: ReportingSet =
      createPrimitiveReportingSets(eventGroupEntries, measurementConsumerData.name).single()

    val eventRangeWithNoReach =
      OpenEndTimeRange.fromClosedDateRange(LocalDate.of(2021, 3, 18)..LocalDate.of(2021, 3, 19))

    val createdMetricCalculationSpec =
      publicMetricCalculationSpecsClient
        .withPrincipalName(measurementConsumerData.name)
        .createMetricCalculationSpec(
          createMetricCalculationSpecRequest {
            parent = measurementConsumerData.name
            metricCalculationSpec = metricCalculationSpec {
              displayName = "union reach"
              metricSpecs +=
                metricSpec {
                    reach = MetricSpecKt.reachParams { privacyParams = DP_PARAMS }
                    vidSamplingInterval = VID_SAMPLING_INTERVAL
                  }
                  .withDefaults(reportingServer.metricSpecConfig)
            }
            metricCalculationSpecId = "fed"
          }
        )

    val report = report {
      reportingMetricEntries +=
        ReportKt.reportingMetricEntry {
          key = createdPrimitiveReportingSet.name
          value =
            ReportKt.reportingMetricCalculationSpec {
              metricCalculationSpecs += createdMetricCalculationSpec.name
            }
        }
      timeIntervals = timeIntervals {
        timeIntervals += EVENT_RANGE.toInterval()
        timeIntervals += eventRangeWithNoReach.toInterval()
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
      val reachResult = resultAttribute.metricResult.reach
      val actualResult =
        MeasurementKt.result { reach = MeasurementKt.ResultKt.reach { value = reachResult.value } }
      val tolerance = computeErrorMargin(reachResult.univariateStatistics.standardDeviation)

      val eventGroupSpecs: Iterable<EventQuery.EventGroupSpec> =
        eventGroupEntries.map { (eventGroup, filter) ->
          buildEventGroupSpec(eventGroup, filter, resultAttribute.timeInterval)
        }
      val sampledVids =
        sampleVids(
          eventGroupSpecs,
          createdMetricCalculationSpec.metricSpecsList.single().vidSamplingInterval,
        )
      val expectedResult = calculateExpectedReachMeasurementResult(sampledVids)

      assertThat(actualResult).reachValue().isWithin(tolerance).of(expectedResult.reach.value)
    }
  }

  @Test
  fun `report with reporting interval has the expected result`() = runBlocking {
    val measurementConsumerData = inProcessCmmsComponents.getMeasurementConsumerData()
    val eventGroups = listEventGroups()
    val eventGroup = eventGroups.first()
    val eventGroupEntries: List<Pair<EventGroup, String>> =
      listOf(eventGroup to "person.age_group == ${Person.AgeGroup.YEARS_18_TO_34_VALUE}")
    val createdPrimitiveReportingSet: ReportingSet =
      createPrimitiveReportingSets(eventGroupEntries, measurementConsumerData.name).single()

    val createdMetricCalculationSpec =
      publicMetricCalculationSpecsClient
        .withPrincipalName(measurementConsumerData.name)
        .createMetricCalculationSpec(
          createMetricCalculationSpecRequest {
            parent = measurementConsumerData.name
            metricCalculationSpec = metricCalculationSpec {
              displayName = "union reach"
              metricSpecs +=
                metricSpec {
                    reach = MetricSpecKt.reachParams { privacyParams = DP_PARAMS }
                    vidSamplingInterval = VID_SAMPLING_INTERVAL
                  }
                  .withDefaults(reportingServer.metricSpecConfig)
              metricFrequencySpec =
                MetricCalculationSpecKt.metricFrequencySpec {
                  daily = MetricCalculationSpec.MetricFrequencySpec.Daily.getDefaultInstance()
                }
              trailingWindow =
                MetricCalculationSpecKt.trailingWindow {
                  count = 1
                  increment = MetricCalculationSpec.TrailingWindow.Increment.DAY
                }
            }
            metricCalculationSpecId = "fed"
          }
        )

    val report = report {
      reportingMetricEntries +=
        ReportKt.reportingMetricEntry {
          key = createdPrimitiveReportingSet.name
          value =
            ReportKt.reportingMetricCalculationSpec {
              metricCalculationSpecs += createdMetricCalculationSpec.name
            }
        }
      reportingInterval =
        ReportKt.reportingInterval {
          reportStart = dateTime {
            year = 2021
            month = 3
            day = 15
            timeZone = timeZone { id = "America/Los_Angeles" }
          }
          reportEnd = date {
            year = 2021
            month = 3
            day = 17
          }
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
      val reachResult = resultAttribute.metricResult.reach
      val actualResult =
        MeasurementKt.result { reach = MeasurementKt.ResultKt.reach { value = reachResult.value } }
      val tolerance = computeErrorMargin(reachResult.univariateStatistics.standardDeviation)

      val eventGroupSpecs: Iterable<EventQuery.EventGroupSpec> =
        eventGroupEntries.map { (eventGroup, filter) ->
          buildEventGroupSpec(eventGroup, filter, resultAttribute.timeInterval)
        }
      val sampledVids =
        sampleVids(
          eventGroupSpecs,
          createdMetricCalculationSpec.metricSpecsList.single().vidSamplingInterval,
        )
      val expectedResult = calculateExpectedReachMeasurementResult(sampledVids)

      assertThat(actualResult).reachValue().isWithin(tolerance).of(expectedResult.reach.value)
    }
  }

  @Test
  fun `report with group by has the expected result`() = runBlocking {
    val measurementConsumerData = inProcessCmmsComponents.getMeasurementConsumerData()
    val eventGroups = listEventGroups()
    val eventGroup = eventGroups.first()
    val eventGroupEntries: List<Pair<EventGroup, String>> = listOf(eventGroup to "")
    val createdPrimitiveReportingSet: ReportingSet =
      createPrimitiveReportingSets(eventGroupEntries, measurementConsumerData.name).single()

    val grouping1Predicate1 = "person.age_group == ${Person.AgeGroup.YEARS_35_TO_54_VALUE}"
    val grouping1Predicate2 = "person.age_group == ${Person.AgeGroup.YEARS_18_TO_34_VALUE}"
    val grouping2Predicate1 = "person.gender == ${Person.Gender.FEMALE_VALUE}"
    val grouping2Predicate2 = "person.gender == ${Person.Gender.MALE_VALUE}"

    val createdMetricCalculationSpec =
      publicMetricCalculationSpecsClient
        .withPrincipalName(measurementConsumerData.name)
        .createMetricCalculationSpec(
          createMetricCalculationSpecRequest {
            parent = measurementConsumerData.name
            metricCalculationSpec = metricCalculationSpec {
              displayName = "union reach"
              metricSpecs +=
                metricSpec {
                    reach = MetricSpecKt.reachParams { privacyParams = DP_PARAMS }
                    vidSamplingInterval = VID_SAMPLING_INTERVAL
                  }
                  .withDefaults(reportingServer.metricSpecConfig)
              groupings +=
                MetricCalculationSpecKt.grouping {
                  predicates += grouping1Predicate1
                  predicates += grouping1Predicate2
                }
              groupings +=
                MetricCalculationSpecKt.grouping {
                  predicates += grouping2Predicate1
                  predicates += grouping2Predicate2
                }
            }
            metricCalculationSpecId = "fed"
          }
        )

    val report = report {
      reportingMetricEntries +=
        ReportKt.reportingMetricEntry {
          key = createdPrimitiveReportingSet.name
          value =
            ReportKt.reportingMetricCalculationSpec {
              metricCalculationSpecs += createdMetricCalculationSpec.name
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

    for (resultAttribute in retrievedReport.metricCalculationResultsList[0].resultAttributesList) {
      val reachResult = resultAttribute.metricResult.reach
      val actualResult =
        MeasurementKt.result { reach = MeasurementKt.ResultKt.reach { value = reachResult.value } }
      val tolerance = computeErrorMargin(reachResult.univariateStatistics.standardDeviation)

      val eventGroupSpecs: Iterable<EventQuery.EventGroupSpec> =
        eventGroupEntries.map { (eventGroup, filter) ->
          val allFilters =
            (resultAttribute.groupingPredicatesList + filter)
              .filter { it.isNotBlank() }
              .joinToString(" && ")
          buildEventGroupSpec(eventGroup, allFilters, EVENT_RANGE.toInterval())
        }
      val sampledVids =
        sampleVids(
          eventGroupSpecs,
          createdMetricCalculationSpec.metricSpecsList.single().vidSamplingInterval,
        )
      val expectedResult = calculateExpectedReachMeasurementResult(sampledVids)

      assertThat(actualResult).reachValue().isWithin(tolerance).of(expectedResult.reach.value)
    }
  }

  @Test
  fun `creating 25 reports at once succeeds`() = runBlocking {
    val numReports = 25
    val measurementConsumerData = inProcessCmmsComponents.getMeasurementConsumerData()
    val eventGroups = listEventGroups()
    val eventGroupEntries: List<Pair<EventGroup, String>> =
      listOf(eventGroups.first() to "person.age_group == ${Person.AgeGroup.YEARS_18_TO_34_VALUE}")
    val createdPrimitiveReportingSet: ReportingSet =
      createPrimitiveReportingSets(eventGroupEntries, measurementConsumerData.name).single()

    val createdMetricCalculationSpec =
      publicMetricCalculationSpecsClient
        .withPrincipalName(measurementConsumerData.name)
        .createMetricCalculationSpec(
          createMetricCalculationSpecRequest {
            parent = measurementConsumerData.name
            metricCalculationSpec = metricCalculationSpec {
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
            metricCalculationSpecId = "fed"
          }
        )

    val report = report {
      reportingMetricEntries +=
        ReportKt.reportingMetricEntry {
          key = createdPrimitiveReportingSet.name
          value =
            ReportKt.reportingMetricCalculationSpec {
              metricCalculationSpecs += createdMetricCalculationSpec.name
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
          Report.METRIC_CALCULATION_RESULTS_FIELD_NUMBER,
        )
        .isEqualTo(report)
    }
  }

  @Test
  fun `reach metric result has the expected result`() = runBlocking {
    val measurementConsumerData = inProcessCmmsComponents.getMeasurementConsumerData()
    val eventGroups = listEventGroups()
    val eventGroup = eventGroups.first()
    val eventGroupEntries: List<Pair<EventGroup, String>> =
      listOf(eventGroup to "person.age_group == ${Person.AgeGroup.YEARS_18_TO_34_VALUE}")
    val createdPrimitiveReportingSet: ReportingSet =
      createPrimitiveReportingSets(eventGroupEntries, measurementConsumerData.name).single()

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

    val eventGroupSpecs: Iterable<EventQuery.EventGroupSpec> =
      eventGroupEntries.map { (eventGroup, filter) ->
        buildEventGroupSpec(eventGroup, filter, EVENT_RANGE.toInterval())
      }
    val sampledVids = sampleVids(eventGroupSpecs, metric.metricSpec.vidSamplingInterval)
    val expectedResult = calculateExpectedReachMeasurementResult(sampledVids)

    val reachResult = retrievedMetric.result.reach
    val actualResult =
      MeasurementKt.result { reach = MeasurementKt.ResultKt.reach { value = reachResult.value } }
    val tolerance = computeErrorMargin(reachResult.univariateStatistics.standardDeviation)
    assertThat(actualResult).reachValue().isWithin(tolerance).of(expectedResult.reach.value)
  }

  @Test
  fun `reach-and-frequency metric has the expected result`() = runBlocking {
    val measurementConsumerData = inProcessCmmsComponents.getMeasurementConsumerData()
    val eventGroups = listEventGroups()
    val eventGroup = eventGroups.first()
    val eventGroupEntries: List<Pair<EventGroup, String>> =
      listOf(eventGroup to "person.age_group == ${Person.AgeGroup.YEARS_18_TO_34_VALUE}")
    val createdPrimitiveReportingSet: ReportingSet =
      createPrimitiveReportingSets(eventGroupEntries, measurementConsumerData.name).single()

    val metric = metric {
      reportingSet = createdPrimitiveReportingSet.name
      timeInterval = EVENT_RANGE.toInterval()
      metricSpec =
        metricSpec {
            reachAndFrequency =
              MetricSpecKt.reachAndFrequencyParams {
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

    val eventGroupSpecs: Iterable<EventQuery.EventGroupSpec> =
      eventGroupEntries.map { (eventGroup, filter) ->
        buildEventGroupSpec(eventGroup, filter, EVENT_RANGE.toInterval())
      }
    val sampledVids = sampleVids(eventGroupSpecs, metric.metricSpec.vidSamplingInterval)
    val expectedResult =
      calculateExpectedReachAndFrequencyMeasurementResult(
        sampledVids,
        metric.metricSpec.reachAndFrequency.maximumFrequency,
      )

    val reachAndFrequencyResult = retrievedMetric.result.reachAndFrequency
    val actualResult =
      MeasurementKt.result {
        reach = MeasurementKt.ResultKt.reach { value = reachAndFrequencyResult.reach.value }
        frequency =
          MeasurementKt.ResultKt.frequency {
            relativeFrequencyDistribution.putAll(
              reachAndFrequencyResult.frequencyHistogram.binsList.associate {
                Pair(it.label.toLong(), it.binResult.value / reachAndFrequencyResult.reach.value)
              }
            )
          }
      }
    val reachTolerance =
      computeErrorMargin(reachAndFrequencyResult.reach.univariateStatistics.standardDeviation)
    val frequencyToleranceMap: Map<Long, Double> =
      reachAndFrequencyResult.frequencyHistogram.binsList.associate { bin ->
        bin.label.toLong() to computeErrorMargin(bin.relativeUnivariateStatistics.standardDeviation)
      }

    assertThat(actualResult).reachValue().isWithin(reachTolerance).of(expectedResult.reach.value)
    assertThat(actualResult)
      .frequencyDistribution()
      .isWithin(frequencyToleranceMap)
      .of(expectedResult.frequency.relativeFrequencyDistributionMap)
  }

  @Test
  fun `impression count metric has the expected result`() = runBlocking {
    val measurementConsumerData = inProcessCmmsComponents.getMeasurementConsumerData()
    val eventGroups = listEventGroups()
    val eventGroup = eventGroups.first()
    val eventGroupEntries: List<Pair<EventGroup, String>> =
      listOf(eventGroup to "person.age_group == ${Person.AgeGroup.YEARS_18_TO_34_VALUE}")
    val createdPrimitiveReportingSet: ReportingSet =
      createPrimitiveReportingSets(eventGroupEntries, measurementConsumerData.name).single()

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

    val eventGroupSpecs: Iterable<EventQuery.EventGroupSpec> =
      eventGroupEntries.map { (eventGroup, filter) ->
        buildEventGroupSpec(eventGroup, filter, EVENT_RANGE.toInterval())
      }
    val sampledVids = sampleVids(eventGroupSpecs, metric.metricSpec.vidSamplingInterval)
    val expectedResult =
      calculateExpectedImpressionMeasurementResult(
        sampledVids,
        metric.metricSpec.impressionCount.maximumFrequencyPerUser,
      )

    val impressionResult = retrievedMetric.result.impressionCount
    val actualResult =
      MeasurementKt.result {
        impression = MeasurementKt.ResultKt.impression { value = impressionResult.value }
      }
    val tolerance = computeErrorMargin(impressionResult.univariateStatistics.standardDeviation)

    assertThat(actualResult)
      .impressionValue()
      .isWithin(tolerance)
      .of(expectedResult.impression.value)
  }

  @Test
  fun `watch duration metric has the expected result`() = runBlocking {
    val measurementConsumerData = inProcessCmmsComponents.getMeasurementConsumerData()
    val eventGroups = listEventGroups()
    val eventGroup = eventGroups.first()
    val eventGroupEntries: List<Pair<EventGroup, String>> =
      listOf(eventGroup to "person.age_group == ${Person.AgeGroup.YEARS_18_TO_34_VALUE}")
    val createdPrimitiveReportingSet: ReportingSet =
      createPrimitiveReportingSets(eventGroupEntries, measurementConsumerData.name).single()

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
    val eventGroup = eventGroups.first()
    val eventGroupEntries: List<Pair<EventGroup, String>> =
      listOf(eventGroup to "person.age_group == ${Person.AgeGroup.YEARS_18_TO_34_VALUE}")
    val createdPrimitiveReportingSet: ReportingSet =
      createPrimitiveReportingSets(eventGroupEntries, measurementConsumerData.name).single()

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

    val eventGroupSpecs: Iterable<EventQuery.EventGroupSpec> =
      eventGroupEntries.map { (eventGroup, filter) ->
        val allFilters =
          (metric.filtersList + filter).filter { it.isNotBlank() }.joinToString(" && ")
        buildEventGroupSpec(eventGroup, allFilters, EVENT_RANGE.toInterval())
      }
    val sampledVids = sampleVids(eventGroupSpecs, metric.metricSpec.vidSamplingInterval)
    val expectedResult = calculateExpectedReachMeasurementResult(sampledVids)

    val reachResult = retrievedMetric.result.reach
    val actualResult =
      MeasurementKt.result { reach = MeasurementKt.ResultKt.reach { value = reachResult.value } }
    val tolerance = computeErrorMargin(reachResult.univariateStatistics.standardDeviation)
    assertThat(actualResult).reachValue().isWithin(tolerance).of(expectedResult.reach.value)
  }

  @Test
  fun `creating 25 metrics at once succeeds`() = runBlocking {
    val numMetrics = 25
    val measurementConsumerData = inProcessCmmsComponents.getMeasurementConsumerData()
    val eventGroups = listEventGroups()
    val eventGroupEntries: List<Pair<EventGroup, String>> =
      listOf(eventGroups.first() to "person.age_group == ${Person.AgeGroup.YEARS_18_TO_34_VALUE}")
    val createdPrimitiveReportingSet: ReportingSet =
      createPrimitiveReportingSets(eventGroupEntries, measurementConsumerData.name).single()

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
          Metric.RESULT_FIELD_NUMBER,
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
      primitive = ReportingSetKt.primitive { cmmsEventGroups += eventGroups.first().cmmsEventGroup }
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
    val dataProviderName = eventGroups.first().cmmsDataProvider

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

  private suspend fun createPrimitiveReportingSets(
    eventGroupEntries: List<Pair<EventGroup, String>>,
    measurementConsumerName: String,
  ): List<ReportingSet> {
    val primitiveReportingSets: List<ReportingSet> =
      eventGroupEntries.mapIndexed { index, (eventGroup, filterExp) ->
        reportingSet {
          displayName = "primitive$index"
          if (filterExp.isNotBlank()) {
            filter = filterExp
          }
          primitive = ReportingSetKt.primitive { cmmsEventGroups += eventGroup.cmmsEventGroup }
        }
      }

    return primitiveReportingSets.mapIndexed { index, primitiveReportingSet ->
      publicReportingSetsClient
        .withPrincipalName(measurementConsumerName)
        .createReportingSet(
          createReportingSetRequest {
            parent = measurementConsumerName
            reportingSet = primitiveReportingSet
            reportingSetId = "abc$index"
          }
        )
    }
  }

  private suspend fun pollForCompletedReport(
    measurementConsumerName: String,
    reportName: String,
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
    metricName: String,
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
    maxFrequency: Int,
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

  private fun calculateExpectedImpressionMeasurementResult(
    sampledVids: Sequence<Long>,
    maxFrequency: Int,
  ): Measurement.Result {
    val impression = MeasurementResults.computeImpression(sampledVids.asIterable(), maxFrequency)
    return MeasurementKt.result {
      this.impression = MeasurementKt.ResultKt.impression { value = impression }
    }
  }

  private fun buildEventGroupSpec(
    eventGroup: EventGroup,
    filter: String,
    collectionInterval: Interval,
  ): EventQuery.EventGroupSpec {
    val cmmsMetadata =
      CmmsEventGroupKt.metadata {
        eventGroupMetadataDescriptor = eventGroup.metadata.eventGroupMetadataDescriptor
        metadata = eventGroup.metadata.metadata
      }
    val encryptedCmmsMetadata =
      encryptMetadata(cmmsMetadata, InProcessCmmsComponents.MC_ENTITY_CONTENT.encryptionPublicKey)
    val cmmsEventGroup = cmmsEventGroup { encryptedMetadata = encryptedCmmsMetadata }

    val eventFilter = RequisitionSpecKt.eventFilter { expression = filter }

    return EventQuery.EventGroupSpec(
      cmmsEventGroup,
      RequisitionSpecKt.EventGroupEntryKt.value {
        this.collectionInterval = collectionInterval
        this.filter = eventFilter
      },
    )
  }

  private fun SyntheticGeneratorEventQuery.getUserVirtualIds(
    eventGroup: EventGroup,
    filter: String,
    collectionInterval: Interval,
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
        },
      )
    )
  }

  private fun sampleVids(
    eventGroupSpecs: Iterable<EventQuery.EventGroupSpec>,
    vidSamplingInterval: VidSamplingInterval,
  ): Sequence<Long> {
    return sampleVids(
        SYNTHETIC_EVENT_QUERY,
        eventGroupSpecs,
        vidSamplingInterval.start,
        vidSamplingInterval.width,
      )
      .asSequence()
  }

  private fun Sequence<Long>.calculateSampledVids(
    vidSamplingInterval: VidSamplingInterval
  ): Sequence<Long> {
    return this.filter { vid ->
      VidSampling.sampler.vidIsInSamplingBucket(
        vid,
        vidSamplingInterval.start,
        vidSamplingInterval.width,
      )
    }
  }

  /** Computes the margin of error, i.e. half width, of a 99.9% confidence interval. */
  private fun computeErrorMargin(standardDeviation: Double): Double {
    return CONFIDENCE_INTERVAL_MULTIPLIER * standardDeviation
  }

  companion object {
    private val SECRETS_DIR: File =
      getRuntimePath(
          Paths.get("wfa_measurement_system", "src", "main", "k8s", "testing", "secretfiles")
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
        InProcessCmmsComponents.MC_ENCRYPTION_PRIVATE_KEY,
      )

    private val EVENT_RANGE =
      OpenEndTimeRange.fromClosedDateRange(LocalDate.of(2021, 3, 15)..LocalDate.of(2021, 3, 17))

    // Set epsilon and delta higher without exceeding privacy budget so the noise is smaller in the
    // integration test. Check sample values in CompositionTest.kt.
    private val DP_PARAMS =
      MetricSpecKt.differentialPrivacyParams {
        epsilon = 1.0
        delta = 1e-15
      }

    private val VID_SAMPLING_INTERVAL =
      MetricSpecKt.vidSamplingInterval {
        start = 0.0f
        width = 1.0f
      }

    // For a 99.9% Confidence Interval.
    private const val CONFIDENCE_INTERVAL_MULTIPLIER = 3.291

    @BeforeClass
    @JvmStatic
    fun initConfig() {
      InProcessCmmsComponents.initConfig()
    }
  }
}
