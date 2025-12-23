/*
 * Copyright 2025 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.reporting.job

import com.google.protobuf.Descriptors
import com.google.protobuf.Timestamp
import com.google.type.Date
import com.google.type.date
import java.time.ZoneId
import java.time.ZoneOffset
import java.util.logging.Level
import java.util.logging.Logger
import kotlin.math.roundToInt
import org.wfanet.measurement.access.client.v1alpha.TrustedPrincipalAuthInterceptor
import org.wfanet.measurement.access.v1alpha.principal
import org.wfanet.measurement.api.v2alpha.EventMessageDescriptor
import org.wfanet.measurement.api.v2alpha.MeasurementConsumerKey
import org.wfanet.measurement.common.api.grpc.ResourceList
import org.wfanet.measurement.common.api.grpc.listResources
import org.wfanet.measurement.common.toInstant
import org.wfanet.measurement.config.reporting.MeasurementConsumerConfigs
import org.wfanet.measurement.internal.reporting.v2.BasicReport
import org.wfanet.measurement.internal.reporting.v2.BasicReportsGrpcKt.BasicReportsCoroutineStub as InternalBasicReportsCoroutineStub
import org.wfanet.measurement.internal.reporting.v2.CreateReportingSetResultRequest
import org.wfanet.measurement.internal.reporting.v2.EventFilter
import org.wfanet.measurement.internal.reporting.v2.EventTemplateField
import org.wfanet.measurement.internal.reporting.v2.EventTemplateFieldKt
import org.wfanet.measurement.internal.reporting.v2.ListBasicReportsPageToken
import org.wfanet.measurement.internal.reporting.v2.ListBasicReportsRequestKt
import org.wfanet.measurement.internal.reporting.v2.ListMetricCalculationSpecsRequestKt
import org.wfanet.measurement.internal.reporting.v2.MetricCalculationSpec
import org.wfanet.measurement.internal.reporting.v2.MetricCalculationSpecsGrpcKt.MetricCalculationSpecsCoroutineStub as InternalMetricCalculationSpecsCoroutineStub
import org.wfanet.measurement.internal.reporting.v2.MetricFrequencySpec
import org.wfanet.measurement.internal.reporting.v2.ReportResult
import org.wfanet.measurement.internal.reporting.v2.ReportResultsGrpcKt.ReportResultsCoroutineStub
import org.wfanet.measurement.internal.reporting.v2.ReportingSetResult.Dimension.VennDiagramRegionType
import org.wfanet.measurement.internal.reporting.v2.ReportingSetResult.ReportingWindowResult.NoisyReportResultValues.NoisyMetricSet
import org.wfanet.measurement.internal.reporting.v2.ReportingSetResultKt
import org.wfanet.measurement.internal.reporting.v2.batchCreateReportingSetResultsRequest
import org.wfanet.measurement.internal.reporting.v2.createReportResultRequest
import org.wfanet.measurement.internal.reporting.v2.createReportingSetResultRequest
import org.wfanet.measurement.internal.reporting.v2.eventTemplateField
import org.wfanet.measurement.internal.reporting.v2.failBasicReportRequest
import org.wfanet.measurement.internal.reporting.v2.listBasicReportsRequest
import org.wfanet.measurement.internal.reporting.v2.listMetricCalculationSpecsRequest
import org.wfanet.measurement.internal.reporting.v2.metricFrequencySpec
import org.wfanet.measurement.internal.reporting.v2.reportResult
import org.wfanet.measurement.internal.reporting.v2.reportingSetResult
import org.wfanet.measurement.reporting.service.api.v2alpha.BasicReportKey
import org.wfanet.measurement.reporting.service.api.v2alpha.MetricCalculationSpecKey
import org.wfanet.measurement.reporting.service.api.v2alpha.ReportKey
import org.wfanet.measurement.reporting.service.api.v2alpha.ReportingSetKey
import org.wfanet.measurement.reporting.service.api.v2alpha.createDimensionSpecFilter
import org.wfanet.measurement.reporting.service.api.v2alpha.createImpressionQualificationFilterSpecsFilter
import org.wfanet.measurement.reporting.service.api.v2alpha.createMetricCalculationSpecFilters
import org.wfanet.measurement.reporting.service.api.v2alpha.toEventFilter
import org.wfanet.measurement.reporting.service.api.v2alpha.toImpressionQualificationFilterSpec
import org.wfanet.measurement.reporting.service.internal.InvalidBasicReportException
import org.wfanet.measurement.reporting.service.internal.Normalization
import org.wfanet.measurement.reporting.v2alpha.MetricResult
import org.wfanet.measurement.reporting.v2alpha.MetricSpec
import org.wfanet.measurement.reporting.v2alpha.Report
import org.wfanet.measurement.reporting.v2alpha.ReportsGrpcKt.ReportsCoroutineStub
import org.wfanet.measurement.reporting.v2alpha.UnivariateStatistics
import org.wfanet.measurement.reporting.v2alpha.getReportRequest

class BasicReportsReportsJob(
  private val measurementConsumerConfigs: MeasurementConsumerConfigs,
  private val internalBasicReportsStub: InternalBasicReportsCoroutineStub,
  private val reportsStub: ReportsCoroutineStub,
  private val internalMetricCalculationSpecsStub: InternalMetricCalculationSpecsCoroutineStub,
  private val reportResultsStub: ReportResultsCoroutineStub,
  private val eventMessageDescriptor: EventMessageDescriptor?,
) {

  /**
   * For every MeasurementConsumer, all BasicReports with State REPORT_CREATED are retrieved. For
   * each of those BasicReports, the Report is retrieved.
   */
  suspend fun execute() {
    val eventTemplateFieldsByPath = eventMessageDescriptor?.eventTemplateFieldsByPath ?: emptyMap()

    val eventTemplateFieldByPredicate =
      buildEventTemplateFieldByPredicateMap(eventTemplateFieldsByPath)

    val measurementConsumerConfigByName =
      measurementConsumerConfigs.configsMap.filterValues { it.offlinePrincipal.isNotEmpty() }

    for ((measurementConsumerName, measurementConsumerConfig) in
      measurementConsumerConfigByName.entries) {
      val cmmsMeasurementConsumerId =
        requireNotNull(MeasurementConsumerKey.fromName(measurementConsumerName))
          .measurementConsumerId

      val resourceLists =
        internalBasicReportsStub.listResources(BATCH_SIZE, null) {
          pageToken: ListBasicReportsPageToken?,
          remaining ->
          val listBasicReportsResponse =
            internalBasicReportsStub.listBasicReports(
              listBasicReportsRequest {
                pageSize = remaining
                if (pageToken != null) {
                  this.pageToken = pageToken
                }
                filter =
                  ListBasicReportsRequestKt.filter {
                    this.cmmsMeasurementConsumerId = cmmsMeasurementConsumerId
                    state = BasicReport.State.REPORT_CREATED
                  }
              }
            )

          val nextPageToken =
            if (listBasicReportsResponse.hasNextPageToken()) {
              listBasicReportsResponse.nextPageToken
            } else {
              null
            }

          ResourceList(listBasicReportsResponse.basicReportsList, nextPageToken)
        }

      resourceLists.collect { resourceList ->
        for (basicReport in resourceList.resources) {
          try {
            val report =
              reportsStub
                .withCallCredentials(
                  TrustedPrincipalAuthInterceptor.Credentials(
                    // TODO(@SanjayVas): Read full Principal from Access.
                    principal { name = measurementConsumerConfig.offlinePrincipal },
                    setOf("reporting.reports.get", "reporting.metrics.get"),
                  )
                )
                .getReport(
                  getReportRequest {
                    name =
                      ReportKey(
                          cmmsMeasurementConsumerId = cmmsMeasurementConsumerId,
                          reportId = basicReport.externalReportId,
                        )
                        .toName()
                  }
                )

            @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA") // Protobuf enums cannot be null.
            when (report.state) {
              Report.State.SUCCEEDED -> {
                val reportResult =
                  reportResultsStub.createReportResult(
                    createReportResultRequest {
                      this.cmmsMeasurementConsumerId = basicReport.cmmsMeasurementConsumerId
                      reportResult = reportResult {
                        reportStart = report.reportingInterval.reportStart
                      }
                    }
                  )
                val reportingSetResultRequests: List<CreateReportingSetResultRequest> =
                  try {
                    transformReportResults(
                      reportResult,
                      basicReport,
                      report,
                      eventTemplateFieldsByPath,
                      eventTemplateFieldByPredicate,
                    )
                    // There is a bug with CreateBasicReports involving the storing of
                    // ImpressionQualificationFilter information that has been fixed. BasicReports
                    // affected by the bug will be FAILED.
                  } catch (e: InvalidBasicReportException) {
                    logger.log(
                      Level.WARNING,
                      "BasicReport is affected by a bug that has been fixed and must be FAILED",
                      e,
                    )
                    failBasicReport(
                      cmmsMeasurementConsumerId = cmmsMeasurementConsumerId,
                      externalBasicReportId = basicReport.externalBasicReportId,
                    )
                    continue
                  }

                reportResultsStub.batchCreateReportingSetResults(
                  batchCreateReportingSetResultsRequest {
                    this.cmmsMeasurementConsumerId = reportResult.cmmsMeasurementConsumerId
                    externalReportResultId = reportResult.externalReportResultId
                    requests += reportingSetResultRequests
                    externalBasicReportId = basicReport.externalBasicReportId
                  }
                )
              }
              Report.State.FAILED -> {
                failBasicReport(
                  cmmsMeasurementConsumerId = cmmsMeasurementConsumerId,
                  externalBasicReportId = basicReport.externalBasicReportId,
                )
              }

              Report.State.STATE_UNSPECIFIED,
              Report.State.RUNNING,
              Report.State.UNRECOGNIZED -> {
                // Do nothing
              }
            }
          } catch (e: Exception) {
            logger.log(
              Level.WARNING,
              "Failed to get Report Results for BasicReport ${BasicReportKey(basicReport.cmmsMeasurementConsumerId, basicReport.externalReportId).toName()}",
              e,
            )
          }
        }
      }
    }
  }

  /**
   * Create [ReportResult] from [BasicReport] and [Report]
   *
   * @param basicReport [BasicReport]
   * @param report [Report] associated with [BasicReport]
   * @param eventTemplateFieldsByPath Map of EventTemplate field path with respect to Event message
   *   to info for the field.
   * @param eventTemplateFieldByPredicate Map of Predicate String from
   *   [MetricCalculationSpec.Grouping] to [EventTemplateField]
   * @return [ReportResult]
   * @throws [InvalidBasicReportException] When [BasicReport] was created incorrectly
   */
  private suspend fun transformReportResults(
    reportResult: ReportResult,
    basicReport: BasicReport,
    report: Report,
    eventTemplateFieldsByPath: Map<String, EventMessageDescriptor.EventTemplateFieldInfo>,
    eventTemplateFieldByPredicate: Map<String, EventTemplateField>,
  ): List<CreateReportingSetResultRequest> {
    val (
      populationCountByPopulationResultKey: Map<PopulationResultKey, Int>,
      reportingSetResultInfoByReportingSetResultInfoKey:
        Map<ReportingSetResultInfoKey, ReportingSetResultInfo>) =
      buildReportResultInfo(
        report,
        basicReport.cmmsMeasurementConsumerId,
        basicReport.externalCampaignGroupId,
      )
    val filterInfoByFilter: Map<String, FilterInfo> =
      buildFilterInfoByFilterString(basicReport, eventTemplateFieldsByPath)

    return reportingSetResultInfoByReportingSetResultInfoKey.entries.map {
      reportingSetResultInfoEntry ->
      createReportingSetResultRequest {
        cmmsMeasurementConsumerId = reportResult.cmmsMeasurementConsumerId
        externalReportResultId = reportResult.externalReportResultId

        reportingSetResult = reportingSetResult {
          val filterInfo =
            filterInfoByFilter[reportingSetResultInfoEntry.key.filter]
              ?: throw InvalidBasicReportException(
                cmmsMeasurementConsumerId = basicReport.cmmsMeasurementConsumerId,
                externalBasicReportId = basicReport.externalBasicReportId,
              )
          dimension =
            ReportingSetResultKt.dimension {
              externalReportingSetId = reportingSetResultInfoEntry.key.externalReportingSetId
              vennDiagramRegionType = VennDiagramRegionType.UNION
              if (filterInfo.externalImpressionQualificationFilterId != null) {
                externalImpressionQualificationFilterId =
                  filterInfo.externalImpressionQualificationFilterId
              } else {
                custom = true
              }
              metricFrequencySpec = reportingSetResultInfoEntry.key.metricFrequencySpec
              grouping =
                ReportingSetResultKt.DimensionKt.grouping {
                  for (predicate in reportingSetResultInfoEntry.key.groupingPredicates) {
                    val templateField = eventTemplateFieldByPredicate.getValue(predicate)
                    valueByPath[templateField.path] = templateField.value
                  }
                }
              eventFilters += Normalization.normalizeEventFilters(filterInfo.dimensionSpecFilters)
            }
          // Population results are not keyed by IQF and ReportingSet, as IQFs include
          // non-Population fields and the ReportingSet EventGroups are irrelevant to the Population
          // Measurement.
          populationSize =
            populationCountByPopulationResultKey.getOrDefault(
              PopulationResultKey(
                filterInfo.dimensionSpecFilter,
                reportingSetResultInfoEntry.key.groupingPredicates,
              ),
              0,
            )

          for (reportingWindowResultInfoEntry: Map.Entry<Date, ReportingWindowResultInfo> in
            reportingSetResultInfoEntry.value.reportingWindowResultInfoByEndDate.entries) {
            val window =
              ReportingSetResultKt.reportingWindow {
                if (reportingWindowResultInfoEntry.value.startDate != null) {
                  nonCumulativeStart = reportingWindowResultInfoEntry.value.startDate!!
                }
                end = reportingWindowResultInfoEntry.key
              }
            reportingWindowResults +=
              ReportingSetResultKt.reportingWindowEntry {
                this.key = window
                value =
                  ReportingSetResultKt.reportingWindowResult {
                    unprocessedReportResultValues =
                      ReportingSetResultKt.ReportingWindowResultKt.noisyReportResultValues {
                        val cumulativeResults: MetricResults? =
                          reportingWindowResultInfoEntry.value.cumulativeResults
                        if (cumulativeResults != null) {
                          this.cumulativeResults =
                            ReportingSetResultKt.ReportingWindowResultKt.NoisyReportResultValuesKt
                              .noisyMetricSet {
                                val reach: MetricResult.ReachResult? = cumulativeResults.reach
                                if (reach != null) {
                                  this.reach = reach.toNoisyMetricSetReachResult()
                                }
                                val frequencyHistogram: MetricResult.HistogramResult? =
                                  cumulativeResults.frequencyHistogram
                                if (frequencyHistogram != null) {
                                  this.frequencyHistogram =
                                    frequencyHistogram.toNoisyMetricSetHistogramResult()
                                }
                                val impressionCount: MetricResult.ImpressionCountResult? =
                                  cumulativeResults.impressionCount
                                if (impressionCount != null) {
                                  this.impressionCount =
                                    impressionCount.toNoisyMetricSetImpressionCountResult()
                                }
                              }
                        }
                        val nonCumulativeResults: MetricResults? =
                          reportingWindowResultInfoEntry.value.nonCumulativeResults
                        if (nonCumulativeResults != null) {
                          this.nonCumulativeResults =
                            ReportingSetResultKt.ReportingWindowResultKt.NoisyReportResultValuesKt
                              .noisyMetricSet {
                                val reach: MetricResult.ReachResult? = nonCumulativeResults.reach
                                if (reach != null) {
                                  this.reach = reach.toNoisyMetricSetReachResult()
                                }
                                val frequencyHistogram: MetricResult.HistogramResult? =
                                  nonCumulativeResults.frequencyHistogram
                                if (frequencyHistogram != null) {
                                  this.frequencyHistogram =
                                    frequencyHistogram.toNoisyMetricSetHistogramResult()
                                }
                                val impressionCount: MetricResult.ImpressionCountResult? =
                                  nonCumulativeResults.impressionCount
                                if (impressionCount != null) {
                                  this.impressionCount =
                                    impressionCount.toNoisyMetricSetImpressionCountResult()
                                }
                              }
                        }
                      }
                  }
              }
          }
        }
      }
    }
  }

  /**
   * Builds a [ReportResultInfo] from [Report]
   *
   * @param report [Report]
   * @param cmmsMeasurementConsumerId CmmsMeasurementConsumerId
   * @param externalCampaignGroupId ExternalReportingSetId for CampaignGroup
   */
  private suspend fun buildReportResultInfo(
    report: Report,
    cmmsMeasurementConsumerId: String,
    externalCampaignGroupId: String,
  ): ReportResultInfo {
    val reportStart = report.reportingInterval.reportStart

    val metricCalculationSpecInfoByName: Map<String, MetricCalculationSpecInfo> =
      buildMetricCalculationSpecInfoByNameMap(
        cmmsMeasurementConsumerId = cmmsMeasurementConsumerId,
        externalCampaignGroupId = externalCampaignGroupId,
      )

    val populationCountByPopulationResultKey: MutableMap<PopulationResultKey, Int> = mutableMapOf()
    val reportingSetResultInfoByReportingSetResultInfoKey:
      Map<ReportingSetResultInfoKey, ReportingSetResultInfo> =
      buildMap {
        for (metricCalculationResult in report.metricCalculationResultsList) {
          val externalReportingSetId =
            requireNotNull(ReportingSetKey.fromName(metricCalculationResult.reportingSet))
              .reportingSetId

          val metricCalculationSpecInfo: MetricCalculationSpecInfo =
            metricCalculationSpecInfoByName.getValue(metricCalculationResult.metricCalculationSpec)

          if (
            metricCalculationResult.resultAttributesList.all {
              it.metricResult.hasPopulationCount()
            }
          ) {
            metricCalculationResult.resultAttributesList.forEach {
              populationCountByPopulationResultKey[
                PopulationResultKey(
                  filter = it.filter,
                  groupingPredicates = it.groupingPredicatesList.toSet(),
                )] = it.metricResult.populationCount.value.toInt()
            }
          } else {
            for (resultAttribute in metricCalculationResult.resultAttributesList) {
              val reportingSetResultInfo: ReportingSetResultInfo =
                getOrPut(
                  ReportingSetResultInfoKey(
                    externalReportingSetId = externalReportingSetId,
                    filter = resultAttribute.filter,
                    groupingPredicates = resultAttribute.groupingPredicatesList.toSet(),
                    metricFrequencySpec =
                      metricCalculationSpecInfo.metricFrequencySpec.toMetricFrequencySpec(),
                  )
                ) {
                  ReportingSetResultInfo(reportingWindowResultInfoByEndDate = mutableMapOf())
                }

              val startDate: Date? =
                if (metricCalculationSpecInfo.hasTrailingWindow) {
                  if (reportStart.hasUtcOffset()) {
                    resultAttribute.timeInterval.startTime.toDate(
                      ZoneOffset.ofTotalSeconds(reportStart.utcOffset.seconds.toInt())
                    )
                  } else {
                    resultAttribute.timeInterval.startTime.toDate(
                      ZoneId.of(reportStart.timeZone.id)
                    )
                  }
                } else {
                  null
                }

              val endDate =
                if (reportStart.hasUtcOffset()) {
                  resultAttribute.timeInterval.endTime.toDate(
                    ZoneOffset.ofTotalSeconds(reportStart.utcOffset.seconds.toInt())
                  )
                } else {
                  resultAttribute.timeInterval.endTime.toDate(ZoneId.of(reportStart.timeZone.id))
                }

              val reportingWindowResultInfo: ReportingWindowResultInfo =
                reportingSetResultInfo.reportingWindowResultInfoByEndDate.getOrPut(endDate) {
                  ReportingWindowResultInfo()
                }

              if (startDate != null) {
                reportingWindowResultInfo.startDate = startDate
              }

              val metricResults =
                if (metricCalculationSpecInfo.hasTrailingWindow) {
                  if (reportingWindowResultInfo.nonCumulativeResults == null) {
                    reportingWindowResultInfo.nonCumulativeResults = MetricResults()
                    reportingWindowResultInfo.nonCumulativeResults
                  } else {
                    reportingWindowResultInfo.nonCumulativeResults
                  }
                } else {
                  if (reportingWindowResultInfo.cumulativeResults == null) {
                    reportingWindowResultInfo.cumulativeResults = MetricResults()
                    reportingWindowResultInfo.cumulativeResults
                  } else {
                    reportingWindowResultInfo.cumulativeResults
                  }
                }

              @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA") // Protobuf oneof cannot be null.
              when (resultAttribute.metricSpec.typeCase) {
                MetricSpec.TypeCase.REACH -> {
                  metricResults!!.reach = resultAttribute.metricResult.reach
                }

                MetricSpec.TypeCase.REACH_AND_FREQUENCY -> {
                  metricResults!!.reach = resultAttribute.metricResult.reachAndFrequency.reach
                  metricResults.frequencyHistogram =
                    resultAttribute.metricResult.reachAndFrequency.frequencyHistogram
                }

                MetricSpec.TypeCase.IMPRESSION_COUNT -> {
                  metricResults!!.impressionCount = resultAttribute.metricResult.impressionCount
                }

                MetricSpec.TypeCase.WATCH_DURATION -> {
                  // Do nothing. Not supported
                }

                MetricSpec.TypeCase.POPULATION_COUNT -> {
                  populationCountByPopulationResultKey[
                    PopulationResultKey(
                      filter = resultAttribute.filter,
                      groupingPredicates = resultAttribute.groupingPredicatesList.toSet(),
                    )] = resultAttribute.metricResult.populationCount.value.toInt()
                }

                MetricSpec.TypeCase.TYPE_NOT_SET -> {
                  // This should be impossible to reach under normal circumstances
                  error("Metric ${resultAttribute.metric} is missing metric_spec.type")
                }
              }
            }
          }
        }
      }

    return ReportResultInfo(
      populationCountByPopulationResultKey,
      reportingSetResultInfoByReportingSetResultInfoKey,
    )
  }

  /**
   * Builds Map of [MetricCalculationSpec] name to [MetricCalculationSpecInfo]
   *
   * @param cmmsMeasurementConsumerId CmmsMeasurementConsumerId
   * @param externalCampaignGroupId ExternalCampaignGroupId from [BasicReport]
   * @return Map of [MetricCalculationSpec] name to [MetricCalculationSpecInfo]
   */
  private suspend fun buildMetricCalculationSpecInfoByNameMap(
    cmmsMeasurementConsumerId: String,
    externalCampaignGroupId: String,
  ): Map<String, MetricCalculationSpecInfo> {
    return internalMetricCalculationSpecsStub
      .listMetricCalculationSpecs(
        listMetricCalculationSpecsRequest {
          this.cmmsMeasurementConsumerId = cmmsMeasurementConsumerId
          filter =
            ListMetricCalculationSpecsRequestKt.filter {
              this.externalCampaignGroupId = externalCampaignGroupId
            }
          limit = Int.MAX_VALUE
        }
      )
      .metricCalculationSpecsList
      .associate {
        MetricCalculationSpecKey(it.cmmsMeasurementConsumerId, it.externalMetricCalculationSpecId)
          .toName() to
          MetricCalculationSpecInfo(it.details.metricFrequencySpec, it.details.hasTrailingWindow())
      }
  }

  /** Transforms a [MetricCalculationSpec.MetricFrequencySpec] into [MetricFrequencySpec] */
  private fun MetricCalculationSpec.MetricFrequencySpec.toMetricFrequencySpec():
    MetricFrequencySpec {
    val source = this

    return metricFrequencySpec {
      when (source.frequencyCase) {
        MetricCalculationSpec.MetricFrequencySpec.FrequencyCase.WEEKLY ->
          weekly = source.weekly.dayOfWeek
        MetricCalculationSpec.MetricFrequencySpec.FrequencyCase.FREQUENCY_NOT_SET -> total = true
        MetricCalculationSpec.MetricFrequencySpec.FrequencyCase.DAILY,
        MetricCalculationSpec.MetricFrequencySpec.FrequencyCase.MONTHLY ->
          error("Unsupported frequency type ${source.frequencyCase}")
      }
    }
  }

  /**
   * Builds Map of Predicate String to [EventTemplateField]. Predicate String is from
   * [MetricCalculationSpec.Grouping].
   *
   * @param eventTemplateFieldsByPath Map of EventTemplate field path with respect to Event message
   *   to info for the field.
   * @return Map of Predicate String to [EventTemplateField]
   */
  private fun buildEventTemplateFieldByPredicateMap(
    eventTemplateFieldsByPath: Map<String, EventMessageDescriptor.EventTemplateFieldInfo>
  ): Map<String, EventTemplateField> {
    return buildMap {
      for (eventTemplateFieldEntry in eventTemplateFieldsByPath.entries) {
        val field = eventTemplateFieldEntry.key
        val fieldInfo = eventTemplateFieldEntry.value
        if (fieldInfo.enumType != null) {
          val fieldInfoEnumType = fieldInfo.enumType as Descriptors.EnumDescriptor
          for (enumValue in fieldInfoEnumType.values) {
            put(
              "$field == ${enumValue.number}",
              eventTemplateField {
                path = field
                value = EventTemplateFieldKt.fieldValue { this.enumValue = enumValue.name }
              },
            )
          }
        }
      }
    }
  }

  /**
   * Builds Map of Filter String to [FilterInfo]
   *
   * @param basicReport [BasicReport]
   * @param eventTemplateFieldsByPath Map of EventTemplate field path with respect to Event message
   *   to info for the field.
   * @return Map of Filter String to [FilterInfo]
   */
  private fun buildFilterInfoByFilterString(
    basicReport: BasicReport,
    eventTemplateFieldsByPath: Map<String, EventMessageDescriptor.EventTemplateFieldInfo>,
  ): Map<String, FilterInfo> {
    return buildMap {
      for (reportingImpressionQualificationFilter in
        basicReport.details.effectiveImpressionQualificationFiltersList) {
        val impressionQualificationFilterString =
          createImpressionQualificationFilterSpecsFilter(
            reportingImpressionQualificationFilter.filterSpecsList.map {
              it.toImpressionQualificationFilterSpec()
            },
            eventTemplateFieldsByPath,
          )

        for (resultGroupSpec in basicReport.details.resultGroupSpecsList) {
          val dimensionSpecFilters = resultGroupSpec.dimensionSpec.filtersList
          val dimensionSpecFilter: String =
            createDimensionSpecFilter(
              dimensionSpecFilters.map { it.toEventFilter() },
              eventTemplateFieldsByPath,
            )
          val filter =
            createMetricCalculationSpecFilters(
                listOf(impressionQualificationFilterString),
                dimensionSpecFilter,
              )
              .first()

          val externalImpressionQualificationFilterId: String? =
            reportingImpressionQualificationFilter.externalImpressionQualificationFilterId.ifEmpty {
              null
            }
          put(
            filter,
            FilterInfo(
              externalImpressionQualificationFilterId,
              dimensionSpecFilters,
              dimensionSpecFilter,
            ),
          )
        }
      }
    }
  }

  private fun Timestamp.toDate(zoneId: ZoneId): Date {
    val localDate = this.toInstant().atZone(zoneId)

    return date {
      year = localDate.year
      month = localDate.monthValue
      day = localDate.dayOfMonth
    }
  }

  private fun UnivariateStatistics.toNoisyMetricSetUnivariateStatistics():
    NoisyMetricSet.UnivariateStatistics {
    val source = this
    return ReportingSetResultKt.ReportingWindowResultKt.NoisyReportResultValuesKt.NoisyMetricSetKt
      .univariateStatistics { standardDeviation = source.standardDeviation }
  }

  private fun MetricResult.ReachResult.toNoisyMetricSetReachResult(): NoisyMetricSet.ReachResult {
    val source = this
    return ReportingSetResultKt.ReportingWindowResultKt.NoisyReportResultValuesKt.NoisyMetricSetKt
      .reachResult {
        value = source.value
        if (source.hasUnivariateStatistics()) {
          univariateStatistics = source.univariateStatistics.toNoisyMetricSetUnivariateStatistics()
        }
      }
  }

  private fun MetricResult.HistogramResult.toNoisyMetricSetHistogramResult():
    NoisyMetricSet.HistogramResult {
    val source = this
    return ReportingSetResultKt.ReportingWindowResultKt.NoisyReportResultValuesKt.NoisyMetricSetKt
      .histogramResult {
        for (bin: MetricResult.HistogramResult.Bin in source.binsList) {
          binResults[bin.label.toInt()] =
            ReportingSetResultKt.ReportingWindowResultKt.NoisyReportResultValuesKt.NoisyMetricSetKt
              .HistogramResultKt
              .binResult {
                value = bin.binResult.value.roundToInt()
                if (bin.hasResultUnivariateStatistics()) {
                  univariateStatistics =
                    bin.resultUnivariateStatistics.toNoisyMetricSetUnivariateStatistics()
                }
              }
        }
      }
  }

  private fun MetricResult.ImpressionCountResult.toNoisyMetricSetImpressionCountResult():
    NoisyMetricSet.ImpressionCountResult {
    val source = this
    return ReportingSetResultKt.ReportingWindowResultKt.NoisyReportResultValuesKt.NoisyMetricSetKt
      .impressionCountResult {
        value = source.value
        if (source.hasUnivariateStatistics()) {
          univariateStatistics = source.univariateStatistics.toNoisyMetricSetUnivariateStatistics()
        }
      }
  }

  private suspend fun failBasicReport(
    cmmsMeasurementConsumerId: String,
    externalBasicReportId: String,
  ) {
    internalBasicReportsStub.failBasicReport(
      failBasicReportRequest {
        this.cmmsMeasurementConsumerId = cmmsMeasurementConsumerId
        this.externalBasicReportId = externalBasicReportId
      }
    )
  }

  private data class MetricCalculationSpecInfo(
    val metricFrequencySpec: MetricCalculationSpec.MetricFrequencySpec,
    val hasTrailingWindow: Boolean,
  )

  private data class ReportResultInfo(
    val populationCountByPopulationResultKey: Map<PopulationResultKey, Int>,
    val reportingSetResultInfoByReportingSetResultInfoKey:
      Map<ReportingSetResultInfoKey, ReportingSetResultInfo>,
  )

  private data class PopulationResultKey(val filter: String, val groupingPredicates: Set<String>)

  private data class ReportingSetResultInfoKey(
    val externalReportingSetId: String,
    val filter: String,
    val groupingPredicates: Set<String>,
    val metricFrequencySpec: MetricFrequencySpec,
  )

  private data class ReportingSetResultInfo(
    val reportingWindowResultInfoByEndDate: MutableMap<Date, ReportingWindowResultInfo>
  )

  private data class ReportingWindowResultInfo(
    var startDate: Date? = null,
    var cumulativeResults: MetricResults? = null,
    var nonCumulativeResults: MetricResults? = null,
  )

  private data class MetricResults(
    var reach: MetricResult.ReachResult? = null,
    var frequencyHistogram: MetricResult.HistogramResult? = null,
    var impressionCount: MetricResult.ImpressionCountResult? = null,
  )

  private data class FilterInfo(
    val externalImpressionQualificationFilterId: String? = null,
    val dimensionSpecFilters: List<EventFilter>,
    // CEL String created from dimensionSpecFilters
    val dimensionSpecFilter: String,
  )

  companion object {
    private val logger: Logger = Logger.getLogger(this::class.java.name)
    private const val BATCH_SIZE = 10
  }
}
