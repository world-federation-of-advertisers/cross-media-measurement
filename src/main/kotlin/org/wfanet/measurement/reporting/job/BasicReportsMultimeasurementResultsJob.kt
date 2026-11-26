/*
 * Copyright 2026 The Cross-Media Measurement Authors
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

import com.google.type.Date
import io.grpc.Status
import io.grpc.StatusException
import java.util.UUID
import java.util.logging.Level
import java.util.logging.Logger
import kotlin.math.sqrt
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.flow.toList
import org.wfanet.measurement.api.v2alpha.Measurement
import org.wfanet.measurement.api.v2alpha.MeasurementConsumerKey
import org.wfanet.measurement.api.v2alpha.MeasurementKey
import org.wfanet.measurement.api.v2alpha.MeasurementsGrpcKt.MeasurementsCoroutineStub as CmmsMeasurementsCoroutineStub
import org.wfanet.measurement.api.v2alpha.PopulationAttributeValue
import org.wfanet.measurement.api.v2alpha.getMeasurementRequest
import org.wfanet.measurement.api.withAuthenticationKey
import org.wfanet.measurement.common.api.grpc.ResourceList
import org.wfanet.measurement.common.api.grpc.listResources
import org.wfanet.measurement.config.reporting.MeasurementConsumerConfigs
import org.wfanet.measurement.internal.reporting.v2.BasicReport
import org.wfanet.measurement.internal.reporting.v2.BasicReportsGrpcKt.BasicReportsCoroutineStub as InternalBasicReportsCoroutineStub
import org.wfanet.measurement.internal.reporting.v2.EventTemplateFieldKt
import org.wfanet.measurement.internal.reporting.v2.ListBasicReportsPageToken
import org.wfanet.measurement.internal.reporting.v2.ListBasicReportsRequestKt
import org.wfanet.measurement.internal.reporting.v2.ReportComputation
import org.wfanet.measurement.internal.reporting.v2.ReportResultsGrpcKt.ReportResultsCoroutineStub
import org.wfanet.measurement.internal.reporting.v2.ReportingSet
import org.wfanet.measurement.internal.reporting.v2.ReportingSetKt
import org.wfanet.measurement.internal.reporting.v2.ReportingSetResult
import org.wfanet.measurement.internal.reporting.v2.ReportingSetResult.ReportingWindowResult.NoisyReportResultValues.NoisyMetricSet
import org.wfanet.measurement.internal.reporting.v2.ReportingSetResultKt
import org.wfanet.measurement.internal.reporting.v2.ReportingSetsGrpcKt.ReportingSetsCoroutineStub as InternalReportingSetsCoroutineStub
import org.wfanet.measurement.internal.reporting.v2.ReportingUnit
import org.wfanet.measurement.internal.reporting.v2.ResultGroupQueryMapping
import org.wfanet.measurement.internal.reporting.v2.ResultGroupSpec
import org.wfanet.measurement.internal.reporting.v2.StreamReportingSetsRequestKt
import org.wfanet.measurement.internal.reporting.v2.batchCreateReportResultAssemblyRequest
import org.wfanet.measurement.internal.reporting.v2.beginReportResultAssemblyRequest
import org.wfanet.measurement.internal.reporting.v2.createReportingSetResultRequest
import org.wfanet.measurement.internal.reporting.v2.discardReportResultRequest
import org.wfanet.measurement.internal.reporting.v2.failBasicReportRequest
import org.wfanet.measurement.internal.reporting.v2.listBasicReportsRequest
import org.wfanet.measurement.internal.reporting.v2.listReportComputationsRequest
import org.wfanet.measurement.internal.reporting.v2.reportResult
import org.wfanet.measurement.internal.reporting.v2.reportingSetResult
import org.wfanet.measurement.internal.reporting.v2.streamReportingSetsRequest
import org.wfanet.measurement.reporting.service.api.v2alpha.BasicReportKey
import org.wfanet.measurement.reporting.service.internal.Normalization

/**
 * Writes the results of multimeasurement [BasicReport]s as unprocessed `ReportingSetResult`s.
 *
 * For every MeasurementConsumer, each [BasicReport] in state
 * [BasicReport.State.MEASUREMENTS_CREATED] whose CMMS `Measurement`s have all succeeded has their
 * cells written in the shape [BasicReportsReportsJob] writes, which moves it to state
 * [BasicReport.State.UNPROCESSED_RESULTS_READY]. One whose `Measurement` failed or was cancelled is
 * failed. One with a `Measurement` still running is left for a later run.
 *
 * @param externalImpressionQualificationFilterIds map of internal impression qualification filter
 *   ID to its external ID
 */
class BasicReportsMultimeasurementResultsJob(
  private val measurementConsumerConfigs: MeasurementConsumerConfigs,
  private val internalBasicReportsStub: InternalBasicReportsCoroutineStub,
  private val internalReportingSetsStub: InternalReportingSetsCoroutineStub,
  private val reportResultsStub: ReportResultsCoroutineStub,
  private val cmmsMeasurementsStub: CmmsMeasurementsCoroutineStub,
  private val externalImpressionQualificationFilterIds: Map<Long, String>,
) {
  suspend fun execute() {
    for ((measurementConsumerName, measurementConsumerConfig) in
      measurementConsumerConfigs.configsMap) {
      if (measurementConsumerConfig.apiKey.isEmpty()) {
        continue
      }
      val cmmsMeasurementConsumerId: String =
        requireNotNull(MeasurementConsumerKey.fromName(measurementConsumerName))
          .measurementConsumerId

      val resourceLists =
        internalBasicReportsStub.listResources { pageToken: ListBasicReportsPageToken? ->
          val response =
            try {
              internalBasicReportsStub.listBasicReports(
                listBasicReportsRequest {
                  pageSize = BATCH_SIZE
                  if (pageToken != null) {
                    this.pageToken = pageToken
                  }
                  filter =
                    ListBasicReportsRequestKt.filter {
                      this.cmmsMeasurementConsumerId = cmmsMeasurementConsumerId
                      state = BasicReport.State.MEASUREMENTS_CREATED
                    }
                }
              )
            } catch (e: StatusException) {
              throw Exception("Unable to list BasicReports", e)
            }
          val nextPageToken = if (response.hasNextPageToken()) response.nextPageToken else null
          ResourceList(response.basicReportsList, nextPageToken)
        }

      resourceLists.collect { resourceList ->
        for (basicReport in resourceList.resources) {
          try {
            processBasicReport(basicReport, measurementConsumerConfig.apiKey)
          } catch (e: CancellationException) {
            throw e
          } catch (e: Exception) {
            val basicReportName: String =
              BasicReportKey(
                  basicReport.cmmsMeasurementConsumerId,
                  basicReport.externalBasicReportId,
                )
                .toName()
            logger.log(
              Level.WARNING,
              "Unable to write multimeasurement results for BasicReport $basicReportName",
              e,
            )
          }
        }
      }
    }
  }

  private suspend fun processBasicReport(basicReport: BasicReport, apiKey: String) {
    val reportComputations: List<ReportComputation> =
      try {
        internalBasicReportsStub
          .listReportComputations(
            listReportComputationsRequest {
              cmmsMeasurementConsumerId = basicReport.cmmsMeasurementConsumerId
              externalBasicReportId = basicReport.externalBasicReportId
            }
          )
          .reportComputationsList
      } catch (e: StatusException) {
        throw Exception("Unable to list ReportComputations", e)
      }
    check(reportComputations.isNotEmpty()) { "BasicReport has no ReportComputations" }

    val measurements: List<Measurement> =
      reportComputations.map { getMeasurement(basicReport.cmmsMeasurementConsumerId, it, apiKey) }
    var allSucceeded = true
    for (measurement in measurements) {
      @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA") // Protobuf enums cannot be null.
      when (measurement.state) {
        Measurement.State.SUCCEEDED -> {}
        Measurement.State.FAILED,
        Measurement.State.CANCELLED -> {
          logger.warning { "Measurement ${measurement.name} is ${measurement.state}" }
          failBasicReport(basicReport)
          return
        }
        Measurement.State.STATE_UNSPECIFIED,
        Measurement.State.AWAITING_REQUISITION_FULFILLMENT,
        Measurement.State.COMPUTING,
        Measurement.State.UNRECOGNIZED -> allSucceeded = false
      }
    }
    if (!allSucceeded) {
      return
    }

    val reportingSetResults: List<ReportingSetResult> =
      try {
        buildReportingSetResults(basicReport, reportComputations.zip(measurements))
      } catch (e: InvalidMultimeasurementResultException) {
        logger.log(Level.WARNING, "Multimeasurement results do not fit the BasicReport", e)
        failBasicReport(basicReport)
        return
      }
    writeReportingSetResults(basicReport, reportingSetResults)
  }

  private suspend fun getMeasurement(
    cmmsMeasurementConsumerId: String,
    reportComputation: ReportComputation,
    apiKey: String,
  ): Measurement {
    check(reportComputation.cmmsMeasurementId.isNotEmpty()) {
      "ReportComputation for impression qualification filter " +
        "${reportComputation.impressionQualificationFilterId} has no Measurement"
    }
    val name =
      MeasurementKey(cmmsMeasurementConsumerId, reportComputation.cmmsMeasurementId).toName()
    return try {
      cmmsMeasurementsStub
        .withAuthenticationKey(apiKey)
        .getMeasurement(getMeasurementRequest { this.name = name })
    } catch (e: StatusException) {
      throw Exception("Unable to get Measurement $name", e)
    }
  }

  /**
   * Builds the unprocessed `ReportingSetResult`s of [basicReport] from the cells of its
   * multimeasurements.
   *
   * @throws InvalidMultimeasurementResultException if a result or mapping does not fit
   *   [basicReport]
   */
  private suspend fun buildReportingSetResults(
    basicReport: BasicReport,
    computedMeasurements: List<Pair<ReportComputation, Measurement>>,
  ): List<ReportingSetResult> {
    val resultGroupSpecs: List<ResultGroupSpec> = basicReport.details.resultGroupSpecsList
    val reportingUnitResolver = ReportingUnitResolver(basicReport)
    val windowsByDimension = mutableMapOf<ReportingSetResult.Dimension, MutableMap<Date, Window>>()
    val populationSizeByDimension = mutableMapOf<ReportingSetResult.Dimension, Long>()

    for ((reportComputation, measurement) in computedMeasurements) {
      val externalImpressionQualificationFilterId: String? =
        getExternalImpressionQualificationFilterId(reportComputation)
      val queryResultsById: Map<String, Measurement.Result.MultimeasurementResult.QueryResult> =
        measurement.resultsList
          .flatMap { it.result.multimeasurementResult.queryResultsList }
          .associateBy { it.queryId }

      for (mapping in reportComputation.mappingsList) {
        val resultGroupSpec: ResultGroupSpec =
          resultGroupSpecs.getOrNull(mapping.resultGroupSpecIndex)
            ?: throw InvalidMultimeasurementResultException(
              "Query ${mapping.queryId} maps to missing ResultGroupSpec " +
                "${mapping.resultGroupSpecIndex}"
            )
        @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA") // Protobuf case fields cannot be null.
        val externalReportingSetId: String =
          when (mapping.slotCase) {
            ResultGroupQueryMapping.SlotCase.REPORTING_UNIT ->
              reportingUnitResolver.getExternalReportingSetId(resultGroupSpec)
            ResultGroupQueryMapping.SlotCase.COMPONENT -> mapping.component.component
            // MultimeasurementCompiler does not compile these.
            ResultGroupQueryMapping.SlotCase.COMPONENT_UNIQUE,
            ResultGroupQueryMapping.SlotCase.INTERSECTION ->
              throw InvalidMultimeasurementResultException(
                "Query ${mapping.queryId} has unsupported slot ${mapping.slotCase}"
              )
            ResultGroupQueryMapping.SlotCase.SLOT_NOT_SET ->
              throw InvalidMultimeasurementResultException("Query ${mapping.queryId} has no slot")
          }
        val queryResult: Measurement.Result.MultimeasurementResult.QueryResult =
          queryResultsById[mapping.queryId]
            ?: throw InvalidMultimeasurementResultException(
              "Measurement ${measurement.name} has no result for query ${mapping.queryId}"
            )

        for (cell in queryResult.cellsList) {
          if (cell.suppressed) {
            continue
          }
          val dimension: ReportingSetResult.Dimension =
            ReportingSetResultKt.dimension {
              this.externalReportingSetId = externalReportingSetId
              vennDiagramRegionType = ReportingSetResult.Dimension.VennDiagramRegionType.UNION
              if (externalImpressionQualificationFilterId != null) {
                this.externalImpressionQualificationFilterId =
                  externalImpressionQualificationFilterId
              } else {
                custom = true
              }
              metricFrequencySpec = resultGroupSpec.metricFrequency
              grouping = buildGrouping(queryResult, cell)
              eventFilters +=
                Normalization.normalizeEventFilters(resultGroupSpec.dimensionSpec.filtersList)
            }
          populationSizeByDimension.putIfAbsent(dimension, cell.populationSize)
          val window: Window =
            windowsByDimension
              .getOrPut(dimension) { mutableMapOf() }
              .getOrPut(mapping.windowEnd) { Window() }
          // ResultGroupSpecs that share a dimension produce the same cells, so the first is kept.
          if (mapping.cumulative) {
            if (window.cumulativeResults == null) {
              window.cumulativeResults = cell.toNoisyMetricSet()
            }
          } else {
            window.nonCumulativeStart = mapping.windowStart
            if (window.nonCumulativeResults == null) {
              window.nonCumulativeResults = cell.toNoisyMetricSet()
            }
          }
        }
      }
    }

    return windowsByDimension.map { (dimension, windowsByEnd) ->
      reportingSetResult {
        this.dimension = dimension
        populationSize = populationSizeByDimension.getValue(dimension)
        for ((end, window) in windowsByEnd) {
          reportingWindowResults +=
            ReportingSetResultKt.reportingWindowEntry {
              key =
                ReportingSetResultKt.reportingWindow {
                  val nonCumulativeStart: Date? = window.nonCumulativeStart
                  if (nonCumulativeStart != null) {
                    this.nonCumulativeStart = nonCumulativeStart
                  }
                  this.end = end
                }
              value =
                ReportingSetResultKt.reportingWindowResult {
                  unprocessedReportResultValues =
                    ReportingSetResultKt.ReportingWindowResultKt.noisyReportResultValues {
                      val cumulativeResults: NoisyMetricSet? = window.cumulativeResults
                      if (cumulativeResults != null) {
                        this.cumulativeResults = cumulativeResults
                      }
                      val nonCumulativeResults: NoisyMetricSet? = window.nonCumulativeResults
                      if (nonCumulativeResults != null) {
                        this.nonCumulativeResults = nonCumulativeResults
                      }
                    }
                }
            }
        }
      }
    }
  }

  private fun getExternalImpressionQualificationFilterId(
    reportComputation: ReportComputation
  ): String? {
    val impressionQualificationFilterId: Long = reportComputation.impressionQualificationFilterId
    if (impressionQualificationFilterId == CUSTOM_IMPRESSION_QUALIFICATION_FILTER_ID) {
      return null
    }
    return externalImpressionQualificationFilterIds[impressionQualificationFilterId]
      ?: throw InvalidMultimeasurementResultException(
        "Impression qualification filter $impressionQualificationFilterId not found"
      )
  }

  private suspend fun writeReportingSetResults(
    basicReport: BasicReport,
    reportingSetResults: List<ReportingSetResult>,
  ) {
    val reportResult =
      try {
        reportResultsStub.beginReportResultAssembly(
          beginReportResultAssemblyRequest {
            cmmsMeasurementConsumerId = basicReport.cmmsMeasurementConsumerId
            externalBasicReportId = basicReport.externalBasicReportId
            requestId = UUID.randomUUID().toString()
            reportResult = reportResult {
              reportStart = basicReport.details.reportingInterval.effectiveReportStart
            }
          }
        )
      } catch (e: StatusException) {
        if (e.status.code == Status.Code.ALREADY_EXISTS) {
          // Another worker is assembling the results.
          return
        }
        throw Exception("Unable to begin report-result assembly", e)
      }

    var resultAssociated = false
    try {
      val requests =
        reportingSetResults.map {
          createReportingSetResultRequest {
            cmmsMeasurementConsumerId = reportResult.cmmsMeasurementConsumerId
            externalReportResultId = reportResult.externalReportResultId
            reportingSetResult = it
          }
        }
      val batches = ReportingSetResultBatching.batch(requests)
      for ((index, batch) in batches.withIndex()) {
        try {
          reportResultsStub.batchCreateReportResultAssembly(
            batchCreateReportResultAssemblyRequest {
              cmmsMeasurementConsumerId = reportResult.cmmsMeasurementConsumerId
              externalReportResultId = reportResult.externalReportResultId
              this.requests += batch
              externalBasicReportId = basicReport.externalBasicReportId
              finalBatch = index == batches.lastIndex
            }
          )
        } catch (e: StatusException) {
          throw Exception("Unable to write report results", e)
        }
      }
      resultAssociated = true
    } finally {
      if (!resultAssociated) {
        try {
          reportResultsStub.discardReportResult(
            discardReportResultRequest {
              cmmsMeasurementConsumerId = reportResult.cmmsMeasurementConsumerId
              externalReportResultId = reportResult.externalReportResultId
            }
          )
        } catch (e: StatusException) {
          logger.log(Level.WARNING, "Unable to discard abandoned report results", e)
        }
      }
    }
  }

  private suspend fun failBasicReport(basicReport: BasicReport) {
    try {
      internalBasicReportsStub.failBasicReport(
        failBasicReportRequest {
          cmmsMeasurementConsumerId = basicReport.cmmsMeasurementConsumerId
          externalBasicReportId = basicReport.externalBasicReportId
        }
      )
    } catch (e: StatusException) {
      throw Exception("Unable to fail BasicReport", e)
    }
  }

  /**
   * Resolves the external `ReportingSet` ID a `ReportingUnit` as a whole is written under, the same
   * way `GetBasicReport` reads it back: its only component, or else the union composite
   * `ReportingSet` of its components in the Campaign Group.
   */
  private inner class ReportingUnitResolver(private val basicReport: BasicReport) {
    private var campaignGroupReportingSets: List<ReportingSet>? = null

    suspend fun getExternalReportingSetId(resultGroupSpec: ResultGroupSpec): String {
      val componentIds: List<String> = getComponentIds(resultGroupSpec.reportingUnit)
      if (componentIds.size == 1) {
        return componentIds.single()
      }
      val unionSetExpression: ReportingSet.SetExpression = buildUnionSetExpression(componentIds)
      return getCampaignGroupReportingSets()
        .lastOrNull { it.hasComposite() && it.composite == unionSetExpression }
        ?.externalReportingSetId
        ?: throw InvalidMultimeasurementResultException(
          "Campaign Group has no union ReportingSet of $componentIds"
        )
    }

    @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA") // Protobuf case fields cannot be null.
    private suspend fun getComponentIds(reportingUnit: ReportingUnit): List<String> {
      val componentIds: List<String> =
        when (reportingUnit.componentsCase) {
          ReportingUnit.ComponentsCase.REPORTING_SET_KEYS ->
            reportingUnit.reportingSetKeys.reportingSetKeysList.map { it.externalReportingSetId }
          ReportingUnit.ComponentsCase.DATA_PROVIDER_KEYS ->
            reportingUnit.dataProviderKeys.dataProviderKeysList.map {
              getPrimitiveReportingSetId(it.cmmsDataProviderId)
            }
          ReportingUnit.ComponentsCase.COMPONENTS_NOT_SET ->
            throw InvalidMultimeasurementResultException("ReportingUnit has no components")
        }
      if (componentIds.isEmpty()) {
        throw InvalidMultimeasurementResultException("ReportingUnit has no components")
      }
      return componentIds
    }

    /** Returns the ID of the Campaign Group's primitive `ReportingSet` for [cmmsDataProviderId]. */
    private suspend fun getPrimitiveReportingSetId(cmmsDataProviderId: String): String {
      val reportingSets: List<ReportingSet> = getCampaignGroupReportingSets()
      val campaignGroup: ReportingSet =
        reportingSets.singleOrNull {
          it.externalReportingSetId == basicReport.externalCampaignGroupId
        }
          ?: throw InvalidMultimeasurementResultException(
            "Campaign Group ${basicReport.externalCampaignGroupId} not found"
          )
      val eventGroupKeys: Set<ReportingSet.Primitive.EventGroupKey> =
        campaignGroup.primitive.eventGroupKeysList
          .filter { it.cmmsDataProviderId == cmmsDataProviderId }
          .toSet()
      return reportingSets
        .lastOrNull {
          it.hasPrimitive() && it.primitive.eventGroupKeysList.toSet() == eventGroupKeys
        }
        ?.externalReportingSetId
        ?: throw InvalidMultimeasurementResultException(
          "Campaign Group has no primitive ReportingSet for DataProvider $cmmsDataProviderId"
        )
    }

    private suspend fun getCampaignGroupReportingSets(): List<ReportingSet> {
      val cached: List<ReportingSet>? = campaignGroupReportingSets
      if (cached != null) {
        return cached
      }
      val reportingSets: List<ReportingSet> =
        try {
          internalReportingSetsStub
            .streamReportingSets(
              streamReportingSetsRequest {
                filter =
                  StreamReportingSetsRequestKt.filter {
                    cmmsMeasurementConsumerId = basicReport.cmmsMeasurementConsumerId
                    externalCampaignGroupId = basicReport.externalCampaignGroupId
                  }
              }
            )
            .toList()
        } catch (e: StatusException) {
          throw Exception("Unable to stream Campaign Group ReportingSets", e)
        }
      campaignGroupReportingSets = reportingSets
      return reportingSets
    }
  }

  /** Noisy values of one reporting window of a `ReportingSetResult`. */
  private class Window(
    var nonCumulativeStart: Date? = null,
    var cumulativeResults: NoisyMetricSet? = null,
    var nonCumulativeResults: NoisyMetricSet? = null,
  )

  private class InvalidMultimeasurementResultException(message: String) : Exception(message)

  companion object {
    private val logger: Logger =
      Logger.getLogger(BasicReportsMultimeasurementResultsJob::class.java.name)
    private const val BATCH_SIZE = 10

    /** Internal ID a `ReportComputation` records for a custom impression qualification filter. */
    private const val CUSTOM_IMPRESSION_QUALIFICATION_FILTER_ID = -1L

    /**
     * Builds the grouping of [cell], mapping each of the `grouping_paths` of [queryResult] to the
     * enum value the cell takes for it.
     */
    private fun buildGrouping(
      queryResult: Measurement.Result.MultimeasurementResult.QueryResult,
      cell: Measurement.Result.MultimeasurementResult.QueryResult.Cell,
    ): ReportingSetResult.Dimension.Grouping {
      if (cell.attributeValuesCount != queryResult.groupingPathsCount) {
        throw InvalidMultimeasurementResultException(
          "Cell of query ${queryResult.queryId} has ${cell.attributeValuesCount} attribute " +
            "values for ${queryResult.groupingPathsCount} grouping paths"
        )
      }
      return ReportingSetResultKt.DimensionKt.grouping {
        for ((path, attributeValue) in
          queryResult.groupingPathsList.zip(cell.attributeValuesList)) {
          if (attributeValue.selectorCase != PopulationAttributeValue.SelectorCase.ENUM_VALUE) {
            throw InvalidMultimeasurementResultException(
              "Cell of query ${queryResult.queryId} has a non-enum value for $path"
            )
          }
          valueByPath[path] =
            EventTemplateFieldKt.fieldValue { enumValue = attributeValue.enumValue }
        }
      }
    }

    private fun Measurement.Result.MultimeasurementResult.QueryResult.Cell.toNoisyMetricSet():
      NoisyMetricSet {
      val source = this
      return ReportingSetResultKt.ReportingWindowResultKt.NoisyReportResultValuesKt.noisyMetricSet {
        reach =
          ReportingSetResultKt.ReportingWindowResultKt.NoisyReportResultValuesKt.NoisyMetricSetKt
            .reachResult {
              value = source.reach
              if (source.hasReachVariance()) {
                univariateStatistics = buildUnivariateStatistics(source.reachVariance)
              }
            }
        if (source.frequencyBinsCount > 0) {
          frequencyHistogram =
            ReportingSetResultKt.ReportingWindowResultKt.NoisyReportResultValuesKt.NoisyMetricSetKt
              .histogramResult {
                for ((index, binValue) in source.frequencyBinsList.withIndex()) {
                  // Bin `k` holds the reach at frequency `k`, the last one at `k+`.
                  binResults[index + 1] =
                    ReportingSetResultKt.ReportingWindowResultKt.NoisyReportResultValuesKt
                      .NoisyMetricSetKt
                      .HistogramResultKt
                      .binResult {
                        value =
                          binValue.toInt().takeIf { it.toLong() == binValue }
                            ?: throw InvalidMultimeasurementResultException(
                              "Frequency bin value $binValue overflows a bin result"
                            )
                        if (index < source.frequencyBinVarianceCount) {
                          univariateStatistics =
                            buildUnivariateStatistics(source.frequencyBinVarianceList[index])
                        }
                      }
                }
              }
        }
        impressionCount =
          ReportingSetResultKt.ReportingWindowResultKt.NoisyReportResultValuesKt.NoisyMetricSetKt
            .impressionCountResult {
              value = source.impressions
              if (source.hasImpressionVariance()) {
                univariateStatistics = buildUnivariateStatistics(source.impressionVariance)
              }
            }
      }
    }

    private fun buildUnivariateStatistics(variance: Double): NoisyMetricSet.UnivariateStatistics {
      return ReportingSetResultKt.ReportingWindowResultKt.NoisyReportResultValuesKt.NoisyMetricSetKt
        .univariateStatistics { standardDeviation = sqrt(variance) }
    }

    /**
     * Builds the union [ReportingSet.SetExpression] of [externalReportingSetIds] in the form
     * `GetBasicReport` looks composite `ReportingSet`s up by.
     */
    private fun buildUnionSetExpression(
      externalReportingSetIds: List<String>
    ): ReportingSet.SetExpression {
      var setExpression =
        ReportingSetKt.setExpression {
          operation = ReportingSet.SetExpression.Operation.UNION
          lhs =
            ReportingSetKt.SetExpressionKt.operand {
              externalReportingSetId = externalReportingSetIds.first()
            }
        }
      for (externalReportingSetId in externalReportingSetIds.drop(1)) {
        val rhsExpression: ReportingSet.SetExpression = setExpression
        setExpression =
          ReportingSetKt.setExpression {
            operation = ReportingSet.SetExpression.Operation.UNION
            lhs =
              ReportingSetKt.SetExpressionKt.operand {
                this.externalReportingSetId = externalReportingSetId
              }
            rhs = ReportingSetKt.SetExpressionKt.operand { expression = rhsExpression }
          }
      }
      return setExpression
    }
  }
}
