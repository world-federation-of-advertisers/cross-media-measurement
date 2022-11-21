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

package org.wfanet.measurement.reporting.service.api.v1alpha

import com.google.protobuf.ByteString
import com.google.protobuf.util.Timestamps
import io.grpc.Status
import io.grpc.StatusException
import java.io.File
import java.security.SecureRandom
import java.time.Instant
import kotlin.math.min
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.launch
import org.wfanet.measurement.api.v2.alpha.ListReportsPageToken
import org.wfanet.measurement.api.v2.alpha.ListReportsPageTokenKt.previousPageEnd
import org.wfanet.measurement.api.v2.alpha.copy
import org.wfanet.measurement.api.v2.alpha.listReportsPageToken
import org.wfanet.measurement.api.v2alpha.CertificatesGrpcKt.CertificatesCoroutineStub
import org.wfanet.measurement.api.v2alpha.DataProvidersGrpcKt.DataProvidersCoroutineStub
import org.wfanet.measurement.api.v2alpha.Measurement
import org.wfanet.measurement.api.v2alpha.MeasurementConsumerKey
import org.wfanet.measurement.api.v2alpha.MeasurementConsumersGrpcKt.MeasurementConsumersCoroutineStub
import org.wfanet.measurement.api.v2alpha.MeasurementsGrpcKt.MeasurementsCoroutineStub
import org.wfanet.measurement.api.v2alpha.getCertificateRequest
import org.wfanet.measurement.api.v2alpha.getMeasurementConsumerRequest
import org.wfanet.measurement.api.withAuthenticationKey
import org.wfanet.measurement.common.base64UrlDecode
import org.wfanet.measurement.common.base64UrlEncode
import org.wfanet.measurement.common.crypto.readCertificate
import org.wfanet.measurement.common.crypto.readPrivateKey
import org.wfanet.measurement.common.grpc.failGrpc
import org.wfanet.measurement.common.grpc.grpcRequire
import org.wfanet.measurement.common.grpc.grpcRequireNotNull
import org.wfanet.measurement.common.identity.apiIdToExternalId
import org.wfanet.measurement.common.identity.externalIdToApiId
import org.wfanet.measurement.common.readByteString
import org.wfanet.measurement.internal.reporting.CreateReportRequest as InternalCreateReportRequest
import org.wfanet.measurement.internal.reporting.CreateReportRequest.MeasurementKey as InternalMeasurementKey
import org.wfanet.measurement.internal.reporting.CreateReportRequestKt.measurementKey as internalMeasurementKey
import org.wfanet.measurement.internal.reporting.Measurement as InternalMeasurement
import org.wfanet.measurement.internal.reporting.MeasurementsGrpcKt.MeasurementsCoroutineStub as InternalMeasurementsCoroutineStub
import org.wfanet.measurement.internal.reporting.Metric as InternalMetric
import org.wfanet.measurement.internal.reporting.Metric.Details as InternalMetricDetails
import org.wfanet.measurement.internal.reporting.Metric.Details.MetricTypeCase as InternalMetricTypeCase
import org.wfanet.measurement.internal.reporting.Metric.FrequencyHistogramParams as InternalFrequencyHistogramParams
import org.wfanet.measurement.internal.reporting.Metric.ImpressionCountParams as InternalImpressionCountParams
import org.wfanet.measurement.internal.reporting.Metric.MeasurementCalculation
import org.wfanet.measurement.internal.reporting.Metric.NamedSetOperation as InternalNamedSetOperation
import org.wfanet.measurement.internal.reporting.Metric.SetOperation as InternalSetOperation
import org.wfanet.measurement.internal.reporting.Metric.SetOperation.Operand as InternalOperand
import org.wfanet.measurement.internal.reporting.Metric.WatchDurationParams as InternalWatchDurationParams
import org.wfanet.measurement.internal.reporting.MetricKt.MeasurementCalculationKt.weightedMeasurement as internalWeightedMeasurement
import org.wfanet.measurement.internal.reporting.MetricKt.SetOperationKt.operand as internalOperand
import org.wfanet.measurement.internal.reporting.MetricKt.SetOperationKt.reportingSetKey
import org.wfanet.measurement.internal.reporting.MetricKt.details as internalMetricDetails
import org.wfanet.measurement.internal.reporting.MetricKt.frequencyHistogramParams as internalFrequencyHistogramParams
import org.wfanet.measurement.internal.reporting.MetricKt.impressionCountParams as internalImpressionCountParams
import org.wfanet.measurement.internal.reporting.MetricKt.measurementCalculation as internalMeasurementCalculation
import org.wfanet.measurement.internal.reporting.MetricKt.namedSetOperation as internalNamedSetOperation
import org.wfanet.measurement.internal.reporting.MetricKt.reachParams as internalReachParams
import org.wfanet.measurement.internal.reporting.MetricKt.setOperation as internalSetOperation
import org.wfanet.measurement.internal.reporting.MetricKt.watchDurationParams as internalWatchDurationParams
import org.wfanet.measurement.internal.reporting.PeriodicTimeInterval as InternalPeriodicTimeInterval
import org.wfanet.measurement.internal.reporting.Report as InternalReport
import org.wfanet.measurement.internal.reporting.ReportKt.details as internalReportDetails
import org.wfanet.measurement.internal.reporting.ReportingSetsGrpcKt.ReportingSetsCoroutineStub as InternalReportingSetsCoroutineStub
import org.wfanet.measurement.internal.reporting.ReportsGrpcKt.ReportsCoroutineStub as InternalReportsCoroutineStub
import org.wfanet.measurement.internal.reporting.StreamReportsRequest as StreamInternalReportsRequest
import org.wfanet.measurement.internal.reporting.StreamReportsRequestKt.filter
import org.wfanet.measurement.internal.reporting.TimeInterval as InternalTimeInterval
import org.wfanet.measurement.internal.reporting.TimeIntervals as InternalTimeIntervals
import org.wfanet.measurement.internal.reporting.createReportRequest as internalCreateReportRequest
import org.wfanet.measurement.internal.reporting.getReportByIdempotencyKeyRequest
import org.wfanet.measurement.internal.reporting.getReportRequest as getInternalReportRequest
import org.wfanet.measurement.internal.reporting.getReportingSetRequest
import org.wfanet.measurement.internal.reporting.metric as internalMetric
import org.wfanet.measurement.internal.reporting.periodicTimeInterval as internalPeriodicTimeInterval
import org.wfanet.measurement.internal.reporting.report as internalReport
import org.wfanet.measurement.internal.reporting.streamReportsRequest as streamInternalReportsRequest
import org.wfanet.measurement.internal.reporting.timeInterval as internalTimeInterval
import org.wfanet.measurement.internal.reporting.timeIntervals as internalTimeIntervals
import org.wfanet.measurement.reporting.v1alpha.CreateReportRequest
import org.wfanet.measurement.reporting.v1alpha.GetReportRequest
import org.wfanet.measurement.reporting.v1alpha.ListReportsRequest
import org.wfanet.measurement.reporting.v1alpha.ListReportsResponse
import org.wfanet.measurement.reporting.v1alpha.Metric
import org.wfanet.measurement.reporting.v1alpha.Metric.FrequencyHistogramParams
import org.wfanet.measurement.reporting.v1alpha.Metric.ImpressionCountParams
import org.wfanet.measurement.reporting.v1alpha.Metric.MetricTypeCase
import org.wfanet.measurement.reporting.v1alpha.Metric.NamedSetOperation
import org.wfanet.measurement.reporting.v1alpha.Metric.SetOperation
import org.wfanet.measurement.reporting.v1alpha.Metric.SetOperation.Operand
import org.wfanet.measurement.reporting.v1alpha.Metric.WatchDurationParams
import org.wfanet.measurement.reporting.v1alpha.MetricKt.SetOperationKt.operand
import org.wfanet.measurement.reporting.v1alpha.MetricKt.frequencyHistogramParams
import org.wfanet.measurement.reporting.v1alpha.MetricKt.impressionCountParams
import org.wfanet.measurement.reporting.v1alpha.MetricKt.namedSetOperation
import org.wfanet.measurement.reporting.v1alpha.MetricKt.reachParams
import org.wfanet.measurement.reporting.v1alpha.MetricKt.setOperation
import org.wfanet.measurement.reporting.v1alpha.MetricKt.watchDurationParams
import org.wfanet.measurement.reporting.v1alpha.PeriodicTimeInterval
import org.wfanet.measurement.reporting.v1alpha.Report
import org.wfanet.measurement.reporting.v1alpha.Report.Result
import org.wfanet.measurement.reporting.v1alpha.ReportKt.EventGroupUniverseKt.eventGroupEntry as eventGroupUniverseEntry
import org.wfanet.measurement.reporting.v1alpha.ReportKt.ResultKt.HistogramTableKt.row
import org.wfanet.measurement.reporting.v1alpha.ReportKt.ResultKt.column
import org.wfanet.measurement.reporting.v1alpha.ReportKt.ResultKt.histogramTable
import org.wfanet.measurement.reporting.v1alpha.ReportKt.ResultKt.scalarTable
import org.wfanet.measurement.reporting.v1alpha.ReportKt.eventGroupUniverse
import org.wfanet.measurement.reporting.v1alpha.ReportKt.result
import org.wfanet.measurement.reporting.v1alpha.ReportsGrpcKt.ReportsCoroutineImplBase
import org.wfanet.measurement.reporting.v1alpha.TimeInterval
import org.wfanet.measurement.reporting.v1alpha.TimeIntervals
import org.wfanet.measurement.reporting.v1alpha.copy
import org.wfanet.measurement.reporting.v1alpha.listReportsResponse
import org.wfanet.measurement.reporting.v1alpha.metric
import org.wfanet.measurement.reporting.v1alpha.periodicTimeInterval
import org.wfanet.measurement.reporting.v1alpha.report
import org.wfanet.measurement.reporting.v1alpha.timeInterval
import org.wfanet.measurement.reporting.v1alpha.timeIntervals

private const val MIN_PAGE_SIZE = 1
private const val DEFAULT_PAGE_SIZE = 50
private const val MAX_PAGE_SIZE = 1000

class ReportsService(
  private val internalReportsStub: InternalReportsCoroutineStub,
  private val internalReportingSetsStub: InternalReportingSetsCoroutineStub,
  private val internalMeasurementsStub: InternalMeasurementsCoroutineStub,
  private val dataProvidersStub: DataProvidersCoroutineStub,
  private val measurementConsumersStub: MeasurementConsumersCoroutineStub,
  private val measurementsStub: MeasurementsCoroutineStub,
  private val certificateStub: CertificatesCoroutineStub,
  private val encryptionKeyPairStore: EncryptionKeyPairStore,
  private val secureRandom: SecureRandom,
  private val signingPrivateKeyDir: File,
) : ReportsCoroutineImplBase() {
  private val setOperationCompiler = SetOperationCompiler()

  override suspend fun createReport(request: CreateReportRequest): Report {
    grpcRequireNotNull(MeasurementConsumerKey.fromName(request.parent)) {
      "Parent is either unspecified or invalid."
    }
    val principal: ReportingPrincipal = principalFromCurrentContext

    when (principal) {
      is MeasurementConsumerPrincipal -> {
        if (request.parent != principal.resourceKey.toName()) {
          failGrpc(Status.PERMISSION_DENIED) {
            "Cannot create a Report for another MeasurementConsumer."
          }
        }
      }
    }

    val resourceKey = principal.resourceKey
    val apiAuthenticationKey: String = principal.config.apiKey

    grpcRequire(request.hasReport()) { "Report is not specified." }

    // TODO(@riemanli) Put the check here as the reportIdempotencyKey will be moved to the request
    //  level in the future.
    grpcRequire(request.report.reportIdempotencyKey.isNotEmpty()) {
      "ReportIdempotencyKey is not specified."
    }
    grpcRequire(request.report.measurementConsumer == request.parent) {
      "Cannot create a Report for another MeasurementConsumer."
    }

    val existingInternalReport: InternalReport? =
      getInternalReport(resourceKey.measurementConsumerId, request.report.reportIdempotencyKey)

    if (existingInternalReport != null) return existingInternalReport.toReport()

    val measurementInfo: MeasurementInfo =
      buildReportInfo(request, resourceKey.measurementConsumerId)

    val namedSetOperationResults: Map<String, SetOperationResult> =
      compileAllSetOperations(
        request,
        measurementInfo,
      )

    val measurementConsumer =
      try {
        measurementConsumersStub
          .withAuthenticationKey(apiAuthenticationKey)
          .getMeasurementConsumer(
            getMeasurementConsumerRequest {
              name = MeasurementConsumerKey(resourceKey.measurementConsumerId).toName()
            }
          )
      } catch (e: StatusException) {
        throw Exception(
          "Unable to retrieve the measurement consumer " +
            "[${MeasurementConsumerKey(resourceKey.measurementConsumerId).toName()}].",
          e
        )
      }

    // TODO: Factor this out to a separate class similar to EncryptionKeyPairStore.
    val signingPrivateKeyDer: ByteString =
      signingPrivateKeyDir.resolve(principal.config.signingPrivateKeyPath).readByteString()

    val signingCertificateDer: ByteString =
      getSigningCertificateDer(apiAuthenticationKey, principal.config.signingCertificateName)

    val signingConfig =
      SigningConfig(
        principal.config.signingCertificateName,
        signingCertificateDer,
        readPrivateKey(
          signingPrivateKeyDer,
          readCertificate(signingCertificateDer).publicKey.algorithm
        )
      )

    val measurementAgent =
      MeasurementAgent(
        internalReportingSetsStub,
        internalMeasurementsStub,
        dataProvidersStub,
        measurementsStub,
        certificateStub,
        measurementConsumer,
        apiAuthenticationKey,
        secureRandom,
      )

    measurementAgent.createMeasurements(
      namedSetOperationResults,
      measurementInfo,
      signingConfig,
    )

    val internalCreateReportRequest: InternalCreateReportRequest =
      buildInternalCreateReportRequest(
        request,
        measurementInfo,
        namedSetOperationResults,
      )
    try {
      return internalReportsStub.createReport(internalCreateReportRequest).toReport()
    } catch (e: StatusException) {
      throw Exception("Unable to create a report in the reporting database.", e)
    }
  }

  /** Gets a signing certificate x509Der in ByteString. */
  private suspend fun getSigningCertificateDer(
    apiAuthenticationKey: String,
    signingCertificateName: String
  ): ByteString {
    // TODO: Replace this with caching certificates or having them stored alongside the private key.
    return try {
      certificateStub
        .withAuthenticationKey(apiAuthenticationKey)
        .getCertificate(getCertificateRequest { name = signingCertificateName })
        .x509Der
    } catch (e: StatusException) {
      throw Exception(
        "Unable to retrieve the signing certificate for the measurement consumer " +
          "[$signingCertificateName].",
        e
      )
    }
  }

  /** Builds a [MeasurementInfo] from a [CreateReportRequest]. */
  private fun buildReportInfo(
    request: CreateReportRequest,
    measurementConsumerReferenceId: String
  ): MeasurementInfo {
    grpcRequire(request.report.hasEventGroupUniverse()) { "EventGroupUniverse is not specified." }
    grpcRequire(request.report.metricsList.isNotEmpty()) { "Metrics in Report cannot be empty." }
    checkSetOperationNamesUniqueness(request.report.metricsList)

    val eventGroupFilters =
      request.report.eventGroupUniverse.eventGroupEntriesList.associate { it.key to it.value }

    return MeasurementInfo(
      measurementConsumerReferenceId,
      request.report.reportIdempotencyKey,
      eventGroupFilters,
    )
  }

  /** Compiles all [SetOperation]s and outputs each result with measurement reference ID. */
  private suspend fun compileAllSetOperations(
    request: CreateReportRequest,
    measurementInfo: MeasurementInfo,
  ): Map<String, SetOperationResult> {
    val namedSetOperationResults = mutableMapOf<String, SetOperationResult>()

    val timeIntervalsList = request.report.timeIntervalsList()
    val cumulativeTimeIntervalsList =
      timeIntervalsList.map { timeInterval ->
        timeInterval.copy { this.startTime = timeIntervalsList.first().startTime }
      }

    coroutineScope {
      for (metric in request.report.metricsList) {
        val metricTimeIntervalsList =
          if (metric.cumulative) cumulativeTimeIntervalsList else timeIntervalsList
        val internalMetricDetails: InternalMetricDetails = buildInternalMetricDetails(metric)

        for (namedSetOperation in metric.setOperationsList) {
          launch {
            checkSetOperationReportingSetCoverage(namedSetOperation.setOperation, measurementInfo)
          }

          val setOperationId =
            buildSetOperationId(
              measurementInfo.idempotencyKey,
              internalMetricDetails,
              namedSetOperation.uniqueName
            )

          launch {
            val weightedMeasurementInfoList =
              compileSetOperation(
                namedSetOperation.setOperation,
                setOperationId,
                metricTimeIntervalsList,
              )
            namedSetOperationResults[setOperationId] =
              SetOperationResult(weightedMeasurementInfoList, internalMetricDetails)
          }
        }
      }
    }

    return namedSetOperationResults.toMap()
  }

  /** Compiles a [SetOperation] and outputs each result with measurement reference ID. */
  private suspend fun compileSetOperation(
    setOperation: SetOperation,
    setOperationId: String,
    timeIntervalsList: List<TimeInterval>,
  ): List<WeightedMeasurementInfo> {
    val weightedMeasurementsList = setOperationCompiler.compileSetOperation(setOperation)

    return timeIntervalsList.flatMap { timeInterval ->
      weightedMeasurementsList.mapIndexed { index, weightedMeasurement ->
        val measurementReferenceId =
          buildMeasurementReferenceId(
            setOperationId,
            timeInterval,
            index,
          )

        WeightedMeasurementInfo(measurementReferenceId, weightedMeasurement, timeInterval)
      }
    }
  }

  /** Checks if all reporting sets under a [SetOperation] are covered by the event filters. */
  private suspend fun checkSetOperationReportingSetCoverage(
    setOperation: SetOperation,
    measurementInfo: MeasurementInfo
  ) {
    checkOperandReportingSetCoverage(setOperation.lhs, measurementInfo)
    checkOperandReportingSetCoverage(setOperation.rhs, measurementInfo)
  }

  /** Checks if all reporting sets under a [Operand] are covered by the event filters. */
  private suspend fun checkOperandReportingSetCoverage(
    operand: Operand,
    measurementInfo: MeasurementInfo
  ) {
    @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA") // Proto enum fields are never null.
    when (operand.operandCase) {
      Operand.OperandCase.OPERATION ->
        checkSetOperationReportingSetCoverage(operand.operation, measurementInfo)
      Operand.OperandCase.REPORTING_SET -> checkReportingSet(operand.reportingSet, measurementInfo)
      Operand.OperandCase.OPERAND_NOT_SET -> {}
    }
  }

  /** Builds an [InternalMetricDetails] from a [Metric]. */
  private fun buildInternalMetricDetails(metric: Metric): InternalMetricDetails {
    return internalMetricDetails {
      @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA") // Proto enum fields are never null.
      when (metric.metricTypeCase) {
        MetricTypeCase.REACH -> reach = internalReachParams {}
        MetricTypeCase.FREQUENCY_HISTOGRAM ->
          frequencyHistogram = metric.frequencyHistogram.toInternal()
        MetricTypeCase.IMPRESSION_COUNT -> impressionCount = metric.impressionCount.toInternal()
        MetricTypeCase.WATCH_DURATION -> watchDuration = metric.watchDuration.toInternal()
        MetricTypeCase.METRICTYPE_NOT_SET ->
          failGrpc(Status.INVALID_ARGUMENT) { "The metric type in Report is not specified." }
      }

      cumulative = metric.cumulative
    }
  }

  /** Gets an [InternalReport]. */
  private suspend fun getInternalReport(
    measurementConsumerReferenceId: String,
    reportIdempotencyKey: String,
  ): InternalReport? {
    return try {
      internalReportsStub.getReportByIdempotencyKey(
        getReportByIdempotencyKeyRequest {
          this.measurementConsumerReferenceId = measurementConsumerReferenceId
          this.reportIdempotencyKey = reportIdempotencyKey
        }
      )
    } catch (e: StatusException) {
      if (e.status.code != Status.Code.NOT_FOUND) {
        throw Exception(
          "Unable to retrieve a report from the reporting database using the provided " +
            "reportIdempotencyKey [$reportIdempotencyKey].",
          e
        )
      }
      null
    }
  }

  override suspend fun listReports(request: ListReportsRequest): ListReportsResponse {
    val listReportsPageToken = request.toListReportsPageToken()

    // Based on AIP-132#Errors
    val principal: ReportingPrincipal = principalFromCurrentContext
    when (principal) {
      is MeasurementConsumerPrincipal -> {
        if (request.parent != principal.resourceKey.toName()) {
          failGrpc(Status.PERMISSION_DENIED) {
            "Cannot list Reports belonging to other MeasurementConsumers."
          }
        }
      }
    }
    val principalName = principal.resourceKey.toName()

    val apiAuthenticationKey: String = principal.config.apiKey

    val streamInternalReportsRequest: StreamInternalReportsRequest =
      listReportsPageToken.toStreamReportsRequest()
    val results: List<InternalReport> =
      try {
        internalReportsStub.streamReports(streamInternalReportsRequest).toList()
      } catch (e: StatusException) {
        throw Exception("Unable to list reports from the reporting database.", e)
      }

    if (results.isEmpty()) {
      return ListReportsResponse.getDefaultInstance()
    }

    val nextPageToken: ListReportsPageToken? =
      if (results.size > listReportsPageToken.pageSize) {
        listReportsPageToken.copy {
          lastReport = previousPageEnd {
            measurementConsumerReferenceId =
              results[results.lastIndex - 1].measurementConsumerReferenceId
            externalReportId = results[results.lastIndex - 1].externalReportId
          }
        }
      } else null

    return listReportsResponse {
      reports +=
        results
          .subList(0, min(results.size, listReportsPageToken.pageSize))
          .map { syncReport(it, apiAuthenticationKey, principalName) }
          .map(InternalReport::toReport)

      if (nextPageToken != null) {
        this.nextPageToken = nextPageToken.toByteString().base64UrlEncode()
      }
    }
  }

  override suspend fun getReport(request: GetReportRequest): Report {
    val reportKey =
      grpcRequireNotNull(ReportKey.fromName(request.name)) {
        "Report name is either unspecified or invalid"
      }

    val principal: ReportingPrincipal = principalFromCurrentContext
    when (principal) {
      is MeasurementConsumerPrincipal -> {
        if (reportKey.measurementConsumerId != principal.resourceKey.measurementConsumerId) {
          failGrpc(Status.PERMISSION_DENIED) {
            "Cannot get Report belonging to other MeasurementConsumers."
          }
        }
      }
    }
    val principalName = principal.resourceKey.toName()

    val apiAuthenticationKey: String = principal.config.apiKey

    val internalReport =
      try {
        internalReportsStub.getReport(
          getInternalReportRequest {
            measurementConsumerReferenceId = reportKey.measurementConsumerId
            externalReportId = apiIdToExternalId(reportKey.reportId)
          }
        )
      } catch (e: StatusException) {
        throw Exception("Unable to get the report from the reporting database.", e)
      }

    val syncedInternalReport = syncReport(internalReport, apiAuthenticationKey, principalName)

    return syncedInternalReport.toReport()
  }

  /** Syncs the [InternalReport] and all [InternalMeasurement]s used by it. */
  private suspend fun syncReport(
    internalReport: InternalReport,
    apiAuthenticationKey: String,
    principalName: String,
  ): InternalReport {
    // Report with SUCCEEDED or FAILED state is already synced.
    if (
      internalReport.state == InternalReport.State.SUCCEEDED ||
        internalReport.state == InternalReport.State.FAILED
    ) {
      return internalReport
    } else if (
      internalReport.state == InternalReport.State.STATE_UNSPECIFIED ||
        internalReport.state == InternalReport.State.UNRECOGNIZED
    ) {
      error(
        "The measurements cannot be synced because the report state was not set correctly as it " +
          "should've been."
      )
    }

    val measurementConsumer =
      try {
        measurementConsumersStub
          .withAuthenticationKey(apiAuthenticationKey)
          .getMeasurementConsumer(
            getMeasurementConsumerRequest {
              name = MeasurementConsumerKey(internalReport.measurementConsumerReferenceId).toName()
            }
          )
      } catch (e: StatusException) {
        throw Exception(
          "Unable to retrieve the measurement consumer " +
            "[${MeasurementConsumerKey(internalReport.measurementConsumerReferenceId).toName()}].",
          e
        )
      }

    val measurementAgent =
      MeasurementAgent(
        internalReportingSetsStub,
        internalMeasurementsStub,
        dataProvidersStub,
        measurementsStub,
        certificateStub,
        measurementConsumer,
        apiAuthenticationKey,
        secureRandom,
      )

    // Syncs measurements
    measurementAgent.syncMeasurements(
      internalReport.measurementsMap,
      encryptionKeyPairStore,
      principalName
    )

    return try {
      internalReportsStub.getReport(
        getInternalReportRequest {
          measurementConsumerReferenceId = internalReport.measurementConsumerReferenceId
          externalReportId = internalReport.externalReportId
        }
      )
    } catch (e: StatusException) {
      val reportName =
        ReportKey(
            internalReport.measurementConsumerReferenceId,
            externalIdToApiId(internalReport.externalReportId)
          )
          .toName()
      throw Exception("Unable to get the report [$reportName] from the reporting database.", e)
    }
  }

  /** Builds an [InternalCreateReportRequest] from a public [CreateReportRequest]. */
  private suspend fun buildInternalCreateReportRequest(
    request: CreateReportRequest,
    measurementInfo: MeasurementInfo,
    namedSetOperationResults: Map<String, SetOperationResult>,
  ): InternalCreateReportRequest {
    val internalReport: InternalReport = internalReport {
      this.measurementConsumerReferenceId = measurementInfo.measurementConsumerReferenceId

      @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA") // Proto enum fields are never null.
      when (request.report.timeCase) {
        Report.TimeCase.TIME_INTERVALS -> {
          this.timeIntervals = request.report.timeIntervals.toInternal()
        }
        Report.TimeCase.PERIODIC_TIME_INTERVAL -> {
          this.periodicTimeInterval = request.report.periodicTimeInterval.toInternal()
        }
        Report.TimeCase.TIME_NOT_SET ->
          failGrpc(Status.INVALID_ARGUMENT) { "The time in Report is not specified." }
      }

      coroutineScope {
        for (metric in request.report.metricsList) {
          launch {
            this@internalReport.metrics +=
              buildInternalMetric(metric, measurementInfo, namedSetOperationResults)
          }
        }
      }

      details = internalReportDetails {
        this.eventGroupFilters.putAll(measurementInfo.eventGroupFilters)
      }

      this.reportIdempotencyKey = measurementInfo.idempotencyKey
    }

    return internalCreateReportRequest {
      report = internalReport
      measurements +=
        internalReport.metricsList.flatMap { internalMetric ->
          buildInternalMeasurementKeys(
            internalMetric,
            measurementInfo.measurementConsumerReferenceId
          )
        }
    }
  }

  /** Builds an [InternalMetric] from a public [Metric]. */
  private suspend fun buildInternalMetric(
    metric: Metric,
    measurementInfo: MeasurementInfo,
    namedSetOperationResults: Map<String, SetOperationResult>,
  ): InternalMetric {
    return internalMetric {
      details = buildInternalMetricDetails(metric)

      coroutineScope {
        metric.setOperationsList.map { setOperation ->
          val setOperationId =
            buildSetOperationId(
              measurementInfo.idempotencyKey,
              details,
              setOperation.uniqueName,
            )

          namedSetOperationResults[setOperationId]?.let { setOperationResult ->
            launch {
              val internalNamedSetOperation =
                buildInternalNamedSetOperation(
                  setOperation,
                  measurementInfo,
                  setOperationResult,
                )
              namedSetOperations += internalNamedSetOperation
            }
          }
        }
      }
    }
  }

  /** Builds an [InternalNamedSetOperation] from a public [NamedSetOperation]. */
  private suspend fun buildInternalNamedSetOperation(
    namedSetOperation: NamedSetOperation,
    measurementInfo: MeasurementInfo,
    setOperationResult: SetOperationResult,
  ): InternalNamedSetOperation {
    return internalNamedSetOperation {
      displayName = namedSetOperation.uniqueName
      setOperation =
        buildInternalSetOperation(
          namedSetOperation.setOperation,
          measurementInfo.measurementConsumerReferenceId
        )

      this.measurementCalculations +=
        buildMeasurementCalculationList(
          setOperationResult,
        )
    }
  }

  /** Builds an [InternalSetOperation] from a public [SetOperation]. */
  private suspend fun buildInternalSetOperation(
    setOperation: SetOperation,
    measurementConsumerReferenceId: String,
  ): InternalSetOperation {
    return internalSetOperation {
      this.type = setOperation.type.toInternal()
      this.lhs = buildInternalOperand(setOperation.lhs, measurementConsumerReferenceId)
      this.rhs = buildInternalOperand(setOperation.rhs, measurementConsumerReferenceId)
    }
  }

  /** Builds an [InternalOperand] from an [Operand]. */
  private suspend fun buildInternalOperand(
    operand: Operand,
    measurementConsumerReferenceId: String,
  ): InternalOperand {
    @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA") // Proto enum fields are never null.
    return when (operand.operandCase) {
      Operand.OperandCase.OPERATION ->
        internalOperand {
          operation = buildInternalSetOperation(operand.operation, measurementConsumerReferenceId)
        }
      Operand.OperandCase.REPORTING_SET -> {
        val reportingSetId =
          grpcRequireNotNull(ReportingSetKey.fromName(operand.reportingSet)) {
              "Invalid reporting set name ${operand.reportingSet}."
            }
            .reportingSetId

        internalOperand {
          this.reportingSetId = reportingSetKey {
            this.measurementConsumerReferenceId = measurementConsumerReferenceId
            externalReportingSetId = apiIdToExternalId(reportingSetId)
          }
        }
      }
      Operand.OperandCase.OPERAND_NOT_SET -> internalOperand {}
    }
  }

  /**
   * Builds a list of [MeasurementCalculation]s from a list of [WeightedMeasurement]s and a list of
   * [InternalTimeInterval]s.
   */
  private fun buildMeasurementCalculationList(
    setOperationResult: SetOperationResult,
  ): List<MeasurementCalculation> {
    return setOperationResult.weightedMeasurementInfoList.map { weightedMeasurementInfo ->
      internalMeasurementCalculation {
        this.timeInterval = weightedMeasurementInfo.timeInterval.toInternal()

        weightedMeasurements += internalWeightedMeasurement {
          this.measurementReferenceId = weightedMeasurementInfo.kingdomMeasurementId!!
          coefficient = weightedMeasurementInfo.weightedMeasurement.coefficient
        }
      }
    }
  }

  /**
   * Check if the event groups in the public [ReportingSet] are covered by the event group universe.
   */
  private suspend fun checkReportingSet(
    reportingSetName: String,
    measurementInfo: MeasurementInfo,
  ) {
    val reportingSetKey =
      grpcRequireNotNull(ReportingSetKey.fromName(reportingSetName)) {
        "Invalid reporting set name $reportingSetName."
      }

    grpcRequire(
      reportingSetKey.measurementConsumerId == measurementInfo.measurementConsumerReferenceId
    ) {
      "No access to the reporting set [$reportingSetName]."
    }

    val internalReportingSet =
      try {
        internalReportingSetsStub.getReportingSet(
          getReportingSetRequest {
            this.measurementConsumerReferenceId = measurementInfo.measurementConsumerReferenceId
            externalReportingSetId = apiIdToExternalId(reportingSetKey.reportingSetId)
          }
        )
      } catch (e: StatusException) {
        throw Exception(
          "Unable to retrieve the reporting set [$reportingSetName] from the reporting database.",
          e
        )
      }

    for (eventGroupKey in internalReportingSet.eventGroupKeysList) {
      val eventGroupName =
        EventGroupKey(
            eventGroupKey.measurementConsumerReferenceId,
            eventGroupKey.dataProviderReferenceId,
            eventGroupKey.eventGroupReferenceId
          )
          .toName()
      val internalReportingSetDisplayName = internalReportingSet.displayName
      grpcRequire(measurementInfo.eventGroupFilters.containsKey(eventGroupName)) {
        "The event group [$eventGroupName] in the reporting set " +
          "[$internalReportingSetDisplayName] is not included in the event group universe."
      }
    }
  }
}

/** Converts the time in [Report] to a list of [TimeInterval]. */
private fun Report.timeIntervalsList(): List<TimeInterval> {
  val source = this
  @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA") // Proto enum fields are never null.
  return when (source.timeCase) {
    Report.TimeCase.TIME_INTERVALS -> {
      source.timeIntervals.timeIntervalsList.map { it }
    }
    Report.TimeCase.PERIODIC_TIME_INTERVAL -> {
      source.periodicTimeInterval.toTimeIntervalsList()
    }
    Report.TimeCase.TIME_NOT_SET ->
      failGrpc(Status.INVALID_ARGUMENT) { "The time in Report is not specified." }
  }
}

/** Check if the names of the set operations within the same metric type are unique. */
private fun checkSetOperationNamesUniqueness(metricsList: List<Metric>) {
  val seenNames = mutableMapOf<MetricTypeCase, MutableSet<String>>().withDefault { mutableSetOf() }

  for (metric in metricsList) {
    for (setOperation in metric.setOperationsList) {
      grpcRequire(!seenNames.getValue(metric.metricTypeCase).contains(setOperation.uniqueName)) {
        "The names of the set operations within the same metric type should be unique."
      }
      seenNames.getOrPut(metric.metricTypeCase, ::mutableSetOf) += setOperation.uniqueName
    }
  }
}

/** Builds a list of [InternalMeasurementKey]s from an [InternalMetric]. */
private fun buildInternalMeasurementKeys(
  internalMetric: InternalMetric,
  measurementConsumerReferenceId: String
): List<InternalMeasurementKey> {
  return internalMetric.namedSetOperationsList
    .flatMap { namedSetOperation ->
      namedSetOperation.measurementCalculationsList.flatMap { measurementCalculation ->
        measurementCalculation.weightedMeasurementsList.map { it.measurementReferenceId }
      }
    }
    .map { measurementReferenceId ->
      internalMeasurementKey {
        this.measurementConsumerReferenceId = measurementConsumerReferenceId
        this.measurementReferenceId = measurementReferenceId
      }
    }
}

/** Builds a unique ID for a [SetOperation]. */
private fun buildSetOperationId(
  reportIdempotencyKey: String,
  internalMetricDetails: InternalMetricDetails,
  setOperationUniqueName: String,
): String {
  @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA") // Proto enum fields are never null.
  val metricType =
    when (internalMetricDetails.metricTypeCase) {
      InternalMetricTypeCase.REACH -> "Reach"
      InternalMetricTypeCase.FREQUENCY_HISTOGRAM -> "FrequencyHistogram"
      InternalMetricTypeCase.IMPRESSION_COUNT -> "ImpressionCount"
      InternalMetricTypeCase.WATCH_DURATION -> "WatchDuration"
      InternalMetricTypeCase.METRICTYPE_NOT_SET ->
        error("Unset metric type should've already raised error.")
    }

  return "$reportIdempotencyKey-$metricType-$setOperationUniqueName"
}

/** Builds a unique reference ID for a [Measurement]. */
private fun buildMeasurementReferenceId(
  setOperationId: String,
  timeInterval: TimeInterval,
  index: Int,
): String {
  val rowHeader = buildRowHeader(timeInterval)
  return "$setOperationId-$rowHeader-measurement-$index"
}

/** Converts a public [SetOperation.Type] to an [InternalSetOperation.Type]. */
private fun SetOperation.Type.toInternal(): InternalSetOperation.Type {
  val source = this

  @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA") // Proto enum fields are never null.
  return when (source) {
    SetOperation.Type.UNION -> InternalSetOperation.Type.UNION
    SetOperation.Type.INTERSECTION -> InternalSetOperation.Type.INTERSECTION
    SetOperation.Type.DIFFERENCE -> InternalSetOperation.Type.DIFFERENCE
    SetOperation.Type.TYPE_UNSPECIFIED -> error("Set operator type is not specified.")
    SetOperation.Type.UNRECOGNIZED -> error("Unrecognized Set operator type.")
  }
}

/** Converts a [WatchDurationParams] to an [InternalWatchDurationParams]. */
private fun WatchDurationParams.toInternal(): InternalWatchDurationParams {
  val source = this
  return internalWatchDurationParams {
    maximumFrequencyPerUser = source.maximumFrequencyPerUser
    maximumWatchDurationPerUser = source.maximumWatchDurationPerUser
  }
}

/** Converts a [ImpressionCountParams] to an [InternalImpressionCountParams]. */
private fun ImpressionCountParams.toInternal(): InternalImpressionCountParams {
  val source = this
  return internalImpressionCountParams { maximumFrequencyPerUser = source.maximumFrequencyPerUser }
}

/** Converts a [FrequencyHistogramParams] to an [InternalFrequencyHistogramParams]. */
private fun FrequencyHistogramParams.toInternal(): InternalFrequencyHistogramParams {
  val source = this
  return internalFrequencyHistogramParams {
    maximumFrequencyPerUser = source.maximumFrequencyPerUser
  }
}

/** Converts a public [PeriodicTimeInterval] to an [InternalPeriodicTimeInterval]. */
private fun PeriodicTimeInterval.toInternal(): InternalPeriodicTimeInterval {
  val source = this
  return internalPeriodicTimeInterval {
    startTime = source.startTime
    increment = source.increment
    intervalCount = source.intervalCount
  }
}

/** Converts a public [TimeInterval] to an [InternalTimeInterval]. */
private fun TimeInterval.toInternal(): InternalTimeInterval {
  val source = this
  return internalTimeInterval {
    startTime = source.startTime
    endTime = source.endTime
  }
}

/** Converts a public [TimeIntervals] to an [InternalTimeIntervals]. */
private fun TimeIntervals.toInternal(): InternalTimeIntervals {
  val source = this
  return internalTimeIntervals {
    for (timeInternal in source.timeIntervalsList) {
      this.timeIntervals += internalTimeInterval {
        startTime = timeInternal.startTime
        endTime = timeInternal.endTime
      }
    }
  }
}

/** Convert an [PeriodicTimeInterval] to a list of [TimeInterval]s. */
private fun PeriodicTimeInterval.toTimeIntervalsList(): List<TimeInterval> {
  val source = this
  var startTime = checkNotNull(source.startTime)
  return (0 until source.intervalCount).map {
    timeInterval {
      this.startTime = startTime
      this.endTime = Timestamps.add(startTime, source.increment)
      startTime = this.endTime
    }
  }
}

/** Builds a row header in String from an [TimeInterval]. */
private fun buildRowHeader(timeInterval: TimeInterval): String {
  val startTimeInstant =
    Instant.ofEpochSecond(timeInterval.startTime.seconds, timeInterval.startTime.nanos.toLong())
  val endTimeInstant =
    Instant.ofEpochSecond(timeInterval.endTime.seconds, timeInterval.endTime.nanos.toLong())
  return "$startTimeInstant-$endTimeInstant"
}

/** Converts an internal [InternalReport] to a public [Report]. */
private fun InternalReport.toReport(): Report {
  val source = this
  val reportResourceName =
    ReportKey(
        measurementConsumerId = source.measurementConsumerReferenceId,
        reportId = externalIdToApiId(source.externalReportId)
      )
      .toName()
  val measurementConsumerResourceName =
    MeasurementConsumerKey(source.measurementConsumerReferenceId).toName()
  val eventGroupEntries =
    source.details.eventGroupFiltersMap.map { (eventGroupResourceName, filterPredicate) ->
      eventGroupUniverseEntry {
        key = eventGroupResourceName
        value = filterPredicate
      }
    }

  return report {
    name = reportResourceName
    reportIdempotencyKey = source.reportIdempotencyKey
    measurementConsumer = measurementConsumerResourceName
    eventGroupUniverse = eventGroupUniverse { this.eventGroupEntries += eventGroupEntries }

    @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA") // Proto enum fields are never null.
    when (source.timeCase) {
      InternalReport.TimeCase.TIME_INTERVALS ->
        this.timeIntervals = source.timeIntervals.toTimeIntervals()
      InternalReport.TimeCase.PERIODIC_TIME_INTERVAL ->
        this.periodicTimeInterval = source.periodicTimeInterval.toPeriodicTimeInterval()
      InternalReport.TimeCase.TIME_NOT_SET ->
        error("The time in the internal report should've been set.")
    }

    for (metric in source.metricsList) {
      this.metrics += metric.toMetric()
    }

    this.state = source.state.toState()
    if (source.details.hasResult()) {
      this.result = source.details.result.toResult()
    }
  }
}

/** Converts an [InternalReport.State] to a public [Report.State]. */
private fun InternalReport.State.toState(): Report.State {
  @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA") // Proto enum fields are never null.
  return when (this) {
    InternalReport.State.RUNNING -> Report.State.RUNNING
    InternalReport.State.SUCCEEDED -> Report.State.SUCCEEDED
    InternalReport.State.FAILED -> Report.State.FAILED
    InternalReport.State.STATE_UNSPECIFIED -> error("Report state should've been set.")
    InternalReport.State.UNRECOGNIZED -> error("Unrecognized report state.")
  }
}

/** Converts an [InternalReport.Details.Result] to a public [Report.Result]. */
private fun InternalReport.Details.Result.toResult(): Result {
  val source = this
  return result {
    scalarTable = scalarTable {
      rowHeaders += source.scalarTable.rowHeadersList
      for (sourceColumn in source.scalarTable.columnsList) {
        columns += column {
          columnHeader = sourceColumn.columnHeader
          setOperations += sourceColumn.setOperationsList
        }
      }
    }
    for (sourceHistogram in source.histogramTablesList) {
      histogramTables += histogramTable {
        for (sourceRow in sourceHistogram.rowsList) {
          rows += row {
            rowHeader = sourceRow.rowHeader
            frequency = sourceRow.frequency
          }
        }
        for (sourceColumn in sourceHistogram.columnsList) {
          columns += column {
            columnHeader = sourceColumn.columnHeader
            setOperations += sourceColumn.setOperationsList
          }
        }
      }
    }
  }
}

/** Converts an internal [InternalMetric] to a public [Metric]. */
private fun InternalMetric.toMetric(): Metric {
  val source = this

  return metric {
    @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA") // Proto enum fields are never null.
    when (source.details.metricTypeCase) {
      InternalMetricTypeCase.REACH -> reach = reachParams {}
      InternalMetricTypeCase.FREQUENCY_HISTOGRAM ->
        frequencyHistogram = source.details.frequencyHistogram.toFrequencyHistogram()
      InternalMetricTypeCase.IMPRESSION_COUNT ->
        impressionCount = source.details.impressionCount.toImpressionCount()
      InternalMetricTypeCase.WATCH_DURATION ->
        watchDuration = source.details.watchDuration.toWatchDuration()
      InternalMetricTypeCase.METRICTYPE_NOT_SET ->
        error("The metric type in the internal report should've been set.")
    }

    cumulative = source.details.cumulative

    for (internalSetOperation in source.namedSetOperationsList) {
      setOperations += internalSetOperation.toNamedSetOperation()
    }
  }
}

/** Converts an internal [InternalNamedSetOperation] to a public [NamedSetOperation]. */
private fun InternalNamedSetOperation.toNamedSetOperation(): NamedSetOperation {
  val source = this

  return namedSetOperation {
    uniqueName = source.displayName
    setOperation = source.setOperation.toSetOperation()
  }
}

/** Converts an internal [InternalSetOperation] to a public [SetOperation]. */
private fun InternalSetOperation.toSetOperation(): SetOperation {
  val source = this

  return setOperation {
    this.type = source.type.toType()
    this.lhs = source.lhs.toOperand()
    this.rhs = source.rhs.toOperand()
  }
}

/** Converts an internal [InternalOperand] to a public [Operand]. */
private fun InternalOperand.toOperand(): Operand {
  val source = this

  @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA") // Proto enum fields are never null.
  return when (source.operandCase) {
    InternalOperand.OperandCase.OPERATION ->
      operand { operation = source.operation.toSetOperation() }
    InternalOperand.OperandCase.REPORTINGSETID ->
      operand {
        reportingSet =
          ReportingSetKey(
              source.reportingSetId.measurementConsumerReferenceId,
              externalIdToApiId(source.reportingSetId.externalReportingSetId)
            )
            .toName()
      }
    InternalOperand.OperandCase.OPERAND_NOT_SET -> operand {}
  }
}

/** Converts an internal [InternalSetOperation.Type] to a public [SetOperation.Type]. */
private fun InternalSetOperation.Type.toType(): SetOperation.Type {
  @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA") // Proto enum fields are never null.
  return when (this) {
    InternalSetOperation.Type.UNION -> SetOperation.Type.UNION
    InternalSetOperation.Type.INTERSECTION -> SetOperation.Type.INTERSECTION
    InternalSetOperation.Type.DIFFERENCE -> SetOperation.Type.DIFFERENCE
    InternalSetOperation.Type.TYPE_UNSPECIFIED -> error("Set operator type should've been set.")
    InternalSetOperation.Type.UNRECOGNIZED -> error("Unrecognized Set operator type.")
  }
}

/** Converts an internal [InternalWatchDurationParams] to a public [WatchDurationParams]. */
private fun InternalWatchDurationParams.toWatchDuration(): WatchDurationParams {
  val source = this
  return watchDurationParams {
    maximumFrequencyPerUser = source.maximumFrequencyPerUser
    maximumWatchDurationPerUser = source.maximumWatchDurationPerUser
  }
}

/** Converts an internal [InternalImpressionCountParams] to a public [ImpressionCountParams]. */
private fun InternalImpressionCountParams.toImpressionCount(): ImpressionCountParams {
  val source = this
  return impressionCountParams { maximumFrequencyPerUser = source.maximumFrequencyPerUser }
}

/**
 * Converts an internal [InternalFrequencyHistogramParams] to a public [FrequencyHistogramParams].
 */
private fun InternalFrequencyHistogramParams.toFrequencyHistogram(): FrequencyHistogramParams {
  val source = this
  return frequencyHistogramParams { maximumFrequencyPerUser = source.maximumFrequencyPerUser }
}

/** Converts an internal [InternalPeriodicTimeInterval] to a public [PeriodicTimeInterval]. */
private fun InternalPeriodicTimeInterval.toPeriodicTimeInterval(): PeriodicTimeInterval {
  val source = this
  return periodicTimeInterval {
    startTime = source.startTime
    increment = source.increment
    intervalCount = source.intervalCount
  }
}

/** Converts an internal [InternalTimeIntervals] to a public [TimeIntervals]. */
private fun InternalTimeIntervals.toTimeIntervals(): TimeIntervals {
  val source = this
  return timeIntervals {
    for (internalTimeInternal in source.timeIntervalsList) {
      this.timeIntervals += timeInterval {
        startTime = internalTimeInternal.startTime
        endTime = internalTimeInternal.endTime
      }
    }
  }
}

/** Converts an internal [ListReportsPageToken] to an internal [StreamInternalReportsRequest]. */
private fun ListReportsPageToken.toStreamReportsRequest(): StreamInternalReportsRequest {
  val source = this
  return streamInternalReportsRequest {
    // get 1 more than the actual page size for deciding whether or not to set page token
    limit = pageSize + 1
    filter = filter {
      measurementConsumerReferenceId = source.measurementConsumerReferenceId
      externalReportIdAfter = source.lastReport.externalReportId
    }
  }
}

/** Converts a public [ListReportsRequest] to an internal [ListReportsPageToken]. */
private fun ListReportsRequest.toListReportsPageToken(): ListReportsPageToken {
  grpcRequire(pageSize >= 0) { "Page size cannot be less than 0" }

  val source = this
  val parentKey: MeasurementConsumerKey =
    grpcRequireNotNull(MeasurementConsumerKey.fromName(parent)) {
      "Parent is either unspecified or invalid."
    }
  val measurementConsumerReferenceId = parentKey.measurementConsumerId

  val isValidPageSize =
    source.pageSize != 0 && source.pageSize >= MIN_PAGE_SIZE && source.pageSize <= MAX_PAGE_SIZE

  return if (pageToken.isNotBlank()) {
    ListReportsPageToken.parseFrom(pageToken.base64UrlDecode()).copy {
      grpcRequire(this.measurementConsumerReferenceId == measurementConsumerReferenceId) {
        "Arguments must be kept the same when using a page token"
      }

      if (isValidPageSize) {
        pageSize = source.pageSize
      }
    }
  } else {
    listReportsPageToken {
      pageSize =
        when {
          source.pageSize < MIN_PAGE_SIZE -> DEFAULT_PAGE_SIZE
          source.pageSize > MAX_PAGE_SIZE -> MAX_PAGE_SIZE
          else -> source.pageSize
        }
      this.measurementConsumerReferenceId = measurementConsumerReferenceId
    }
  }
}
