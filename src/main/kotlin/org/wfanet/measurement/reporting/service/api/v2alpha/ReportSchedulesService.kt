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

package org.wfanet.measurement.reporting.service.api.v2alpha

import com.google.protobuf.Timestamp
import com.google.protobuf.util.Timestamps
import com.google.type.Date
import com.google.type.DateTime
import com.google.type.DayOfWeek
import com.google.type.date
import io.grpc.Status
import io.grpc.StatusException
import java.time.DateTimeException
import java.time.LocalDate
import java.time.zone.ZoneRulesException
import kotlin.math.min
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.toList
import org.projectnessie.cel.Env
import org.wfanet.measurement.api.v2alpha.DataProvider
import org.wfanet.measurement.api.v2alpha.DataProviderKey
import org.wfanet.measurement.api.v2alpha.DataProvidersGrpcKt.DataProvidersCoroutineStub
import org.wfanet.measurement.api.v2alpha.EventGroup
import org.wfanet.measurement.api.v2alpha.EventGroupKey
import org.wfanet.measurement.api.v2alpha.EventGroupsGrpcKt.EventGroupsCoroutineStub
import org.wfanet.measurement.api.v2alpha.MeasurementConsumerKey
import org.wfanet.measurement.api.v2alpha.getDataProviderRequest
import org.wfanet.measurement.api.v2alpha.getEventGroupRequest
import org.wfanet.measurement.common.base64UrlDecode
import org.wfanet.measurement.common.base64UrlEncode
import org.wfanet.measurement.common.grpc.failGrpc
import org.wfanet.measurement.common.grpc.grpcRequire
import org.wfanet.measurement.common.grpc.grpcRequireNotNull
import org.wfanet.measurement.internal.reporting.v2.BatchGetMetricCalculationSpecsResponse
import org.wfanet.measurement.internal.reporting.v2.BatchGetReportingSetsResponse
import org.wfanet.measurement.internal.reporting.v2.ListReportSchedulesRequest as InternalListReportSchedulesRequest
import org.wfanet.measurement.internal.reporting.v2.ListReportSchedulesRequestKt
import org.wfanet.measurement.internal.reporting.v2.MetricCalculationSpec as InternalMetricCalculationSpec
import org.wfanet.measurement.internal.reporting.v2.MetricCalculationSpecsGrpcKt.MetricCalculationSpecsCoroutineStub
import org.wfanet.measurement.internal.reporting.v2.Report as InternalReport
import org.wfanet.measurement.internal.reporting.v2.ReportKt
import org.wfanet.measurement.internal.reporting.v2.ReportSchedule as InternalReportSchedule
import org.wfanet.measurement.internal.reporting.v2.ReportScheduleKt as InternalReportScheduleKt
import org.wfanet.measurement.internal.reporting.v2.ReportSchedulesGrpcKt.ReportSchedulesCoroutineStub
import org.wfanet.measurement.internal.reporting.v2.ReportingSet as InternalReportingSet
import org.wfanet.measurement.internal.reporting.v2.ReportingSet
import org.wfanet.measurement.internal.reporting.v2.ReportingSetsGrpcKt.ReportingSetsCoroutineStub
import org.wfanet.measurement.internal.reporting.v2.batchGetMetricCalculationSpecsRequest
import org.wfanet.measurement.internal.reporting.v2.batchGetReportingSetsRequest
import org.wfanet.measurement.internal.reporting.v2.createReportScheduleRequest
import org.wfanet.measurement.internal.reporting.v2.getReportScheduleRequest
import org.wfanet.measurement.internal.reporting.v2.listReportSchedulesRequest
import org.wfanet.measurement.internal.reporting.v2.report as internalReport
import org.wfanet.measurement.internal.reporting.v2.reportSchedule as internalReportSchedule
import org.wfanet.measurement.internal.reporting.v2.stopReportScheduleRequest
import org.wfanet.measurement.reporting.service.api.submitBatchRequests
import org.wfanet.measurement.reporting.v2alpha.CreateReportScheduleRequest
import org.wfanet.measurement.reporting.v2alpha.GetReportScheduleRequest
import org.wfanet.measurement.reporting.v2alpha.ListReportSchedulesPageToken
import org.wfanet.measurement.reporting.v2alpha.ListReportSchedulesPageTokenKt
import org.wfanet.measurement.reporting.v2alpha.ListReportSchedulesRequest
import org.wfanet.measurement.reporting.v2alpha.ListReportSchedulesResponse
import org.wfanet.measurement.reporting.v2alpha.Report
import org.wfanet.measurement.reporting.v2alpha.ReportSchedule
import org.wfanet.measurement.reporting.v2alpha.ReportScheduleKt
import org.wfanet.measurement.reporting.v2alpha.ReportSchedulesGrpcKt.ReportSchedulesCoroutineImplBase
import org.wfanet.measurement.reporting.v2alpha.StopReportScheduleRequest
import org.wfanet.measurement.reporting.v2alpha.copy
import org.wfanet.measurement.reporting.v2alpha.listReportSchedulesPageToken
import org.wfanet.measurement.reporting.v2alpha.listReportSchedulesResponse
import org.wfanet.measurement.reporting.v2alpha.report
import org.wfanet.measurement.reporting.v2alpha.reportSchedule

class ReportSchedulesService(
  private val internalReportSchedulesStub: ReportSchedulesCoroutineStub,
  private val internalReportingSetsStub: ReportingSetsCoroutineStub,
  private val internalMetricCalculationSpecsStub: MetricCalculationSpecsCoroutineStub,
  private val dataProvidersStub: DataProvidersCoroutineStub,
  private val eventGroupsStub: EventGroupsCoroutineStub
) : ReportSchedulesCoroutineImplBase() {
  override suspend fun createReportSchedule(request: CreateReportScheduleRequest): ReportSchedule {
    val parentKey: MeasurementConsumerKey =
      grpcRequireNotNull(MeasurementConsumerKey.fromName(request.parent)) {
        "Parent is either unspecified or invalid."
      }

    when (val principal: ReportingPrincipal = principalFromCurrentContext) {
      is MeasurementConsumerPrincipal -> {
        if (parentKey != principal.resourceKey) {
          failGrpc(Status.PERMISSION_DENIED) {
            "Cannot create a ReportSchedule for another MeasurementConsumer."
          }
        }
      }
    }

    grpcRequire(request.hasReportSchedule()) { "report_schedule is not specified." }
    grpcRequire(request.reportScheduleId.matches(RESOURCE_ID_REGEX)) {
      "report_schedule_id is invalid."
    }
    if (request.requestId.isNotEmpty()) {
      grpcRequire(request.requestId.matches(REQUEST_ID_REGEX)) { "request_id is invalid." }
    }

    val internalReportScheduleForRequest = request.reportSchedule.toInternal(parentKey)

    val eventStartTimestamp = request.reportSchedule.eventStart.toTimestamp()

    val internalReportingSets: List<InternalReportingSet> =
      getInternalReportingSets(
        request.reportSchedule.reportTemplate,
        parentKey.measurementConsumerId,
        internalReportingSetsStub
      )
    checkDataAvailability(
      request.reportSchedule,
      eventStartTimestamp,
      internalReportingSets,
      dataProvidersStub,
      eventGroupsStub
    )

    val internalReportSchedule =
      try {
        internalReportSchedulesStub.createReportSchedule(
          createReportScheduleRequest {
            reportSchedule = internalReportScheduleForRequest
            externalReportScheduleId = request.reportScheduleId
            requestId = request.requestId
          }
        )
      } catch (e: StatusException) {
        throw when (e.status.code) {
            Status.Code.ALREADY_EXISTS ->
              Status.ALREADY_EXISTS.withDescription(
                "ReportSchedule with ID ${request.reportScheduleId} already exists under ${request.parent}"
              )
            Status.Code.FAILED_PRECONDITION ->
              Status.FAILED_PRECONDITION.withDescription(
                "Unable to create ReportSchedule. The MeasurementConsumer not found."
              )
            else -> Status.UNKNOWN.withDescription("Unable to create ReportSchedule.")
          }
          .withCause(e)
          .asRuntimeException()
      }

    return internalReportSchedule.toPublic()
  }

  override suspend fun stopReportSchedule(request: StopReportScheduleRequest): ReportSchedule {
    val reportScheduleKey =
      grpcRequireNotNull(ReportScheduleKey.fromName(request.name)) {
        "ReportSchedule name is either unspecified or invalid"
      }

    when (val principal: ReportingPrincipal = principalFromCurrentContext) {
      is MeasurementConsumerPrincipal -> {
        if (reportScheduleKey.parentKey != principal.resourceKey) {
          failGrpc(Status.PERMISSION_DENIED) {
            "Cannot stop ReportSchedule belonging to other MeasurementConsumers."
          }
        }
      }
    }

    val internalReportSchedule =
      try {
        internalReportSchedulesStub.stopReportSchedule(
          stopReportScheduleRequest {
            cmmsMeasurementConsumerId = reportScheduleKey.cmmsMeasurementConsumerId
            externalReportScheduleId = reportScheduleKey.reportScheduleId
          }
        )
      } catch (e: StatusException) {
        throw when (e.status.code) {
            Status.Code.DEADLINE_EXCEEDED -> Status.DEADLINE_EXCEEDED
            Status.Code.CANCELLED -> Status.CANCELLED
            Status.Code.NOT_FOUND -> Status.NOT_FOUND
            else -> Status.UNKNOWN
          }
          .withCause(e)
          .withDescription("Unable to stop ReportSchedule.")
          .asRuntimeException()
      }

    return internalReportSchedule.toPublic()
  }

  override suspend fun getReportSchedule(request: GetReportScheduleRequest): ReportSchedule {
    val reportScheduleKey =
      grpcRequireNotNull(ReportScheduleKey.fromName(request.name)) {
        "ReportSchedule name is either unspecified or invalid"
      }

    when (val principal: ReportingPrincipal = principalFromCurrentContext) {
      is MeasurementConsumerPrincipal -> {
        if (reportScheduleKey.parentKey != principal.resourceKey) {
          failGrpc(Status.PERMISSION_DENIED) {
            "Cannot get ReportSchedule belonging to other MeasurementConsumers."
          }
        }
      }
    }

    val internalReportSchedule =
      try {
        internalReportSchedulesStub.getReportSchedule(
          getReportScheduleRequest {
            cmmsMeasurementConsumerId = reportScheduleKey.cmmsMeasurementConsumerId
            externalReportScheduleId = reportScheduleKey.reportScheduleId
          }
        )
      } catch (e: StatusException) {
        throw when (e.status.code) {
            Status.Code.DEADLINE_EXCEEDED -> Status.DEADLINE_EXCEEDED
            Status.Code.CANCELLED -> Status.CANCELLED
            Status.Code.NOT_FOUND -> Status.NOT_FOUND
            else -> Status.UNKNOWN
          }
          .withCause(e)
          .withDescription("Unable to get ReportSchedule.")
          .asRuntimeException()
      }

    return internalReportSchedule.toPublic()
  }

  override suspend fun listReportSchedules(
    request: ListReportSchedulesRequest
  ): ListReportSchedulesResponse {
    val parentKey: MeasurementConsumerKey =
      grpcRequireNotNull(MeasurementConsumerKey.fromName(request.parent)) {
        "Parent is either unspecified or invalid."
      }
    val listReportSchedulesPageToken = request.toListReportSchedulesPageToken()

    when (val principal: ReportingPrincipal = principalFromCurrentContext) {
      is MeasurementConsumerPrincipal -> {
        if (parentKey.measurementConsumerId != principal.resourceKey.measurementConsumerId) {
          failGrpc(Status.PERMISSION_DENIED) {
            "Cannot list ReportSchedules belonging to other MeasurementConsumers."
          }
        }
      }
    }

    val internalListReportSchedulesRequest: InternalListReportSchedulesRequest =
      listReportSchedulesPageToken.toListReportSchedulesRequest()

    val results: List<InternalReportSchedule> =
      try {
        internalReportSchedulesStub
          .listReportSchedules(internalListReportSchedulesRequest)
          .reportSchedulesList
      } catch (e: StatusException) {
        throw when (e.status.code) {
            Status.Code.DEADLINE_EXCEEDED -> Status.DEADLINE_EXCEEDED
            Status.Code.CANCELLED -> Status.CANCELLED
            else -> Status.UNKNOWN
          }
          .withCause(e)
          .withDescription("Unable to list ReportSchedules.")
          .asRuntimeException()
      }

    if (results.isEmpty()) {
      return ListReportSchedulesResponse.getDefaultInstance()
    }

    val nextPageToken: ListReportSchedulesPageToken? =
      if (results.size > listReportSchedulesPageToken.pageSize) {
        val lastResult = results[results.lastIndex - 1]
        listReportSchedulesPageToken.copy {
          lastReportSchedule =
            ListReportSchedulesPageTokenKt.previousPageEnd {
              cmmsMeasurementConsumerId = lastResult.cmmsMeasurementConsumerId
              externalReportScheduleId = lastResult.externalReportScheduleId
            }
        }
      } else {
        null
      }

    val subResults: List<InternalReportSchedule> =
      results.subList(0, min(results.size, listReportSchedulesPageToken.pageSize))

    return listReportSchedulesResponse {
      reportSchedules +=
        filterReportSchedules(
          subResults.map { internalReportSchedule -> internalReportSchedule.toPublic() },
          request.filter
        )

      if (nextPageToken != null) {
        this.nextPageToken = nextPageToken.toByteString().base64UrlEncode()
      }
    }
  }

  /** Converts a public [ReportSchedule] to an internal [InternalReportSchedule]. */
  private suspend fun ReportSchedule.toInternal(
    measurementConsumerKey: MeasurementConsumerKey
  ): InternalReportSchedule {
    val source = this

    val eventStart = source.eventStart
    grpcRequire(
      eventStart.year > 0 &&
        eventStart.month > 0 &&
        eventStart.day > 0 &&
        (eventStart.hasUtcOffset() || eventStart.hasTimeZone())
    ) {
      "event_start missing either year, month, day, or time_offset."
    }

    if (source.hasEventEnd()) {
      val eventEnd = source.eventEnd
      grpcRequire(eventEnd.year > 0 && eventEnd.month > 0 && eventEnd.day > 0) {
        "event_end not a full date."
      }

      grpcRequire(
        date {
            year = eventStart.year
            month = eventStart.month
            day = eventStart.day
          }
          .isBefore(eventEnd)
      ) {
        "event_end must be after event_start."
      }
    }

    return internalReportSchedule {
      cmmsMeasurementConsumerId = measurementConsumerKey.measurementConsumerId
      details =
        InternalReportScheduleKt.details {
          displayName = source.displayName
          description = source.description
          reportTemplate = source.reportTemplate.toInternal(measurementConsumerKey)
          this.eventStart = eventStart
          eventEnd = source.eventEnd
          frequency = source.frequency.toInternal()
          reportWindow = source.reportWindow.toInternal(source.eventStart)
        }
      nextReportCreationTime =
        try {
          source.eventStart.toTimestamp()
        } catch (e: ZoneRulesException) {
          throw Status.INVALID_ARGUMENT.withDescription("event_start.time_zone.id is invalid")
            .asRuntimeException()
        } catch (e: DateTimeException) {
          throw Status.INVALID_ARGUMENT.withDescription(
              "event_start.utc_offset is not in valid range."
            )
            .asRuntimeException()
        }
    }
  }

  /**
   * Converts a public [Report] used as a template to an internal [InternalReport] used as a
   * template.
   */
  private suspend fun Report.toInternal(
    measurementConsumerKey: MeasurementConsumerKey
  ): InternalReport {
    val source = this

    grpcRequire(source.reportingMetricEntriesList.isNotEmpty()) {
      "reporting_metric_entries in report_template is empty."
    }

    val metricCalculationSpecIds = mutableListOf<String>()

    val reportingMetricEntries: Map<String, InternalReport.ReportingMetricCalculationSpec> =
      source.reportingMetricEntriesList.associate { reportingMetricEntry ->
        if (reportingMetricEntry.value.metricCalculationSpecsList.isEmpty()) {
          throw Status.INVALID_ARGUMENT.withDescription(
              "Entry in reporting_metric_entries has value with empty metric_calculation_specs"
            )
            .asRuntimeException()
        }

        val reportingSetKey =
          grpcRequireNotNull(ReportingSetKey.fromName(reportingMetricEntry.key)) {
            "ReportingSet name ${reportingMetricEntry.key} is invalid."
          }

        grpcRequire(
          reportingSetKey.cmmsMeasurementConsumerId == measurementConsumerKey.measurementConsumerId
        ) {
          "MeasurementConsumer in ReportingSet name ${reportingMetricEntry.key} does not match."
        }

        val reportingSetId = reportingSetKey.reportingSetId
        reportingSetId to
          ReportKt.reportingMetricCalculationSpec {
            metricCalculationSpecReportingMetrics +=
              reportingMetricEntry.value.metricCalculationSpecsList.map { metricCalculationSpecName
                ->
                val metricCalculationSpecKey =
                  grpcRequireNotNull(MetricCalculationSpecKey.fromName(metricCalculationSpecName)) {
                    "MetricCalculationSpec name $metricCalculationSpecName is invalid."
                  }

                grpcRequire(
                  metricCalculationSpecKey.cmmsMeasurementConsumerId ==
                    measurementConsumerKey.measurementConsumerId
                ) {
                  "MeasurementConsumer in MetricCalculationSpec name $metricCalculationSpecName does not match."
                }
                metricCalculationSpecIds.add(metricCalculationSpecKey.metricCalculationSpecId)
                ReportKt.metricCalculationSpecReportingMetrics {
                  externalMetricCalculationSpecId = metricCalculationSpecKey.metricCalculationSpecId
                }
              }
          }
      }

    val internalMetricCalculationSpecs: List<InternalMetricCalculationSpec> =
      getInternalMetricCalculationSpecs(metricCalculationSpecIds, measurementConsumerKey)
    for (internalMetricCalculationSpec in internalMetricCalculationSpecs) {
      if (internalMetricCalculationSpec.details.cumulative) {
        throw Status.INVALID_ARGUMENT.withDescription(
            "metric_calculation_spec cannot have cumulative set to true."
          )
          .asRuntimeException()
      }
    }

    return internalReport {
      this.reportingMetricEntries.putAll(reportingMetricEntries)
      details = ReportKt.details { tags.putAll(source.tagsMap) }
    }
  }

  private suspend fun getInternalMetricCalculationSpecs(
    externalMetricCalculationSpecIds: List<String>,
    measurementConsumerKey: MeasurementConsumerKey
  ): List<InternalMetricCalculationSpec> {
    val callRpc: suspend (List<String>) -> BatchGetMetricCalculationSpecsResponse = { items ->
      try {
        internalMetricCalculationSpecsStub.batchGetMetricCalculationSpecs(
          batchGetMetricCalculationSpecsRequest {
            cmmsMeasurementConsumerId = measurementConsumerKey.measurementConsumerId
            this.externalMetricCalculationSpecIds += items
          }
        )
      } catch (e: StatusException) {
        throw when (e.status.code) {
            Status.Code.DEADLINE_EXCEEDED -> Status.DEADLINE_EXCEEDED
            Status.Code.CANCELLED -> Status.CANCELLED
            Status.Code.NOT_FOUND -> Status.NOT_FOUND
            else -> Status.UNKNOWN
          }
          .withCause(e)
          .withDescription("Unable to get MetricCalculationSpecs.")
          .asRuntimeException()
      }
    }
    return submitBatchRequests(
        externalMetricCalculationSpecIds.distinct().asFlow(),
        BATCH_GET_METRIC_CALCULATION_SPECS_LIMIT,
        callRpc
      ) { response ->
        response.metricCalculationSpecsList
      }
      .toList()
  }

  companion object {
    private val ENV: Env = buildCelEnvironment(ReportSchedule.getDefaultInstance())
    private val RESOURCE_ID_REGEX = Regex("^[a-z]([a-z0-9-]{0,61}[a-z0-9])?$")
    private val REQUEST_ID_REGEX = Regex("^[a-zA-Z0-9]{1,36}$")

    private const val MIN_PAGE_SIZE = 1
    private const val DEFAULT_PAGE_SIZE = 10
    private const val MAX_PAGE_SIZE = 100

    private const val BATCH_GET_METRIC_CALCULATION_SPECS_LIMIT = 100
    private const val BATCH_GET_REPORTING_SETS_LIMIT = 1000

    private fun filterReportSchedules(
      reportSchedules: List<ReportSchedule>,
      filter: String
    ): List<ReportSchedule> {
      return try {
        filterList(ENV, reportSchedules, filter)
      } catch (e: IllegalArgumentException) {
        throw Status.INVALID_ARGUMENT.withDescription(e.message).asRuntimeException()
      }
    }

    /** Converts a public [ListReportSchedulesRequest] to a [ListReportSchedulesPageToken]. */
    private fun ListReportSchedulesRequest.toListReportSchedulesPageToken():
      ListReportSchedulesPageToken {
      grpcRequire(pageSize >= 0) { "Page size cannot be less than 0" }

      val source = this
      val parentKey: MeasurementConsumerKey =
        grpcRequireNotNull(MeasurementConsumerKey.fromName(parent)) {
          "Parent is either unspecified or invalid."
        }
      val cmmsMeasurementConsumerId = parentKey.measurementConsumerId

      return if (pageToken.isNotBlank()) {
        ListReportSchedulesPageToken.parseFrom(pageToken.base64UrlDecode()).copy {
          grpcRequire(this.cmmsMeasurementConsumerId == cmmsMeasurementConsumerId) {
            "Arguments must be kept the same when using a page token"
          }

          if (source.pageSize in MIN_PAGE_SIZE..MAX_PAGE_SIZE) {
            pageSize = source.pageSize
          }
        }
      } else {
        listReportSchedulesPageToken {
          pageSize =
            when {
              source.pageSize < MIN_PAGE_SIZE -> DEFAULT_PAGE_SIZE
              source.pageSize > MAX_PAGE_SIZE -> MAX_PAGE_SIZE
              else -> source.pageSize
            }
          this.cmmsMeasurementConsumerId = cmmsMeasurementConsumerId
        }
      }
    }

    /** Converts a [ListReportSchedulesPageToken] to an internal [ListReportSchedulesRequest]. */
    private fun ListReportSchedulesPageToken.toListReportSchedulesRequest():
      InternalListReportSchedulesRequest {
      val source = this
      return listReportSchedulesRequest {
        // get one more than the actual page size for deciding whether to set page token
        limit = pageSize + 1
        filter =
          ListReportSchedulesRequestKt.filter {
            cmmsMeasurementConsumerId = source.cmmsMeasurementConsumerId
            if (source.hasLastReportSchedule()) {
              externalReportScheduleIdAfter = source.lastReportSchedule.externalReportScheduleId
            }
          }
      }
    }

    /** Converts an internal [InternalReportSchedule.State] to a public [ReportSchedule.State]. */
    private fun InternalReportSchedule.State.toPublic(): ReportSchedule.State {
      return when (this) {
        InternalReportSchedule.State.ACTIVE -> ReportSchedule.State.ACTIVE
        InternalReportSchedule.State.STOPPED -> ReportSchedule.State.STOPPED
        InternalReportSchedule.State.STATE_UNSPECIFIED -> ReportSchedule.State.STATE_UNSPECIFIED
        InternalReportSchedule.State.UNRECOGNIZED ->
          // State is set by the system so if this is reached, something went wrong.
          throw Status.UNKNOWN.withDescription(
              "There is an unknown problem with the ReportSchedule"
            )
            .asRuntimeException()
      }
    }

    /**
     * Converts an internal [InternalReportSchedule.Frequency] to a public
     * [ReportSchedule.Frequency].
     */
    private fun InternalReportSchedule.Frequency.toPublic(): ReportSchedule.Frequency {
      val source = this
      @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA")
      return when (source.frequencyCase) {
        InternalReportSchedule.Frequency.FrequencyCase.DAILY ->
          ReportScheduleKt.frequency { daily = ReportSchedule.Frequency.Daily.getDefaultInstance() }
        InternalReportSchedule.Frequency.FrequencyCase.WEEKLY ->
          ReportScheduleKt.frequency {
            weekly = ReportScheduleKt.FrequencyKt.weekly { dayOfWeek = source.weekly.dayOfWeek }
          }
        InternalReportSchedule.Frequency.FrequencyCase.MONTHLY ->
          ReportScheduleKt.frequency {
            monthly =
              ReportScheduleKt.FrequencyKt.monthly { dayOfMonth = source.monthly.dayOfMonth }
          }
        InternalReportSchedule.Frequency.FrequencyCase.FREQUENCY_NOT_SET ->
          throw Status.FAILED_PRECONDITION.withDescription("ReportSchedule missing frequency")
            .asRuntimeException()
      }
    }

    /**
     * Converts an internal [InternalReportSchedule.ReportWindow] to a public
     * [ReportSchedule.ReportWindow].
     */
    private fun InternalReportSchedule.ReportWindow.toPublic(): ReportSchedule.ReportWindow {
      val source = this
      @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA")
      return when (source.windowCase) {
        InternalReportSchedule.ReportWindow.WindowCase.TRAILING_WINDOW ->
          ReportScheduleKt.reportWindow {
            trailingWindow =
              ReportScheduleKt.ReportWindowKt.trailingWindow {
                count = source.trailingWindow.count
                increment =
                  ReportSchedule.ReportWindow.TrailingWindow.Increment.forNumber(
                    source.trailingWindow.increment.number
                  )
                    ?: throw Status.UNKNOWN.withDescription(
                        "There is an unknown problem with the ReportSchedule"
                      )
                      .asRuntimeException()
              }
          }
        InternalReportSchedule.ReportWindow.WindowCase.FIXED_WINDOW ->
          ReportScheduleKt.reportWindow { fixedWindow = source.fixedWindow }
        InternalReportSchedule.ReportWindow.WindowCase.WINDOW_NOT_SET ->
          throw Status.FAILED_PRECONDITION.withDescription("ReportSchedule missing report_window")
            .asRuntimeException()
      }
    }

    /**
     * Converts a public [ReportSchedule.Frequency] to an internal
     * [InternalReportSchedule.Frequency].
     */
    private fun ReportSchedule.Frequency.toInternal(): InternalReportSchedule.Frequency {
      val source = this
      @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA") // Proto enum fields are never null.
      return InternalReportScheduleKt.frequency {
        when (source.frequencyCase) {
          ReportSchedule.Frequency.FrequencyCase.DAILY -> {
            daily = InternalReportSchedule.Frequency.Daily.getDefaultInstance()
          }
          ReportSchedule.Frequency.FrequencyCase.WEEKLY -> {
            grpcRequire(
              source.weekly.dayOfWeek != DayOfWeek.DAY_OF_WEEK_UNSPECIFIED &&
                source.weekly.dayOfWeek != DayOfWeek.UNRECOGNIZED
            ) {
              "day_of_week in weekly frequency is unspecified or invalid."
            }
            weekly =
              InternalReportScheduleKt.FrequencyKt.weekly { dayOfWeek = source.weekly.dayOfWeek }
          }
          ReportSchedule.Frequency.FrequencyCase.MONTHLY -> {
            grpcRequire(source.monthly.dayOfMonth > 0) {
              "day_of_month in monthly frequency is unspecified or invalid."
            }
            monthly =
              InternalReportScheduleKt.FrequencyKt.monthly {
                dayOfMonth = source.monthly.dayOfMonth
              }
          }
          ReportSchedule.Frequency.FrequencyCase.FREQUENCY_NOT_SET ->
            throw Status.INVALID_ARGUMENT.withDescription("frequency is not set")
              .asRuntimeException()
        }
      }
    }

    /**
     * Converts a public [ReportSchedule.ReportWindow] to an internal
     * [InternalReportSchedule.ReportWindow].
     */
    private fun ReportSchedule.ReportWindow.toInternal(
      eventStart: DateTime
    ): InternalReportSchedule.ReportWindow {
      val source = this
      @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA") // Proto enum fields are never null.
      return InternalReportScheduleKt.reportWindow {
        when (source.windowCase) {
          ReportSchedule.ReportWindow.WindowCase.TRAILING_WINDOW -> {
            grpcRequire(source.trailingWindow.count >= 1) {
              "count in trailing_window must be greater than 0."
            }
            trailingWindow =
              InternalReportScheduleKt.ReportWindowKt.trailingWindow {
                count = source.trailingWindow.count
                increment =
                  when (source.trailingWindow.increment) {
                    ReportSchedule.ReportWindow.TrailingWindow.Increment.DAY ->
                      InternalReportSchedule.ReportWindow.TrailingWindow.Increment.DAY
                    ReportSchedule.ReportWindow.TrailingWindow.Increment.WEEK ->
                      InternalReportSchedule.ReportWindow.TrailingWindow.Increment.WEEK
                    ReportSchedule.ReportWindow.TrailingWindow.Increment.MONTH ->
                      InternalReportSchedule.ReportWindow.TrailingWindow.Increment.MONTH
                    ReportSchedule.ReportWindow.TrailingWindow.Increment.UNRECOGNIZED,
                    ReportSchedule.ReportWindow.TrailingWindow.Increment.INCREMENT_UNSPECIFIED ->
                      throw Status.INVALID_ARGUMENT.withDescription(
                          "increment in trailing_window is not specified."
                        )
                        .asRuntimeException()
                  }
              }
          }
          ReportSchedule.ReportWindow.WindowCase.FIXED_WINDOW -> {
            grpcRequire(
              source.fixedWindow.year > 0 &&
                source.fixedWindow.month > 0 &&
                source.fixedWindow.day > 0
            ) {
              "fixed_window in report_window is not a full date."
            }
            grpcRequire(
              source.fixedWindow.isBefore(
                date {
                  year = eventStart.year
                  month = eventStart.month
                  day = eventStart.day
                }
              )
            ) {
              "fixed_window is not before event_start."
            }
            fixedWindow = source.fixedWindow
          }
          ReportSchedule.ReportWindow.WindowCase.WINDOW_NOT_SET ->
            failGrpc { "report_window is not set." }
        }
      }
    }

    /** Converts an internal [InternalReportSchedule] to a public [ReportSchedule]. */
    private fun InternalReportSchedule.toPublic(): ReportSchedule {
      val source = this

      val reportScheduleName =
        ReportScheduleKey(source.cmmsMeasurementConsumerId, source.externalReportScheduleId)
          .toName()
      val reportTemplate = report {
        reportingMetricEntries +=
          source.details.reportTemplate.reportingMetricEntriesMap.map { internalReportingMetricEntry
            ->
            internalReportingMetricEntry.toReportingMetricEntry(source.cmmsMeasurementConsumerId)
          }
        tags.putAll(source.details.reportTemplate.details.tagsMap)
      }

      return reportSchedule {
        name = reportScheduleName
        displayName = source.details.displayName
        description = source.details.description
        this.reportTemplate = reportTemplate
        eventStart = source.details.eventStart
        eventEnd = source.details.eventEnd
        frequency = source.details.frequency.toPublic()
        reportWindow = source.details.reportWindow.toPublic()
        state = source.state.toPublic()
        nextReportCreationTime = source.nextReportCreationTime
        createTime = source.createTime
        updateTime = source.updateTime
      }
    }

    private suspend fun getInternalReportingSets(
      reportTemplate: Report,
      cmmsMeasurementConsumerId: String,
      reportingSetsStub: ReportingSetsCoroutineStub
    ): List<InternalReportingSet> {
      val reportingSetIds: Set<String> =
        reportTemplate.reportingMetricEntriesList
          .map { reportingMetricEntry ->
            ReportingSetKey.fromName(reportingMetricEntry.key)!!.reportingSetId
          }
          .toSet()

      val callRpc: suspend (List<String>) -> BatchGetReportingSetsResponse = { items ->
        try {
          reportingSetsStub.batchGetReportingSets(
            batchGetReportingSetsRequest {
              this.cmmsMeasurementConsumerId = cmmsMeasurementConsumerId
              externalReportingSetIds += items
            }
          )
        } catch (e: StatusException) {
          throw when (e.status.code) {
              Status.Code.DEADLINE_EXCEEDED -> Status.DEADLINE_EXCEEDED
              Status.Code.CANCELLED -> Status.CANCELLED
              Status.Code.NOT_FOUND -> Status.NOT_FOUND
              else -> Status.UNKNOWN
            }
            .withCause(e)
            .withDescription("Unable to get ReportingSets.")
            .asRuntimeException()
        }
      }

      // Set of reporting set IDs that have been used to retrieve Reporting Sets
      val retrievedExternalReportingSetIdSet = mutableSetOf<String>()
      // Set of reporting set IDs that will be used to retrieve Reporting Sets
      val externalReportingSetIdSet = mutableSetOf<String>()
      val reportingSets = mutableListOf<InternalReportingSet>()

      // Each loop gets new reporting set IDs from the operands of the composite reporting sets
      // retrieved in the previous loop.
      externalReportingSetIdSet.addAll(reportingSetIds)
      while (externalReportingSetIdSet.isNotEmpty()) {
        retrievedExternalReportingSetIdSet.addAll(externalReportingSetIdSet)

        submitBatchRequests(
            externalReportingSetIdSet.asFlow(),
            BATCH_GET_REPORTING_SETS_LIMIT,
            callRpc
          ) { response ->
            externalReportingSetIdSet.clear()
            response.reportingSetsList
          }
          .collect {
            if (it.hasComposite()) {
              val lhsExternalReportingSetId = it.composite.lhs.externalReportingSetId
              if (lhsExternalReportingSetId.isNotEmpty()) {
                if (!retrievedExternalReportingSetIdSet.contains(lhsExternalReportingSetId)) {
                  externalReportingSetIdSet.add(lhsExternalReportingSetId)
                }
              }

              val rhsExternalReportingSetId = it.composite.rhs.externalReportingSetId
              if (rhsExternalReportingSetId.isNotEmpty()) {
                if (!retrievedExternalReportingSetIdSet.contains(rhsExternalReportingSetId)) {
                  externalReportingSetIdSet.add(rhsExternalReportingSetId)
                }
              }
            }
            reportingSets.add(it)
          }
      }

      return reportingSets
    }

    /**
     * Determines whether the first [Report] time is before any data availability interval. Under
     * the assumption that data availability intervals only move forward, this is a possible cause
     * for the rejection of the [ReportSchedule].
     */
    private suspend fun checkDataAvailability(
      reportSchedule: ReportSchedule,
      eventTimestamp: Timestamp,
      internalReportingSets: List<InternalReportingSet>,
      dataProvidersStub: DataProvidersCoroutineStub,
      eventGroupsStub: EventGroupsCoroutineStub
    ) {
      val windowStart: Timestamp = buildReportWindowStartTimestamp(reportSchedule, eventTimestamp)
      val eventGroupMap = mutableMapOf<ReportingSet.Primitive.EventGroupKey, EventGroup>()
      val dataProviderMap = mutableMapOf<String, DataProvider>()

      for (internalReportingSet in internalReportingSets) {
        if (internalReportingSet.hasPrimitive()) {
          for (eventGroupKey in internalReportingSet.primitive.eventGroupKeysList) {
            if (!eventGroupMap.containsKey(eventGroupKey)) {
              val eventGroupName =
                EventGroupKey(
                    dataProviderId = eventGroupKey.cmmsDataProviderId,
                    eventGroupId = eventGroupKey.cmmsEventGroupId
                  )
                  .toName()
              val eventGroup =
                try {
                  eventGroupsStub.getEventGroup(getEventGroupRequest { name = eventGroupName })
                } catch (e: StatusException) {
                  throw when (e.status.code) {
                      Status.Code.DEADLINE_EXCEEDED -> Status.DEADLINE_EXCEEDED
                      Status.Code.CANCELLED -> Status.CANCELLED
                      Status.Code.NOT_FOUND -> Status.NOT_FOUND
                      else -> Status.UNKNOWN
                    }
                    .withCause(e)
                    .withDescription("Unable to get EventGroup with name $eventGroupName.")
                    .asRuntimeException()
                }
              eventGroupMap[eventGroupKey] = eventGroup

              if (
                Timestamps.compare(windowStart, eventGroup.dataAvailabilityInterval.startTime) < 0
              ) {
                throw Status.FAILED_PRECONDITION.withDescription(
                    "ReportSchedule event_start is invalid due to the data_availability_interval of the EventGroup with name ${eventGroup.name}"
                  )
                  .asRuntimeException()
              }
            }

            if (!dataProviderMap.containsKey(eventGroupKey.cmmsDataProviderId)) {
              val dataProviderName = DataProviderKey(eventGroupKey.cmmsDataProviderId).toName()
              val dataProvider =
                try {
                  dataProvidersStub.getDataProvider(
                    getDataProviderRequest { name = dataProviderName }
                  )
                } catch (e: StatusException) {
                  throw when (e.status.code) {
                      Status.Code.DEADLINE_EXCEEDED -> Status.DEADLINE_EXCEEDED
                      Status.Code.CANCELLED -> Status.CANCELLED
                      Status.Code.NOT_FOUND -> Status.NOT_FOUND
                      else -> Status.UNKNOWN
                    }
                    .withCause(e)
                    .withDescription("Unable to get DataProvider with name $dataProviderName.")
                    .asRuntimeException()
                }
              dataProviderMap[eventGroupKey.cmmsDataProviderId] = dataProvider

              if (
                Timestamps.compare(windowStart, dataProvider.dataAvailabilityInterval.startTime) < 0
              ) {
                throw Status.FAILED_PRECONDITION.withDescription(
                    "ReportSchedule event_start is invalid due to the data_availability_interval of the DataProvider with name ${dataProvider.name}"
                  )
                  .asRuntimeException()
              }
            }
          }
        }
      }
    }

    private fun Date.isBefore(other: Date): Boolean {
      return LocalDate.of(this.year, this.month, this.day)
        .isBefore(LocalDate.of(other.year, other.month, other.day))
    }
  }
}
