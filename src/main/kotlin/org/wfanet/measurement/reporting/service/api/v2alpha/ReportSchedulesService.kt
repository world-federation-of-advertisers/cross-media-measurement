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

import io.grpc.Status
import io.grpc.StatusException
import kotlin.math.min
import org.projectnessie.cel.Env
import org.wfanet.measurement.api.v2alpha.MeasurementConsumerKey
import org.wfanet.measurement.common.base64UrlDecode
import org.wfanet.measurement.common.base64UrlEncode
import org.wfanet.measurement.common.grpc.failGrpc
import org.wfanet.measurement.common.grpc.grpcRequire
import org.wfanet.measurement.common.grpc.grpcRequireNotNull
import org.wfanet.measurement.internal.reporting.v2.ListReportSchedulesRequest as InternalListReportSchedulesRequest
import org.wfanet.measurement.internal.reporting.v2.ListReportSchedulesRequestKt
import org.wfanet.measurement.internal.reporting.v2.ReportSchedule as InternalReportSchedule
import org.wfanet.measurement.internal.reporting.v2.ReportSchedulesGrpcKt.ReportSchedulesCoroutineStub
import org.wfanet.measurement.internal.reporting.v2.getReportScheduleRequest
import org.wfanet.measurement.internal.reporting.v2.listReportSchedulesRequest
import org.wfanet.measurement.internal.reporting.v2.stopReportScheduleRequest
import org.wfanet.measurement.reporting.v2alpha.CreateReportScheduleRequest
import org.wfanet.measurement.reporting.v2alpha.GetReportScheduleRequest
import org.wfanet.measurement.reporting.v2alpha.ListReportSchedulesPageToken
import org.wfanet.measurement.reporting.v2alpha.ListReportSchedulesPageTokenKt
import org.wfanet.measurement.reporting.v2alpha.ListReportSchedulesRequest
import org.wfanet.measurement.reporting.v2alpha.ListReportSchedulesResponse
import org.wfanet.measurement.reporting.v2alpha.ReportSchedule
import org.wfanet.measurement.reporting.v2alpha.ReportScheduleKt
import org.wfanet.measurement.reporting.v2alpha.ReportSchedulesGrpcKt
import org.wfanet.measurement.reporting.v2alpha.StopReportScheduleRequest
import org.wfanet.measurement.reporting.v2alpha.copy
import org.wfanet.measurement.reporting.v2alpha.listReportSchedulesPageToken
import org.wfanet.measurement.reporting.v2alpha.listReportSchedulesResponse
import org.wfanet.measurement.reporting.v2alpha.report
import org.wfanet.measurement.reporting.v2alpha.reportSchedule

private const val MIN_PAGE_SIZE = 1
private const val DEFAULT_PAGE_SIZE = 10
private const val MAX_PAGE_SIZE = 100

class ReportSchedulesService(
  private val internalReportSchedulesStub: ReportSchedulesCoroutineStub
) : ReportSchedulesGrpcKt.ReportSchedulesCoroutineImplBase() {
  override suspend fun createReportSchedule(request: CreateReportScheduleRequest): ReportSchedule {
    return super.createReportSchedule(request)
  }

  override suspend fun stopReportSchedule(request: StopReportScheduleRequest): ReportSchedule {
    val reportScheduleKey =
      grpcRequireNotNull(ReportScheduleKey.fromName(request.name)) {
        "ReportSchedule name is either unspecified or invalid"
      }

    when (val principal: ReportingPrincipal = principalFromCurrentContext) {
      is MeasurementConsumerPrincipal -> {
        if (
          reportScheduleKey.cmmsMeasurementConsumerId != principal.resourceKey.measurementConsumerId
        ) {
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
        if (
          reportScheduleKey.cmmsMeasurementConsumerId != principal.resourceKey.measurementConsumerId
        ) {
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

  companion object {
    private val ENV: Env = buildCelEnvironment(ReportSchedule.getDefaultInstance())
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
    InternalReportSchedule.State.STATE_UNSPECIFIED,
    InternalReportSchedule.State.UNRECOGNIZED -> ReportSchedule.State.STATE_UNSPECIFIED
  }
}

/**
 * Converts an internal [InternalReportSchedule.Frequency] to a public [ReportSchedule.Frequency].
 */
private fun InternalReportSchedule.Frequency.toPublic(): ReportSchedule.Frequency {
  val source = this
  @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA")
  return when (source.frequencyCase) {
    InternalReportSchedule.Frequency.FrequencyCase.DAILY ->
      ReportScheduleKt.frequency { daily = ReportScheduleKt.FrequencyKt.daily {} }
    InternalReportSchedule.Frequency.FrequencyCase.WEEKLY ->
      ReportScheduleKt.frequency {
        weekly = ReportScheduleKt.FrequencyKt.weekly { dayOfWeek = source.weekly.dayOfWeek }
      }
    InternalReportSchedule.Frequency.FrequencyCase.MONTHLY ->
      ReportScheduleKt.frequency {
        monthly = ReportScheduleKt.FrequencyKt.monthly { dayOfMonth = source.monthly.dayOfMonth }
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
              ReportSchedule.ReportWindow.TrailingWindow.Increment.valueOf(
                source.trailingWindow.increment.name
              )
          }
      }
    InternalReportSchedule.ReportWindow.WindowCase.FIXED_WINDOW ->
      ReportScheduleKt.reportWindow { fixedWindow = source.fixedWindow }
    InternalReportSchedule.ReportWindow.WindowCase.WINDOW_NOT_SET ->
      throw Status.FAILED_PRECONDITION.withDescription("ReportSchedule missing report_window")
        .asRuntimeException()
  }
}

/** Converts an internal [InternalReportSchedule] to a public [ReportSchedule]. */
private fun InternalReportSchedule.toPublic(): ReportSchedule {
  val source = this

  val reportScheduleName =
    ReportScheduleKey(source.cmmsMeasurementConsumerId, source.externalReportScheduleId).toName()
  val reportTemplate = report {
    reportingMetricEntries +=
      source.details.reportTemplate.reportingMetricEntriesMap.map { internalReportingMetricEntry ->
        internalReportingMetricEntry.toReportingMetricEntry(source.cmmsMeasurementConsumerId)
      }
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
