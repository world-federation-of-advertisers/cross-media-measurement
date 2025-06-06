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
import kotlin.coroutines.CoroutineContext
import kotlin.coroutines.EmptyCoroutineContext
import kotlin.math.min
import org.projectnessie.cel.Env
import org.wfanet.measurement.access.client.v1alpha.Authorization
import org.wfanet.measurement.access.client.v1alpha.check
import org.wfanet.measurement.common.base64UrlDecode
import org.wfanet.measurement.common.base64UrlEncode
import org.wfanet.measurement.common.grpc.grpcRequire
import org.wfanet.measurement.common.grpc.grpcRequireNotNull
import org.wfanet.measurement.internal.reporting.v2.ListReportScheduleIterationsRequest as InternalListReportScheduleIterationsRequest
import org.wfanet.measurement.internal.reporting.v2.ListReportScheduleIterationsRequestKt
import org.wfanet.measurement.internal.reporting.v2.ReportScheduleIteration as InternalReportScheduleIteration
import org.wfanet.measurement.internal.reporting.v2.ReportScheduleIterationsGrpcKt.ReportScheduleIterationsCoroutineStub
import org.wfanet.measurement.internal.reporting.v2.getReportScheduleIterationRequest
import org.wfanet.measurement.internal.reporting.v2.listReportScheduleIterationsRequest
import org.wfanet.measurement.reporting.v2alpha.GetReportScheduleIterationRequest
import org.wfanet.measurement.reporting.v2alpha.ListReportScheduleIterationsPageToken
import org.wfanet.measurement.reporting.v2alpha.ListReportScheduleIterationsPageTokenKt
import org.wfanet.measurement.reporting.v2alpha.ListReportScheduleIterationsRequest
import org.wfanet.measurement.reporting.v2alpha.ListReportScheduleIterationsResponse
import org.wfanet.measurement.reporting.v2alpha.ReportScheduleIteration
import org.wfanet.measurement.reporting.v2alpha.ReportScheduleIterationsGrpcKt.ReportScheduleIterationsCoroutineImplBase
import org.wfanet.measurement.reporting.v2alpha.copy
import org.wfanet.measurement.reporting.v2alpha.listReportScheduleIterationsPageToken
import org.wfanet.measurement.reporting.v2alpha.listReportScheduleIterationsResponse
import org.wfanet.measurement.reporting.v2alpha.reportScheduleIteration

private const val MIN_PAGE_SIZE = 1
private const val DEFAULT_PAGE_SIZE = 50
private const val MAX_PAGE_SIZE = 1000

class ReportScheduleIterationsService(
  private val internalReportScheduleIterationsStub: ReportScheduleIterationsCoroutineStub,
  private val authorization: Authorization,
  coroutineContext: CoroutineContext = EmptyCoroutineContext,
) : ReportScheduleIterationsCoroutineImplBase(coroutineContext) {
  override suspend fun getReportScheduleIteration(
    request: GetReportScheduleIterationRequest
  ): ReportScheduleIteration {
    val reportScheduleIterationKey =
      grpcRequireNotNull(ReportScheduleIterationKey.fromName(request.name)) {
        "ReportScheduleIteration name is either unspecified or invalid"
      }

    authorization.check(
      listOf(
        reportScheduleIterationKey.parentKey.toName(),
        reportScheduleIterationKey.parentKey.parentKey.toName(),
      ),
      GET_REPORT_SCHEDULE_PERMISSION,
    )

    val internalReportScheduleIteration =
      try {
        internalReportScheduleIterationsStub.getReportScheduleIteration(
          getReportScheduleIterationRequest {
            cmmsMeasurementConsumerId = reportScheduleIterationKey.cmmsMeasurementConsumerId
            externalReportScheduleId = reportScheduleIterationKey.reportScheduleId
            externalReportScheduleIterationId = reportScheduleIterationKey.reportScheduleIterationId
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
          .withDescription("Unable to get ReportScheduleIteration.")
          .asRuntimeException()
      }

    return internalReportScheduleIteration.toPublic()
  }

  override suspend fun listReportScheduleIterations(
    request: ListReportScheduleIterationsRequest
  ): ListReportScheduleIterationsResponse {
    val parentKey: ReportScheduleKey =
      grpcRequireNotNull(ReportScheduleKey.fromName(request.parent)) {
        "Parent is either unspecified or invalid."
      }
    val listReportScheduleIterationsPageToken = request.toListReportScheduleIterationsPageToken()

    authorization.check(
      listOf(request.parent, parentKey.parentKey.toName()),
      GET_REPORT_SCHEDULE_PERMISSION,
    )

    val internalListReportScheduleIterationsRequest: InternalListReportScheduleIterationsRequest =
      listReportScheduleIterationsPageToken.toListReportScheduleIterationsRequest()

    val results: List<InternalReportScheduleIteration> =
      try {
        internalReportScheduleIterationsStub
          .listReportScheduleIterations(internalListReportScheduleIterationsRequest)
          .reportScheduleIterationsList
      } catch (e: StatusException) {
        throw when (e.status.code) {
            Status.Code.DEADLINE_EXCEEDED -> Status.DEADLINE_EXCEEDED
            Status.Code.CANCELLED -> Status.CANCELLED
            else -> Status.UNKNOWN
          }
          .withCause(e)
          .withDescription("Unable to list ReportScheduleIterations.")
          .asRuntimeException()
      }

    if (results.isEmpty()) {
      return ListReportScheduleIterationsResponse.getDefaultInstance()
    }

    val nextPageToken: ListReportScheduleIterationsPageToken? =
      if (results.size > listReportScheduleIterationsPageToken.pageSize) {
        val lastResult = results[results.lastIndex - 1]
        listReportScheduleIterationsPageToken.copy {
          lastReportScheduleIteration =
            ListReportScheduleIterationsPageTokenKt.previousPageEnd {
              cmmsMeasurementConsumerId = lastResult.cmmsMeasurementConsumerId
              externalReportScheduleId = lastResult.externalReportScheduleId
              reportEventTime = lastResult.reportEventTime
            }
        }
      } else {
        null
      }

    val subResults: List<InternalReportScheduleIteration> =
      results.subList(0, min(results.size, listReportScheduleIterationsPageToken.pageSize))

    return listReportScheduleIterationsResponse {
      reportScheduleIterations +=
        filterReportScheduleIterations(
          subResults.map { internalReportScheduleIteration ->
            internalReportScheduleIteration.toPublic()
          },
          request.filter,
        )

      if (nextPageToken != null) {
        this.nextPageToken = nextPageToken.toByteString().base64UrlEncode()
      }
    }
  }

  private fun filterReportScheduleIterations(
    reportScheduleIterations: List<ReportScheduleIteration>,
    filter: String,
  ): List<ReportScheduleIteration> {
    return try {
      filterList(ENV, reportScheduleIterations, filter)
    } catch (e: IllegalArgumentException) {
      throw Status.INVALID_ARGUMENT.withDescription(e.message).asRuntimeException()
    }
  }

  companion object {
    const val GET_REPORT_SCHEDULE_PERMISSION = "reporting.reportSchedules.get"
    private val ENV: Env = buildCelEnvironment(ReportScheduleIteration.getDefaultInstance())
  }
}

/**
 * Converts a public [ListReportScheduleIterationsRequest] to a
 * [ListReportScheduleIterationsPageToken].
 */
private fun ListReportScheduleIterationsRequest.toListReportScheduleIterationsPageToken():
  ListReportScheduleIterationsPageToken {
  grpcRequire(pageSize >= 0) { "Page size cannot be less than 0" }

  val source = this
  val parentKey: ReportScheduleKey =
    grpcRequireNotNull(ReportScheduleKey.fromName(parent)) {
      "Parent is either unspecified or invalid."
    }
  val cmmsMeasurementConsumerId = parentKey.cmmsMeasurementConsumerId
  val externalReportScheduleId = parentKey.reportScheduleId

  return if (pageToken.isNotBlank()) {
    ListReportScheduleIterationsPageToken.parseFrom(pageToken.base64UrlDecode()).copy {
      grpcRequire(this.cmmsMeasurementConsumerId == cmmsMeasurementConsumerId) {
        "Arguments must be kept the same when using a page token"
      }

      grpcRequire(this.externalReportScheduleId == externalReportScheduleId) {
        "Arguments must be kept the same when using a page token"
      }

      if (source.pageSize in MIN_PAGE_SIZE..MAX_PAGE_SIZE) {
        pageSize = source.pageSize
      }
    }
  } else {
    listReportScheduleIterationsPageToken {
      pageSize =
        when {
          source.pageSize < MIN_PAGE_SIZE -> DEFAULT_PAGE_SIZE
          source.pageSize > MAX_PAGE_SIZE -> MAX_PAGE_SIZE
          else -> source.pageSize
        }
      this.cmmsMeasurementConsumerId = cmmsMeasurementConsumerId
      this.externalReportScheduleId = externalReportScheduleId
    }
  }
}

/**
 * Converts a [ListReportScheduleIterationsPageToken] to an internal
 * [ListReportScheduleIterationsRequest].
 */
private fun ListReportScheduleIterationsPageToken.toListReportScheduleIterationsRequest():
  InternalListReportScheduleIterationsRequest {
  val source = this
  return listReportScheduleIterationsRequest {
    // get one more than the actual page size for deciding whether to set page token
    limit = pageSize + 1
    filter =
      ListReportScheduleIterationsRequestKt.filter {
        cmmsMeasurementConsumerId = source.cmmsMeasurementConsumerId
        externalReportScheduleId = source.externalReportScheduleId
        if (source.hasLastReportScheduleIteration()) {
          reportEventTimeBefore = source.lastReportScheduleIteration.reportEventTime
        }
      }
  }
}

/**
 * Converts an internal [InternalReportScheduleIteration.State] to a public
 * [ReportScheduleIteration.State].
 */
private fun InternalReportScheduleIteration.State.toPublic(): ReportScheduleIteration.State {
  return when (this) {
    InternalReportScheduleIteration.State.WAITING_FOR_DATA_AVAILABILITY ->
      ReportScheduleIteration.State.WAITING_FOR_DATA_AVAILABILITY
    InternalReportScheduleIteration.State.RETRYING_REPORT_CREATION ->
      ReportScheduleIteration.State.RETRYING_REPORT_CREATION
    InternalReportScheduleIteration.State.REPORT_CREATED ->
      ReportScheduleIteration.State.REPORT_CREATED
    InternalReportScheduleIteration.State.STATE_UNSPECIFIED ->
      ReportScheduleIteration.State.STATE_UNSPECIFIED
    InternalReportScheduleIteration.State.UNRECOGNIZED ->
      // State is set by the system so if this is reached, something went wrong.
      throw Status.UNKNOWN.withDescription(
          "There is an unknown problem with the ReportScheduleIteration"
        )
        .asRuntimeException()
  }
}

/** Converts an internal [InternalReportScheduleIteration] to a public [ReportScheduleIteration]. */
private fun InternalReportScheduleIteration.toPublic(): ReportScheduleIteration {
  val source = this
  val reportScheduleIterationKey =
    ReportScheduleIterationKey(
      source.cmmsMeasurementConsumerId,
      source.externalReportScheduleId,
      source.externalReportScheduleIterationId,
    )

  return reportScheduleIteration {
    name = reportScheduleIterationKey.toName()
    state = source.state.toPublic()
    numAttempts = source.numAttempts
    if (externalReportId.isNotEmpty()) {
      val reportKey = ReportKey(source.cmmsMeasurementConsumerId, source.externalReportId)
      report = reportKey.toName()
    }
    reportEventTime = source.reportEventTime
    createTime = source.createTime
    updateTime = source.updateTime
  }
}
