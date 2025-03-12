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

package org.wfanet.measurement.reporting.service.api.v2alpha

import com.google.longrunning.Operation
import com.google.protobuf.InvalidProtocolBufferException
import io.grpc.Status
import io.grpc.StatusException
import org.wfanet.measurement.api.v2alpha.MeasurementConsumerKey
import org.wfanet.measurement.common.base64UrlDecode
import org.wfanet.measurement.common.base64UrlEncode
import org.wfanet.measurement.internal.reporting.v2.BasicReportsGrpcKt.BasicReportsCoroutineStub
import org.wfanet.measurement.internal.reporting.v2.ListBasicReportsRequest as InternalListBasicReportsRequest
import org.wfanet.measurement.internal.reporting.v2.ListBasicReportsRequestKt as InternalListBasicReportsRequestKt
import org.wfanet.measurement.internal.reporting.v2.getBasicReportRequest as internalGetBasicReportRequest
import org.wfanet.measurement.internal.reporting.v2.listBasicReportsRequest as internalListBasicReportsRequest
import org.wfanet.measurement.reporting.service.api.BasicReportNotFoundException
import org.wfanet.measurement.reporting.service.api.FieldValueDoesNotMatchPageTokenException
import org.wfanet.measurement.reporting.service.api.InvalidFieldValueException
import org.wfanet.measurement.reporting.service.api.RequiredFieldNotSetException
import org.wfanet.measurement.reporting.service.internal.Errors as InternalErrors
import org.wfanet.measurement.reporting.v2alpha.BasicReport
import org.wfanet.measurement.reporting.v2alpha.BasicReportsGrpcKt.BasicReportsCoroutineImplBase
import org.wfanet.measurement.reporting.v2alpha.CreateBasicReportRequest
import org.wfanet.measurement.reporting.v2alpha.GetBasicReportRequest
import org.wfanet.measurement.reporting.v2alpha.ListBasicReportsPageToken
import org.wfanet.measurement.reporting.v2alpha.ListBasicReportsPageTokenKt
import org.wfanet.measurement.reporting.v2alpha.ListBasicReportsRequest
import org.wfanet.measurement.reporting.v2alpha.ListBasicReportsResponse
import org.wfanet.measurement.reporting.v2alpha.listBasicReportsPageToken
import org.wfanet.measurement.reporting.v2alpha.listBasicReportsResponse

class BasicReportsService(private val internalBasicReportsStub: BasicReportsCoroutineStub) :
  BasicReportsCoroutineImplBase() {

  override suspend fun createBasicReport(request: CreateBasicReportRequest): Operation {
    // TODO(@tristanvuong2021): Will be implemented for phase 2
    throw NotImplementedError()
  }

  override suspend fun getBasicReport(request: GetBasicReportRequest): BasicReport {
    if (request.name.isEmpty()) {
      throw RequiredFieldNotSetException("name")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }

    val (measurementConsumerKey, basicReportId) =
      BasicReportKey.fromName(request.name)
        ?: throw InvalidFieldValueException("name")
          .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)

    val internalBasicReport =
      try {
        internalBasicReportsStub.getBasicReport(
          internalGetBasicReportRequest {
            cmmsMeasurementConsumerId = measurementConsumerKey.measurementConsumerId
            externalBasicReportId = basicReportId
          }
        )
      } catch (e: StatusException) {
        throw when (InternalErrors.getReason(e)) {
          InternalErrors.Reason.BASIC_REPORT_NOT_FOUND ->
            BasicReportNotFoundException.fromInternal(e)
              .asStatusRuntimeException(Status.Code.NOT_FOUND)
          InternalErrors.Reason.MEASUREMENT_CONSUMER_NOT_FOUND,
          InternalErrors.Reason.BASIC_REPORT_ALREADY_EXISTS,
          InternalErrors.Reason.REQUIRED_FIELD_NOT_SET,
          null -> Status.INTERNAL.withCause(e).asRuntimeException()
        }
      }

    return internalBasicReport.toBasicReport()
  }

  override suspend fun listBasicReports(
    request: ListBasicReportsRequest
  ): ListBasicReportsResponse {
    if (request.parent.isEmpty()) {
      throw RequiredFieldNotSetException("parent")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }

    MeasurementConsumerKey.fromName(request.parent)
      ?: throw InvalidFieldValueException("parent")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)

    if (request.pageSize < 0) {
      throw InvalidFieldValueException("page_size")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }

    val internalRequest = request.toInternal()
    val basicReports: List<BasicReport> =
      try {
        internalBasicReportsStub
          .listBasicReports(internalRequest)
          .basicReportsList
          .map { it.toBasicReport() }
          .toList()
      } catch (e: StatusException) {
        throw Status.INTERNAL.withCause(e).asRuntimeException()
      }

    if (basicReports.isEmpty()) {
      return listBasicReportsResponse {}
    }

    val nextPageToken: ListBasicReportsPageToken? =
      if (basicReports.size == internalRequest.limit) {
        listBasicReportsPageToken {
          pageSize = internalRequest.limit - 1
          cmmsMeasurementConsumerId = internalRequest.filter.cmmsMeasurementConsumerId
          if (internalRequest.filter.hasCreateTimeAfter()) {
            filter =
              ListBasicReportsPageTokenKt.filter {
                createTimeAfter = internalRequest.filter.createTimeAfter
              }
          }
          lastBasicReport =
            ListBasicReportsPageTokenKt.previousPageEnd {
              createTime = basicReports[basicReports.lastIndex - 1].createTime
              externalBasicReportId =
                BasicReportKey.fromName(basicReports[basicReports.lastIndex - 1].name)!!
                  .basicReportId
            }
        }
      } else {
        null
      }

    return listBasicReportsResponse {
      if (nextPageToken != null) {
        this.basicReports += basicReports.subList(0, basicReports.lastIndex)
        this.nextPageToken = nextPageToken.toByteString().base64UrlEncode()
      } else {
        this.basicReports += basicReports
      }
    }
  }

  private fun ListBasicReportsRequest.toInternal(): InternalListBasicReportsRequest {
    val source = this
    val cmmsMeasurementConsumerId =
      MeasurementConsumerKey.fromName(source.parent)!!.measurementConsumerId

    return if (source.pageToken.isNotBlank()) {
      val decodedPageToken =
        try {
          ListBasicReportsPageToken.parseFrom(source.pageToken.base64UrlDecode())
        } catch (e: InvalidProtocolBufferException) {
          throw InvalidFieldValueException("page_token")
            .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
        }

      if (!decodedPageToken.filter.createTimeAfter.equals(source.filter.createTimeAfter)) {
        throw FieldValueDoesNotMatchPageTokenException("filter.create_time_after")
          .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
      }

      val finalPageSize =
        if (source.pageSize in 1..MAX_PAGE_SIZE) {
          source.pageSize
        } else if (source.pageSize > MAX_PAGE_SIZE) {
          MAX_PAGE_SIZE
        } else decodedPageToken.pageSize

      internalListBasicReportsRequest {
        this.filter =
          InternalListBasicReportsRequestKt.filter {
            this.cmmsMeasurementConsumerId = cmmsMeasurementConsumerId
            after =
              InternalListBasicReportsRequestKt.afterFilter {
                createTime = decodedPageToken.lastBasicReport.createTime
                externalBasicReportId = decodedPageToken.lastBasicReport.externalBasicReportId
              }
          }
        limit = finalPageSize + 1
      }
    } else {
      val finalPageSize =
        if (source.pageSize in 1..MAX_PAGE_SIZE) {
          source.pageSize
        } else if (source.pageSize > MAX_PAGE_SIZE) {
          MAX_PAGE_SIZE
        } else DEFAULT_PAGE_SIZE

      internalListBasicReportsRequest {
        this.filter =
          InternalListBasicReportsRequestKt.filter {
            this.cmmsMeasurementConsumerId = cmmsMeasurementConsumerId
            if (source.filter.hasCreateTimeAfter()) {
              createTimeAfter = source.filter.createTimeAfter
            }
          }
        limit = finalPageSize + 1
      }
    }
  }

  companion object {
    private const val DEFAULT_PAGE_SIZE = 10
    private const val MAX_PAGE_SIZE = 25
  }
}
