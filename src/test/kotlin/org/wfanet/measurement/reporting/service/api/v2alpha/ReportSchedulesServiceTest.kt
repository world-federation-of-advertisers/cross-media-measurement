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

import com.google.common.truth.Truth.assertThat
import com.google.common.truth.extensions.proto.ProtoTruth.assertThat
import com.google.protobuf.timestamp
import com.google.type.date
import com.google.type.dateTime
import com.google.type.timeZone
import io.grpc.Status
import io.grpc.StatusRuntimeException
import kotlin.test.assertFailsWith
import kotlinx.coroutines.runBlocking
import org.junit.Before
import org.junit.Rule
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.mockito.kotlin.any
import org.mockito.kotlin.eq
import org.mockito.kotlin.whenever
import org.wfanet.measurement.api.v2alpha.DataProviderKey
import org.wfanet.measurement.api.v2alpha.withDataProviderPrincipal
import org.wfanet.measurement.common.base64UrlEncode
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.common.grpc.testing.mockService
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.common.testing.verifyProtoArgument
import org.wfanet.measurement.config.reporting.measurementConsumerConfig
import org.wfanet.measurement.internal.reporting.v2.ListReportSchedulesRequestKt as InternalListReportSchedulesRequestKt
import org.wfanet.measurement.internal.reporting.v2.MetricSpec as InternalMetricSpec
import org.wfanet.measurement.internal.reporting.v2.MetricSpecKt as InternalMetricSpecKt
import org.wfanet.measurement.internal.reporting.v2.ReportKt as InternalReportKt
import org.wfanet.measurement.internal.reporting.v2.ReportSchedule as InternalReportSchedule
import org.wfanet.measurement.internal.reporting.v2.ReportScheduleKt as InternalReportScheduleKt
import org.wfanet.measurement.internal.reporting.v2.ReportSchedulesGrpcKt.ReportSchedulesCoroutineImplBase
import org.wfanet.measurement.internal.reporting.v2.ReportSchedulesGrpcKt.ReportSchedulesCoroutineStub
import org.wfanet.measurement.internal.reporting.v2.copy
import org.wfanet.measurement.internal.reporting.v2.getReportScheduleRequest as internalGetReportScheduleRequest
import org.wfanet.measurement.internal.reporting.v2.listReportSchedulesRequest as internalListReportSchedulesRequest
import org.wfanet.measurement.internal.reporting.v2.listReportSchedulesResponse as internalListReportSchedulesResponse
import org.wfanet.measurement.internal.reporting.v2.metricSpec as internalMetricSpec
import org.wfanet.measurement.internal.reporting.v2.report as internalReport
import org.wfanet.measurement.internal.reporting.v2.reportSchedule as internalReportSchedule
import org.wfanet.measurement.internal.reporting.v2.stopReportScheduleRequest as internalStopReportScheduleRequest
import org.wfanet.measurement.reporting.v2alpha.ListReportSchedulesPageTokenKt
import org.wfanet.measurement.reporting.v2alpha.MetricSpec
import org.wfanet.measurement.reporting.v2alpha.MetricSpecKt
import org.wfanet.measurement.reporting.v2alpha.ReportKt
import org.wfanet.measurement.reporting.v2alpha.ReportSchedule
import org.wfanet.measurement.reporting.v2alpha.ReportScheduleKt
import org.wfanet.measurement.reporting.v2alpha.copy
import org.wfanet.measurement.reporting.v2alpha.getReportScheduleRequest
import org.wfanet.measurement.reporting.v2alpha.listReportSchedulesPageToken
import org.wfanet.measurement.reporting.v2alpha.listReportSchedulesRequest
import org.wfanet.measurement.reporting.v2alpha.listReportSchedulesResponse
import org.wfanet.measurement.reporting.v2alpha.metricSpec
import org.wfanet.measurement.reporting.v2alpha.report
import org.wfanet.measurement.reporting.v2alpha.reportSchedule
import org.wfanet.measurement.reporting.v2alpha.stopReportScheduleRequest

@RunWith(JUnit4::class)
class ReportSchedulesServiceTest {
  private val internalReportSchedulesMock: ReportSchedulesCoroutineImplBase = mockService {
    onBlocking { getReportSchedule(any()) }.thenReturn(INTERNAL_REPORT_SCHEDULE)

    onBlocking { listReportSchedules(any()) }
      .thenReturn(
        internalListReportSchedulesResponse {
          reportSchedules += INTERNAL_REPORT_SCHEDULE
          reportSchedules += INTERNAL_REPORT_SCHEDULE_2
        }
      )

    onBlocking { stopReportSchedule(any()) }.thenReturn(INTERNAL_REPORT_SCHEDULE)
  }

  @get:Rule val grpcTestServerRule = GrpcTestServerRule { addService(internalReportSchedulesMock) }

  private lateinit var service: ReportSchedulesService

  @Before
  fun initService() {
    service =
      ReportSchedulesService(
        ReportSchedulesCoroutineStub(grpcTestServerRule.channel),
      )
  }

  @Test
  fun `getReportSchedule returns schedule`() = runBlocking {
    whenever(
        internalReportSchedulesMock.getReportSchedule(
          eq(
            internalGetReportScheduleRequest {
              cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
              externalReportScheduleId = REPORT_SCHEDULE_ID
            }
          )
        )
      )
      .thenReturn(INTERNAL_REPORT_SCHEDULE)

    val request = getReportScheduleRequest { name = REPORT_SCHEDULE_NAME }

    val reportSchedule =
      withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME, CONFIG) {
        runBlocking { service.getReportSchedule(request) }
      }

    assertThat(reportSchedule).isEqualTo(REPORT_SCHEDULE)
  }

  @Test
  fun `getReportSchedule throws NOT_FOUND when report schedule not found`() = runBlocking {
    whenever(
        internalReportSchedulesMock.getReportSchedule(
          eq(
            internalGetReportScheduleRequest {
              cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
              externalReportScheduleId = REPORT_SCHEDULE_ID
            }
          )
        )
      )
      .thenThrow(StatusRuntimeException(Status.NOT_FOUND))

    val request = getReportScheduleRequest { name = REPORT_SCHEDULE_NAME }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME, CONFIG) {
          runBlocking { service.getReportSchedule(request) }
        }
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
    assertThat(exception.message).contains("Unable to get")
  }

  @Test
  fun `getReportSchedule throws INVALID_ARGUMENT when name is invalid`() {
    val invalidReportScheduleName =
      "measurementConsumers/$CMMS_MEASUREMENT_CONSUMER_ID/reportSchedules"
    val request = getReportScheduleRequest { name = invalidReportScheduleName }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME, CONFIG) {
          runBlocking { service.getReportSchedule(request) }
        }
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.message).contains("name")
  }

  @Test
  fun `getReportSchedule throws PERMISSION_DENIED when MC's identity does not match`() {
    val request = getReportScheduleRequest { name = REPORT_SCHEDULE_NAME }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME + 1, CONFIG) {
          runBlocking { service.getReportSchedule(request) }
        }
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.PERMISSION_DENIED)
    assertThat(exception.message).contains("other MeasurementConsumers")
  }

  @Test
  fun `getReportSchedule throws UNAUTHENTICATED when the caller is not a MC`() {
    val reportScheduleName = REPORT_SCHEDULE_NAME
    val request = getReportScheduleRequest { name = reportScheduleName }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withDataProviderPrincipal(DataProviderKey(ExternalId(550L).apiId.value).toName()) {
          runBlocking { service.getReportSchedule(request) }
        }
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.UNAUTHENTICATED)
  }

  @Test
  fun `listReportSchedules returns with next page token when there is another page`() {
    val pageSize = 1
    val request = listReportSchedulesRequest {
      parent = MEASUREMENT_CONSUMER_NAME
      this.pageSize = pageSize
    }

    val result =
      withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME, CONFIG) {
        runBlocking { service.listReportSchedules(request) }
      }

    val expected = listReportSchedulesResponse {
      reportSchedules += REPORT_SCHEDULE
      nextPageToken =
        listReportSchedulesPageToken {
            this.pageSize = pageSize
            cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
            lastReportSchedule =
              ListReportSchedulesPageTokenKt.previousPageEnd {
                cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
                externalReportScheduleId = REPORT_SCHEDULE_ID
              }
          }
          .toByteString()
          .base64UrlEncode()
    }

    assertThat(result).ignoringRepeatedFieldOrder().isEqualTo(expected)

    verifyProtoArgument(
        internalReportSchedulesMock,
        ReportSchedulesCoroutineImplBase::listReportSchedules
      )
      .isEqualTo(
        internalListReportSchedulesRequest {
          limit = pageSize + 1
          filter =
            InternalListReportSchedulesRequestKt.filter {
              cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
            }
        }
      )
  }

  @Test
  fun `listReportSchedules returns with no next page token when no other page`() {
    val pageSize = 2
    val request = listReportSchedulesRequest {
      parent = MEASUREMENT_CONSUMER_NAME
      this.pageSize = pageSize
    }

    val result =
      withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME, CONFIG) {
        runBlocking { service.listReportSchedules(request) }
      }

    val expected = listReportSchedulesResponse {
      reportSchedules += REPORT_SCHEDULE
      reportSchedules += REPORT_SCHEDULE_2
    }

    assertThat(result).ignoringRepeatedFieldOrder().isEqualTo(expected)

    verifyProtoArgument(
        internalReportSchedulesMock,
        ReportSchedulesCoroutineImplBase::listReportSchedules
      )
      .isEqualTo(
        internalListReportSchedulesRequest {
          limit = pageSize + 1
          filter =
            InternalListReportSchedulesRequestKt.filter {
              cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
            }
        }
      )
  }

  @Test
  fun `listReportSchedules succeeds when there is a page token`() {
    val pageSize = 2
    val request = listReportSchedulesRequest {
      parent = MEASUREMENT_CONSUMER_NAME
      this.pageSize = pageSize
      pageToken =
        listReportSchedulesPageToken {
            this.pageSize = pageSize
            cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
            lastReportSchedule =
              ListReportSchedulesPageTokenKt.previousPageEnd {
                cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
                externalReportScheduleId = REPORT_SCHEDULE_ID
              }
          }
          .toByteString()
          .base64UrlEncode()
    }

    val result =
      withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME, CONFIG) {
        runBlocking { service.listReportSchedules(request) }
      }

    val expected = listReportSchedulesResponse {
      reportSchedules += REPORT_SCHEDULE
      reportSchedules += REPORT_SCHEDULE_2
    }

    assertThat(result).ignoringRepeatedFieldOrder().isEqualTo(expected)

    verifyProtoArgument(
        internalReportSchedulesMock,
        ReportSchedulesCoroutineImplBase::listReportSchedules
      )
      .isEqualTo(
        internalListReportSchedulesRequest {
          limit = pageSize + 1
          filter =
            InternalListReportSchedulesRequestKt.filter {
              cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
              externalReportScheduleIdAfter = REPORT_SCHEDULE_ID
            }
        }
      )
  }

  @Test
  fun `listReportSchedules with page size too large replaced by max page size`() {
    val pageSize = MAX_PAGE_SIZE + 1
    val request = listReportSchedulesRequest {
      parent = MEASUREMENT_CONSUMER_NAME
      this.pageSize = pageSize
    }

    val result =
      withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME, CONFIG) {
        runBlocking { service.listReportSchedules(request) }
      }

    val expected = listReportSchedulesResponse {
      reportSchedules += REPORT_SCHEDULE
      reportSchedules += REPORT_SCHEDULE_2
    }

    assertThat(result).ignoringRepeatedFieldOrder().isEqualTo(expected)

    verifyProtoArgument(
        internalReportSchedulesMock,
        ReportSchedulesCoroutineImplBase::listReportSchedules
      )
      .isEqualTo(
        internalListReportSchedulesRequest {
          limit = MAX_PAGE_SIZE + 1
          filter =
            InternalListReportSchedulesRequestKt.filter {
              cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
            }
        }
      )
  }

  @Test
  fun `listReportSchedules with page size too large replaced by one in page token`() {
    val pageSize = MAX_PAGE_SIZE + 1
    val oldPageSize = 5
    val request = listReportSchedulesRequest {
      parent = MEASUREMENT_CONSUMER_NAME
      this.pageSize = pageSize
      pageToken =
        listReportSchedulesPageToken {
            this.pageSize = oldPageSize
            cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
            lastReportSchedule =
              ListReportSchedulesPageTokenKt.previousPageEnd {
                cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
                externalReportScheduleId = REPORT_SCHEDULE_ID
              }
          }
          .toByteString()
          .base64UrlEncode()
    }

    val result =
      withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME, CONFIG) {
        runBlocking { service.listReportSchedules(request) }
      }

    val expected = listReportSchedulesResponse {
      reportSchedules += REPORT_SCHEDULE
      reportSchedules += REPORT_SCHEDULE_2
    }

    assertThat(result).ignoringRepeatedFieldOrder().isEqualTo(expected)

    verifyProtoArgument(
        internalReportSchedulesMock,
        ReportSchedulesCoroutineImplBase::listReportSchedules
      )
      .isEqualTo(
        internalListReportSchedulesRequest {
          limit = oldPageSize + 1
          filter =
            InternalListReportSchedulesRequestKt.filter {
              cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
              externalReportScheduleIdAfter = REPORT_SCHEDULE_ID
            }
        }
      )
  }

  @Test
  fun `listReportSchedules with no page size replaced by default size`() {
    val request = listReportSchedulesRequest { parent = MEASUREMENT_CONSUMER_NAME }

    val result =
      withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME, CONFIG) {
        runBlocking { service.listReportSchedules(request) }
      }

    val expected = listReportSchedulesResponse {
      reportSchedules += REPORT_SCHEDULE
      reportSchedules += REPORT_SCHEDULE_2
    }

    assertThat(result).ignoringRepeatedFieldOrder().isEqualTo(expected)

    verifyProtoArgument(
        internalReportSchedulesMock,
        ReportSchedulesCoroutineImplBase::listReportSchedules
      )
      .isEqualTo(
        internalListReportSchedulesRequest {
          limit = DEFAULT_PAGE_SIZE + 1
          filter =
            InternalListReportSchedulesRequestKt.filter {
              cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
            }
        }
      )
  }

  @Test
  fun `listReportSchedules with page size replaces size in page token`() {
    val pageSize = 6
    val oldPageSize = 5
    val request = listReportSchedulesRequest {
      parent = MEASUREMENT_CONSUMER_NAME
      this.pageSize = pageSize
      pageToken =
        listReportSchedulesPageToken {
            this.pageSize = oldPageSize
            cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
            lastReportSchedule =
              ListReportSchedulesPageTokenKt.previousPageEnd {
                cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
                externalReportScheduleId = REPORT_SCHEDULE_ID
              }
          }
          .toByteString()
          .base64UrlEncode()
    }

    val result =
      withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME, CONFIG) {
        runBlocking { service.listReportSchedules(request) }
      }

    val expected = listReportSchedulesResponse {
      reportSchedules += REPORT_SCHEDULE
      reportSchedules += REPORT_SCHEDULE_2
    }

    assertThat(result).ignoringRepeatedFieldOrder().isEqualTo(expected)

    verifyProtoArgument(
        internalReportSchedulesMock,
        ReportSchedulesCoroutineImplBase::listReportSchedules
      )
      .isEqualTo(
        internalListReportSchedulesRequest {
          limit = pageSize + 1
          filter =
            InternalListReportSchedulesRequestKt.filter {
              cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
              externalReportScheduleIdAfter = REPORT_SCHEDULE_ID
            }
        }
      )
  }

  @Test
  fun `listReportSchedules with a filter returns filtered results`() {
    val pageSize = 2
    val request = listReportSchedulesRequest {
      parent = MEASUREMENT_CONSUMER_NAME
      this.pageSize = pageSize
      filter = "name != '$REPORT_SCHEDULE_NAME_2'"
    }

    val result =
      withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME, CONFIG) {
        runBlocking { service.listReportSchedules(request) }
      }

    val expected = listReportSchedulesResponse { reportSchedules += REPORT_SCHEDULE }

    assertThat(result).ignoringRepeatedFieldOrder().isEqualTo(expected)

    verifyProtoArgument(
        internalReportSchedulesMock,
        ReportSchedulesCoroutineImplBase::listReportSchedules
      )
      .isEqualTo(
        internalListReportSchedulesRequest {
          limit = pageSize + 1
          filter =
            InternalListReportSchedulesRequestKt.filter {
              cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
            }
        }
      )
  }

  @Test
  fun `listReportSchedules throws CANCELLED when cancelled`() = runBlocking {
    whenever(internalReportSchedulesMock.listReportSchedules(any()))
      .thenThrow(StatusRuntimeException(Status.CANCELLED))

    val request = listReportSchedulesRequest { parent = MEASUREMENT_CONSUMER_NAME }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME, CONFIG) {
          runBlocking { service.listReportSchedules(request) }
        }
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.CANCELLED)
    assertThat(exception.message).contains("Unable to list")
  }

  @Test
  fun `listReportSchedules throws UNAUTHENTICATED when no principal is found`() {
    val request = listReportSchedulesRequest { parent = MEASUREMENT_CONSUMER_NAME }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        runBlocking { service.listReportSchedules(request) }
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.UNAUTHENTICATED)
  }

  @Test
  fun `listReportSchedules throws PERMISSION_DENIED when MC caller doesn't match`() {
    val request = listReportSchedulesRequest { parent = MEASUREMENT_CONSUMER_NAME + 1 }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME, CONFIG) {
          runBlocking { service.listReportSchedules(request) }
        }
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.PERMISSION_DENIED)
    assertThat(exception.message).contains("other MeasurementConsumers")
  }

  @Test
  fun `listReportSchedules throws UNAUTHENTICATED when the caller is not MC`() {
    val request = listReportSchedulesRequest { parent = MEASUREMENT_CONSUMER_NAME }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withDataProviderPrincipal(DataProviderKey(ExternalId(550L).apiId.value).toName()) {
          runBlocking { service.listReportSchedules(request) }
        }
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.UNAUTHENTICATED)
  }

  @Test
  fun `listReportSchedules throws INVALID_ARGUMENT when page size is less than 0`() {
    val request = listReportSchedulesRequest {
      parent = MEASUREMENT_CONSUMER_NAME
      pageSize = -1
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME, CONFIG) {
          runBlocking { service.listReportSchedules(request) }
        }
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.message).contains("less than 0")
  }

  @Test
  fun `listReportSchedules throws INVALID_ARGUMENT when parent is unspecified`() {
    val request = listReportSchedulesRequest { pageSize = 1 }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME, CONFIG) {
          runBlocking { service.listReportSchedules(request) }
        }
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.message).contains("unspecified")
  }

  @Test
  fun `listReportSchedules throws INVALID_ARGUMENT when mc id doesn't match page token`() {
    val pageToken =
      listReportSchedulesPageToken {
          this.pageSize = 2
          cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
          lastReportSchedule =
            ListReportSchedulesPageTokenKt.previousPageEnd {
              cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
              externalReportScheduleId = REPORT_SCHEDULE_ID
            }
        }
        .toByteString()
        .base64UrlEncode()

    val request = listReportSchedulesRequest {
      pageSize = 2
      parent = MEASUREMENT_CONSUMER_NAME + 1
      this.pageToken = pageToken
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME, CONFIG) {
          runBlocking { service.listReportSchedules(request) }
        }
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.message).contains("kept the same")
  }

  @Test
  fun `listReportSchedules throws INVALID_ARGUMENT when filter is not valid CEL`() {
    val request = listReportSchedulesRequest {
      pageSize = 2
      parent = MEASUREMENT_CONSUMER_NAME
      filter = "name >>> 5"
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME, CONFIG) {
          runBlocking { service.listReportSchedules(request) }
        }
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.message).contains("not a valid CEL")
  }

  @Test
  fun `stopReportSchedule returns schedule`() = runBlocking {
    whenever(
        internalReportSchedulesMock.stopReportSchedule(
          eq(
            internalStopReportScheduleRequest {
              cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
              externalReportScheduleId = REPORT_SCHEDULE_ID
            }
          )
        )
      )
      .thenReturn(INTERNAL_REPORT_SCHEDULE)

    val request = stopReportScheduleRequest { name = REPORT_SCHEDULE_NAME }

    val reportSchedule =
      withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME, CONFIG) {
        runBlocking { service.stopReportSchedule(request) }
      }

    assertThat(reportSchedule).isEqualTo(REPORT_SCHEDULE)
  }

  @Test
  fun `stopReportSchedule throws NOT_FOUND when report schedule not found`() = runBlocking {
    whenever(
        internalReportSchedulesMock.stopReportSchedule(
          eq(
            internalStopReportScheduleRequest {
              cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
              externalReportScheduleId = REPORT_SCHEDULE_ID
            }
          )
        )
      )
      .thenThrow(StatusRuntimeException(Status.NOT_FOUND))

    val request = stopReportScheduleRequest { name = REPORT_SCHEDULE_NAME }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME, CONFIG) {
          runBlocking { service.stopReportSchedule(request) }
        }
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
    assertThat(exception.message).contains("Unable to stop")
  }

  @Test
  fun `stopReportSchedule throws INVALID_ARGUMENT when name is invalid`() {
    val invalidReportScheduleName =
      "measurementConsumers/$CMMS_MEASUREMENT_CONSUMER_ID/reportSchedules"
    val request = stopReportScheduleRequest { name = invalidReportScheduleName }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME, CONFIG) {
          runBlocking { service.stopReportSchedule(request) }
        }
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.message).contains("name")
  }

  @Test
  fun `stopReportSchedule throws PERMISSION_DENIED when MC's identity does not match`() {
    val request = stopReportScheduleRequest { name = REPORT_SCHEDULE_NAME }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME + 1, CONFIG) {
          runBlocking { service.stopReportSchedule(request) }
        }
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.PERMISSION_DENIED)
    assertThat(exception.message).contains("other MeasurementConsumers")
  }

  @Test
  fun `stopReportSchedule throws UNAUTHENTICATED when the caller is not a MC`() {
    val reportScheduleName = REPORT_SCHEDULE_NAME
    val request = stopReportScheduleRequest { name = reportScheduleName }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withDataProviderPrincipal(DataProviderKey(ExternalId(550L).apiId.value).toName()) {
          runBlocking { service.stopReportSchedule(request) }
        }
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.UNAUTHENTICATED)
  }

  companion object {
    private const val DEFAULT_PAGE_SIZE = 10
    private const val MAX_PAGE_SIZE = 100

    private const val CMMS_MEASUREMENT_CONSUMER_ID = "A123"
    private const val MEASUREMENT_CONSUMER_NAME =
      "measurementConsumers/$CMMS_MEASUREMENT_CONSUMER_ID"

    private const val API_AUTHENTICATION_KEY = "nR5QPN7ptx"
    private val CONFIG = measurementConsumerConfig { apiKey = API_AUTHENTICATION_KEY }

    private const val REPORT_SCHEDULE_ID = "B123"
    private const val REPORT_SCHEDULE_NAME =
      "$MEASUREMENT_CONSUMER_NAME/reportSchedules/$REPORT_SCHEDULE_ID"
    private const val REPORT_SCHEDULE_ID_2 = "B124"
    private const val REPORT_SCHEDULE_NAME_2 =
      "$MEASUREMENT_CONSUMER_NAME/reportSchedules/$REPORT_SCHEDULE_ID_2"

    private const val REPORTING_SET_ID = "C123"
    private const val REPORTING_SET_NAME =
      "$MEASUREMENT_CONSUMER_NAME/reportingSets/$REPORTING_SET_ID"

    private const val EPSILON = 0.0033
    private const val DELTA = 1e-12
    private const val NUMBER_VID_BUCKETS = 300
    private const val VID_SAMPLING_WIDTH = 3.0f / NUMBER_VID_BUCKETS
    private const val VID_SAMPLING_START = 0.0f

    private val METRIC_SPEC: MetricSpec = metricSpec {
      reach =
        MetricSpecKt.reachParams {
          privacyParams =
            MetricSpecKt.differentialPrivacyParams {
              epsilon = EPSILON
              delta = DELTA
            }
        }
      vidSamplingInterval =
        MetricSpecKt.vidSamplingInterval {
          start = VID_SAMPLING_START
          width = VID_SAMPLING_WIDTH
        }
    }

    private val INTERNAL_METRIC_SPEC: InternalMetricSpec = internalMetricSpec {
      reach =
        InternalMetricSpecKt.reachParams {
          privacyParams =
            InternalMetricSpecKt.differentialPrivacyParams {
              epsilon = EPSILON
              delta = DELTA
            }
        }
      vidSamplingInterval =
        InternalMetricSpecKt.vidSamplingInterval {
          start = VID_SAMPLING_START
          width = VID_SAMPLING_WIDTH
        }
    }

    private val INTERNAL_REPORT_SCHEDULE = internalReportSchedule {
      cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
      externalReportScheduleId = REPORT_SCHEDULE_ID
      state = InternalReportSchedule.State.ACTIVE
      details =
        InternalReportScheduleKt.details {
          displayName = "display"
          description = "description"
          reportTemplate = internalReport {
            reportingMetricEntries[REPORTING_SET_ID] =
              InternalReportKt.reportingMetricCalculationSpec {
                metricCalculationSpecs +=
                  InternalReportKt.metricCalculationSpec {
                    details =
                      InternalReportKt.MetricCalculationSpecKt.details {
                        this.displayName = "display-name"
                        metricSpecs += INTERNAL_METRIC_SPEC
                        this.groupings +=
                          InternalReportKt.MetricCalculationSpecKt.grouping {
                            predicates += "gender.value == MALE"
                          }
                        this.cumulative = false
                      }
                  }
              }
          }
          eventStart = dateTime {
            year = 2023
            month = 10
            day = 1
            hours = 6
            timeZone = timeZone { id = "America/New_York" }
          }
          eventEnd = date {
            year = 2024
            month = 12
            day = 1
          }
          frequency =
            InternalReportScheduleKt.frequency {
              daily = InternalReportScheduleKt.FrequencyKt.daily {}
            }
          reportWindow =
            InternalReportScheduleKt.reportWindow {
              trailingWindow =
                InternalReportScheduleKt.ReportWindowKt.trailingWindow {
                  count = 1
                  increment = InternalReportSchedule.ReportWindow.TrailingWindow.Increment.DAY
                }
            }
        }
      nextReportCreationTime = timestamp { seconds = 200 }
      createTime = timestamp { seconds = 50 }
      updateTime = timestamp { seconds = 150 }
    }

    private val REPORT_SCHEDULE = reportSchedule {
      name = REPORT_SCHEDULE_NAME
      displayName = INTERNAL_REPORT_SCHEDULE.details.displayName
      description = INTERNAL_REPORT_SCHEDULE.details.description
      reportTemplate = report {
        reportingMetricEntries +=
          ReportKt.reportingMetricEntry {
            key = REPORTING_SET_NAME
            value =
              ReportKt.reportingMetricCalculationSpec {
                metricCalculationSpecs +=
                  ReportKt.metricCalculationSpec {
                    displayName = "display-name"
                    metricSpecs += METRIC_SPEC
                    this.groupings += ReportKt.grouping { predicates += "gender.value == MALE" }
                    cumulative = false
                  }
              }
          }
      }
      state = ReportSchedule.State.valueOf(INTERNAL_REPORT_SCHEDULE.state.name)
      eventStart = INTERNAL_REPORT_SCHEDULE.details.eventStart
      eventEnd = INTERNAL_REPORT_SCHEDULE.details.eventEnd
      frequency = ReportScheduleKt.frequency { daily = ReportScheduleKt.FrequencyKt.daily {} }
      reportWindow =
        ReportScheduleKt.reportWindow {
          trailingWindow =
            ReportScheduleKt.ReportWindowKt.trailingWindow {
              count = INTERNAL_REPORT_SCHEDULE.details.reportWindow.trailingWindow.count
              increment =
                ReportSchedule.ReportWindow.TrailingWindow.Increment.valueOf(
                  INTERNAL_REPORT_SCHEDULE.details.reportWindow.trailingWindow.increment.name
                )
            }
        }
      nextReportCreationTime = INTERNAL_REPORT_SCHEDULE.nextReportCreationTime
      createTime = INTERNAL_REPORT_SCHEDULE.createTime
      updateTime = INTERNAL_REPORT_SCHEDULE.updateTime
    }

    private val INTERNAL_REPORT_SCHEDULE_2 =
      INTERNAL_REPORT_SCHEDULE.copy { externalReportScheduleId = REPORT_SCHEDULE_ID_2 }

    private val REPORT_SCHEDULE_2 = REPORT_SCHEDULE.copy { name = REPORT_SCHEDULE_NAME_2 }
  }
}
