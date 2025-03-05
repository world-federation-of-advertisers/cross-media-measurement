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
import org.mockito.kotlin.doAnswer
import org.mockito.kotlin.doReturn
import org.mockito.kotlin.eq
import org.mockito.kotlin.whenever
import org.wfanet.measurement.access.client.v1alpha.Authorization
import org.wfanet.measurement.access.client.v1alpha.testing.Authentication.withPrincipalAndScopes
import org.wfanet.measurement.access.client.v1alpha.testing.PrincipalMatcher.Companion.hasPrincipal
import org.wfanet.measurement.access.v1alpha.CheckPermissionsRequest
import org.wfanet.measurement.access.v1alpha.CheckPermissionsResponse
import org.wfanet.measurement.access.v1alpha.PermissionsGrpcKt
import org.wfanet.measurement.access.v1alpha.checkPermissionsResponse
import org.wfanet.measurement.access.v1alpha.copy
import org.wfanet.measurement.access.v1alpha.principal
import org.wfanet.measurement.common.base64UrlEncode
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.common.grpc.testing.mockService
import org.wfanet.measurement.common.testing.verifyProtoArgument
import org.wfanet.measurement.internal.reporting.v2.ListReportScheduleIterationsRequestKt as InternalListReportScheduleIterationsRequestKt
import org.wfanet.measurement.internal.reporting.v2.ReportScheduleIteration as InternalReportScheduleIteration
import org.wfanet.measurement.internal.reporting.v2.ReportScheduleIterationsGrpcKt.ReportScheduleIterationsCoroutineImplBase
import org.wfanet.measurement.internal.reporting.v2.ReportScheduleIterationsGrpcKt.ReportScheduleIterationsCoroutineStub
import org.wfanet.measurement.internal.reporting.v2.copy
import org.wfanet.measurement.internal.reporting.v2.getReportScheduleIterationRequest as internalGetReportScheduleIterationRequest
import org.wfanet.measurement.internal.reporting.v2.listReportScheduleIterationsRequest as internalListReportScheduleIterationsRequest
import org.wfanet.measurement.internal.reporting.v2.listReportScheduleIterationsResponse as internalListReportScheduleIterationsResponse
import org.wfanet.measurement.internal.reporting.v2.reportScheduleIteration as internalReportScheduleIteration
import org.wfanet.measurement.reporting.v2alpha.ListReportScheduleIterationsPageTokenKt
import org.wfanet.measurement.reporting.v2alpha.ReportScheduleIteration
import org.wfanet.measurement.reporting.v2alpha.copy
import org.wfanet.measurement.reporting.v2alpha.getReportScheduleIterationRequest
import org.wfanet.measurement.reporting.v2alpha.listReportScheduleIterationsPageToken
import org.wfanet.measurement.reporting.v2alpha.listReportScheduleIterationsRequest
import org.wfanet.measurement.reporting.v2alpha.listReportScheduleIterationsResponse
import org.wfanet.measurement.reporting.v2alpha.reportScheduleIteration

@RunWith(JUnit4::class)
class ReportScheduleIterationsServiceTest {
  private val internalReportScheduleIterationsMock: ReportScheduleIterationsCoroutineImplBase =
    mockService {
      onBlocking { getReportScheduleIteration(any()) }
        .thenReturn(INTERNAL_REPORT_SCHEDULE_ITERATION)

      onBlocking { listReportScheduleIterations(any()) }
        .thenReturn(
          internalListReportScheduleIterationsResponse {
            reportScheduleIterations += INTERNAL_REPORT_SCHEDULE_ITERATION_LATER_EVENT_TIME
            reportScheduleIterations += INTERNAL_REPORT_SCHEDULE_ITERATION
          }
        )
    }

  private val permissionsServiceMock: PermissionsGrpcKt.PermissionsCoroutineImplBase = mockService {
    onBlocking { checkPermissions(any()) } doReturn CheckPermissionsResponse.getDefaultInstance()

    // Grant all permissions to PRINCIPAL.
    onBlocking { checkPermissions(hasPrincipal(PRINCIPAL.name)) } doAnswer
      { invocation ->
        val request: CheckPermissionsRequest = invocation.getArgument(0)
        checkPermissionsResponse { permissions += request.permissionsList }
      }
  }

  @get:Rule
  val grpcTestServerRule = GrpcTestServerRule {
    addService(internalReportScheduleIterationsMock)
    addService(permissionsServiceMock)
  }

  private lateinit var service: ReportScheduleIterationsService

  @Before
  fun initService() {
    service =
      ReportScheduleIterationsService(
        ReportScheduleIterationsCoroutineStub(grpcTestServerRule.channel),
        Authorization(PermissionsGrpcKt.PermissionsCoroutineStub(grpcTestServerRule.channel)),
      )
  }

  @Test
  fun `getReportScheduleIteration returns iteration when no report name`() = runBlocking {
    whenever(
        internalReportScheduleIterationsMock.getReportScheduleIteration(
          eq(
            internalGetReportScheduleIterationRequest {
              cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
              externalReportScheduleId = REPORT_SCHEDULE_ID
              externalReportScheduleIterationId = REPORT_SCHEDULE_ITERATION_ID
            }
          )
        )
      )
      .thenReturn(INTERNAL_REPORT_SCHEDULE_ITERATION.copy { clearExternalReportId() })

    val request = getReportScheduleIterationRequest { name = REPORT_SCHEDULE_ITERATION_NAME }

    val reportScheduleIteration =
      withPrincipalAndScopes(PRINCIPAL, SCOPES) {
        runBlocking { service.getReportScheduleIteration(request) }
      }

    val expected = REPORT_SCHEDULE_ITERATION.copy { clearReport() }

    assertThat(reportScheduleIteration).isEqualTo(expected)
  }

  @Test
  fun `getReportScheduleIteration returns iteration when there is report name`() = runBlocking {
    val reportId = "r123"
    whenever(
        internalReportScheduleIterationsMock.getReportScheduleIteration(
          eq(
            internalGetReportScheduleIterationRequest {
              cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
              externalReportScheduleId = REPORT_SCHEDULE_ID
              externalReportScheduleIterationId = REPORT_SCHEDULE_ITERATION_ID
            }
          )
        )
      )
      .thenReturn(INTERNAL_REPORT_SCHEDULE_ITERATION.copy { externalReportId = reportId })

    val request = getReportScheduleIterationRequest { name = REPORT_SCHEDULE_ITERATION_NAME }

    val reportScheduleIteration =
      withPrincipalAndScopes(PRINCIPAL, SCOPES) {
        runBlocking { service.getReportScheduleIteration(request) }
      }

    val reportName = "measurementConsumers/$CMMS_MEASUREMENT_CONSUMER_ID/reports/$reportId"
    val expected = REPORT_SCHEDULE_ITERATION.copy { report = reportName }

    assertThat(reportScheduleIteration).isEqualTo(expected)
  }

  @Test
  fun `getReportScheduleIteration throws NOT_FOUND when iteration not found`() = runBlocking {
    whenever(
        internalReportScheduleIterationsMock.getReportScheduleIteration(
          eq(
            internalGetReportScheduleIterationRequest {
              cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
              externalReportScheduleId = REPORT_SCHEDULE_ID
              externalReportScheduleIterationId = REPORT_SCHEDULE_ITERATION_ID
            }
          )
        )
      )
      .thenThrow(StatusRuntimeException(Status.NOT_FOUND))

    val request = getReportScheduleIterationRequest { name = REPORT_SCHEDULE_ITERATION_NAME }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withPrincipalAndScopes(PRINCIPAL, SCOPES) {
          runBlocking { service.getReportScheduleIteration(request) }
        }
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
    assertThat(exception.message).contains("Unable to get")
  }

  @Test
  fun `getReportScheduleIteration throws INVALID_ARGUMENT when name is invalid`() {
    val invalidReportScheduleIterationName =
      "measurementConsumers/$CMMS_MEASUREMENT_CONSUMER_ID/reportSchedules/$REPORT_SCHEDULE_ID"
    val request = getReportScheduleIterationRequest { name = invalidReportScheduleIterationName }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withPrincipalAndScopes(PRINCIPAL, SCOPES) {
          runBlocking { service.getReportScheduleIteration(request) }
        }
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.message).contains("name")
  }

  @Test
  fun `getReportScheduleIteration throws PERMISSION_DENIED when MC's identity does not match`() {
    val request = getReportScheduleIterationRequest { name = REPORT_SCHEDULE_ITERATION_NAME }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withPrincipalAndScopes(PRINCIPAL.copy { name = "principals/mc-2-user" }, SCOPES) {
          runBlocking { service.getReportScheduleIteration(request) }
        }
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.PERMISSION_DENIED)
    assertThat(exception)
      .hasMessageThat()
      .contains(ReportScheduleIterationsService.GET_REPORT_SCHEDULE_PERMISSION)
  }

  @Test
  fun `listReportScheduleIterations returns with next page token when there is another page`() {
    val pageSize = 1
    val request = listReportScheduleIterationsRequest {
      parent = REPORT_SCHEDULE_NAME
      this.pageSize = pageSize
    }

    val result =
      withPrincipalAndScopes(PRINCIPAL, SCOPES) {
        runBlocking { service.listReportScheduleIterations(request) }
      }

    val expected = listReportScheduleIterationsResponse {
      reportScheduleIterations += REPORT_SCHEDULE_ITERATION_LATER_EVENT_TIME
      nextPageToken =
        listReportScheduleIterationsPageToken {
            this.pageSize = pageSize
            cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
            externalReportScheduleId = REPORT_SCHEDULE_ID
            lastReportScheduleIteration =
              ListReportScheduleIterationsPageTokenKt.previousPageEnd {
                cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
                externalReportScheduleId = REPORT_SCHEDULE_ID
                reportEventTime =
                  INTERNAL_REPORT_SCHEDULE_ITERATION_LATER_EVENT_TIME.reportEventTime
              }
          }
          .toByteString()
          .base64UrlEncode()
    }

    assertThat(result).ignoringRepeatedFieldOrder().isEqualTo(expected)

    verifyProtoArgument(
        internalReportScheduleIterationsMock,
        ReportScheduleIterationsCoroutineImplBase::listReportScheduleIterations,
      )
      .isEqualTo(
        internalListReportScheduleIterationsRequest {
          limit = pageSize + 1
          filter =
            InternalListReportScheduleIterationsRequestKt.filter {
              cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
              externalReportScheduleId = REPORT_SCHEDULE_ID
            }
        }
      )
  }

  @Test
  fun `listReportScheduleIterations returns with no next page token when no other page`() {
    val pageSize = 2
    val request = listReportScheduleIterationsRequest {
      parent = REPORT_SCHEDULE_NAME
      this.pageSize = pageSize
    }

    val result =
      withPrincipalAndScopes(PRINCIPAL, SCOPES) {
        runBlocking { service.listReportScheduleIterations(request) }
      }

    val expected = listReportScheduleIterationsResponse {
      reportScheduleIterations += REPORT_SCHEDULE_ITERATION_LATER_EVENT_TIME
      reportScheduleIterations += REPORT_SCHEDULE_ITERATION
    }

    assertThat(result).ignoringRepeatedFieldOrder().isEqualTo(expected)

    verifyProtoArgument(
        internalReportScheduleIterationsMock,
        ReportScheduleIterationsCoroutineImplBase::listReportScheduleIterations,
      )
      .isEqualTo(
        internalListReportScheduleIterationsRequest {
          limit = pageSize + 1
          filter =
            InternalListReportScheduleIterationsRequestKt.filter {
              cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
              externalReportScheduleId = REPORT_SCHEDULE_ID
            }
        }
      )
  }

  @Test
  fun `listReportScheduleIterations succeeds when there is a previous page token`() {
    val pageSize = 2
    val request = listReportScheduleIterationsRequest {
      parent = REPORT_SCHEDULE_NAME
      this.pageSize = pageSize
      pageToken =
        listReportScheduleIterationsPageToken {
            this.pageSize = pageSize
            cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
            externalReportScheduleId = REPORT_SCHEDULE_ID
            lastReportScheduleIteration =
              ListReportScheduleIterationsPageTokenKt.previousPageEnd {
                cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
                externalReportScheduleId = REPORT_SCHEDULE_ID
                reportEventTime = INTERNAL_REPORT_SCHEDULE_ITERATION.reportEventTime
              }
          }
          .toByteString()
          .base64UrlEncode()
    }

    val result =
      withPrincipalAndScopes(PRINCIPAL, SCOPES) {
        runBlocking { service.listReportScheduleIterations(request) }
      }

    val expected = listReportScheduleIterationsResponse {
      reportScheduleIterations += REPORT_SCHEDULE_ITERATION_LATER_EVENT_TIME
      reportScheduleIterations += REPORT_SCHEDULE_ITERATION
    }

    assertThat(result).ignoringRepeatedFieldOrder().isEqualTo(expected)

    verifyProtoArgument(
        internalReportScheduleIterationsMock,
        ReportScheduleIterationsCoroutineImplBase::listReportScheduleIterations,
      )
      .isEqualTo(
        internalListReportScheduleIterationsRequest {
          limit = pageSize + 1
          filter =
            InternalListReportScheduleIterationsRequestKt.filter {
              cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
              externalReportScheduleId = REPORT_SCHEDULE_ID
              reportEventTimeBefore = INTERNAL_REPORT_SCHEDULE_ITERATION.reportEventTime
            }
        }
      )
  }

  @Test
  fun `listReportScheduleIterations with page size too large replaced by max page size`() {
    val pageSize = MAX_PAGE_SIZE + 1
    val request = listReportScheduleIterationsRequest {
      parent = REPORT_SCHEDULE_NAME
      this.pageSize = pageSize
    }

    val result =
      withPrincipalAndScopes(PRINCIPAL, SCOPES) {
        runBlocking { service.listReportScheduleIterations(request) }
      }

    val expected = listReportScheduleIterationsResponse {
      reportScheduleIterations += REPORT_SCHEDULE_ITERATION_LATER_EVENT_TIME
      reportScheduleIterations += REPORT_SCHEDULE_ITERATION
    }

    assertThat(result).ignoringRepeatedFieldOrder().isEqualTo(expected)

    verifyProtoArgument(
        internalReportScheduleIterationsMock,
        ReportScheduleIterationsCoroutineImplBase::listReportScheduleIterations,
      )
      .isEqualTo(
        internalListReportScheduleIterationsRequest {
          limit = MAX_PAGE_SIZE + 1
          filter =
            InternalListReportScheduleIterationsRequestKt.filter {
              cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
              externalReportScheduleId = REPORT_SCHEDULE_ID
            }
        }
      )
  }

  @Test
  fun `listReportScheduleIterations with page size too large replaced by one in prev page token`() {
    val pageSize = MAX_PAGE_SIZE + 1
    val oldPageSize = 5
    val request = listReportScheduleIterationsRequest {
      parent = REPORT_SCHEDULE_NAME
      this.pageSize = pageSize
      pageToken =
        listReportScheduleIterationsPageToken {
            this.pageSize = oldPageSize
            cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
            externalReportScheduleId = REPORT_SCHEDULE_ID
            lastReportScheduleIteration =
              ListReportScheduleIterationsPageTokenKt.previousPageEnd {
                cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
                externalReportScheduleId = REPORT_SCHEDULE_ID
                reportEventTime = INTERNAL_REPORT_SCHEDULE_ITERATION.reportEventTime
              }
          }
          .toByteString()
          .base64UrlEncode()
    }

    val result =
      withPrincipalAndScopes(PRINCIPAL, SCOPES) {
        runBlocking { service.listReportScheduleIterations(request) }
      }

    val expected = listReportScheduleIterationsResponse {
      reportScheduleIterations += REPORT_SCHEDULE_ITERATION_LATER_EVENT_TIME
      reportScheduleIterations += REPORT_SCHEDULE_ITERATION
    }

    assertThat(result).ignoringRepeatedFieldOrder().isEqualTo(expected)

    verifyProtoArgument(
        internalReportScheduleIterationsMock,
        ReportScheduleIterationsCoroutineImplBase::listReportScheduleIterations,
      )
      .isEqualTo(
        internalListReportScheduleIterationsRequest {
          limit = oldPageSize + 1
          filter =
            InternalListReportScheduleIterationsRequestKt.filter {
              cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
              externalReportScheduleId = REPORT_SCHEDULE_ID
              reportEventTimeBefore = INTERNAL_REPORT_SCHEDULE_ITERATION.reportEventTime
            }
        }
      )
  }

  @Test
  fun `listReportScheduleIterations with no page size replaced by default size`() {
    val request = listReportScheduleIterationsRequest { parent = REPORT_SCHEDULE_NAME }

    val result =
      withPrincipalAndScopes(PRINCIPAL, SCOPES) {
        runBlocking { service.listReportScheduleIterations(request) }
      }

    val expected = listReportScheduleIterationsResponse {
      reportScheduleIterations += REPORT_SCHEDULE_ITERATION_LATER_EVENT_TIME
      reportScheduleIterations += REPORT_SCHEDULE_ITERATION
    }

    assertThat(result).ignoringRepeatedFieldOrder().isEqualTo(expected)

    verifyProtoArgument(
        internalReportScheduleIterationsMock,
        ReportScheduleIterationsCoroutineImplBase::listReportScheduleIterations,
      )
      .isEqualTo(
        internalListReportScheduleIterationsRequest {
          limit = DEFAULT_PAGE_SIZE + 1
          filter =
            InternalListReportScheduleIterationsRequestKt.filter {
              cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
              externalReportScheduleId = REPORT_SCHEDULE_ID
            }
        }
      )
  }

  @Test
  fun `listReportScheduleIterations with page size replaces size in page token`() {
    val pageSize = 6
    val oldPageSize = 5
    val request = listReportScheduleIterationsRequest {
      parent = REPORT_SCHEDULE_NAME
      this.pageSize = pageSize
      pageToken =
        listReportScheduleIterationsPageToken {
            this.pageSize = oldPageSize
            cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
            externalReportScheduleId = REPORT_SCHEDULE_ID
            lastReportScheduleIteration =
              ListReportScheduleIterationsPageTokenKt.previousPageEnd {
                cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
                externalReportScheduleId = REPORT_SCHEDULE_ID
                reportEventTime = INTERNAL_REPORT_SCHEDULE_ITERATION.reportEventTime
              }
          }
          .toByteString()
          .base64UrlEncode()
    }

    val result =
      withPrincipalAndScopes(PRINCIPAL, SCOPES) {
        runBlocking { service.listReportScheduleIterations(request) }
      }

    val expected = listReportScheduleIterationsResponse {
      reportScheduleIterations += REPORT_SCHEDULE_ITERATION_LATER_EVENT_TIME
      reportScheduleIterations += REPORT_SCHEDULE_ITERATION
    }

    assertThat(result).ignoringRepeatedFieldOrder().isEqualTo(expected)

    verifyProtoArgument(
        internalReportScheduleIterationsMock,
        ReportScheduleIterationsCoroutineImplBase::listReportScheduleIterations,
      )
      .isEqualTo(
        internalListReportScheduleIterationsRequest {
          limit = pageSize + 1
          filter =
            InternalListReportScheduleIterationsRequestKt.filter {
              cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
              externalReportScheduleId = REPORT_SCHEDULE_ID
              reportEventTimeBefore = INTERNAL_REPORT_SCHEDULE_ITERATION.reportEventTime
            }
        }
      )
  }

  @Test
  fun `listReportScheduleIterations with a filter returns filtered results`() {
    val pageSize = 2
    val request = listReportScheduleIterationsRequest {
      parent = REPORT_SCHEDULE_NAME
      this.pageSize = pageSize
      filter = "name != '$REPORT_SCHEDULE_ITERATION_NAME_2'"
    }

    val result =
      withPrincipalAndScopes(PRINCIPAL, SCOPES) {
        runBlocking { service.listReportScheduleIterations(request) }
      }

    val expected = listReportScheduleIterationsResponse {
      reportScheduleIterations += REPORT_SCHEDULE_ITERATION
    }

    assertThat(result).ignoringRepeatedFieldOrder().isEqualTo(expected)

    verifyProtoArgument(
        internalReportScheduleIterationsMock,
        ReportScheduleIterationsCoroutineImplBase::listReportScheduleIterations,
      )
      .isEqualTo(
        internalListReportScheduleIterationsRequest {
          limit = pageSize + 1
          filter =
            InternalListReportScheduleIterationsRequestKt.filter {
              cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
              externalReportScheduleId = REPORT_SCHEDULE_ID
            }
        }
      )
  }

  @Test
  fun `listReportScheduleIterations throws CANCELLED when cancelled`() = runBlocking {
    whenever(internalReportScheduleIterationsMock.listReportScheduleIterations(any()))
      .thenThrow(StatusRuntimeException(Status.CANCELLED))

    val request = listReportScheduleIterationsRequest { parent = REPORT_SCHEDULE_NAME }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withPrincipalAndScopes(PRINCIPAL, SCOPES) {
          runBlocking { service.listReportScheduleIterations(request) }
        }
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.CANCELLED)
    assertThat(exception.message).contains("Unable to list")
  }

  @Test
  fun `listReportScheduleIterations throws UNAUTHENTICATED when no principal is found`() {
    val request = listReportScheduleIterationsRequest { parent = REPORT_SCHEDULE_NAME }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        runBlocking { service.listReportScheduleIterations(request) }
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.UNAUTHENTICATED)
  }

  @Test
  fun `listReportScheduleIterations throws PERMISSION_DENIED when MC caller doesn't match`() {
    val request = listReportScheduleIterationsRequest {
      parent = "$MEASUREMENT_CONSUMER_NAME/reportSchedules/$REPORT_SCHEDULE_ID"
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withPrincipalAndScopes(PRINCIPAL.copy { name = "principals/mc-2-user" }, SCOPES) {
          runBlocking { service.listReportScheduleIterations(request) }
        }
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.PERMISSION_DENIED)
    assertThat(exception)
      .hasMessageThat()
      .contains(ReportScheduleIterationsService.GET_REPORT_SCHEDULE_PERMISSION)
  }

  @Test
  fun `listReportScheduleIterations throws INVALID_ARGUMENT when page size is less than 0`() {
    val request = listReportScheduleIterationsRequest {
      parent = REPORT_SCHEDULE_NAME
      pageSize = -1
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withPrincipalAndScopes(PRINCIPAL, SCOPES) {
          runBlocking { service.listReportScheduleIterations(request) }
        }
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.message).contains("less than 0")
  }

  @Test
  fun `listReportScheduleIterations throws INVALID_ARGUMENT when parent is unspecified`() {
    val request = listReportScheduleIterationsRequest { pageSize = 1 }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withPrincipalAndScopes(PRINCIPAL, SCOPES) {
          runBlocking { service.listReportScheduleIterations(request) }
        }
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.message).contains("unspecified")
  }

  @Test
  fun `listReportScheduleIterations throws INVALID_ARGUMENT when mc id doesn't match page token`() {
    val pageToken =
      listReportScheduleIterationsPageToken {
          this.pageSize = 2
          cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
          externalReportScheduleId = REPORT_SCHEDULE_ID
          lastReportScheduleIteration =
            ListReportScheduleIterationsPageTokenKt.previousPageEnd {
              cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
              externalReportScheduleId = REPORT_SCHEDULE_ID
              reportEventTime = INTERNAL_REPORT_SCHEDULE_ITERATION.reportEventTime
            }
        }
        .toByteString()
        .base64UrlEncode()

    val request = listReportScheduleIterationsRequest {
      pageSize = 2
      parent =
        "measurementConsumers/${CMMS_MEASUREMENT_CONSUMER_ID + 1}/reportSchedules/$REPORT_SCHEDULE_ID"
      this.pageToken = pageToken
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withPrincipalAndScopes(PRINCIPAL, SCOPES) {
          runBlocking { service.listReportScheduleIterations(request) }
        }
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.message).contains("kept the same")
  }

  @Test
  fun `listReportScheduleIterations throws INVALID_ARGUMENT when sched id doesn't match token`() {
    val pageToken =
      listReportScheduleIterationsPageToken {
          this.pageSize = 2
          cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
          externalReportScheduleId = REPORT_SCHEDULE_ID
          lastReportScheduleIteration =
            ListReportScheduleIterationsPageTokenKt.previousPageEnd {
              cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
              externalReportScheduleId = REPORT_SCHEDULE_ID
              reportEventTime = INTERNAL_REPORT_SCHEDULE_ITERATION.reportEventTime
            }
        }
        .toByteString()
        .base64UrlEncode()

    val request = listReportScheduleIterationsRequest {
      pageSize = 2
      parent =
        "measurementConsumers/$CMMS_MEASUREMENT_CONSUMER_ID/reportSchedules/${REPORT_SCHEDULE_ID + 1}"
      this.pageToken = pageToken
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withPrincipalAndScopes(PRINCIPAL, SCOPES) {
          runBlocking { service.listReportScheduleIterations(request) }
        }
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.message).contains("kept the same")
  }

  @Test
  fun `listReportScheduleIterations throws INVALID_ARGUMENT when filter is not valid CEL`() {
    val request = listReportScheduleIterationsRequest {
      pageSize = 2
      parent = REPORT_SCHEDULE_NAME
      filter = "name >>> 5"
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withPrincipalAndScopes(PRINCIPAL, SCOPES) {
          runBlocking { service.listReportScheduleIterations(request) }
        }
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.message).contains("not a valid CEL")
  }

  companion object {
    private const val DEFAULT_PAGE_SIZE = 50
    private const val MAX_PAGE_SIZE = 1000

    private val PRINCIPAL = principal { name = "principals/mc-user" }
    private val SCOPES = setOf("*")

    private const val CMMS_MEASUREMENT_CONSUMER_ID = "A123"
    private const val MEASUREMENT_CONSUMER_NAME =
      "measurementConsumers/$CMMS_MEASUREMENT_CONSUMER_ID"

    private const val REPORT_SCHEDULE_ID = "B123"
    private const val REPORT_SCHEDULE_NAME =
      "$MEASUREMENT_CONSUMER_NAME/reportSchedules/$REPORT_SCHEDULE_ID"

    private const val REPORT_SCHEDULE_ITERATION_ID = "C123"
    private const val REPORT_SCHEDULE_ITERATION_ID_2 = "C124"
    private const val REPORT_SCHEDULE_ITERATION_NAME =
      "$REPORT_SCHEDULE_NAME/iterations/$REPORT_SCHEDULE_ITERATION_ID"
    private const val REPORT_SCHEDULE_ITERATION_NAME_2 =
      "$REPORT_SCHEDULE_NAME/iterations/$REPORT_SCHEDULE_ITERATION_ID_2"

    private val INTERNAL_REPORT_SCHEDULE_ITERATION = internalReportScheduleIteration {
      cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
      externalReportScheduleId = REPORT_SCHEDULE_ID
      externalReportScheduleIterationId = REPORT_SCHEDULE_ITERATION_ID
      createReportRequestId = "12345"
      state = InternalReportScheduleIteration.State.RETRYING_REPORT_CREATION
      numAttempts = 2
      reportEventTime = timestamp { seconds = 100 }
      createTime = timestamp { seconds = 50 }
      updateTime = timestamp { seconds = 150 }
    }

    private val REPORT_SCHEDULE_ITERATION = reportScheduleIteration {
      name = REPORT_SCHEDULE_ITERATION_NAME
      state =
        ReportScheduleIteration.State.forNumber(INTERNAL_REPORT_SCHEDULE_ITERATION.state.number)
      numAttempts = INTERNAL_REPORT_SCHEDULE_ITERATION.numAttempts
      reportEventTime = INTERNAL_REPORT_SCHEDULE_ITERATION.reportEventTime
      createTime = INTERNAL_REPORT_SCHEDULE_ITERATION.createTime
      updateTime = INTERNAL_REPORT_SCHEDULE_ITERATION.updateTime
    }

    private val INTERNAL_REPORT_SCHEDULE_ITERATION_LATER_EVENT_TIME =
      internalReportScheduleIteration {
        cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
        externalReportScheduleId = REPORT_SCHEDULE_ID
        externalReportScheduleIterationId = REPORT_SCHEDULE_ITERATION_ID_2
        createReportRequestId = "12346"
        state = InternalReportScheduleIteration.State.RETRYING_REPORT_CREATION
        numAttempts = 2
        reportEventTime = timestamp { seconds = 150 }
        createTime = timestamp { seconds = 50 }
        updateTime = timestamp { seconds = 150 }
      }

    private val REPORT_SCHEDULE_ITERATION_LATER_EVENT_TIME = reportScheduleIteration {
      name = REPORT_SCHEDULE_ITERATION_NAME_2
      state =
        ReportScheduleIteration.State.forNumber(
          INTERNAL_REPORT_SCHEDULE_ITERATION_LATER_EVENT_TIME.state.number
        )
      numAttempts = INTERNAL_REPORT_SCHEDULE_ITERATION_LATER_EVENT_TIME.numAttempts
      reportEventTime = INTERNAL_REPORT_SCHEDULE_ITERATION_LATER_EVENT_TIME.reportEventTime
      createTime = INTERNAL_REPORT_SCHEDULE_ITERATION_LATER_EVENT_TIME.createTime
      updateTime = INTERNAL_REPORT_SCHEDULE_ITERATION_LATER_EVENT_TIME.updateTime
    }
  }
}
