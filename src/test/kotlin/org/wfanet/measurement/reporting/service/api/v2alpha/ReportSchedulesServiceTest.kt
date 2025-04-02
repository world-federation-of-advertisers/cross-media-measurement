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
import com.google.protobuf.duration
import com.google.protobuf.timestamp
import com.google.type.DayOfWeek
import com.google.type.copy
import com.google.type.date
import com.google.type.dateTime
import com.google.type.interval
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
import org.mockito.kotlin.and
import org.mockito.kotlin.any
import org.mockito.kotlin.doAnswer
import org.mockito.kotlin.doReturn
import org.mockito.kotlin.eq
import org.mockito.kotlin.reset
import org.mockito.kotlin.whenever
import org.mockito.kotlin.wheneverBlocking
import org.wfanet.measurement.access.client.v1alpha.Authorization
import org.wfanet.measurement.access.client.v1alpha.testing.Authentication.withPrincipalAndScopes
import org.wfanet.measurement.access.client.v1alpha.testing.PermissionMatcher.Companion.hasPermissionId
import org.wfanet.measurement.access.client.v1alpha.testing.PrincipalMatcher.Companion.hasPrincipal
import org.wfanet.measurement.access.client.v1alpha.testing.ProtectedResourceMatcher.Companion.hasProtectedResource
import org.wfanet.measurement.access.service.PermissionKey
import org.wfanet.measurement.access.v1alpha.CheckPermissionsRequest
import org.wfanet.measurement.access.v1alpha.CheckPermissionsResponse
import org.wfanet.measurement.access.v1alpha.PermissionsGrpcKt
import org.wfanet.measurement.access.v1alpha.checkPermissionsResponse
import org.wfanet.measurement.access.v1alpha.copy
import org.wfanet.measurement.access.v1alpha.principal
import org.wfanet.measurement.api.v2alpha.DataProvidersGrpcKt.DataProvidersCoroutineImplBase
import org.wfanet.measurement.api.v2alpha.DataProvidersGrpcKt.DataProvidersCoroutineStub
import org.wfanet.measurement.api.v2alpha.EventGroupsGrpcKt.EventGroupsCoroutineImplBase
import org.wfanet.measurement.api.v2alpha.EventGroupsGrpcKt.EventGroupsCoroutineStub
import org.wfanet.measurement.api.v2alpha.copy
import org.wfanet.measurement.api.v2alpha.dataProvider
import org.wfanet.measurement.api.v2alpha.eventGroup
import org.wfanet.measurement.api.v2alpha.getDataProviderRequest
import org.wfanet.measurement.api.v2alpha.getEventGroupRequest
import org.wfanet.measurement.common.base64UrlEncode
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.common.grpc.testing.mockService
import org.wfanet.measurement.common.testing.verifyProtoArgument
import org.wfanet.measurement.config.reporting.measurementConsumerConfig
import org.wfanet.measurement.config.reporting.measurementConsumerConfigs
import org.wfanet.measurement.internal.reporting.v2.BatchGetReportingSetsRequest
import org.wfanet.measurement.internal.reporting.v2.ListReportSchedulesRequestKt as InternalListReportSchedulesRequestKt
import org.wfanet.measurement.internal.reporting.v2.MetricCalculationSpec
import org.wfanet.measurement.internal.reporting.v2.MetricCalculationSpecKt
import org.wfanet.measurement.internal.reporting.v2.MetricSpec as InternalMetricSpec
import org.wfanet.measurement.internal.reporting.v2.MetricSpecKt as InternalMetricSpecKt
import org.wfanet.measurement.internal.reporting.v2.ReportKt as InternalReportKt
import org.wfanet.measurement.internal.reporting.v2.ReportSchedule as InternalReportSchedule
import org.wfanet.measurement.internal.reporting.v2.ReportScheduleKt as InternalReportScheduleKt
import org.wfanet.measurement.internal.reporting.v2.ReportSchedulesGrpcKt.ReportSchedulesCoroutineImplBase
import org.wfanet.measurement.internal.reporting.v2.ReportSchedulesGrpcKt.ReportSchedulesCoroutineStub
import org.wfanet.measurement.internal.reporting.v2.ReportingSet
import org.wfanet.measurement.internal.reporting.v2.ReportingSetKt
import org.wfanet.measurement.internal.reporting.v2.ReportingSetsGrpcKt.ReportingSetsCoroutineImplBase
import org.wfanet.measurement.internal.reporting.v2.ReportingSetsGrpcKt.ReportingSetsCoroutineStub
import org.wfanet.measurement.internal.reporting.v2.batchGetReportingSetsRequest
import org.wfanet.measurement.internal.reporting.v2.batchGetReportingSetsResponse
import org.wfanet.measurement.internal.reporting.v2.copy
import org.wfanet.measurement.internal.reporting.v2.createReportScheduleRequest as internalCreateReportScheduleRequest
import org.wfanet.measurement.internal.reporting.v2.getReportScheduleRequest as internalGetReportScheduleRequest
import org.wfanet.measurement.internal.reporting.v2.listReportSchedulesRequest as internalListReportSchedulesRequest
import org.wfanet.measurement.internal.reporting.v2.listReportSchedulesResponse as internalListReportSchedulesResponse
import org.wfanet.measurement.internal.reporting.v2.metricCalculationSpec
import org.wfanet.measurement.internal.reporting.v2.metricSpec as internalMetricSpec
import org.wfanet.measurement.internal.reporting.v2.report as internalReport
import org.wfanet.measurement.internal.reporting.v2.reportSchedule as internalReportSchedule
import org.wfanet.measurement.internal.reporting.v2.reportingSet
import org.wfanet.measurement.internal.reporting.v2.stopReportScheduleRequest as internalStopReportScheduleRequest
import org.wfanet.measurement.reporting.v2alpha.ListReportSchedulesPageTokenKt
import org.wfanet.measurement.reporting.v2alpha.Report
import org.wfanet.measurement.reporting.v2alpha.ReportKt
import org.wfanet.measurement.reporting.v2alpha.ReportSchedule
import org.wfanet.measurement.reporting.v2alpha.ReportSchedule.ReportWindow
import org.wfanet.measurement.reporting.v2alpha.ReportScheduleKt
import org.wfanet.measurement.reporting.v2alpha.copy
import org.wfanet.measurement.reporting.v2alpha.createReportScheduleRequest
import org.wfanet.measurement.reporting.v2alpha.getReportScheduleRequest
import org.wfanet.measurement.reporting.v2alpha.listReportSchedulesPageToken
import org.wfanet.measurement.reporting.v2alpha.listReportSchedulesRequest
import org.wfanet.measurement.reporting.v2alpha.listReportSchedulesResponse
import org.wfanet.measurement.reporting.v2alpha.report
import org.wfanet.measurement.reporting.v2alpha.reportSchedule
import org.wfanet.measurement.reporting.v2alpha.stopReportScheduleRequest

@RunWith(JUnit4::class)
class ReportSchedulesServiceTest {
  private val internalReportSchedulesMock: ReportSchedulesCoroutineImplBase = mockService {
    onBlocking { createReportSchedule(any()) }.thenReturn(INTERNAL_REPORT_SCHEDULE)

    onBlocking { getReportSchedule(any()) }.thenReturn(INTERNAL_REPORT_SCHEDULE)

    onBlocking { listReportSchedules(any()) }
      .thenReturn(
        internalListReportSchedulesResponse {
          reportSchedules += INTERNAL_REPORT_SCHEDULE
          reportSchedules += INTERNAL_REPORT_SCHEDULE_2
        }
      )

    onBlocking { stopReportSchedule(any()) }
      .thenReturn(INTERNAL_REPORT_SCHEDULE.copy { state = InternalReportSchedule.State.STOPPED })
  }

  private val internalReportingSetsMock: ReportingSetsCoroutineImplBase = mockService {
    onBlocking { batchGetReportingSets(any()) }
      .thenAnswer {
        val request = it.arguments[0] as BatchGetReportingSetsRequest
        val reportingSetsMap =
          mapOf(
            INTERNAL_PRIMITIVE_REPORTING_SET.externalReportingSetId to
              INTERNAL_PRIMITIVE_REPORTING_SET
          )
        batchGetReportingSetsResponse {
          reportingSets +=
            request.externalReportingSetIdsList.map { id -> reportingSetsMap.getValue(id) }
        }
      }
  }

  private val dataProvidersMock: DataProvidersCoroutineImplBase = mockService {
    onBlocking { getDataProvider(eq(getDataProviderRequest { name = DATA_PROVIDER_NAME })) }
      .thenReturn(DATA_PROVIDER)
  }

  private val eventGroupsMock: EventGroupsCoroutineImplBase = mockService {
    onBlocking { getEventGroup(eq(getEventGroupRequest { name = EVENT_GROUP_NAME })) }
      .thenReturn(EVENT_GROUP)
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
    addService(internalReportSchedulesMock)
    addService(internalReportingSetsMock)
    addService(dataProvidersMock)
    addService(eventGroupsMock)
    addService(permissionsServiceMock)
  }

  private lateinit var service: ReportSchedulesService

  @Before
  fun initService() {
    service =
      ReportSchedulesService(
        ReportSchedulesCoroutineStub(grpcTestServerRule.channel),
        ReportingSetsCoroutineStub(grpcTestServerRule.channel),
        DataProvidersCoroutineStub(grpcTestServerRule.channel),
        EventGroupsCoroutineStub(grpcTestServerRule.channel),
        Authorization(PermissionsGrpcKt.PermissionsCoroutineStub(grpcTestServerRule.channel)),
        CONFIGS,
      )
  }

  @Test
  fun `createReportSchedule returns report schedule when daily frequency`() = runBlocking {
    val internalReportSchedule = internalReportSchedule {
      cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
      externalReportScheduleId = REPORT_SCHEDULE_ID
      state = InternalReportSchedule.State.ACTIVE
      details =
        InternalReportScheduleKt.details {
          displayName = "display"
          description = "description"
          reportTemplate = internalReport {
            reportingMetricEntries[INTERNAL_PRIMITIVE_REPORTING_SET.externalReportingSetId] =
              InternalReportKt.reportingMetricCalculationSpec {
                metricCalculationSpecReportingMetrics +=
                  InternalReportKt.metricCalculationSpecReportingMetrics {
                    externalMetricCalculationSpecId =
                      INTERNAL_METRIC_CALCULATION_SPEC.externalMetricCalculationSpecId
                  }
              }
            details = InternalReportKt.details {}
          }
          eventStart = dateTime {
            year = 3000
            month = 1
            day = 1
            hours = 13
            timeZone = timeZone { id = "America/Los_Angeles" }
          }
          eventEnd = date {
            year = 3001
            month = 12
            day = 1
          }
          frequency =
            InternalReportScheduleKt.frequency {
              daily = InternalReportSchedule.Frequency.Daily.getDefaultInstance()
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
      nextReportCreationTime = timestamp {
        seconds = 32503755600 // Wednesday, January 1, 3000 at 1 PM, America/Los_Angeles
      }
      createTime = timestamp { seconds = 50 }
      updateTime = timestamp { seconds = 150 }
    }

    whenever(internalReportSchedulesMock.createReportSchedule(any()))
      .thenReturn(internalReportSchedule)

    val reportSchedule = reportSchedule {
      displayName = internalReportSchedule.details.displayName
      description = internalReportSchedule.details.description
      reportTemplate = report {
        reportingMetricEntries +=
          ReportKt.reportingMetricEntry {
            key = PRIMITIVE_REPORTING_SET_NAME
            value =
              ReportKt.reportingMetricCalculationSpec {
                metricCalculationSpecs += METRIC_CALCULATION_SPEC_NAME
              }
          }
      }
      eventStart = internalReportSchedule.details.eventStart
      eventEnd = internalReportSchedule.details.eventEnd
      frequency =
        ReportScheduleKt.frequency { daily = ReportSchedule.Frequency.Daily.getDefaultInstance() }
      reportWindow =
        ReportScheduleKt.reportWindow {
          trailingWindow =
            ReportScheduleKt.ReportWindowKt.trailingWindow {
              count = internalReportSchedule.details.reportWindow.trailingWindow.count
              increment =
                ReportWindow.TrailingWindow.Increment.forNumber(
                  internalReportSchedule.details.reportWindow.trailingWindow.increment.number
                )
            }
        }
    }

    val request = createReportScheduleRequest {
      parent = MEASUREMENT_CONSUMER_NAME
      this.reportSchedule = reportSchedule
      reportScheduleId = REPORT_SCHEDULE_ID
    }

    val createdReportSchedule =
      withPrincipalAndScopes(PRINCIPAL, SCOPES) {
        runBlocking { service.createReportSchedule(request) }
      }

    assertThat(createdReportSchedule)
      .isEqualTo(
        reportSchedule.copy {
          name = REPORT_SCHEDULE_NAME
          state = ReportSchedule.State.ACTIVE
          nextReportCreationTime = internalReportSchedule.nextReportCreationTime
          createTime = internalReportSchedule.createTime
          updateTime = internalReportSchedule.updateTime
        }
      )

    verifyProtoArgument(
        internalReportSchedulesMock,
        ReportSchedulesCoroutineImplBase::createReportSchedule,
      )
      .isEqualTo(
        internalCreateReportScheduleRequest {
          this.reportSchedule = internalReportSchedule {
            cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
            nextReportCreationTime = internalReportSchedule.nextReportCreationTime
            details =
              InternalReportScheduleKt.details {
                displayName = internalReportSchedule.details.displayName
                description = internalReportSchedule.details.description
                reportTemplate = internalReportSchedule.details.reportTemplate
                eventStart = internalReportSchedule.details.eventStart
                eventEnd = internalReportSchedule.details.eventEnd
                frequency = internalReportSchedule.details.frequency
                reportWindow = internalReportSchedule.details.reportWindow
              }
          }
          externalReportScheduleId = REPORT_SCHEDULE_ID
        }
      )

    verifyProtoArgument(
        internalReportingSetsMock,
        ReportingSetsCoroutineImplBase::batchGetReportingSets,
      )
      .isEqualTo(
        batchGetReportingSetsRequest {
          cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
          externalReportingSetIds += PRIMITIVE_REPORTING_SET_ID
        }
      )

    verifyProtoArgument(dataProvidersMock, DataProvidersCoroutineImplBase::getDataProvider)
      .isEqualTo(getDataProviderRequest { name = DATA_PROVIDER_NAME })

    verifyProtoArgument(eventGroupsMock, EventGroupsCoroutineImplBase::getEventGroup)
      .isEqualTo(getEventGroupRequest { name = EVENT_GROUP_NAME })
  }

  @Test
  fun `createReportSchedule sets next report to start when daily frequency and offset`() =
    runBlocking {
      val eventStart = dateTime {
        year = 3000
        month = 1
        day = 1
        hours = 13
        utcOffset = duration { seconds = -28800 }
      }
      val eventEnd = date {
        year = 3001
        month = 12
        day = 1
      }

      val nextReportCreationTime = timestamp {
        seconds = 32503755600 // Wednesday, January 1, 3000 at 1 PM, America/Los_Angeles
      }

      whenever(internalReportSchedulesMock.createReportSchedule(any()))
        .thenReturn(
          INTERNAL_REPORT_SCHEDULE.copy {
            details =
              INTERNAL_REPORT_SCHEDULE.details.copy {
                this.eventStart = eventStart
                this.eventEnd = eventEnd
                frequency =
                  InternalReportScheduleKt.frequency {
                    weekly =
                      InternalReportScheduleKt.FrequencyKt.weekly {
                        dayOfWeek = DayOfWeek.WEDNESDAY
                      }
                  }
              }
            this.nextReportCreationTime = nextReportCreationTime
          }
        )

      val reportSchedule =
        REPORT_SCHEDULE.copy {
          this.eventStart = eventStart
          this.eventEnd = eventEnd
          frequency =
            ReportScheduleKt.frequency {
              weekly = ReportScheduleKt.FrequencyKt.weekly { dayOfWeek = DayOfWeek.WEDNESDAY }
            }
        }

      val request = createReportScheduleRequest {
        parent = MEASUREMENT_CONSUMER_NAME
        this.reportSchedule = reportSchedule
        reportScheduleId = REPORT_SCHEDULE_ID
      }

      val createdReportSchedule =
        withPrincipalAndScopes(PRINCIPAL, SCOPES) {
          runBlocking { service.createReportSchedule(request) }
        }

      assertThat(createdReportSchedule)
        .isEqualTo(
          reportSchedule.copy {
            name = REPORT_SCHEDULE_NAME
            state = ReportSchedule.State.ACTIVE
            this.nextReportCreationTime = nextReportCreationTime
            createTime = INTERNAL_REPORT_SCHEDULE.createTime
            updateTime = INTERNAL_REPORT_SCHEDULE.updateTime
          }
        )

      verifyProtoArgument(
          internalReportSchedulesMock,
          ReportSchedulesCoroutineImplBase::createReportSchedule,
        )
        .isEqualTo(
          internalCreateReportScheduleRequest {
            this.reportSchedule = internalReportSchedule {
              cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
              this.nextReportCreationTime = nextReportCreationTime
              details =
                InternalReportScheduleKt.details {
                  displayName = INTERNAL_REPORT_SCHEDULE.details.displayName
                  description = INTERNAL_REPORT_SCHEDULE.details.description
                  reportTemplate = INTERNAL_REPORT_SCHEDULE.details.reportTemplate
                  this.eventStart = eventStart
                  this.eventEnd = eventEnd
                  frequency =
                    InternalReportScheduleKt.frequency {
                      weekly =
                        InternalReportScheduleKt.FrequencyKt.weekly {
                          dayOfWeek = DayOfWeek.WEDNESDAY
                        }
                    }
                  reportWindow = INTERNAL_REPORT_SCHEDULE.details.reportWindow
                }
            }
            externalReportScheduleId = REPORT_SCHEDULE_ID
          }
        )
    }

  @Test
  fun `createReportSchedule sets next report time to start when day of week same as start`() =
    runBlocking {
      val eventStart = dateTime {
        year = 3000
        month = 1
        day = 1
        hours = 13
        timeZone = timeZone { id = "America/Los_Angeles" }
      }
      val eventEnd = date {
        year = 3001
        month = 12
        day = 1
      }

      val nextReportCreationTime = timestamp {
        seconds = 32503755600 // Wednesday, January 1, 3000 at 1 PM, America/Los_Angeles
      }

      whenever(internalReportSchedulesMock.createReportSchedule(any()))
        .thenReturn(
          INTERNAL_REPORT_SCHEDULE.copy {
            details =
              INTERNAL_REPORT_SCHEDULE.details.copy {
                this.eventStart = eventStart
                this.eventEnd = eventEnd
                frequency =
                  InternalReportScheduleKt.frequency {
                    weekly =
                      InternalReportScheduleKt.FrequencyKt.weekly {
                        dayOfWeek = DayOfWeek.WEDNESDAY
                      }
                  }
              }
            this.nextReportCreationTime = nextReportCreationTime
          }
        )

      val reportSchedule =
        REPORT_SCHEDULE.copy {
          this.eventStart = eventStart
          this.eventEnd = eventEnd
          frequency =
            ReportScheduleKt.frequency {
              weekly = ReportScheduleKt.FrequencyKt.weekly { dayOfWeek = DayOfWeek.WEDNESDAY }
            }
        }

      val request = createReportScheduleRequest {
        parent = MEASUREMENT_CONSUMER_NAME
        this.reportSchedule = reportSchedule
        reportScheduleId = REPORT_SCHEDULE_ID
      }

      val createdReportSchedule =
        withPrincipalAndScopes(PRINCIPAL, SCOPES) {
          runBlocking { service.createReportSchedule(request) }
        }

      assertThat(createdReportSchedule)
        .isEqualTo(
          reportSchedule.copy {
            name = REPORT_SCHEDULE_NAME
            state = ReportSchedule.State.ACTIVE
            this.nextReportCreationTime = nextReportCreationTime
            createTime = INTERNAL_REPORT_SCHEDULE.createTime
            updateTime = INTERNAL_REPORT_SCHEDULE.updateTime
          }
        )

      verifyProtoArgument(
          internalReportSchedulesMock,
          ReportSchedulesCoroutineImplBase::createReportSchedule,
        )
        .isEqualTo(
          internalCreateReportScheduleRequest {
            this.reportSchedule = internalReportSchedule {
              cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
              this.nextReportCreationTime = nextReportCreationTime
              details =
                InternalReportScheduleKt.details {
                  displayName = INTERNAL_REPORT_SCHEDULE.details.displayName
                  description = INTERNAL_REPORT_SCHEDULE.details.description
                  reportTemplate = INTERNAL_REPORT_SCHEDULE.details.reportTemplate
                  this.eventStart = eventStart
                  this.eventEnd = eventEnd
                  frequency =
                    InternalReportScheduleKt.frequency {
                      weekly =
                        InternalReportScheduleKt.FrequencyKt.weekly {
                          dayOfWeek = DayOfWeek.WEDNESDAY
                        }
                    }
                  reportWindow = INTERNAL_REPORT_SCHEDULE.details.reportWindow
                }
            }
            externalReportScheduleId = REPORT_SCHEDULE_ID
          }
        )
    }

  @Test
  fun `createReportSchedule sets next report to after start when day of week not same as start`() =
    runBlocking {
      val eventStart = dateTime {
        year = 2999
        month = 12
        day = 31
        hours = 13
        timeZone = timeZone { id = "America/Los_Angeles" }
      }
      val eventEnd = date {
        year = 3001
        month = 12
        day = 1
      }

      val nextReportCreationTime = timestamp {
        seconds = 32503755600 // Wednesday, January 1, 3000 at 1 PM, America/Los_Angeles
      }

      whenever(internalReportSchedulesMock.createReportSchedule(any()))
        .thenReturn(
          INTERNAL_REPORT_SCHEDULE.copy {
            details =
              INTERNAL_REPORT_SCHEDULE.details.copy {
                this.eventStart = eventStart
                this.eventEnd = eventEnd
                frequency =
                  InternalReportScheduleKt.frequency {
                    weekly =
                      InternalReportScheduleKt.FrequencyKt.weekly {
                        dayOfWeek = DayOfWeek.WEDNESDAY
                      }
                  }
              }
            this.nextReportCreationTime = nextReportCreationTime
          }
        )

      val reportSchedule =
        REPORT_SCHEDULE.copy {
          this.eventStart = eventStart
          this.eventEnd = eventEnd
          frequency =
            ReportScheduleKt.frequency {
              weekly = ReportScheduleKt.FrequencyKt.weekly { dayOfWeek = DayOfWeek.WEDNESDAY }
            }
        }

      val request = createReportScheduleRequest {
        parent = MEASUREMENT_CONSUMER_NAME
        this.reportSchedule = reportSchedule
        reportScheduleId = REPORT_SCHEDULE_ID
      }

      val createdReportSchedule =
        withPrincipalAndScopes(PRINCIPAL, SCOPES) {
          runBlocking { service.createReportSchedule(request) }
        }

      assertThat(createdReportSchedule)
        .isEqualTo(
          reportSchedule.copy {
            name = REPORT_SCHEDULE_NAME
            state = ReportSchedule.State.ACTIVE
            this.nextReportCreationTime = nextReportCreationTime
            createTime = INTERNAL_REPORT_SCHEDULE.createTime
            updateTime = INTERNAL_REPORT_SCHEDULE.updateTime
          }
        )

      verifyProtoArgument(
          internalReportSchedulesMock,
          ReportSchedulesCoroutineImplBase::createReportSchedule,
        )
        .isEqualTo(
          internalCreateReportScheduleRequest {
            this.reportSchedule = internalReportSchedule {
              cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
              this.nextReportCreationTime = nextReportCreationTime
              details =
                InternalReportScheduleKt.details {
                  displayName = INTERNAL_REPORT_SCHEDULE.details.displayName
                  description = INTERNAL_REPORT_SCHEDULE.details.description
                  reportTemplate = INTERNAL_REPORT_SCHEDULE.details.reportTemplate
                  this.eventStart = eventStart
                  this.eventEnd = eventEnd
                  frequency =
                    InternalReportScheduleKt.frequency {
                      weekly =
                        InternalReportScheduleKt.FrequencyKt.weekly {
                          dayOfWeek = DayOfWeek.WEDNESDAY
                        }
                    }
                  reportWindow = INTERNAL_REPORT_SCHEDULE.details.reportWindow
                }
            }
            externalReportScheduleId = REPORT_SCHEDULE_ID
          }
        )
    }

  @Test
  fun `createReportSchedule sets next report time to start when day of month same as start`() =
    runBlocking {
      val eventStart = dateTime {
        year = 3000
        month = 1
        day = 1
        hours = 13
        timeZone = timeZone { id = "America/Los_Angeles" }
      }
      val eventEnd = date {
        year = 3001
        month = 12
        day = 1
      }

      val nextReportCreationTime = timestamp {
        seconds = 32503755600 // Wednesday, January 1, 3000 at 1 PM, America/Los_Angeles
      }

      whenever(internalReportSchedulesMock.createReportSchedule(any()))
        .thenReturn(
          INTERNAL_REPORT_SCHEDULE.copy {
            details =
              INTERNAL_REPORT_SCHEDULE.details.copy {
                this.eventStart = eventStart
                this.eventEnd = eventEnd
                frequency =
                  InternalReportScheduleKt.frequency {
                    monthly = InternalReportScheduleKt.FrequencyKt.monthly { dayOfMonth = 1 }
                  }
              }
            this.nextReportCreationTime = nextReportCreationTime
          }
        )

      val reportSchedule =
        REPORT_SCHEDULE.copy {
          this.eventStart = eventStart
          this.eventEnd = eventEnd
          frequency =
            ReportScheduleKt.frequency {
              monthly = ReportScheduleKt.FrequencyKt.monthly { dayOfMonth = 1 }
            }
        }

      val request = createReportScheduleRequest {
        parent = MEASUREMENT_CONSUMER_NAME
        this.reportSchedule = reportSchedule
        reportScheduleId = REPORT_SCHEDULE_ID
      }

      val createdReportSchedule =
        withPrincipalAndScopes(PRINCIPAL, SCOPES) {
          runBlocking { service.createReportSchedule(request) }
        }

      assertThat(createdReportSchedule)
        .isEqualTo(
          reportSchedule.copy {
            name = REPORT_SCHEDULE_NAME
            state = ReportSchedule.State.ACTIVE
            this.nextReportCreationTime = nextReportCreationTime
            createTime = INTERNAL_REPORT_SCHEDULE.createTime
            updateTime = INTERNAL_REPORT_SCHEDULE.updateTime
          }
        )

      verifyProtoArgument(
          internalReportSchedulesMock,
          ReportSchedulesCoroutineImplBase::createReportSchedule,
        )
        .isEqualTo(
          internalCreateReportScheduleRequest {
            this.reportSchedule = internalReportSchedule {
              cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
              this.nextReportCreationTime = nextReportCreationTime
              details =
                InternalReportScheduleKt.details {
                  displayName = INTERNAL_REPORT_SCHEDULE.details.displayName
                  description = INTERNAL_REPORT_SCHEDULE.details.description
                  reportTemplate = INTERNAL_REPORT_SCHEDULE.details.reportTemplate
                  this.eventStart = eventStart
                  this.eventEnd = eventEnd
                  frequency =
                    InternalReportScheduleKt.frequency {
                      monthly = InternalReportScheduleKt.FrequencyKt.monthly { dayOfMonth = 1 }
                    }
                  reportWindow = INTERNAL_REPORT_SCHEDULE.details.reportWindow
                }
            }
            externalReportScheduleId = REPORT_SCHEDULE_ID
          }
        )
    }

  @Test
  fun `createReportSchedule sets next after start when day of month after start in same month`() =
    runBlocking {
      val eventStart = dateTime {
        year = 3000
        month = 1
        day = 1
        hours = 13
        timeZone = timeZone { id = "America/Los_Angeles" }
      }
      val eventEnd = date {
        year = 3001
        month = 12
        day = 1
      }

      val nextReportCreationTime = timestamp {
        seconds = 32505397200 // Monday, January 20, 3000 at 1 PM, America/Los_Angeles
      }

      whenever(internalReportSchedulesMock.createReportSchedule(any()))
        .thenReturn(
          INTERNAL_REPORT_SCHEDULE.copy {
            details =
              INTERNAL_REPORT_SCHEDULE.details.copy {
                this.eventStart = eventStart
                this.eventEnd = eventEnd
                frequency =
                  InternalReportScheduleKt.frequency {
                    monthly = InternalReportScheduleKt.FrequencyKt.monthly { dayOfMonth = 20 }
                  }
              }
            this.nextReportCreationTime = nextReportCreationTime
          }
        )

      val reportSchedule =
        REPORT_SCHEDULE.copy {
          this.eventStart = eventStart
          this.eventEnd = eventEnd
          frequency =
            ReportScheduleKt.frequency {
              monthly = ReportScheduleKt.FrequencyKt.monthly { dayOfMonth = 20 }
            }
        }

      val request = createReportScheduleRequest {
        parent = MEASUREMENT_CONSUMER_NAME
        this.reportSchedule = reportSchedule
        reportScheduleId = REPORT_SCHEDULE_ID
      }

      val createdReportSchedule =
        withPrincipalAndScopes(PRINCIPAL, SCOPES) {
          runBlocking { service.createReportSchedule(request) }
        }

      assertThat(createdReportSchedule)
        .isEqualTo(
          reportSchedule.copy {
            name = REPORT_SCHEDULE_NAME
            state = ReportSchedule.State.ACTIVE
            this.nextReportCreationTime = nextReportCreationTime
            createTime = INTERNAL_REPORT_SCHEDULE.createTime
            updateTime = INTERNAL_REPORT_SCHEDULE.updateTime
          }
        )

      verifyProtoArgument(
          internalReportSchedulesMock,
          ReportSchedulesCoroutineImplBase::createReportSchedule,
        )
        .isEqualTo(
          internalCreateReportScheduleRequest {
            this.reportSchedule = internalReportSchedule {
              cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
              this.nextReportCreationTime = nextReportCreationTime
              details =
                InternalReportScheduleKt.details {
                  displayName = INTERNAL_REPORT_SCHEDULE.details.displayName
                  description = INTERNAL_REPORT_SCHEDULE.details.description
                  reportTemplate = INTERNAL_REPORT_SCHEDULE.details.reportTemplate
                  this.eventStart = eventStart
                  this.eventEnd = eventEnd
                  frequency =
                    InternalReportScheduleKt.frequency {
                      monthly = InternalReportScheduleKt.FrequencyKt.monthly { dayOfMonth = 20 }
                    }
                  reportWindow = INTERNAL_REPORT_SCHEDULE.details.reportWindow
                }
            }
            externalReportScheduleId = REPORT_SCHEDULE_ID
          }
        )
    }

  @Test
  fun `createReportSchedule sets next after start when day of month after start in next month`() =
    runBlocking {
      val eventStart = dateTime {
        year = 3000
        month = 1
        day = 31
        hours = 13
        timeZone = timeZone { id = "America/Los_Angeles" }
      }
      val eventEnd = date {
        year = 3001
        month = 12
        day = 1
      }

      val nextReportCreationTime = timestamp {
        seconds = 32506434000 // Saturday, February 1, 3000 at 1 PM, America/Los_Angeles
      }

      whenever(internalReportSchedulesMock.createReportSchedule(any()))
        .thenReturn(
          INTERNAL_REPORT_SCHEDULE.copy {
            details =
              INTERNAL_REPORT_SCHEDULE.details.copy {
                this.eventStart = eventStart
                this.eventEnd = eventEnd
                frequency =
                  InternalReportScheduleKt.frequency {
                    monthly = InternalReportScheduleKt.FrequencyKt.monthly { dayOfMonth = 1 }
                  }
              }
            this.nextReportCreationTime = nextReportCreationTime
          }
        )

      val reportSchedule =
        REPORT_SCHEDULE.copy {
          this.eventStart = eventStart
          this.eventEnd = eventEnd
          frequency =
            ReportScheduleKt.frequency {
              monthly = ReportScheduleKt.FrequencyKt.monthly { dayOfMonth = 1 }
            }
        }

      val request = createReportScheduleRequest {
        parent = MEASUREMENT_CONSUMER_NAME
        this.reportSchedule = reportSchedule
        reportScheduleId = REPORT_SCHEDULE_ID
      }

      val createdReportSchedule =
        withPrincipalAndScopes(PRINCIPAL, SCOPES) {
          runBlocking { service.createReportSchedule(request) }
        }

      assertThat(createdReportSchedule)
        .isEqualTo(
          reportSchedule.copy {
            name = REPORT_SCHEDULE_NAME
            state = ReportSchedule.State.ACTIVE
            this.nextReportCreationTime = nextReportCreationTime
            createTime = INTERNAL_REPORT_SCHEDULE.createTime
            updateTime = INTERNAL_REPORT_SCHEDULE.updateTime
          }
        )

      verifyProtoArgument(
          internalReportSchedulesMock,
          ReportSchedulesCoroutineImplBase::createReportSchedule,
        )
        .isEqualTo(
          internalCreateReportScheduleRequest {
            this.reportSchedule = internalReportSchedule {
              cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
              this.nextReportCreationTime = nextReportCreationTime
              details =
                InternalReportScheduleKt.details {
                  displayName = INTERNAL_REPORT_SCHEDULE.details.displayName
                  description = INTERNAL_REPORT_SCHEDULE.details.description
                  reportTemplate = INTERNAL_REPORT_SCHEDULE.details.reportTemplate
                  this.eventStart = eventStart
                  this.eventEnd = eventEnd
                  frequency =
                    InternalReportScheduleKt.frequency {
                      monthly = InternalReportScheduleKt.FrequencyKt.monthly { dayOfMonth = 1 }
                    }
                  reportWindow = INTERNAL_REPORT_SCHEDULE.details.reportWindow
                }
            }
            externalReportScheduleId = REPORT_SCHEDULE_ID
          }
        )
    }

  @Test
  fun `createReportSchedule sets next after start when day of month larger than last day`() =
    runBlocking {
      val eventStart = dateTime {
        year = 3000
        month = 1
        day = 1
        hours = 13
        timeZone = timeZone { id = "America/Los_Angeles" }
      }
      val eventEnd = date {
        year = 3001
        month = 12
        day = 1
      }

      val nextReportCreationTime = timestamp {
        seconds = 32506347600 // Friday, January 31, 3000 at 1 PM, America/Los_Angeles
      }

      whenever(internalReportSchedulesMock.createReportSchedule(any()))
        .thenReturn(
          INTERNAL_REPORT_SCHEDULE.copy {
            details =
              INTERNAL_REPORT_SCHEDULE.details.copy {
                this.eventStart = eventStart
                this.eventEnd = eventEnd
                frequency =
                  InternalReportScheduleKt.frequency {
                    monthly = InternalReportScheduleKt.FrequencyKt.monthly { dayOfMonth = 32 }
                  }
              }
            this.nextReportCreationTime = nextReportCreationTime
          }
        )

      val reportSchedule =
        REPORT_SCHEDULE.copy {
          this.eventStart = eventStart
          this.eventEnd = eventEnd
          frequency =
            ReportScheduleKt.frequency {
              monthly = ReportScheduleKt.FrequencyKt.monthly { dayOfMonth = 32 }
            }
        }

      val request = createReportScheduleRequest {
        parent = MEASUREMENT_CONSUMER_NAME
        this.reportSchedule = reportSchedule
        reportScheduleId = REPORT_SCHEDULE_ID
      }

      val createdReportSchedule =
        withPrincipalAndScopes(PRINCIPAL, SCOPES) {
          runBlocking { service.createReportSchedule(request) }
        }

      assertThat(createdReportSchedule)
        .isEqualTo(
          reportSchedule.copy {
            name = REPORT_SCHEDULE_NAME
            state = ReportSchedule.State.ACTIVE
            this.nextReportCreationTime = nextReportCreationTime
            createTime = INTERNAL_REPORT_SCHEDULE.createTime
            updateTime = INTERNAL_REPORT_SCHEDULE.updateTime
          }
        )

      verifyProtoArgument(
          internalReportSchedulesMock,
          ReportSchedulesCoroutineImplBase::createReportSchedule,
        )
        .isEqualTo(
          internalCreateReportScheduleRequest {
            this.reportSchedule = internalReportSchedule {
              cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
              this.nextReportCreationTime = nextReportCreationTime
              details =
                InternalReportScheduleKt.details {
                  displayName = INTERNAL_REPORT_SCHEDULE.details.displayName
                  description = INTERNAL_REPORT_SCHEDULE.details.description
                  reportTemplate = INTERNAL_REPORT_SCHEDULE.details.reportTemplate
                  this.eventStart = eventStart
                  this.eventEnd = eventEnd
                  frequency =
                    InternalReportScheduleKt.frequency {
                      monthly = InternalReportScheduleKt.FrequencyKt.monthly { dayOfMonth = 32 }
                    }
                  reportWindow = INTERNAL_REPORT_SCHEDULE.details.reportWindow
                }
            }
            externalReportScheduleId = REPORT_SCHEDULE_ID
          }
        )
    }

  @Test
  fun `createReportSchedule returns report schedule when composite reporting set used`() {
    val compositeReportingSetId = "a123"
    val compositeReportingSetName =
      "$MEASUREMENT_CONSUMER_NAME/reportingSets/$compositeReportingSetId"
    val compositeReportingSet = reportingSet {
      cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
      externalReportingSetId = compositeReportingSetId
      composite =
        ReportingSetKt.setExpression {
          operation = ReportingSet.SetExpression.Operation.UNION
          lhs =
            ReportingSetKt.SetExpressionKt.operand {
              externalReportingSetId = INTERNAL_PRIMITIVE_REPORTING_SET.externalReportingSetId
            }
        }
      displayName = "composite"
      weightedSubsetUnions +=
        ReportingSetKt.weightedSubsetUnion {
          primitiveReportingSetBases +=
            ReportingSetKt.primitiveReportingSetBasis {
              externalReportingSetId = PRIMITIVE_REPORTING_SET_ID
            }
          weight = 1
          binaryRepresentation = 1
        }
    }

    val internalReportSchedule =
      INTERNAL_REPORT_SCHEDULE.copy {
        details =
          INTERNAL_REPORT_SCHEDULE.details.copy {
            reportTemplate = internalReport {
              reportingMetricEntries[compositeReportingSetId] =
                InternalReportKt.reportingMetricCalculationSpec {
                  metricCalculationSpecReportingMetrics +=
                    InternalReportKt.metricCalculationSpecReportingMetrics {
                      externalMetricCalculationSpecId =
                        INTERNAL_METRIC_CALCULATION_SPEC.externalMetricCalculationSpecId
                    }
                }
              details = InternalReportKt.details {}
            }
          }
      }

    runBlocking {
      whenever(internalReportingSetsMock.batchGetReportingSets(any())).thenAnswer {
        val request = it.arguments[0] as BatchGetReportingSetsRequest
        val reportingSetsMap =
          mapOf(
            compositeReportingSet.externalReportingSetId to compositeReportingSet,
            INTERNAL_PRIMITIVE_REPORTING_SET.externalReportingSetId to
              INTERNAL_PRIMITIVE_REPORTING_SET,
          )
        batchGetReportingSetsResponse {
          reportingSets +=
            request.externalReportingSetIdsList.map { id -> reportingSetsMap.getValue(id) }
        }
      }

      whenever(internalReportSchedulesMock.createReportSchedule(any()))
        .thenReturn(internalReportSchedule)
    }

    val reportSchedule =
      REPORT_SCHEDULE.copy {
        reportTemplate = report {
          reportingMetricEntries +=
            ReportKt.reportingMetricEntry {
              key = compositeReportingSetName
              value =
                ReportKt.reportingMetricCalculationSpec {
                  metricCalculationSpecs += METRIC_CALCULATION_SPEC_NAME
                }
            }
        }
      }

    val request = createReportScheduleRequest {
      parent = MEASUREMENT_CONSUMER_NAME
      this.reportSchedule = reportSchedule
      reportScheduleId = REPORT_SCHEDULE_ID
    }

    val createdReportSchedule =
      withPrincipalAndScopes(PRINCIPAL, SCOPES) {
        runBlocking { service.createReportSchedule(request) }
      }

    assertThat(createdReportSchedule)
      .isEqualTo(
        reportSchedule.copy {
          name = REPORT_SCHEDULE_NAME
          state = ReportSchedule.State.forNumber(internalReportSchedule.state.number)
          nextReportCreationTime = internalReportSchedule.nextReportCreationTime
          createTime = internalReportSchedule.createTime
          updateTime = internalReportSchedule.updateTime
        }
      )
  }

  @Test
  fun `createReportSchedule throws NOT_FOUND when reporting set not found`() {
    runBlocking {
      whenever(internalReportingSetsMock.batchGetReportingSets(any()))
        .thenThrow(Status.NOT_FOUND.asRuntimeException())
    }

    val request = createReportScheduleRequest {
      parent = MEASUREMENT_CONSUMER_NAME
      reportScheduleId = "a123"
      reportSchedule = REPORT_SCHEDULE
      requestId = "test"
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withPrincipalAndScopes(PRINCIPAL, SCOPES) {
          runBlocking { service.createReportSchedule(request) }
        }
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
    assertThat(exception.message).contains("ReportingSet")
  }

  @Test
  fun `createReportSchedule throws NOT_FOUND when data provider not found`() {
    runBlocking {
      whenever(dataProvidersMock.getDataProvider(any()))
        .thenThrow(Status.NOT_FOUND.asRuntimeException())
    }

    val request = createReportScheduleRequest {
      parent = MEASUREMENT_CONSUMER_NAME
      reportScheduleId = "a123"
      reportSchedule = REPORT_SCHEDULE
      requestId = "test"
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withPrincipalAndScopes(PRINCIPAL, SCOPES) {
          runBlocking { service.createReportSchedule(request) }
        }
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
    assertThat(exception.message).contains("DataProvider")
  }

  @Test
  fun `createReportSchedule throws NOT_FOUND when event group not found`() {
    runBlocking {
      whenever(eventGroupsMock.getEventGroup(any()))
        .thenThrow(Status.NOT_FOUND.asRuntimeException())
    }

    val request = createReportScheduleRequest {
      parent = MEASUREMENT_CONSUMER_NAME
      reportScheduleId = "a123"
      reportSchedule = REPORT_SCHEDULE
      requestId = "test"
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withPrincipalAndScopes(PRINCIPAL, SCOPES) {
          runBlocking { service.createReportSchedule(request) }
        }
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
    assertThat(exception.message).contains("EventGroup")
  }

  @Test
  fun `createReportSchedule throws INVALID_ARGUMENT when parent is missing`() {
    val request = createReportScheduleRequest {
      reportScheduleId = "a123"
      reportSchedule = REPORT_SCHEDULE
      requestId = "test"
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withPrincipalAndScopes(PRINCIPAL, SCOPES) {
          runBlocking { service.createReportSchedule(request) }
        }
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.message).contains("Parent")
  }

  @Test
  fun `createReportSchedule throws INVALID_ARGUMENT when parent is invalid`() {
    val request = createReportScheduleRequest {
      parent = "invalid"
      reportScheduleId = "a123"
      reportSchedule = REPORT_SCHEDULE
      requestId = "test"
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withPrincipalAndScopes(PRINCIPAL, SCOPES) {
          runBlocking { service.createReportSchedule(request) }
        }
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.message).contains("Parent")
  }

  @Test
  fun `createReportSchedule throws INVALID_ARGUMENT when report_schedule_id is missing`() {
    val request = createReportScheduleRequest {
      parent = MEASUREMENT_CONSUMER_NAME
      reportSchedule = REPORT_SCHEDULE
      requestId = "test"
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withPrincipalAndScopes(PRINCIPAL, SCOPES) {
          runBlocking { service.createReportSchedule(request) }
        }
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.message).contains("report_schedule_id")
  }

  @Test
  fun `createReportSchedule throws INVALID_ARGUMENT when report_schedule_id is invalid`() {
    val request = createReportScheduleRequest {
      parent = MEASUREMENT_CONSUMER_NAME
      reportScheduleId = "A123"
      reportSchedule = REPORT_SCHEDULE
      requestId = "test"
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withPrincipalAndScopes(PRINCIPAL, SCOPES) {
          runBlocking { service.createReportSchedule(request) }
        }
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.message).contains("report_schedule_id")
  }

  @Test
  fun `createReportSchedule throws INVALID_ARGUMENT when request_id is too long`() {
    val request = createReportScheduleRequest {
      parent = MEASUREMENT_CONSUMER_NAME
      reportScheduleId = "a123"
      reportSchedule = REPORT_SCHEDULE
      requestId = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withPrincipalAndScopes(PRINCIPAL, SCOPES) {
          runBlocking { service.createReportSchedule(request) }
        }
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.message).contains("request_id")
  }

  @Test
  fun `createReportSchedule throws PERMISSION_DENIED when MC caller does not match`() {
    val request = createReportScheduleRequest {
      parent = MEASUREMENT_CONSUMER_NAME
      reportScheduleId = "a123"
      reportSchedule = REPORT_SCHEDULE
      requestId = "a123"
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withPrincipalAndScopes(PRINCIPAL.copy { name = "principals/mc-2-user" }, SCOPES) {
          runBlocking { service.createReportSchedule(request) }
        }
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.PERMISSION_DENIED)
    assertThat(exception.message).contains(ReportSchedulesService.Permission.CREATE)
  }

  @Test
  fun `createReportSchedule throws ALREADY_EXISTS when report schedule id already used`() =
    runBlocking {
      whenever(internalReportSchedulesMock.createReportSchedule(any()))
        .thenThrow(StatusRuntimeException(Status.ALREADY_EXISTS))

      val request = createReportScheduleRequest {
        parent = MEASUREMENT_CONSUMER_NAME
        reportScheduleId = "a123"
        reportSchedule = REPORT_SCHEDULE
        requestId = "a123"
      }

      val exception =
        assertFailsWith<StatusRuntimeException> {
          withPrincipalAndScopes(PRINCIPAL, SCOPES) {
            runBlocking { service.createReportSchedule(request) }
          }
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.ALREADY_EXISTS)
      assertThat(exception.message).contains("already exists")
    }

  @Test
  fun `createReportSchedule throws INVALID_ARGUMENT when report_template is missing`() {
    val request = createReportScheduleRequest {
      parent = MEASUREMENT_CONSUMER_NAME
      reportScheduleId = "a123"
      reportSchedule = REPORT_SCHEDULE.copy { clearReportTemplate() }
      requestId = "a123"
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withPrincipalAndScopes(PRINCIPAL, SCOPES) {
          runBlocking { service.createReportSchedule(request) }
        }
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.message).contains("report_template")
  }

  @Test
  fun `createReportSchedule throws INVALID_ARGUMENT when entries in report_template is empty`() {
    val request = createReportScheduleRequest {
      parent = MEASUREMENT_CONSUMER_NAME
      reportScheduleId = "a123"
      reportSchedule =
        REPORT_SCHEDULE.copy {
          reportTemplate = REPORT_SCHEDULE.reportTemplate.copy { reportingMetricEntries.clear() }
        }
      requestId = "a123"
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withPrincipalAndScopes(PRINCIPAL, SCOPES) {
          runBlocking { service.createReportSchedule(request) }
        }
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.message).contains("reporting_metric_entries")
  }

  @Test
  fun `createReportSchedule throws INVALID_ARGUMENT when entry in report_template has no key`() {
    val request = createReportScheduleRequest {
      parent = MEASUREMENT_CONSUMER_NAME
      reportScheduleId = "a123"
      reportSchedule =
        REPORT_SCHEDULE.copy {
          reportTemplate =
            REPORT_SCHEDULE.reportTemplate.copy {
              reportingMetricEntries +=
                REPORT_SCHEDULE.reportTemplate.reportingMetricEntriesList[0].copy { clearKey() }
            }
        }
      requestId = "a123"
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withPrincipalAndScopes(PRINCIPAL, SCOPES) {
          runBlocking { service.createReportSchedule(request) }
        }
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.message).contains("ReportingSet name")
  }

  @Test
  fun `createReportSchedule throws INVALID_ARGUMENT when entry in report_template has bad key`() {
    val request = createReportScheduleRequest {
      parent = MEASUREMENT_CONSUMER_NAME
      reportScheduleId = "a123"
      reportSchedule =
        REPORT_SCHEDULE.copy {
          reportTemplate =
            REPORT_SCHEDULE.reportTemplate.copy {
              reportingMetricEntries +=
                REPORT_SCHEDULE.reportTemplate.reportingMetricEntriesList[0].copy { key = "bad" }
            }
        }
      requestId = "a123"
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withPrincipalAndScopes(PRINCIPAL, SCOPES) {
          runBlocking { service.createReportSchedule(request) }
        }
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.message).contains("ReportingSet name")
  }

  @Test
  fun `createReportSchedule throws INVALID_ARGUMENT when entry in report_template has no value`() {
    val request = createReportScheduleRequest {
      parent = MEASUREMENT_CONSUMER_NAME
      reportScheduleId = "a123"
      reportSchedule =
        REPORT_SCHEDULE.copy {
          reportTemplate =
            REPORT_SCHEDULE.reportTemplate.copy {
              reportingMetricEntries +=
                REPORT_SCHEDULE.reportTemplate.reportingMetricEntriesList[0].copy { clearValue() }
            }
        }
      requestId = "a123"
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withPrincipalAndScopes(PRINCIPAL, SCOPES) {
          runBlocking { service.createReportSchedule(request) }
        }
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.message).contains("reporting_metric_entries")
  }

  @Test
  fun `createReportSchedule throws INVALID_ARGUMENT when entry in report_template has bad value`() {
    val request = createReportScheduleRequest {
      parent = MEASUREMENT_CONSUMER_NAME
      reportScheduleId = "a123"
      reportSchedule =
        REPORT_SCHEDULE.copy {
          reportTemplate =
            REPORT_SCHEDULE.reportTemplate.copy {
              reportingMetricEntries +=
                REPORT_SCHEDULE.reportTemplate.reportingMetricEntriesList[0].copy {
                  value = Report.ReportingMetricCalculationSpec.getDefaultInstance()
                }
            }
        }
      requestId = "a123"
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withPrincipalAndScopes(PRINCIPAL, SCOPES) {
          runBlocking { service.createReportSchedule(request) }
        }
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.message).contains("Entry")
  }

  @Test
  fun `createReportSchedule throws INVALID_ARGUMENT when event_start is missing`() {
    val request = createReportScheduleRequest {
      parent = MEASUREMENT_CONSUMER_NAME
      reportScheduleId = "a123"
      reportSchedule = REPORT_SCHEDULE.copy { clearEventStart() }
      requestId = "a123"
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withPrincipalAndScopes(PRINCIPAL, SCOPES) {
          runBlocking { service.createReportSchedule(request) }
        }
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.message).contains("event_start")
  }

  @Test
  fun `createReportSchedule throws INVALID_ARGUMENT when event_start missing required fields`() {
    val request = createReportScheduleRequest {
      parent = MEASUREMENT_CONSUMER_NAME
      reportScheduleId = "a123"
      reportSchedule =
        REPORT_SCHEDULE.copy {
          eventStart = dateTime {
            year = 3000
            month = 1
            timeZone = timeZone { id = "America/New_York" }
          }
        }
      requestId = "a123"
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withPrincipalAndScopes(PRINCIPAL, SCOPES) {
          runBlocking { service.createReportSchedule(request) }
        }
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.message).contains("event_start missing")
  }

  @Test
  fun `createReportSchedule throws INVALID_ARGUMENT when event_start date is invalid`() {
    val request = createReportScheduleRequest {
      parent = MEASUREMENT_CONSUMER_NAME
      reportScheduleId = "a123"
      reportSchedule =
        REPORT_SCHEDULE.copy {
          eventStart = dateTime {
            year = 3000
            month = 1
            day = 40
            timeZone = timeZone { id = "America/New_York" }
          }
        }
      requestId = "a123"
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withPrincipalAndScopes(PRINCIPAL, SCOPES) {
          runBlocking { service.createReportSchedule(request) }
        }
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.message).contains("event_start date")
  }

  @Test
  fun `createReportSchedule throws INVALID_ARGUMENT when event_start has invalid time zone`() {
    val request = createReportScheduleRequest {
      parent = MEASUREMENT_CONSUMER_NAME
      reportScheduleId = "a123"
      reportSchedule =
        REPORT_SCHEDULE.copy {
          eventStart =
            REPORT_SCHEDULE.eventStart.copy { timeZone = timeZone { id = "America/New_New_York" } }
        }
      requestId = "a123"
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withPrincipalAndScopes(PRINCIPAL, SCOPES) {
          runBlocking { service.createReportSchedule(request) }
        }
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.message).contains("event_start.time_zone.id")
  }

  @Test
  fun `createReportSchedule throws INVALID_ARGUMENT when event_start has invalid offset`() {
    val request = createReportScheduleRequest {
      parent = MEASUREMENT_CONSUMER_NAME
      reportScheduleId = "a123"
      reportSchedule =
        REPORT_SCHEDULE.copy {
          eventStart = REPORT_SCHEDULE.eventStart.copy { utcOffset = duration { seconds = 999999 } }
        }
      requestId = "a123"
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withPrincipalAndScopes(PRINCIPAL, SCOPES) {
          runBlocking { service.createReportSchedule(request) }
        }
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.message).contains("event_start.utc_offset")
  }

  @Test
  fun `createReportSchedule throws INVALID_ARGUMENT when event_start after event_end`() {
    val request = createReportScheduleRequest {
      parent = MEASUREMENT_CONSUMER_NAME
      reportScheduleId = "a123"
      reportSchedule =
        REPORT_SCHEDULE.copy {
          eventStart = dateTime {
            year = 2024
            month = 1
            day = 1
            timeZone = timeZone { id = "America/New_York" }
          }
          eventEnd = date {
            year = 2023
            month = 1
            day = 1
          }
        }
      requestId = "a123"
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withPrincipalAndScopes(PRINCIPAL, SCOPES) {
          runBlocking { service.createReportSchedule(request) }
        }
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.message).contains("event_end")
  }

  @Test
  fun `createReportSchedule throws INVALID_ARGUMENT when event_end not a full date`() {
    val request = createReportScheduleRequest {
      parent = MEASUREMENT_CONSUMER_NAME
      reportScheduleId = "a123"
      reportSchedule =
        REPORT_SCHEDULE.copy {
          eventEnd = date {
            year = 4000
            month = 1
          }
        }
      requestId = "a123"
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withPrincipalAndScopes(PRINCIPAL, SCOPES) {
          runBlocking { service.createReportSchedule(request) }
        }
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.message).contains("not a full date")
  }

  @Test
  fun `createReportSchedule throws INVALID_ARGUMENT when event_end date invalid`() {
    val request = createReportScheduleRequest {
      parent = MEASUREMENT_CONSUMER_NAME
      reportScheduleId = "a123"
      reportSchedule =
        REPORT_SCHEDULE.copy {
          eventEnd = date {
            year = 4000
            month = 1
            day = 35
          }
        }
      requestId = "a123"
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withPrincipalAndScopes(PRINCIPAL, SCOPES) {
          runBlocking { service.createReportSchedule(request) }
        }
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.message).contains("event_end")
  }

  @Test
  fun `createReportSchedule throws INVALID_ARGUMENT when frequency is missing`() {
    val request = createReportScheduleRequest {
      parent = MEASUREMENT_CONSUMER_NAME
      reportScheduleId = "a123"
      reportSchedule = REPORT_SCHEDULE.copy { clearFrequency() }
      requestId = "a123"
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withPrincipalAndScopes(PRINCIPAL, SCOPES) {
          runBlocking { service.createReportSchedule(request) }
        }
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.message).contains("frequency")
  }

  @Test
  fun `createReportSchedule throws INVALID_ARGUMENT when frequency missing frequency`() {
    val request = createReportScheduleRequest {
      parent = MEASUREMENT_CONSUMER_NAME
      reportScheduleId = "a123"
      reportSchedule =
        REPORT_SCHEDULE.copy { frequency = ReportSchedule.Frequency.getDefaultInstance() }
      requestId = "a123"
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withPrincipalAndScopes(PRINCIPAL, SCOPES) {
          runBlocking { service.createReportSchedule(request) }
        }
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.message).contains("frequency")
  }

  @Test
  fun `createReportSchedule throws INVALID_ARGUMENT when weekly frequency missing day_of_week`() {
    val request = createReportScheduleRequest {
      parent = MEASUREMENT_CONSUMER_NAME
      reportScheduleId = "a123"
      reportSchedule =
        REPORT_SCHEDULE.copy {
          frequency =
            ReportScheduleKt.frequency {
              weekly = ReportSchedule.Frequency.Weekly.getDefaultInstance()
            }
        }
      requestId = "a123"
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withPrincipalAndScopes(PRINCIPAL, SCOPES) {
          runBlocking { service.createReportSchedule(request) }
        }
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.message).contains("frequency")
  }

  @Test
  fun `createReportSchedule throws INVALID_ARGUMENT when monthly frequency missing day_of_month`() {
    val request = createReportScheduleRequest {
      parent = MEASUREMENT_CONSUMER_NAME
      reportScheduleId = "a123"
      reportSchedule =
        REPORT_SCHEDULE.copy {
          frequency =
            ReportScheduleKt.frequency {
              monthly = ReportSchedule.Frequency.Monthly.getDefaultInstance()
            }
        }
      requestId = "a123"
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withPrincipalAndScopes(PRINCIPAL, SCOPES) {
          runBlocking { service.createReportSchedule(request) }
        }
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.message).contains("frequency")
  }

  @Test
  fun `createReportSchedule throws INVALID_ARGUMENT when report_window is missing`() {
    val request = createReportScheduleRequest {
      parent = MEASUREMENT_CONSUMER_NAME
      reportScheduleId = "a123"
      reportSchedule = REPORT_SCHEDULE.copy { clearReportWindow() }
      requestId = "a123"
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withPrincipalAndScopes(PRINCIPAL, SCOPES) {
          runBlocking { service.createReportSchedule(request) }
        }
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.message).contains("report_window")
  }

  @Test
  fun `createReportSchedule throws INVALID_ARGUMENT when report_window missing window`() {
    val request = createReportScheduleRequest {
      parent = MEASUREMENT_CONSUMER_NAME
      reportScheduleId = "a123"
      reportSchedule = REPORT_SCHEDULE.copy { reportWindow = ReportWindow.getDefaultInstance() }
      requestId = "a123"
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withPrincipalAndScopes(PRINCIPAL, SCOPES) {
          runBlocking { service.createReportSchedule(request) }
        }
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.message).contains("report_window")
  }

  @Test
  fun `createReportSchedule throws INVALID_ARGUMENT when trailing_window missing count`() {
    val request = createReportScheduleRequest {
      parent = MEASUREMENT_CONSUMER_NAME
      reportScheduleId = "a123"
      reportSchedule =
        REPORT_SCHEDULE.copy {
          reportWindow =
            ReportScheduleKt.reportWindow {
              trailingWindow =
                ReportScheduleKt.ReportWindowKt.trailingWindow {
                  increment = ReportWindow.TrailingWindow.Increment.DAY
                }
            }
        }
      requestId = "a123"
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withPrincipalAndScopes(PRINCIPAL, SCOPES) {
          runBlocking { service.createReportSchedule(request) }
        }
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.message).contains("trailing_window")
  }

  @Test
  fun `createReportSchedule throws INVALID_ARGUMENT when trailing_window missing increment`() {
    val request = createReportScheduleRequest {
      parent = MEASUREMENT_CONSUMER_NAME
      reportScheduleId = "a123"
      reportSchedule =
        REPORT_SCHEDULE.copy {
          reportWindow =
            ReportScheduleKt.reportWindow {
              trailingWindow = ReportScheduleKt.ReportWindowKt.trailingWindow { count = 1 }
            }
        }
      requestId = "a123"
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withPrincipalAndScopes(PRINCIPAL, SCOPES) {
          runBlocking { service.createReportSchedule(request) }
        }
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.message).contains("trailing_window")
  }

  @Test
  fun `createReportSchedule throws INVALID_ARGUMENT when fixed_window not full date`() {
    val request = createReportScheduleRequest {
      parent = MEASUREMENT_CONSUMER_NAME
      reportScheduleId = "a123"
      reportSchedule =
        REPORT_SCHEDULE.copy {
          reportWindow = ReportScheduleKt.reportWindow { fixedWindow = date { year = 2000 } }
        }
      requestId = "a123"
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withPrincipalAndScopes(PRINCIPAL, SCOPES) {
          runBlocking { service.createReportSchedule(request) }
        }
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.message).contains("not a full date")
  }

  @Test
  fun `createReportSchedule throws INVALID_ARGUMENT when fixed_window not before event_start`() {
    val request = createReportScheduleRequest {
      parent = MEASUREMENT_CONSUMER_NAME
      reportScheduleId = "a123"
      reportSchedule =
        REPORT_SCHEDULE.copy {
          reportWindow =
            ReportScheduleKt.reportWindow {
              fixedWindow = date {
                year = REPORT_SCHEDULE.eventStart.year
                month = REPORT_SCHEDULE.eventStart.month
                day = REPORT_SCHEDULE.eventStart.day
              }
            }
        }
      requestId = "a123"
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withPrincipalAndScopes(PRINCIPAL, SCOPES) {
          runBlocking { service.createReportSchedule(request) }
        }
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.message).contains("not before event_start")
  }

  @Test
  fun `createReportSchedule throws error when first report interval before eg data interval`() {
    runBlocking {
      whenever(dataProvidersMock.getDataProvider(any()))
        .thenReturn(
          DATA_PROVIDER.copy {
            dataAvailabilityInterval = interval {
              startTime = timestamp {
                seconds = 946760400 // January 1, 2000 at 1 PM, America/Los_Angeles
              }
              endTime = timestamp {
                seconds = 64060664400 // January 1, 4000 at 1 PM, America/Los_Angeles
              }
            }
          }
        )

      whenever(eventGroupsMock.getEventGroup(any()))
        .thenReturn(
          EVENT_GROUP.copy {
            dataAvailabilityInterval = interval {
              startTime = timestamp {
                seconds = 48282210000 // January 1, 3500 at 1 PM, America/Los_Angeles
              }
              endTime = timestamp {
                seconds = 64060664400 // January 1, 4000 at 1 PM, America/Los_Angeles
              }
            }
          }
        )
    }

    val request = createReportScheduleRequest {
      parent = MEASUREMENT_CONSUMER_NAME
      reportScheduleId = "a123"
      reportSchedule =
        REPORT_SCHEDULE.copy {
          eventStart = dateTime {
            year = 3000
            month = 10
            day = 1
            hours = 6
            timeZone = timeZone { id = "America/Los_Angeles" }
          }
          eventEnd = date {
            year = 3001
            month = 12
            day = 1
          }
        }
      requestId = "a123"
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withPrincipalAndScopes(PRINCIPAL, SCOPES) {
          runBlocking { service.createReportSchedule(request) }
        }
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.FAILED_PRECONDITION)
    assertThat(exception.message).contains("data_availability_interval of the EventGroup")
  }

  @Test
  fun `createReportSchedule throws error when first report interval before edp data interval`() {
    runBlocking {
      whenever(dataProvidersMock.getDataProvider(any()))
        .thenReturn(
          DATA_PROVIDER.copy {
            dataAvailabilityInterval = interval {
              startTime = timestamp {
                seconds = 48282210000 // January 1, 3500 at 1 PM, America/Los_Angeles
              }
              endTime = timestamp {
                seconds = 64060664400 // January 1, 4000 at 1 PM, America/Los_Angeles
              }
            }
          }
        )

      whenever(eventGroupsMock.getEventGroup(any()))
        .thenReturn(
          EVENT_GROUP.copy {
            dataAvailabilityInterval = interval {
              startTime = timestamp {
                seconds = 946760400 // January 1, 2000 at 1 PM, America/Los_Angeles
              }
              endTime = timestamp {
                seconds = 64060664400 // January 1, 4000 at 1 PM, America/Los_Angeles
              }
            }
          }
        )
    }

    val request = createReportScheduleRequest {
      parent = MEASUREMENT_CONSUMER_NAME
      reportScheduleId = "a123"
      reportSchedule =
        REPORT_SCHEDULE.copy {
          eventStart = dateTime {
            year = 3000
            month = 10
            day = 1
            hours = 6
            timeZone = timeZone { id = "America/Los_Angeles" }
          }
          eventEnd = date {
            year = 3001
            month = 12
            day = 1
          }
        }
      requestId = "a123"
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withPrincipalAndScopes(PRINCIPAL, SCOPES) {
          runBlocking { service.createReportSchedule(request) }
        }
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.FAILED_PRECONDITION)
    assertThat(exception.message).contains("data_availability_interval of the DataProvider")
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
      withPrincipalAndScopes(PRINCIPAL, SCOPES) {
        runBlocking { service.getReportSchedule(request) }
      }

    assertThat(reportSchedule).isEqualTo(REPORT_SCHEDULE)
  }

  @Test
  fun `getReportSchedule returns schedule when principal has permission on resource`() {
    val request = getReportScheduleRequest { name = REPORT_SCHEDULE_NAME }
    reset(permissionsServiceMock)
    wheneverBlocking {
      permissionsServiceMock.checkPermissions(
        and(
          and(hasPrincipal(PRINCIPAL.name), hasProtectedResource(request.name)),
          hasPermissionId(ReportSchedulesService.Permission.GET),
        )
      )
    } doReturn
      checkPermissionsResponse {
        permissions += PermissionKey(ReportSchedulesService.Permission.GET).toName()
      }
    wheneverBlocking {
        internalReportSchedulesMock.getReportSchedule(
          eq(
            internalGetReportScheduleRequest {
              cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
              externalReportScheduleId = REPORT_SCHEDULE_ID
            }
          )
        )
      }
      .thenReturn(INTERNAL_REPORT_SCHEDULE)

    val reportSchedule =
      withPrincipalAndScopes(PRINCIPAL, SCOPES) {
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
        withPrincipalAndScopes(PRINCIPAL, SCOPES) {
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
        withPrincipalAndScopes(PRINCIPAL, SCOPES) {
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
        withPrincipalAndScopes(PRINCIPAL.copy { name = "principals/mc-2-user" }, SCOPES) {
          runBlocking { service.getReportSchedule(request) }
        }
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.PERMISSION_DENIED)
    assertThat(exception.message).contains(ReportSchedulesService.Permission.GET)
  }

  @Test
  fun `listReportSchedules returns with next page token when there is another page`() {
    val pageSize = 1
    val request = listReportSchedulesRequest {
      parent = MEASUREMENT_CONSUMER_NAME
      this.pageSize = pageSize
    }

    val result =
      withPrincipalAndScopes(PRINCIPAL, SCOPES) {
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
        ReportSchedulesCoroutineImplBase::listReportSchedules,
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
      withPrincipalAndScopes(PRINCIPAL, SCOPES) {
        runBlocking { service.listReportSchedules(request) }
      }

    val expected = listReportSchedulesResponse {
      reportSchedules += REPORT_SCHEDULE
      reportSchedules += REPORT_SCHEDULE_2
    }

    assertThat(result).ignoringRepeatedFieldOrder().isEqualTo(expected)

    verifyProtoArgument(
        internalReportSchedulesMock,
        ReportSchedulesCoroutineImplBase::listReportSchedules,
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
      withPrincipalAndScopes(PRINCIPAL, SCOPES) {
        runBlocking { service.listReportSchedules(request) }
      }

    val expected = listReportSchedulesResponse {
      reportSchedules += REPORT_SCHEDULE
      reportSchedules += REPORT_SCHEDULE_2
    }

    assertThat(result).ignoringRepeatedFieldOrder().isEqualTo(expected)

    verifyProtoArgument(
        internalReportSchedulesMock,
        ReportSchedulesCoroutineImplBase::listReportSchedules,
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
      withPrincipalAndScopes(PRINCIPAL, SCOPES) {
        runBlocking { service.listReportSchedules(request) }
      }

    val expected = listReportSchedulesResponse {
      reportSchedules += REPORT_SCHEDULE
      reportSchedules += REPORT_SCHEDULE_2
    }

    assertThat(result).ignoringRepeatedFieldOrder().isEqualTo(expected)

    verifyProtoArgument(
        internalReportSchedulesMock,
        ReportSchedulesCoroutineImplBase::listReportSchedules,
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
      withPrincipalAndScopes(PRINCIPAL, SCOPES) {
        runBlocking { service.listReportSchedules(request) }
      }

    val expected = listReportSchedulesResponse {
      reportSchedules += REPORT_SCHEDULE
      reportSchedules += REPORT_SCHEDULE_2
    }

    assertThat(result).ignoringRepeatedFieldOrder().isEqualTo(expected)

    verifyProtoArgument(
        internalReportSchedulesMock,
        ReportSchedulesCoroutineImplBase::listReportSchedules,
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
      withPrincipalAndScopes(PRINCIPAL, SCOPES) {
        runBlocking { service.listReportSchedules(request) }
      }

    val expected = listReportSchedulesResponse {
      reportSchedules += REPORT_SCHEDULE
      reportSchedules += REPORT_SCHEDULE_2
    }

    assertThat(result).ignoringRepeatedFieldOrder().isEqualTo(expected)

    verifyProtoArgument(
        internalReportSchedulesMock,
        ReportSchedulesCoroutineImplBase::listReportSchedules,
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
      withPrincipalAndScopes(PRINCIPAL, SCOPES) {
        runBlocking { service.listReportSchedules(request) }
      }

    val expected = listReportSchedulesResponse {
      reportSchedules += REPORT_SCHEDULE
      reportSchedules += REPORT_SCHEDULE_2
    }

    assertThat(result).ignoringRepeatedFieldOrder().isEqualTo(expected)

    verifyProtoArgument(
        internalReportSchedulesMock,
        ReportSchedulesCoroutineImplBase::listReportSchedules,
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
      withPrincipalAndScopes(PRINCIPAL, SCOPES) {
        runBlocking { service.listReportSchedules(request) }
      }

    val expected = listReportSchedulesResponse { reportSchedules += REPORT_SCHEDULE }

    assertThat(result).ignoringRepeatedFieldOrder().isEqualTo(expected)

    verifyProtoArgument(
        internalReportSchedulesMock,
        ReportSchedulesCoroutineImplBase::listReportSchedules,
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
        withPrincipalAndScopes(PRINCIPAL, SCOPES) {
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
    val request = listReportSchedulesRequest { parent = MEASUREMENT_CONSUMER_NAME }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withPrincipalAndScopes(PRINCIPAL.copy { name = "principals/mc-2-user" }, SCOPES) {
          runBlocking { service.listReportSchedules(request) }
        }
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.PERMISSION_DENIED)
    assertThat(exception.message).contains(ReportSchedulesService.Permission.LIST)
  }

  @Test
  fun `listReportSchedules throws INVALID_ARGUMENT when page size is less than 0`() {
    val request = listReportSchedulesRequest {
      parent = MEASUREMENT_CONSUMER_NAME
      pageSize = -1
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withPrincipalAndScopes(PRINCIPAL, SCOPES) {
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
        withPrincipalAndScopes(PRINCIPAL, SCOPES) {
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
        withPrincipalAndScopes(PRINCIPAL, SCOPES) {
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
        withPrincipalAndScopes(PRINCIPAL, SCOPES) {
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
      withPrincipalAndScopes(PRINCIPAL, SCOPES) {
        runBlocking { service.stopReportSchedule(request) }
      }

    assertThat(reportSchedule).isEqualTo(REPORT_SCHEDULE)
  }

  @Test
  fun `stopReportSchedule returns schedule when principal has permission on resource`() {
    val request = stopReportScheduleRequest { name = REPORT_SCHEDULE_NAME }
    reset(permissionsServiceMock)
    wheneverBlocking {
      permissionsServiceMock.checkPermissions(
        and(
          and(hasPrincipal(PRINCIPAL.name), hasProtectedResource(request.name)),
          hasPermissionId(ReportSchedulesService.Permission.STOP),
        )
      )
    } doReturn
      checkPermissionsResponse {
        permissions += PermissionKey(ReportSchedulesService.Permission.STOP).toName()
      }
    wheneverBlocking {
        internalReportSchedulesMock.stopReportSchedule(
          eq(
            internalStopReportScheduleRequest {
              cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
              externalReportScheduleId = REPORT_SCHEDULE_ID
            }
          )
        )
      }
      .thenReturn(INTERNAL_REPORT_SCHEDULE)

    val reportSchedule =
      withPrincipalAndScopes(PRINCIPAL, SCOPES) {
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
        withPrincipalAndScopes(PRINCIPAL, SCOPES) {
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
        withPrincipalAndScopes(PRINCIPAL, SCOPES) {
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
        withPrincipalAndScopes(PRINCIPAL.copy { name = "principals/mc-2-user" }, SCOPES) {
          runBlocking { service.stopReportSchedule(request) }
        }
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.PERMISSION_DENIED)
    assertThat(exception.message).contains(ReportSchedulesService.Permission.STOP)
  }

  companion object {
    private const val DEFAULT_PAGE_SIZE = 10
    private const val MAX_PAGE_SIZE = 100

    private val PRINCIPAL = principal { name = "principals/mc-user" }
    private val SCOPES = setOf("*")

    private const val CMMS_MEASUREMENT_CONSUMER_ID = "A123"
    private const val MEASUREMENT_CONSUMER_NAME =
      "measurementConsumers/$CMMS_MEASUREMENT_CONSUMER_ID"

    private const val API_AUTHENTICATION_KEY = "nR5QPN7ptx"
    private val CONFIG = measurementConsumerConfig { apiKey = API_AUTHENTICATION_KEY }
    private val CONFIGS = measurementConsumerConfigs { configs[MEASUREMENT_CONSUMER_NAME] = CONFIG }

    private const val REPORT_SCHEDULE_ID = "b123"
    private const val REPORT_SCHEDULE_NAME =
      "$MEASUREMENT_CONSUMER_NAME/reportSchedules/$REPORT_SCHEDULE_ID"
    private const val REPORT_SCHEDULE_ID_2 = "b124"
    private const val REPORT_SCHEDULE_NAME_2 =
      "$MEASUREMENT_CONSUMER_NAME/reportSchedules/$REPORT_SCHEDULE_ID_2"

    private const val PRIMITIVE_REPORTING_SET_ID = "c123"
    private const val PRIMITIVE_REPORTING_SET_NAME =
      "$MEASUREMENT_CONSUMER_NAME/reportingSets/$PRIMITIVE_REPORTING_SET_ID"

    private const val METRIC_CALCULATION_SPEC_ID = "m123"
    private const val METRIC_CALCULATION_SPEC_NAME =
      "$MEASUREMENT_CONSUMER_NAME/metricCalculationSpecs/$METRIC_CALCULATION_SPEC_ID"

    private const val DATA_PROVIDER_ID = "D123"
    private const val DATA_PROVIDER_NAME = "dataProviders/$DATA_PROVIDER_ID"

    private const val EVENT_GROUP_ID = "E123"
    private const val EVENT_GROUP_NAME = "$DATA_PROVIDER_NAME/eventGroups/$EVENT_GROUP_ID"

    private const val EPSILON = 0.0033
    private const val DELTA = 1e-12
    private const val NUMBER_VID_BUCKETS = 300
    private const val VID_SAMPLING_WIDTH = 3.0f / NUMBER_VID_BUCKETS
    private const val VID_SAMPLING_START = 0.0f

    private val INTERNAL_METRIC_SPEC: InternalMetricSpec = internalMetricSpec {
      reach =
        InternalMetricSpecKt.reachParams {
          multipleDataProviderParams =
            InternalMetricSpecKt.samplingAndPrivacyParams {
              privacyParams =
                InternalMetricSpecKt.differentialPrivacyParams {
                  epsilon = EPSILON
                  delta = DELTA
                }
              vidSamplingInterval =
                InternalMetricSpecKt.vidSamplingInterval {
                  start = VID_SAMPLING_START
                  width = VID_SAMPLING_WIDTH
                }
            }
        }
    }

    private val INTERNAL_METRIC_CALCULATION_SPEC = metricCalculationSpec {
      externalMetricCalculationSpecId = METRIC_CALCULATION_SPEC_ID
      details =
        MetricCalculationSpecKt.details {
          displayName = "display"
          metricSpecs += INTERNAL_METRIC_SPEC
          metricFrequencySpec =
            MetricCalculationSpecKt.metricFrequencySpec {
              daily = MetricCalculationSpec.MetricFrequencySpec.Daily.getDefaultInstance()
            }
          trailingWindow =
            MetricCalculationSpecKt.trailingWindow {
              count = 5
              increment = MetricCalculationSpec.TrailingWindow.Increment.DAY
            }
        }
    }

    private val INTERNAL_PRIMITIVE_REPORTING_SET = reportingSet {
      cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
      externalReportingSetId = PRIMITIVE_REPORTING_SET_ID
      primitive =
        ReportingSetKt.primitive {
          eventGroupKeys +=
            ReportingSetKt.PrimitiveKt.eventGroupKey {
              cmmsDataProviderId = DATA_PROVIDER_ID
              cmmsEventGroupId = EVENT_GROUP_ID
            }
        }
      displayName = "primitive"
      weightedSubsetUnions +=
        ReportingSetKt.weightedSubsetUnion {
          primitiveReportingSetBases +=
            ReportingSetKt.primitiveReportingSetBasis {
              externalReportingSetId = PRIMITIVE_REPORTING_SET_ID
            }
          weight = 1
          binaryRepresentation = 1
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
            reportingMetricEntries[INTERNAL_PRIMITIVE_REPORTING_SET.externalReportingSetId] =
              InternalReportKt.reportingMetricCalculationSpec {
                metricCalculationSpecReportingMetrics +=
                  InternalReportKt.metricCalculationSpecReportingMetrics {
                    externalMetricCalculationSpecId =
                      INTERNAL_METRIC_CALCULATION_SPEC.externalMetricCalculationSpecId
                  }
              }
            details = InternalReportKt.details {}
          }
          eventStart = dateTime {
            year = 3000
            month = 1
            day = 1
            hours = 13
            timeZone = timeZone { id = "America/Los_Angeles" }
          }
          eventEnd = date {
            year = 3001
            month = 12
            day = 1
          }
          frequency =
            InternalReportScheduleKt.frequency {
              daily = InternalReportSchedule.Frequency.Daily.getDefaultInstance()
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
      nextReportCreationTime = timestamp {
        seconds = 32503755600 // January 1, 3000 at 1 PM, America/Los_Angeles
      }
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
            key = PRIMITIVE_REPORTING_SET_NAME
            value =
              ReportKt.reportingMetricCalculationSpec {
                metricCalculationSpecs += METRIC_CALCULATION_SPEC_NAME
              }
          }
      }
      state = ReportSchedule.State.forNumber(INTERNAL_REPORT_SCHEDULE.state.number)
      eventStart = INTERNAL_REPORT_SCHEDULE.details.eventStart
      eventEnd = INTERNAL_REPORT_SCHEDULE.details.eventEnd
      frequency =
        ReportScheduleKt.frequency { daily = ReportSchedule.Frequency.Daily.getDefaultInstance() }
      reportWindow =
        ReportScheduleKt.reportWindow {
          trailingWindow =
            ReportScheduleKt.ReportWindowKt.trailingWindow {
              count = INTERNAL_REPORT_SCHEDULE.details.reportWindow.trailingWindow.count
              increment =
                ReportWindow.TrailingWindow.Increment.forNumber(
                  INTERNAL_REPORT_SCHEDULE.details.reportWindow.trailingWindow.increment.number
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

    private val DATA_PROVIDER = dataProvider {
      dataAvailabilityInterval = interval {
        startTime = timestamp {
          seconds = 946760400 // January 1, 2000 at 1 PM, America/Los_Angeles
        }
        endTime = timestamp {
          seconds = 64060664400 // January 1, 4000 at 1 PM, America/Los_Angeles
        }
      }
    }

    private val EVENT_GROUP = eventGroup {
      dataAvailabilityInterval = interval {
        startTime = timestamp {
          seconds = 946760400 // January 1, 2000 at 1 PM, America/Los_Angeles
        }
        endTime = timestamp {
          seconds = 64060664400 // January 1, 4000 at 1 PM, America/Los_Angeles
        }
      }
    }
  }
}
