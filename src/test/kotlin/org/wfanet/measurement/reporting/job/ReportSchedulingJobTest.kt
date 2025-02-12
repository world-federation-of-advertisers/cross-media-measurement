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

package org.wfanet.measurement.reporting.job

import com.google.common.truth.extensions.proto.ProtoTruth.assertThat
import com.google.protobuf.timestamp
import com.google.type.DayOfWeek
import com.google.type.date
import com.google.type.dateTime
import com.google.type.interval
import com.google.type.timeZone
import io.grpc.Status
import kotlinx.coroutines.runBlocking
import org.junit.Before
import org.junit.Rule
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.mockito.kotlin.KArgumentCaptor
import org.mockito.kotlin.any
import org.mockito.kotlin.argumentCaptor
import org.mockito.kotlin.eq
import org.mockito.kotlin.times
import org.mockito.kotlin.verifyBlocking
import org.mockito.kotlin.whenever
import org.wfanet.measurement.api.v2alpha.DataProvidersGrpcKt.DataProvidersCoroutineImplBase
import org.wfanet.measurement.api.v2alpha.DataProvidersGrpcKt.DataProvidersCoroutineStub
import org.wfanet.measurement.api.v2alpha.EventGroupsGrpcKt.EventGroupsCoroutineImplBase
import org.wfanet.measurement.api.v2alpha.EventGroupsGrpcKt.EventGroupsCoroutineStub
import org.wfanet.measurement.api.v2alpha.GetDataProviderRequest
import org.wfanet.measurement.api.v2alpha.GetEventGroupRequest
import org.wfanet.measurement.api.v2alpha.copy
import org.wfanet.measurement.api.v2alpha.dataProvider
import org.wfanet.measurement.api.v2alpha.eventGroup
import org.wfanet.measurement.api.v2alpha.getDataProviderRequest
import org.wfanet.measurement.api.v2alpha.getEventGroupRequest
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.common.grpc.testing.mockService
import org.wfanet.measurement.common.testing.verifyProtoArgument
import org.wfanet.measurement.config.reporting.measurementConsumerConfig
import org.wfanet.measurement.config.reporting.measurementConsumerConfigs
import org.wfanet.measurement.internal.reporting.v2.BatchGetReportingSetsRequest
import org.wfanet.measurement.internal.reporting.v2.ListReportSchedulesRequest
import org.wfanet.measurement.internal.reporting.v2.ListReportSchedulesRequestKt
import org.wfanet.measurement.internal.reporting.v2.ReportKt as InternalReportKt
import org.wfanet.measurement.internal.reporting.v2.ReportSchedule
import org.wfanet.measurement.internal.reporting.v2.ReportScheduleIteration
import org.wfanet.measurement.internal.reporting.v2.ReportScheduleIterationsGrpcKt.ReportScheduleIterationsCoroutineImplBase
import org.wfanet.measurement.internal.reporting.v2.ReportScheduleIterationsGrpcKt.ReportScheduleIterationsCoroutineStub
import org.wfanet.measurement.internal.reporting.v2.ReportScheduleKt
import org.wfanet.measurement.internal.reporting.v2.ReportSchedulesGrpcKt.ReportSchedulesCoroutineImplBase
import org.wfanet.measurement.internal.reporting.v2.ReportSchedulesGrpcKt.ReportSchedulesCoroutineStub
import org.wfanet.measurement.internal.reporting.v2.ReportingSet
import org.wfanet.measurement.internal.reporting.v2.ReportingSetKt
import org.wfanet.measurement.internal.reporting.v2.ReportingSetsGrpcKt.ReportingSetsCoroutineImplBase
import org.wfanet.measurement.internal.reporting.v2.ReportingSetsGrpcKt.ReportingSetsCoroutineStub
import org.wfanet.measurement.internal.reporting.v2.SetReportScheduleIterationStateRequest
import org.wfanet.measurement.internal.reporting.v2.batchGetReportingSetsResponse
import org.wfanet.measurement.internal.reporting.v2.copy
import org.wfanet.measurement.internal.reporting.v2.listReportSchedulesRequest
import org.wfanet.measurement.internal.reporting.v2.listReportSchedulesResponse
import org.wfanet.measurement.internal.reporting.v2.report as internalReport
import org.wfanet.measurement.internal.reporting.v2.reportSchedule
import org.wfanet.measurement.internal.reporting.v2.reportScheduleIteration
import org.wfanet.measurement.internal.reporting.v2.reportingSet
import org.wfanet.measurement.internal.reporting.v2.setReportScheduleIterationStateRequest
import org.wfanet.measurement.internal.reporting.v2.stopReportScheduleRequest
import org.wfanet.measurement.reporting.service.api.v2alpha.MetricCalculationSpecKey
import org.wfanet.measurement.reporting.service.api.v2alpha.ReportingSetKey
import org.wfanet.measurement.reporting.service.api.v2alpha.toPublic
import org.wfanet.measurement.reporting.v2alpha.CreateReportRequest
import org.wfanet.measurement.reporting.v2alpha.Report
import org.wfanet.measurement.reporting.v2alpha.ReportKt
import org.wfanet.measurement.reporting.v2alpha.ReportsGrpcKt.ReportsCoroutineImplBase
import org.wfanet.measurement.reporting.v2alpha.ReportsGrpcKt.ReportsCoroutineStub
import org.wfanet.measurement.reporting.v2alpha.copy
import org.wfanet.measurement.reporting.v2alpha.createReportRequest
import org.wfanet.measurement.reporting.v2alpha.report

@RunWith(JUnit4::class)
class ReportSchedulingJobTest {
  private val dataProvidersMock: DataProvidersCoroutineImplBase = mockService {
    onBlocking { getDataProvider(eq(getDataProviderRequest { name = DATA_PROVIDER_NAME })) }
      .thenReturn(DATA_PROVIDER)
  }

  private val eventGroupsMock: EventGroupsCoroutineImplBase = mockService {
    onBlocking { getEventGroup(eq(getEventGroupRequest { name = EVENT_GROUP_NAME })) }
      .thenReturn(EVENT_GROUP)
  }
  private val reportingSetsMock: ReportingSetsCoroutineImplBase = mockService {
    onBlocking { batchGetReportingSets(any()) }
      .thenAnswer {
        val request = it.arguments[0] as BatchGetReportingSetsRequest
        val reportingSetsMap =
          mapOf(
            INTERNAL_COMPOSITE_REPORTING_SET.externalReportingSetId to
              INTERNAL_COMPOSITE_REPORTING_SET,
            INTERNAL_PRIMITIVE_REPORTING_SET.externalReportingSetId to
              INTERNAL_PRIMITIVE_REPORTING_SET,
          )
        batchGetReportingSetsResponse {
          reportingSets +=
            request.externalReportingSetIdsList.map { id -> reportingSetsMap.getValue(id) }
        }
      }
  }
  private val reportScheduleIterationsMock: ReportScheduleIterationsCoroutineImplBase =
    mockService {
      onBlocking { createReportScheduleIteration(any()) }
        .thenReturn(INTERNAL_REPORT_SCHEDULE_ITERATION)

      onBlocking { setReportScheduleIterationState(any()) }
        .thenReturn(INTERNAL_REPORT_SCHEDULE_ITERATION)
    }
  private val reportSchedulesMock: ReportSchedulesCoroutineImplBase = mockService {
    onBlocking { listReportSchedules(any()) }
      .thenReturn(listReportSchedulesResponse { reportSchedules += INTERNAL_REPORT_SCHEDULE })

    onBlocking { stopReportSchedule(any()) }.thenReturn(INTERNAL_REPORT_SCHEDULE)
  }
  private val reportsMock: ReportsCoroutineImplBase = mockService {
    onBlocking { createReport(any()) }.thenReturn(REPORT)
  }

  @get:Rule
  val grpcTestServerRule = GrpcTestServerRule {
    addService(dataProvidersMock)
    addService(eventGroupsMock)
    addService(reportingSetsMock)
    addService(reportScheduleIterationsMock)
    addService(reportSchedulesMock)
    addService(reportsMock)
  }

  private lateinit var job: ReportSchedulingJob

  @Before
  fun initJob() {
    job =
      ReportSchedulingJob(
        MEASUREMENT_CONSUMER_CONFIGS,
        DataProvidersCoroutineStub(grpcTestServerRule.channel),
        EventGroupsCoroutineStub(grpcTestServerRule.channel),
        ReportingSetsCoroutineStub(grpcTestServerRule.channel),
        ReportScheduleIterationsCoroutineStub(grpcTestServerRule.channel),
        ReportSchedulesCoroutineStub(grpcTestServerRule.channel),
        ReportsCoroutineStub(grpcTestServerRule.channel),
      )
  }

  @Test
  fun `execute creates report for a single schedule for a new iteration`() = runBlocking {
    whenever(reportSchedulesMock.listReportSchedules(any()))
      .thenReturn(
        listReportSchedulesResponse {
          reportSchedules += INTERNAL_REPORT_SCHEDULE.copy { clearLatestIteration() }
        }
      )

    job.execute()

    verifyProtoArgument(reportSchedulesMock, ReportSchedulesCoroutineImplBase::listReportSchedules)
      .isEqualTo(
        listReportSchedulesRequest {
          filter =
            ListReportSchedulesRequestKt.filter {
              cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
              state = ReportSchedule.State.ACTIVE
            }
          limit = BATCH_SIZE
        }
      )

    verifyProtoArgument(
        reportScheduleIterationsMock,
        ReportScheduleIterationsCoroutineImplBase::createReportScheduleIteration,
      )
      .ignoringFields(ReportScheduleIteration.CREATE_REPORT_REQUEST_ID_FIELD_NUMBER)
      .isEqualTo(
        reportScheduleIteration {
          cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
          externalReportScheduleId = REPORT_SCHEDULE_ID
          reportEventTime = INTERNAL_REPORT_SCHEDULE.nextReportCreationTime
        }
      )

    val publicReportSchedule = INTERNAL_REPORT_SCHEDULE.toPublic()
    verifyProtoArgument(reportsMock, ReportsCoroutineImplBase::createReport)
      .ignoringFields(CreateReportRequest.REPORT_ID_FIELD_NUMBER)
      .isEqualTo(
        createReportRequest {
          parent = MEASUREMENT_CONSUMER_NAME
          requestId = INTERNAL_REPORT_SCHEDULE_ITERATION.createReportRequestId
          report =
            publicReportSchedule.reportTemplate.copy {
              reportingInterval =
                ReportKt.reportingInterval {
                  reportStart = dateTime {
                    year = 2022
                    month = 1
                    day = 1
                    hours = 13
                    timeZone = timeZone { id = "America/Los_Angeles" }
                  }
                  reportEnd = date {
                    year = 2022
                    month = 1
                    day = 2
                  }
                }
            }
        }
      )

    verifyProtoArgument(
        reportScheduleIterationsMock,
        ReportScheduleIterationsCoroutineImplBase::setReportScheduleIterationState,
      )
      .isEqualTo(
        setReportScheduleIterationStateRequest {
          cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
          externalReportScheduleId = REPORT_SCHEDULE_ID
          externalReportScheduleIterationId = REPORT_SCHEDULE_ITERATION_ID

          state = ReportScheduleIteration.State.REPORT_CREATED
        }
      )
  }

  @Test
  fun `execute stops report schedule after report creation when next time after event end`() =
    runBlocking {
      whenever(reportSchedulesMock.listReportSchedules(any()))
        .thenReturn(
          listReportSchedulesResponse {
            reportSchedules +=
              INTERNAL_REPORT_SCHEDULE.copy {
                clearLatestIteration()
                details =
                  INTERNAL_REPORT_SCHEDULE.details.copy {
                    eventStart = dateTime {
                      year = 2022
                      month = 1
                      day = 2 // Saturday
                      hours = 13
                      timeZone = timeZone { id = "America/Los_Angeles" }
                    }
                    eventEnd = date {
                      year = 2022
                      month = 1
                      day = 3
                    }
                    frequency =
                      ReportScheduleKt.frequency {
                        weekly =
                          ReportScheduleKt.FrequencyKt.weekly { dayOfWeek = DayOfWeek.SATURDAY }
                      }
                    reportWindow =
                      ReportScheduleKt.reportWindow {
                        trailingWindow =
                          ReportScheduleKt.ReportWindowKt.trailingWindow {
                            count = 1
                            increment = ReportSchedule.ReportWindow.TrailingWindow.Increment.DAY
                          }
                      }
                  }
              }
          }
        )

      job.execute()

      verifyProtoArgument(
          reportSchedulesMock,
          ReportSchedulesCoroutineImplBase::listReportSchedules,
        )
        .isEqualTo(
          listReportSchedulesRequest {
            filter =
              ListReportSchedulesRequestKt.filter {
                cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
                state = ReportSchedule.State.ACTIVE
              }
            limit = BATCH_SIZE
          }
        )

      verifyProtoArgument(
          reportScheduleIterationsMock,
          ReportScheduleIterationsCoroutineImplBase::createReportScheduleIteration,
        )
        .ignoringFields(ReportScheduleIteration.CREATE_REPORT_REQUEST_ID_FIELD_NUMBER)
        .isEqualTo(
          reportScheduleIteration {
            cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
            externalReportScheduleId = REPORT_SCHEDULE_ID
            reportEventTime = INTERNAL_REPORT_SCHEDULE.nextReportCreationTime
          }
        )

      val publicReportSchedule = INTERNAL_REPORT_SCHEDULE.toPublic()
      verifyProtoArgument(reportsMock, ReportsCoroutineImplBase::createReport)
        .ignoringFields(CreateReportRequest.REPORT_ID_FIELD_NUMBER)
        .isEqualTo(
          createReportRequest {
            parent = MEASUREMENT_CONSUMER_NAME
            requestId = INTERNAL_REPORT_SCHEDULE_ITERATION.createReportRequestId
            report =
              publicReportSchedule.reportTemplate.copy {
                reportingInterval =
                  ReportKt.reportingInterval {
                    reportStart = dateTime {
                      year = 2022
                      month = 1
                      day = 1
                      hours = 13
                      timeZone = timeZone { id = "America/Los_Angeles" }
                    }
                    reportEnd = date {
                      year = 2022
                      month = 1
                      day = 2
                    }
                  }
              }
          }
        )

      verifyProtoArgument(reportSchedulesMock, ReportSchedulesCoroutineImplBase::stopReportSchedule)
        .isEqualTo(
          stopReportScheduleRequest {
            cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
            externalReportScheduleId = REPORT_SCHEDULE_ID
          }
        )

      verifyProtoArgument(
          reportScheduleIterationsMock,
          ReportScheduleIterationsCoroutineImplBase::setReportScheduleIterationState,
        )
        .isEqualTo(
          setReportScheduleIterationStateRequest {
            cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
            externalReportScheduleId = REPORT_SCHEDULE_ID
            externalReportScheduleIterationId = REPORT_SCHEDULE_ITERATION_ID

            state = ReportScheduleIteration.State.REPORT_CREATED
          }
        )
    }

  @Test
  fun `execute creates reports for multiple schedules for 1 mc for new iterations`(): Unit =
    runBlocking {
      val otherReportScheduleId = REPORT_SCHEDULE_ID + "a"
      whenever(reportSchedulesMock.listReportSchedules(any()))
        .thenReturn(
          listReportSchedulesResponse {
            reportSchedules += INTERNAL_REPORT_SCHEDULE.copy { clearLatestIteration() }
            reportSchedules +=
              INTERNAL_REPORT_SCHEDULE.copy {
                externalReportScheduleId = otherReportScheduleId
                clearLatestIteration()
              }
          }
        )

      job.execute()

      verifyProtoArgument(
          reportSchedulesMock,
          ReportSchedulesCoroutineImplBase::listReportSchedules,
        )
        .isEqualTo(
          listReportSchedulesRequest {
            filter =
              ListReportSchedulesRequestKt.filter {
                cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
                state = ReportSchedule.State.ACTIVE
              }
            limit = BATCH_SIZE
          }
        )

      val createReportScheduleIterationCaptor: KArgumentCaptor<ReportScheduleIteration> =
        argumentCaptor()
      verifyBlocking(reportScheduleIterationsMock, times(2)) {
        createReportScheduleIteration(createReportScheduleIterationCaptor.capture())
      }
      val reportScheduleIteration = reportScheduleIteration {
        cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
        externalReportScheduleId = REPORT_SCHEDULE_ID
        reportEventTime = INTERNAL_REPORT_SCHEDULE.nextReportCreationTime
      }
      assertThat(createReportScheduleIterationCaptor.allValues)
        .ignoringFields(ReportScheduleIteration.CREATE_REPORT_REQUEST_ID_FIELD_NUMBER)
        .containsExactly(
          reportScheduleIteration,
          reportScheduleIteration.copy { externalReportScheduleId = otherReportScheduleId },
        )

      val getDataProviderCaptor: KArgumentCaptor<GetDataProviderRequest> = argumentCaptor()
      verifyBlocking(dataProvidersMock, times(1)) {
        getDataProvider(getDataProviderCaptor.capture())
      }

      val getEventGroupCaptor: KArgumentCaptor<GetEventGroupRequest> = argumentCaptor()
      verifyBlocking(eventGroupsMock, times(1)) { getEventGroup(getEventGroupCaptor.capture()) }

      val createReportCaptor: KArgumentCaptor<CreateReportRequest> = argumentCaptor()
      verifyBlocking(reportsMock, times(2)) { createReport(createReportCaptor.capture()) }
      val publicReportSchedule = INTERNAL_REPORT_SCHEDULE.toPublic()
      val createReportRequest = createReportRequest {
        parent = MEASUREMENT_CONSUMER_NAME
        requestId = INTERNAL_REPORT_SCHEDULE_ITERATION.createReportRequestId
        report =
          publicReportSchedule.reportTemplate.copy {
            reportingInterval =
              ReportKt.reportingInterval {
                reportStart = dateTime {
                  year = 2022
                  month = 1
                  day = 1
                  hours = 13
                  timeZone = timeZone { id = "America/Los_Angeles" }
                }
                reportEnd = date {
                  year = 2022
                  month = 1
                  day = 2
                }
              }
          }
      }
      assertThat(createReportCaptor.allValues)
        .ignoringFields(CreateReportRequest.REPORT_ID_FIELD_NUMBER)
        .containsExactly(createReportRequest, createReportRequest)

      val setReportScheduleIterationStateCaptor:
        KArgumentCaptor<SetReportScheduleIterationStateRequest> =
        argumentCaptor()
      verifyBlocking(reportScheduleIterationsMock, times(2)) {
        setReportScheduleIterationState(setReportScheduleIterationStateCaptor.capture())
      }
      val setReportScheduleIterationStateRequest = setReportScheduleIterationStateRequest {
        cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
        externalReportScheduleId = REPORT_SCHEDULE_ID
        externalReportScheduleIterationId = REPORT_SCHEDULE_ITERATION_ID

        state = ReportScheduleIteration.State.REPORT_CREATED
      }
      assertThat(setReportScheduleIterationStateCaptor.allValues)
        .containsExactly(
          setReportScheduleIterationStateRequest,
          setReportScheduleIterationStateRequest.copy {
            externalReportScheduleId = otherReportScheduleId
          },
        )
    }

  @Test
  fun `execute creates report for schedule if other has error creating report`(): Unit =
    runBlocking {
      val otherReportScheduleId = REPORT_SCHEDULE_ID + "a"
      whenever(reportSchedulesMock.listReportSchedules(any()))
        .thenReturn(
          listReportSchedulesResponse {
            reportSchedules += INTERNAL_REPORT_SCHEDULE.copy { clearLatestIteration() }
            reportSchedules +=
              INTERNAL_REPORT_SCHEDULE.copy {
                externalReportScheduleId = otherReportScheduleId
                clearLatestIteration()
              }
          }
        )

      whenever(reportsMock.createReport(any()))
        .thenReturn(REPORT)
        .thenThrow(Status.UNKNOWN.asRuntimeException())

      job.execute()

      val createReportScheduleIterationCaptor: KArgumentCaptor<ReportScheduleIteration> =
        argumentCaptor()
      verifyBlocking(reportScheduleIterationsMock, times(2)) {
        createReportScheduleIteration(createReportScheduleIterationCaptor.capture())
      }
      val reportScheduleIteration = reportScheduleIteration {
        cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
        externalReportScheduleId = REPORT_SCHEDULE_ID
        reportEventTime = INTERNAL_REPORT_SCHEDULE.nextReportCreationTime
      }
      assertThat(createReportScheduleIterationCaptor.allValues)
        .ignoringFields(ReportScheduleIteration.CREATE_REPORT_REQUEST_ID_FIELD_NUMBER)
        .containsExactly(
          reportScheduleIteration,
          reportScheduleIteration.copy { externalReportScheduleId = otherReportScheduleId },
        )

      val getDataProviderCaptor: KArgumentCaptor<GetDataProviderRequest> = argumentCaptor()
      verifyBlocking(dataProvidersMock, times(1)) {
        getDataProvider(getDataProviderCaptor.capture())
      }

      val getEventGroupCaptor: KArgumentCaptor<GetEventGroupRequest> = argumentCaptor()
      verifyBlocking(eventGroupsMock, times(1)) { getEventGroup(getEventGroupCaptor.capture()) }

      val createReportCaptor: KArgumentCaptor<CreateReportRequest> = argumentCaptor()
      verifyBlocking(reportsMock, times(2)) { createReport(createReportCaptor.capture()) }
      val publicReportSchedule = INTERNAL_REPORT_SCHEDULE.toPublic()
      val createReportRequest = createReportRequest {
        parent = MEASUREMENT_CONSUMER_NAME
        requestId = INTERNAL_REPORT_SCHEDULE_ITERATION.createReportRequestId
        report =
          publicReportSchedule.reportTemplate.copy {
            reportingInterval =
              ReportKt.reportingInterval {
                reportStart = dateTime {
                  year = 2022
                  month = 1
                  day = 1
                  hours = 13
                  timeZone = timeZone { id = "America/Los_Angeles" }
                }
                reportEnd = date {
                  year = 2022
                  month = 1
                  day = 2
                }
              }
          }
      }
      assertThat(createReportCaptor.allValues)
        .ignoringFields(CreateReportRequest.REPORT_ID_FIELD_NUMBER)
        .containsExactly(createReportRequest, createReportRequest)

      val setReportScheduleIterationStateCaptor:
        KArgumentCaptor<SetReportScheduleIterationStateRequest> =
        argumentCaptor()
      verifyBlocking(reportScheduleIterationsMock, times(2)) {
        setReportScheduleIterationState(setReportScheduleIterationStateCaptor.capture())
      }
      val setReportScheduleIterationStateSuccessRequest = setReportScheduleIterationStateRequest {
        cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
        externalReportScheduleId = REPORT_SCHEDULE_ID
        externalReportScheduleIterationId = REPORT_SCHEDULE_ITERATION_ID

        state = ReportScheduleIteration.State.REPORT_CREATED
      }
      val setReportScheduleIterationStateFailureRequest = setReportScheduleIterationStateRequest {
        cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
        externalReportScheduleId = otherReportScheduleId
        externalReportScheduleIterationId = REPORT_SCHEDULE_ITERATION_ID

        state = ReportScheduleIteration.State.RETRYING_REPORT_CREATION
      }
      assertThat(setReportScheduleIterationStateCaptor.allValues)
        .containsExactly(
          setReportScheduleIterationStateSuccessRequest,
          setReportScheduleIterationStateFailureRequest,
        )
    }

  @Test
  fun `execute creates report for schedule if other has error getting data provider`(): Unit =
    runBlocking {
      val otherReportScheduleId = REPORT_SCHEDULE_ID + "a"
      whenever(reportSchedulesMock.listReportSchedules(any()))
        .thenReturn(
          listReportSchedulesResponse {
            reportSchedules += INTERNAL_REPORT_SCHEDULE.copy { clearLatestIteration() }
            reportSchedules +=
              INTERNAL_REPORT_SCHEDULE.copy {
                externalReportScheduleId = otherReportScheduleId
                clearLatestIteration()
              }
          }
        )

      whenever(dataProvidersMock.getDataProvider(any()))
        .thenThrow(Status.UNKNOWN.asRuntimeException())
        .thenReturn(DATA_PROVIDER)

      job.execute()

      val createReportScheduleIterationCaptor: KArgumentCaptor<ReportScheduleIteration> =
        argumentCaptor()
      verifyBlocking(reportScheduleIterationsMock, times(2)) {
        createReportScheduleIteration(createReportScheduleIterationCaptor.capture())
      }
      val reportScheduleIteration = reportScheduleIteration {
        cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
        externalReportScheduleId = REPORT_SCHEDULE_ID
        reportEventTime = INTERNAL_REPORT_SCHEDULE.nextReportCreationTime
      }
      assertThat(createReportScheduleIterationCaptor.allValues)
        .ignoringFields(ReportScheduleIteration.CREATE_REPORT_REQUEST_ID_FIELD_NUMBER)
        .containsExactly(
          reportScheduleIteration,
          reportScheduleIteration.copy { externalReportScheduleId = otherReportScheduleId },
        )

      val getDataProviderCaptor: KArgumentCaptor<GetDataProviderRequest> = argumentCaptor()
      verifyBlocking(dataProvidersMock, times(2)) {
        getDataProvider(getDataProviderCaptor.capture())
      }

      val getEventGroupCaptor: KArgumentCaptor<GetEventGroupRequest> = argumentCaptor()
      verifyBlocking(eventGroupsMock, times(1)) { getEventGroup(getEventGroupCaptor.capture()) }

      val createReportCaptor: KArgumentCaptor<CreateReportRequest> = argumentCaptor()
      verifyBlocking(reportsMock, times(1)) { createReport(createReportCaptor.capture()) }
      val publicReportSchedule = INTERNAL_REPORT_SCHEDULE.toPublic()
      val createReportRequest = createReportRequest {
        parent = MEASUREMENT_CONSUMER_NAME
        requestId = INTERNAL_REPORT_SCHEDULE_ITERATION.createReportRequestId
        report =
          publicReportSchedule.reportTemplate.copy {
            reportingInterval =
              ReportKt.reportingInterval {
                reportStart = dateTime {
                  year = 2022
                  month = 1
                  day = 1
                  hours = 13
                  timeZone = timeZone { id = "America/Los_Angeles" }
                }
                reportEnd = date {
                  year = 2022
                  month = 1
                  day = 2
                }
              }
          }
      }
      assertThat(createReportCaptor.allValues)
        .ignoringFields(CreateReportRequest.REPORT_ID_FIELD_NUMBER)
        .containsExactly(createReportRequest)

      val setReportScheduleIterationStateCaptor:
        KArgumentCaptor<SetReportScheduleIterationStateRequest> =
        argumentCaptor()
      verifyBlocking(reportScheduleIterationsMock, times(1)) {
        setReportScheduleIterationState(setReportScheduleIterationStateCaptor.capture())
      }
      val setReportScheduleIterationStateSuccessRequest = setReportScheduleIterationStateRequest {
        cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
        externalReportScheduleId = otherReportScheduleId
        externalReportScheduleIterationId = REPORT_SCHEDULE_ITERATION_ID

        state = ReportScheduleIteration.State.REPORT_CREATED
      }
      assertThat(setReportScheduleIterationStateCaptor.allValues)
        .containsExactly(setReportScheduleIterationStateSuccessRequest)
    }

  @Test
  fun `execute creates reports for multiple schedules across 2 mcs for new iterations`(): Unit =
    runBlocking {
      val measurementConsumerConfigs = measurementConsumerConfigs {
        configs[MEASUREMENT_CONSUMER_NAME] = measurementConsumerConfig {
          apiKey = "123"
          offlinePrincipal = "principals/mc-user"
        }
        configs[MEASUREMENT_CONSUMER_NAME_2] = measurementConsumerConfig {
          apiKey = "123"
          offlinePrincipal = "principals/mc2-user"
        }
      }

      job =
        ReportSchedulingJob(
          measurementConsumerConfigs,
          DataProvidersCoroutineStub(grpcTestServerRule.channel),
          EventGroupsCoroutineStub(grpcTestServerRule.channel),
          ReportingSetsCoroutineStub(grpcTestServerRule.channel),
          ReportScheduleIterationsCoroutineStub(grpcTestServerRule.channel),
          ReportSchedulesCoroutineStub(grpcTestServerRule.channel),
          ReportsCoroutineStub(grpcTestServerRule.channel),
        )

      whenever(reportSchedulesMock.listReportSchedules(any())).thenAnswer {
        val request = it.arguments[0] as ListReportSchedulesRequest
        listReportSchedulesResponse {
          reportSchedules +=
            INTERNAL_REPORT_SCHEDULE.copy {
              cmmsMeasurementConsumerId = request.filter.cmmsMeasurementConsumerId
              clearLatestIteration()
            }
        }
      }

      job.execute()

      val createReportScheduleIterationCaptor: KArgumentCaptor<ReportScheduleIteration> =
        argumentCaptor()
      verifyBlocking(reportScheduleIterationsMock, times(2)) {
        createReportScheduleIteration(createReportScheduleIterationCaptor.capture())
      }
      val reportScheduleIteration = reportScheduleIteration {
        cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
        externalReportScheduleId = REPORT_SCHEDULE_ID
        reportEventTime = INTERNAL_REPORT_SCHEDULE.nextReportCreationTime
      }
      assertThat(createReportScheduleIterationCaptor.allValues)
        .ignoringFields(ReportScheduleIteration.CREATE_REPORT_REQUEST_ID_FIELD_NUMBER)
        .containsExactly(
          reportScheduleIteration,
          reportScheduleIteration.copy {
            cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID_2
          },
        )

      val getDataProviderCaptor: KArgumentCaptor<GetDataProviderRequest> = argumentCaptor()
      verifyBlocking(dataProvidersMock, times(1)) {
        getDataProvider(getDataProviderCaptor.capture())
      }

      val getEventGroupCaptor: KArgumentCaptor<GetEventGroupRequest> = argumentCaptor()
      verifyBlocking(eventGroupsMock, times(2)) { getEventGroup(getEventGroupCaptor.capture()) }

      val createReportCaptor: KArgumentCaptor<CreateReportRequest> = argumentCaptor()
      verifyBlocking(reportsMock, times(2)) { createReport(createReportCaptor.capture()) }
      val publicReportSchedule = INTERNAL_REPORT_SCHEDULE.toPublic()
      val createReportRequest = createReportRequest {
        parent = MEASUREMENT_CONSUMER_NAME
        requestId = INTERNAL_REPORT_SCHEDULE_ITERATION.createReportRequestId
        report =
          publicReportSchedule.reportTemplate.copy {
            reportingInterval =
              ReportKt.reportingInterval {
                reportStart = dateTime {
                  year = 2022
                  month = 1
                  day = 1
                  hours = 13
                  timeZone = timeZone { id = "America/Los_Angeles" }
                }
                reportEnd = date {
                  year = 2022
                  month = 1
                  day = 2
                }
              }
          }
      }
      assertThat(createReportCaptor.allValues)
        .ignoringFields(CreateReportRequest.REPORT_ID_FIELD_NUMBER)
        .containsExactly(
          createReportRequest,
          createReportRequest.copy {
            parent = MEASUREMENT_CONSUMER_NAME_2
            report =
              createReportRequest.report.copy {
                reportingMetricEntries.clear()
                reportingMetricEntries +=
                  ReportKt.reportingMetricEntry {
                    key =
                      ReportingSetKey(CMMS_MEASUREMENT_CONSUMER_ID_2, PRIMITIVE_REPORTING_SET_ID)
                        .toName()
                    value =
                      ReportKt.reportingMetricCalculationSpec {
                        metricCalculationSpecs +=
                          MetricCalculationSpecKey(
                              CMMS_MEASUREMENT_CONSUMER_ID_2,
                              METRIC_CALCULATION_SPEC_ID,
                            )
                            .toName()
                      }
                  }
              }
          },
        )

      val setReportScheduleIterationStateCaptor:
        KArgumentCaptor<SetReportScheduleIterationStateRequest> =
        argumentCaptor()
      verifyBlocking(reportScheduleIterationsMock, times(2)) {
        setReportScheduleIterationState(setReportScheduleIterationStateCaptor.capture())
      }
      val setReportScheduleIterationStateRequest = setReportScheduleIterationStateRequest {
        cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
        externalReportScheduleId = REPORT_SCHEDULE_ID
        externalReportScheduleIterationId = REPORT_SCHEDULE_ITERATION_ID

        state = ReportScheduleIteration.State.REPORT_CREATED
      }
      assertThat(setReportScheduleIterationStateCaptor.allValues)
        .containsExactly(
          setReportScheduleIterationStateRequest,
          setReportScheduleIterationStateRequest.copy {
            cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID_2
          },
        )
    }

  @Test
  fun `execute does not create report when data provider start after window start`() = runBlocking {
    whenever(
        dataProvidersMock.getDataProvider(eq(getDataProviderRequest { name = DATA_PROVIDER_NAME }))
      )
      .thenReturn(
        DATA_PROVIDER.copy {
          dataAvailabilityInterval = interval {
            startTime = timestamp {
              seconds = 221845467600 // January 1, 9000 at 1 PM, America/Los_Angeles
            }

            endTime = timestamp {
              seconds = 221877003600 // January 1, 9001 at 1 PM, America/Los_Angeles
            }
          }
        }
      )

    whenever(eventGroupsMock.getEventGroup(eq(getEventGroupRequest { name = EVENT_GROUP_NAME })))
      .thenReturn(
        EVENT_GROUP.copy {
          dataAvailabilityInterval = interval {
            startTime = timestamp {
              seconds = 946760400 // January 1, 2000 at 1 PM, America/Los_Angeles
            }

            endTime = timestamp {
              seconds = 95617659600 // January 1, 5000 at 1 PM, America/Los_Angeles
            }
          }
        }
      )

    job.execute()

    verifyProtoArgument(
        reportScheduleIterationsMock,
        ReportScheduleIterationsCoroutineImplBase::createReportScheduleIteration,
      )
      .ignoringFields(ReportScheduleIteration.CREATE_REPORT_REQUEST_ID_FIELD_NUMBER)
      .isEqualTo(
        reportScheduleIteration {
          cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
          externalReportScheduleId = REPORT_SCHEDULE_ID
          reportEventTime = INTERNAL_REPORT_SCHEDULE.nextReportCreationTime
        }
      )

    verifyProtoArgument(dataProvidersMock, DataProvidersCoroutineImplBase::getDataProvider)
      .isEqualTo(getDataProviderRequest { name = DATA_PROVIDER_NAME })

    verifyProtoArgument(eventGroupsMock, EventGroupsCoroutineImplBase::getEventGroup)
      .isEqualTo(getEventGroupRequest { name = EVENT_GROUP_NAME })

    val setReportScheduleIterationStateCaptor:
      KArgumentCaptor<SetReportScheduleIterationStateRequest> =
      argumentCaptor()
    verifyBlocking(reportScheduleIterationsMock, times(0)) {
      setReportScheduleIterationState(setReportScheduleIterationStateCaptor.capture())
    }
  }

  @Test
  fun `execute does not create report when data provider end before window end`() = runBlocking {
    whenever(
        dataProvidersMock.getDataProvider(eq(getDataProviderRequest { name = DATA_PROVIDER_NAME }))
      )
      .thenReturn(
        DATA_PROVIDER.copy {
          dataAvailabilityInterval = interval {
            startTime = timestamp {
              seconds = 631227600 // January 1, 1990 at 1 PM, America/Los_Angeles
            }

            endTime = timestamp {
              seconds = 946760400 // January 1, 2000 at 1 PM, America/Los_Angeles
            }
          }
        }
      )

    whenever(eventGroupsMock.getEventGroup(eq(getEventGroupRequest { name = EVENT_GROUP_NAME })))
      .thenReturn(
        EVENT_GROUP.copy {
          dataAvailabilityInterval = interval {
            startTime = timestamp {
              seconds = 631227600 // January 1, 1990 at 1 PM, America/Los_Angeles
            }

            endTime = timestamp {
              seconds = 95617659600 // January 1, 5000 at 1 PM, America/Los_Angeles
            }
          }
        }
      )

    job.execute()

    verifyProtoArgument(
        reportScheduleIterationsMock,
        ReportScheduleIterationsCoroutineImplBase::createReportScheduleIteration,
      )
      .ignoringFields(ReportScheduleIteration.CREATE_REPORT_REQUEST_ID_FIELD_NUMBER)
      .isEqualTo(
        reportScheduleIteration {
          cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
          externalReportScheduleId = REPORT_SCHEDULE_ID
          reportEventTime = INTERNAL_REPORT_SCHEDULE.nextReportCreationTime
        }
      )

    verifyProtoArgument(dataProvidersMock, DataProvidersCoroutineImplBase::getDataProvider)
      .isEqualTo(getDataProviderRequest { name = DATA_PROVIDER_NAME })

    verifyProtoArgument(eventGroupsMock, EventGroupsCoroutineImplBase::getEventGroup)
      .isEqualTo(getEventGroupRequest { name = EVENT_GROUP_NAME })

    val setReportScheduleIterationStateCaptor:
      KArgumentCaptor<SetReportScheduleIterationStateRequest> =
      argumentCaptor()
    verifyBlocking(reportScheduleIterationsMock, times(0)) {
      setReportScheduleIterationState(setReportScheduleIterationStateCaptor.capture())
    }
  }

  @Test
  fun `execute does not create report when event group start after window start`() = runBlocking {
    whenever(
        dataProvidersMock.getDataProvider(eq(getDataProviderRequest { name = DATA_PROVIDER_NAME }))
      )
      .thenReturn(
        DATA_PROVIDER.copy {
          dataAvailabilityInterval = interval {
            startTime = timestamp {
              seconds = 946760400 // January 1, 2000 at 1 PM, America/Los_Angeles
            }

            endTime = timestamp {
              seconds = 221877003600 // January 1, 9001 at 1 PM, America/Los_Angeles
            }
          }
        }
      )

    whenever(eventGroupsMock.getEventGroup(eq(getEventGroupRequest { name = EVENT_GROUP_NAME })))
      .thenReturn(
        EVENT_GROUP.copy {
          dataAvailabilityInterval = interval {
            startTime = timestamp {
              seconds = 221845467600 // January 1, 9000 at 1 PM, America/Los_Angeles
            }

            endTime = timestamp {
              seconds = 221877003600 // January 1, 9001 at 1 PM, America/Los_Angeles
            }
          }
        }
      )

    job.execute()

    verifyProtoArgument(
        reportScheduleIterationsMock,
        ReportScheduleIterationsCoroutineImplBase::createReportScheduleIteration,
      )
      .ignoringFields(ReportScheduleIteration.CREATE_REPORT_REQUEST_ID_FIELD_NUMBER)
      .isEqualTo(
        reportScheduleIteration {
          cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
          externalReportScheduleId = REPORT_SCHEDULE_ID
          reportEventTime = INTERNAL_REPORT_SCHEDULE.nextReportCreationTime
        }
      )

    verifyProtoArgument(dataProvidersMock, DataProvidersCoroutineImplBase::getDataProvider)
      .isEqualTo(getDataProviderRequest { name = DATA_PROVIDER_NAME })

    verifyProtoArgument(eventGroupsMock, EventGroupsCoroutineImplBase::getEventGroup)
      .isEqualTo(getEventGroupRequest { name = EVENT_GROUP_NAME })

    val setReportScheduleIterationStateCaptor:
      KArgumentCaptor<SetReportScheduleIterationStateRequest> =
      argumentCaptor()
    verifyBlocking(reportScheduleIterationsMock, times(0)) {
      setReportScheduleIterationState(setReportScheduleIterationStateCaptor.capture())
    }
  }

  @Test
  fun `execute does not create report when event group end before window end`() = runBlocking {
    whenever(
        dataProvidersMock.getDataProvider(eq(getDataProviderRequest { name = DATA_PROVIDER_NAME }))
      )
      .thenReturn(
        DATA_PROVIDER.copy {
          dataAvailabilityInterval = interval {
            startTime = timestamp {
              seconds = 631227600 // January 1, 1990 at 1 PM, America/Los_Angeles
            }

            endTime = timestamp {
              seconds = 95617659600 // January 1, 5000 at 1 PM, America/Los_Angeles
            }
          }
        }
      )

    whenever(eventGroupsMock.getEventGroup(eq(getEventGroupRequest { name = EVENT_GROUP_NAME })))
      .thenReturn(
        EVENT_GROUP.copy {
          dataAvailabilityInterval = interval {
            startTime = timestamp {
              seconds = 631227600 // January 1, 1990 at 1 PM, America/Los_Angeles
            }

            endTime = timestamp {
              seconds = 946760400 // January 1, 2000 at 1 PM, America/Los_Angeles
            }
          }
        }
      )

    job.execute()

    verifyProtoArgument(
        reportScheduleIterationsMock,
        ReportScheduleIterationsCoroutineImplBase::createReportScheduleIteration,
      )
      .ignoringFields(ReportScheduleIteration.CREATE_REPORT_REQUEST_ID_FIELD_NUMBER)
      .isEqualTo(
        reportScheduleIteration {
          cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
          externalReportScheduleId = REPORT_SCHEDULE_ID
          reportEventTime = INTERNAL_REPORT_SCHEDULE.nextReportCreationTime
        }
      )

    verifyProtoArgument(dataProvidersMock, DataProvidersCoroutineImplBase::getDataProvider)
      .isEqualTo(getDataProviderRequest { name = DATA_PROVIDER_NAME })

    verifyProtoArgument(eventGroupsMock, EventGroupsCoroutineImplBase::getEventGroup)
      .isEqualTo(getEventGroupRequest { name = EVENT_GROUP_NAME })

    val setReportScheduleIterationStateCaptor:
      KArgumentCaptor<SetReportScheduleIterationStateRequest> =
      argumentCaptor()
    verifyBlocking(reportScheduleIterationsMock, times(0)) {
      setReportScheduleIterationState(setReportScheduleIterationStateCaptor.capture())
    }
  }

  @Test
  fun `execute does not create report when data not available for event group due to edp`() =
    runBlocking {
      whenever(
          dataProvidersMock.getDataProvider(
            eq(getDataProviderRequest { name = DATA_PROVIDER_NAME })
          )
        )
        .thenReturn(
          DATA_PROVIDER.copy {
            dataAvailabilityInterval = interval {
              startTime = timestamp {
                seconds = 946760400 // January 1, 2000 at 1 PM, America/Los_Angeles
              }

              endTime = timestamp {
                seconds = 1104613200 // January 1, 2005 at 1 PM, America/Los_Angeles
              }
            }
          }
        )

      whenever(eventGroupsMock.getEventGroup(eq(getEventGroupRequest { name = EVENT_GROUP_NAME })))
        .thenReturn(
          EVENT_GROUP.copy {
            dataAvailabilityInterval = interval {
              startTime = timestamp {
                seconds = 946760400 // January 1, 2000 at 1 PM, America/Los_Angeles
              }

              endTime = timestamp {
                seconds = 221877003600 // January 1, 9001 at 1 PM, America/Los_Angeles
              }
            }
          }
        )

      job.execute()

      verifyProtoArgument(
          reportScheduleIterationsMock,
          ReportScheduleIterationsCoroutineImplBase::createReportScheduleIteration,
        )
        .ignoringFields(ReportScheduleIteration.CREATE_REPORT_REQUEST_ID_FIELD_NUMBER)
        .isEqualTo(
          reportScheduleIteration {
            cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
            externalReportScheduleId = REPORT_SCHEDULE_ID
            reportEventTime = INTERNAL_REPORT_SCHEDULE.nextReportCreationTime
          }
        )

      verifyProtoArgument(dataProvidersMock, DataProvidersCoroutineImplBase::getDataProvider)
        .isEqualTo(getDataProviderRequest { name = DATA_PROVIDER_NAME })

      verifyProtoArgument(eventGroupsMock, EventGroupsCoroutineImplBase::getEventGroup)
        .isEqualTo(getEventGroupRequest { name = EVENT_GROUP_NAME })

      val setReportScheduleIterationStateCaptor:
        KArgumentCaptor<SetReportScheduleIterationStateRequest> =
        argumentCaptor()
      verifyBlocking(reportScheduleIterationsMock, times(0)) {
        setReportScheduleIterationState(setReportScheduleIterationStateCaptor.capture())
      }
    }

  @Test
  fun `execute does not create report when next report time after event end`() = runBlocking {
    whenever(reportSchedulesMock.listReportSchedules(any()))
      .thenReturn(
        listReportSchedulesResponse {
          reportSchedules +=
            INTERNAL_REPORT_SCHEDULE.copy {
              latestIteration =
                INTERNAL_REPORT_SCHEDULE_ITERATION.copy {
                  state = ReportScheduleIteration.State.WAITING_FOR_DATA_AVAILABILITY
                }
              details =
                INTERNAL_REPORT_SCHEDULE.details.copy {
                  eventStart = dateTime {
                    year = 2022
                    month = 1
                    day = 2
                    hours = 13
                    timeZone = timeZone { id = "America/Los_Angeles" }
                  }
                  eventEnd = date {
                    year = 2022
                    month = 1
                    day = 3
                  }
                  frequency =
                    ReportScheduleKt.frequency {
                      daily = ReportSchedule.Frequency.Daily.getDefaultInstance()
                    }
                }
              nextReportCreationTime = timestamp {
                seconds = 1641330000 // January 4, 2022 at 1 PM, America/Los_Angeles
              }
            }
        }
      )

    job.execute()

    val createReportScheduleIterationCaptor: KArgumentCaptor<ReportScheduleIteration> =
      argumentCaptor()
    verifyBlocking(reportScheduleIterationsMock, times(0)) {
      createReportScheduleIteration(createReportScheduleIterationCaptor.capture())
    }

    val createReportCaptor: KArgumentCaptor<CreateReportRequest> = argumentCaptor()
    verifyBlocking(reportsMock, times(0)) { createReport(createReportCaptor.capture()) }

    verifyProtoArgument(reportSchedulesMock, ReportSchedulesCoroutineImplBase::stopReportSchedule)
      .isEqualTo(
        stopReportScheduleRequest {
          cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
          externalReportScheduleId = REPORT_SCHEDULE_ID
        }
      )

    verifyProtoArgument(
        reportScheduleIterationsMock,
        ReportScheduleIterationsCoroutineImplBase::setReportScheduleIterationState,
      )
      .isEqualTo(
        setReportScheduleIterationStateRequest {
          cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
          externalReportScheduleId = REPORT_SCHEDULE_ID
          externalReportScheduleIterationId = REPORT_SCHEDULE_ITERATION_ID

          state = ReportScheduleIteration.State.REPORT_CREATED
        }
      )
  }

  @Test
  fun `execute sets iteration state to RETRYING_REPORT_CREATION when creation fails`() =
    runBlocking {
      whenever(reportsMock.createReport(any())).thenThrow(Status.UNKNOWN.asRuntimeException())

      job.execute()

      val reportsCaptor: KArgumentCaptor<CreateReportRequest> = argumentCaptor()
      verifyBlocking(reportsMock, times(1)) { createReport(reportsCaptor.capture()) }

      verifyProtoArgument(
          reportScheduleIterationsMock,
          ReportScheduleIterationsCoroutineImplBase::setReportScheduleIterationState,
        )
        .isEqualTo(
          setReportScheduleIterationStateRequest {
            cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
            externalReportScheduleId = REPORT_SCHEDULE_ID
            externalReportScheduleIterationId = REPORT_SCHEDULE_ITERATION_ID

            state = ReportScheduleIteration.State.RETRYING_REPORT_CREATION
          }
        )
    }

  @Test
  fun `execute creates report when iteration state is WAITING_FOR_DATA_AVAILABILITY`() =
    runBlocking {
      whenever(reportSchedulesMock.listReportSchedules(any()))
        .thenReturn(
          listReportSchedulesResponse {
            reportSchedules +=
              INTERNAL_REPORT_SCHEDULE.copy {
                latestIteration =
                  INTERNAL_REPORT_SCHEDULE_ITERATION.copy {
                    state = ReportScheduleIteration.State.WAITING_FOR_DATA_AVAILABILITY
                  }
              }
          }
        )

      job.execute()

      val createReportScheduleIterationCaptor: KArgumentCaptor<ReportScheduleIteration> =
        argumentCaptor()
      verifyBlocking(reportScheduleIterationsMock, times(0)) {
        createReportScheduleIteration(createReportScheduleIterationCaptor.capture())
      }

      val publicReportSchedule = INTERNAL_REPORT_SCHEDULE.toPublic()
      verifyProtoArgument(reportsMock, ReportsCoroutineImplBase::createReport)
        .ignoringFields(CreateReportRequest.REPORT_ID_FIELD_NUMBER)
        .isEqualTo(
          createReportRequest {
            parent = MEASUREMENT_CONSUMER_NAME
            requestId = INTERNAL_REPORT_SCHEDULE_ITERATION.createReportRequestId
            report =
              publicReportSchedule.reportTemplate.copy {
                reportingInterval =
                  ReportKt.reportingInterval {
                    reportStart = dateTime {
                      year = 2022
                      month = 1
                      day = 1
                      hours = 13
                      timeZone = timeZone { id = "America/Los_Angeles" }
                    }
                    reportEnd = date {
                      year = 2022
                      month = 1
                      day = 2
                    }
                  }
              }
          }
        )

      verifyProtoArgument(
          reportScheduleIterationsMock,
          ReportScheduleIterationsCoroutineImplBase::setReportScheduleIterationState,
        )
        .isEqualTo(
          setReportScheduleIterationStateRequest {
            cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
            externalReportScheduleId = REPORT_SCHEDULE_ID
            externalReportScheduleIterationId = REPORT_SCHEDULE_ITERATION_ID

            state = ReportScheduleIteration.State.REPORT_CREATED
          }
        )
    }

  @Test
  fun `execute creates report when iteration state is RETRYING_REPORT_CREATION`() = runBlocking {
    whenever(reportSchedulesMock.listReportSchedules(any()))
      .thenReturn(
        listReportSchedulesResponse {
          reportSchedules +=
            INTERNAL_REPORT_SCHEDULE.copy {
              latestIteration =
                INTERNAL_REPORT_SCHEDULE_ITERATION.copy {
                  state = ReportScheduleIteration.State.RETRYING_REPORT_CREATION
                }
            }
        }
      )

    job.execute()

    val createReportScheduleIterationCaptor: KArgumentCaptor<ReportScheduleIteration> =
      argumentCaptor()
    verifyBlocking(reportScheduleIterationsMock, times(0)) {
      createReportScheduleIteration(createReportScheduleIterationCaptor.capture())
    }

    val publicReportSchedule = INTERNAL_REPORT_SCHEDULE.toPublic()
    verifyProtoArgument(reportsMock, ReportsCoroutineImplBase::createReport)
      .ignoringFields(CreateReportRequest.REPORT_ID_FIELD_NUMBER)
      .isEqualTo(
        createReportRequest {
          parent = MEASUREMENT_CONSUMER_NAME
          requestId = INTERNAL_REPORT_SCHEDULE_ITERATION.createReportRequestId
          report =
            publicReportSchedule.reportTemplate.copy {
              reportingInterval =
                ReportKt.reportingInterval {
                  reportStart = dateTime {
                    year = 2022
                    month = 1
                    day = 1
                    hours = 13
                    timeZone = timeZone { id = "America/Los_Angeles" }
                  }
                  reportEnd = date {
                    year = 2022
                    month = 1
                    day = 2
                  }
                }
            }
        }
      )

    verifyProtoArgument(
        reportScheduleIterationsMock,
        ReportScheduleIterationsCoroutineImplBase::setReportScheduleIterationState,
      )
      .isEqualTo(
        setReportScheduleIterationStateRequest {
          cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
          externalReportScheduleId = REPORT_SCHEDULE_ID
          externalReportScheduleIterationId = REPORT_SCHEDULE_ITERATION_ID

          state = ReportScheduleIteration.State.REPORT_CREATED
        }
      )
  }

  @Test
  fun `execute creates report when last iteration state is REPORT_CREATED`() = runBlocking {
    whenever(reportSchedulesMock.listReportSchedules(any()))
      .thenReturn(
        listReportSchedulesResponse {
          reportSchedules +=
            INTERNAL_REPORT_SCHEDULE.copy {
              latestIteration =
                INTERNAL_REPORT_SCHEDULE_ITERATION.copy {
                  state = ReportScheduleIteration.State.REPORT_CREATED
                }
            }
        }
      )

    job.execute()

    val createReportScheduleIterationCaptor: KArgumentCaptor<ReportScheduleIteration> =
      argumentCaptor()
    verifyBlocking(reportScheduleIterationsMock, times(1)) {
      createReportScheduleIteration(createReportScheduleIterationCaptor.capture())
    }

    val publicReportSchedule = INTERNAL_REPORT_SCHEDULE.toPublic()
    verifyProtoArgument(reportsMock, ReportsCoroutineImplBase::createReport)
      .ignoringFields(CreateReportRequest.REPORT_ID_FIELD_NUMBER)
      .isEqualTo(
        createReportRequest {
          parent = MEASUREMENT_CONSUMER_NAME
          requestId = INTERNAL_REPORT_SCHEDULE_ITERATION.createReportRequestId
          report =
            publicReportSchedule.reportTemplate.copy {
              reportingInterval =
                ReportKt.reportingInterval {
                  reportStart = dateTime {
                    year = 2022
                    month = 1
                    day = 1
                    hours = 13
                    timeZone = timeZone { id = "America/Los_Angeles" }
                  }
                  reportEnd = date {
                    year = 2022
                    month = 1
                    day = 2
                  }
                }
            }
        }
      )

    verifyProtoArgument(
        reportScheduleIterationsMock,
        ReportScheduleIterationsCoroutineImplBase::setReportScheduleIterationState,
      )
      .isEqualTo(
        setReportScheduleIterationStateRequest {
          cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
          externalReportScheduleId = REPORT_SCHEDULE_ID
          externalReportScheduleIterationId = REPORT_SCHEDULE_ITERATION_ID

          state = ReportScheduleIteration.State.REPORT_CREATED
        }
      )
  }

  companion object {
    private const val BATCH_SIZE = 50

    private const val DATA_PROVIDER_ID = "D123"
    private const val DATA_PROVIDER_NAME = "dataProviders/$DATA_PROVIDER_ID"

    private const val EVENT_GROUP_ID = "E123"
    private const val EVENT_GROUP_NAME = "$DATA_PROVIDER_NAME/eventGroups/$EVENT_GROUP_ID"

    private const val CMMS_MEASUREMENT_CONSUMER_ID = "A123"
    private const val MEASUREMENT_CONSUMER_NAME =
      "measurementConsumers/${CMMS_MEASUREMENT_CONSUMER_ID}"
    private const val CMMS_MEASUREMENT_CONSUMER_ID_2 = "A1234"
    private const val MEASUREMENT_CONSUMER_NAME_2 =
      "measurementConsumers/${CMMS_MEASUREMENT_CONSUMER_ID_2}"

    private const val PRIMITIVE_REPORTING_SET_ID = "c123"
    private const val COMPOSITE_REPORTING_SET_ID = "c124"
    private const val COMPOSITE_REPORTING_SET_NAME =
      "${MEASUREMENT_CONSUMER_NAME}/reportingSets/${COMPOSITE_REPORTING_SET_ID}"

    private const val REPORT_SCHEDULE_ID = "b123"
    private const val REPORT_SCHEDULE_NAME =
      "${MEASUREMENT_CONSUMER_NAME}/reportSchedules/${REPORT_SCHEDULE_ID}"

    private const val REPORT_SCHEDULE_ITERATION_ID = "b1234"

    private const val METRIC_CALCULATION_SPEC_ID = "m123"
    private const val METRIC_CALCULATION_SPEC_NAME =
      "${MEASUREMENT_CONSUMER_NAME}/metricCalculationSpecs/${METRIC_CALCULATION_SPEC_ID}"

    private const val REPORT_ID = "r123"
    private const val REPORT_NAME = "${MEASUREMENT_CONSUMER_NAME}/reports/${REPORT_ID}"

    private val MEASUREMENT_CONSUMER_CONFIGS = measurementConsumerConfigs {
      configs[MEASUREMENT_CONSUMER_NAME] = measurementConsumerConfig {
        apiKey = "123"
        offlinePrincipal = "principals/mc-user"
      }
    }

    private val DATA_PROVIDER = dataProvider {
      dataAvailabilityInterval = interval {
        startTime = timestamp {
          seconds = 1641070800 // January 1, 2022 at 1 PM, America/Los_Angeles
        }

        endTime = timestamp {
          seconds = 4070984400 // January 1, 2099 at 1 PM, America/Los_Angeles
        }
      }
    }

    private val EVENT_GROUP = eventGroup {
      dataAvailabilityInterval = interval {
        startTime = timestamp {
          seconds = 1641070800 // January 1, 2022 at 1 PM, America/Los_Angeles
        }

        endTime = timestamp {
          seconds = 4070984400 // January 1, 2099 at 1 PM, America/Los_Angeles
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

    private val INTERNAL_COMPOSITE_REPORTING_SET = reportingSet {
      cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
      externalReportingSetId = COMPOSITE_REPORTING_SET_ID
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
              externalReportingSetId = INTERNAL_PRIMITIVE_REPORTING_SET.externalReportingSetId
            }
          weight = 1
          binaryRepresentation = 1
        }
    }

    private val INTERNAL_REPORT_SCHEDULE = reportSchedule {
      cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
      externalReportScheduleId = REPORT_SCHEDULE_ID
      state = ReportSchedule.State.ACTIVE
      details =
        ReportScheduleKt.details {
          displayName = "display"
          description = "description"
          reportTemplate = internalReport {
            reportingMetricEntries[INTERNAL_PRIMITIVE_REPORTING_SET.externalReportingSetId] =
              InternalReportKt.reportingMetricCalculationSpec {
                metricCalculationSpecReportingMetrics +=
                  InternalReportKt.metricCalculationSpecReportingMetrics {
                    externalMetricCalculationSpecId = METRIC_CALCULATION_SPEC_ID
                  }
              }
          }
          eventStart = dateTime {
            year = 2022
            month = 1
            day = 2
            hours = 13
            timeZone = timeZone { id = "America/Los_Angeles" }
          }
          eventEnd = date {
            year = 2090
            month = 1
            day = 1
          }
          frequency =
            ReportScheduleKt.frequency {
              daily = ReportSchedule.Frequency.Daily.getDefaultInstance()
            }
          reportWindow =
            ReportScheduleKt.reportWindow {
              trailingWindow =
                ReportScheduleKt.ReportWindowKt.trailingWindow {
                  count = 1
                  increment = ReportSchedule.ReportWindow.TrailingWindow.Increment.DAY
                }
            }
        }
      nextReportCreationTime = timestamp {
        seconds = 1641157200 // January 2, 2022 at 1 PM, America/Los_Angeles
      }
      createTime = timestamp { seconds = 1700673846 }
      updateTime = timestamp { seconds = 1700673846 }
    }

    private val INTERNAL_REPORT_SCHEDULE_ITERATION = reportScheduleIteration {
      cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
      externalReportScheduleId = INTERNAL_REPORT_SCHEDULE.externalReportScheduleId
      externalReportScheduleIterationId = REPORT_SCHEDULE_ITERATION_ID
      createReportRequestId = "123"
      state = ReportScheduleIteration.State.WAITING_FOR_DATA_AVAILABILITY
      numAttempts = 0
      reportEventTime = INTERNAL_REPORT_SCHEDULE.nextReportCreationTime
      createTime = timestamp { seconds = 1700673846 }
      updateTime = timestamp { seconds = 1700673846 }
    }

    private val REPORT = report {
      name = REPORT_NAME
      reportingMetricEntries +=
        ReportKt.reportingMetricEntry {
          key = COMPOSITE_REPORTING_SET_NAME
          value =
            ReportKt.reportingMetricCalculationSpec {
              metricCalculationSpecs += METRIC_CALCULATION_SPEC_NAME
            }
        }
      state = Report.State.RUNNING
      reportingInterval =
        ReportKt.reportingInterval {
          reportStart = dateTime {
            year = 2022
            month = 1
            day = 1
            hours = 13
            timeZone = timeZone { id = "America/Los_Angeles" }
          }
          reportEnd = date {
            year = 2022
            month = 1
            day = 2
          }
        }
      createTime = timestamp { seconds = 50 }
      reportSchedule = REPORT_SCHEDULE_NAME
    }
  }
}
