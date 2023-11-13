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

import org.junit.Before
import org.junit.Ignore
import org.junit.Rule
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.api.v2alpha.DataProvidersGrpcKt.DataProvidersCoroutineImplBase
import org.wfanet.measurement.api.v2alpha.DataProvidersGrpcKt.DataProvidersCoroutineStub
import org.wfanet.measurement.api.v2alpha.EventGroupsGrpcKt.EventGroupsCoroutineImplBase
import org.wfanet.measurement.api.v2alpha.EventGroupsGrpcKt.EventGroupsCoroutineStub
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.common.grpc.testing.mockService
import org.wfanet.measurement.internal.reporting.v2.ReportScheduleIterationsGrpcKt.ReportScheduleIterationsCoroutineImplBase
import org.wfanet.measurement.internal.reporting.v2.ReportScheduleIterationsGrpcKt.ReportScheduleIterationsCoroutineStub
import org.wfanet.measurement.internal.reporting.v2.ReportSchedulesGrpcKt.ReportSchedulesCoroutineImplBase
import org.wfanet.measurement.internal.reporting.v2.ReportSchedulesGrpcKt.ReportSchedulesCoroutineStub
import org.wfanet.measurement.internal.reporting.v2.ReportingSetsGrpcKt.ReportingSetsCoroutineImplBase
import org.wfanet.measurement.internal.reporting.v2.ReportingSetsGrpcKt.ReportingSetsCoroutineStub
import org.wfanet.measurement.reporting.v2alpha.ReportsGrpcKt.ReportsCoroutineImplBase
import org.wfanet.measurement.reporting.v2alpha.ReportsGrpcKt.ReportsCoroutineStub

@RunWith(JUnit4::class)
class ReportSchedulingJobTest {
  private val dataProvidersMock: DataProvidersCoroutineImplBase = mockService()
  private val eventGroupsMock: EventGroupsCoroutineImplBase = mockService()
  private val reportingSetsMock: ReportingSetsCoroutineImplBase = mockService()
  private val reportScheduleIterationsMock: ReportScheduleIterationsCoroutineImplBase =
    mockService()
  private val reportSchedulesMock: ReportSchedulesCoroutineImplBase = mockService()
  private val reportsMock: ReportsCoroutineImplBase = mockService()

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
        DataProvidersCoroutineStub(grpcTestServerRule.channel),
        EventGroupsCoroutineStub(grpcTestServerRule.channel),
        ReportingSetsCoroutineStub(grpcTestServerRule.channel),
        ReportScheduleIterationsCoroutineStub(grpcTestServerRule.channel),
        ReportSchedulesCoroutineStub(grpcTestServerRule.channel),
        ReportsCoroutineStub(grpcTestServerRule.channel),
      )
  }

  @Ignore @Test fun `execute creates report for a single schedule for a new iteration`() {}

  @Ignore @Test fun `execute creates reports for multiple schedules for new iterations`() {}

  @Ignore
  @Test
  fun `execute does not create report when data not yet available for data provider`() {}

  @Ignore
  @Test
  fun `execute does not create report when data not yet available for event group`() {}

  @Ignore
  @Test
  fun `execute sets iteration state to RETRYING_REPORT_CREATION when creation fails`() {}

  @Ignore
  @Test
  fun `execute creates report when iteration state is WAITING_FOR_DATA_AVAILABILITY`() {}

  @Ignore @Test fun `execute creates report when iteration state is RETRYING_REPORT_CREATION`() {}
}
