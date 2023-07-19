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

package org.wfanet.measurement.reporting.service.api.v2alpha.tools

import com.google.common.truth.Truth.assertThat
import com.google.type.interval
import io.grpc.Server
import io.grpc.ServerServiceDefinition
import io.grpc.netty.NettyServerBuilder
import java.nio.file.Path
import java.nio.file.Paths
import java.time.Duration
import java.time.Instant
import java.util.concurrent.TimeUnit.SECONDS
import org.junit.After
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.mockito.kotlin.any
import org.wfanet.measurement.common.crypto.SigningCerts
import org.wfanet.measurement.common.getRuntimePath
import org.wfanet.measurement.common.grpc.testing.mockService
import org.wfanet.measurement.common.grpc.toServerTlsContext
import org.wfanet.measurement.common.parseTextProto
import org.wfanet.measurement.common.testing.CommandLineTesting
import org.wfanet.measurement.common.testing.verifyProtoArgument
import org.wfanet.measurement.common.toProtoDuration
import org.wfanet.measurement.common.toProtoTime
import org.wfanet.measurement.reporting.v2alpha.EventGroupsGrpcKt.EventGroupsCoroutineImplBase
import org.wfanet.measurement.reporting.v2alpha.ListEventGroupsResponse
import org.wfanet.measurement.reporting.v2alpha.ListReportingSetsResponse
import org.wfanet.measurement.reporting.v2alpha.Report
import org.wfanet.measurement.reporting.v2alpha.ReportingSet
import org.wfanet.measurement.reporting.v2alpha.ReportingSetKt
import org.wfanet.measurement.reporting.v2alpha.ReportingSetsGrpcKt.ReportingSetsCoroutineImplBase
import org.wfanet.measurement.reporting.v2alpha.ReportsGrpcKt.ReportsCoroutineImplBase
import org.wfanet.measurement.reporting.v2alpha.createReportRequest
import org.wfanet.measurement.reporting.v2alpha.createReportingSetRequest
import org.wfanet.measurement.reporting.v2alpha.eventGroup
import org.wfanet.measurement.reporting.v2alpha.getReportRequest
import org.wfanet.measurement.reporting.v2alpha.listEventGroupsRequest
import org.wfanet.measurement.reporting.v2alpha.listEventGroupsResponse
import org.wfanet.measurement.reporting.v2alpha.listReportingSetsRequest
import org.wfanet.measurement.reporting.v2alpha.listReportingSetsResponse
import org.wfanet.measurement.reporting.v2alpha.listReportsRequest
import org.wfanet.measurement.reporting.v2alpha.listReportsResponse
import org.wfanet.measurement.reporting.v2alpha.periodicTimeInterval
import org.wfanet.measurement.reporting.v2alpha.report
import org.wfanet.measurement.reporting.v2alpha.reportingSet
import org.wfanet.measurement.reporting.v2alpha.timeIntervals

@RunWith(JUnit4::class)
class ReportingTest {
  private val reportingSetsServiceMock: ReportingSetsCoroutineImplBase = mockService {
    onBlocking { createReportingSet(any()) }.thenReturn(REPORTING_SET)
    onBlocking { listReportingSets(any()) }
      .thenReturn(listReportingSetsResponse { reportingSets += REPORTING_SET })
  }
  private val reportsServiceMock: ReportsCoroutineImplBase = mockService {
    onBlocking { createReport(any()) }.thenReturn(REPORT)
    onBlocking { listReports(any()) }.thenReturn(listReportsResponse { reports += REPORT })
    onBlocking { getReport(any()) }.thenReturn(REPORT)
  }
  private val eventGroupsServiceMock: EventGroupsCoroutineImplBase = mockService {
    onBlocking { listEventGroups(any()) }
      .thenReturn(listEventGroupsResponse { eventGroups += EVENT_GROUP })
  }

  private val serverCerts =
    SigningCerts.fromPemFiles(
      certificateFile = SECRETS_DIR.resolve("reporting_tls.pem").toFile(),
      privateKeyFile = SECRETS_DIR.resolve("reporting_tls.key").toFile(),
      trustedCertCollectionFile = SECRETS_DIR.resolve("reporting_root.pem").toFile(),
    )

  private val services: List<ServerServiceDefinition> =
    listOf(
      reportingSetsServiceMock.bindService(),
      reportsServiceMock.bindService(),
      eventGroupsServiceMock.bindService(),
    )

  private val server: Server =
    NettyServerBuilder.forPort(0)
      .sslContext(serverCerts.toServerTlsContext())
      .addServices(services)
      .build()

  @Before
  fun initServer() {
    server.start()
  }

  @After
  fun shutdownServer() {
    server.shutdown()
    server.awaitTermination(1, SECONDS)
  }

  private fun callCli(args: Array<String>): String {
    return CommandLineTesting.capturingSystemOut {
      CommandLineTesting.assertExitsWith(0) { Reporting.main(args) }
    }
  }

  @Test
  fun `create reporting set with --cmms-event-group calls api with valid request`() {
    val args =
      arrayOf(
        "--tls-cert-file=$SECRETS_DIR/mc_tls.pem",
        "--tls-key-file=$SECRETS_DIR/mc_tls.key",
        "--cert-collection-file=$SECRETS_DIR/reporting_root.pem",
        "--reporting-server-api-target=$HOST:${server.port}",
        "reporting-sets",
        "create",
        "--parent=$MEASUREMENT_CONSUMER_NAME",
        "--cmms-event-group=$CMMS_EVENT_GROUP_NAME_1",
        "--cmms-event-group=$CMMS_EVENT_GROUP_NAME_2",
        "--filter=person.age_group == 1",
        "--display-name=reporting-set",
        "--id=$REPORTING_SET_ID",
      )

    val output = callCli(args)

    verifyProtoArgument(
        reportingSetsServiceMock,
        ReportingSetsCoroutineImplBase::createReportingSet
      )
      .isEqualTo(
        createReportingSetRequest {
          parent = MEASUREMENT_CONSUMER_NAME
          reportingSet = reportingSet {
            filter = "person.age_group == 1"
            displayName = "reporting-set"
            primitive =
              ReportingSetKt.primitive {
                cmmsEventGroups += CMMS_EVENT_GROUP_NAME_1
                cmmsEventGroups += CMMS_EVENT_GROUP_NAME_2
              }
          }
          reportingSetId = REPORTING_SET_ID
        }
      )

    assertThat(parseTextProto(output.reader(), ReportingSet.getDefaultInstance()))
      .isEqualTo(REPORTING_SET)
  }

  @Test
  fun `create reporting set with --set-expression calls api with valid request`() {
    val setExpression =
      """
        operation: UNION
        lhs {
          reporting_set: "$REPORTING_SET_NAME"
        }
      """
        .trimIndent()

    val args =
      arrayOf(
        "--tls-cert-file=$SECRETS_DIR/mc_tls.pem",
        "--tls-key-file=$SECRETS_DIR/mc_tls.key",
        "--cert-collection-file=$SECRETS_DIR/reporting_root.pem",
        "--reporting-server-api-target=$HOST:${server.port}",
        "reporting-sets",
        "create",
        "--parent=$MEASUREMENT_CONSUMER_NAME",
        "--set-expression=$setExpression",
        "--filter=person.age_group == 1",
        "--display-name=reporting-set",
        "--id=$REPORTING_SET_ID",
      )

    val output = callCli(args)

    verifyProtoArgument(
        reportingSetsServiceMock,
        ReportingSetsCoroutineImplBase::createReportingSet
      )
      .isEqualTo(
        createReportingSetRequest {
          parent = MEASUREMENT_CONSUMER_NAME
          reportingSet = reportingSet {
            filter = "person.age_group == 1"
            displayName = "reporting-set"
            composite =
              ReportingSetKt.composite {
                expression =
                  ReportingSetKt.setExpression {
                    operation = ReportingSet.SetExpression.Operation.UNION
                    lhs =
                      ReportingSetKt.SetExpressionKt.operand { reportingSet = REPORTING_SET_NAME }
                  }
              }
          }
          reportingSetId = REPORTING_SET_ID
        }
      )

    assertThat(parseTextProto(output.reader(), ReportingSet.getDefaultInstance()))
      .isEqualTo(REPORTING_SET)
  }

  @Test
  fun `create reporting set with both --set-expression and --cmms-event-groups fails`() {
    val setExpression =
      """
        operation: UNION
        lhs {
          reporting_set: "$REPORTING_SET_NAME"
        }
      """
        .trimIndent()

    val args =
      arrayOf(
        "--tls-cert-file=$SECRETS_DIR/mc_tls.pem",
        "--tls-key-file=$SECRETS_DIR/mc_tls.key",
        "--cert-collection-file=$SECRETS_DIR/reporting_root.pem",
        "--reporting-server-api-target=$HOST:${server.port}",
        "reporting-sets",
        "create",
        "--parent=$MEASUREMENT_CONSUMER_NAME",
        "--cmms-event-group=$CMMS_EVENT_GROUP_NAME_1",
        "--cmms-event-group=$CMMS_EVENT_GROUP_NAME_2",
        "--set-expression=$setExpression",
        "--filter=person.age_group == 1",
        "--display-name=reporting-set",
        "--id=$REPORTING_SET_ID",
      )

    CommandLineTesting.assertExitsWith(2) { Reporting.main(args) }
  }

  @Test
  fun `list reporting sets calls api with valid request`() {
    val args =
      arrayOf(
        "--tls-cert-file=$SECRETS_DIR/mc_tls.pem",
        "--tls-key-file=$SECRETS_DIR/mc_tls.key",
        "--cert-collection-file=$SECRETS_DIR/reporting_root.pem",
        "--reporting-server-api-target=$HOST:${server.port}",
        "reporting-sets",
        "list",
        "--parent=$MEASUREMENT_CONSUMER_NAME",
        "--page-size=50",
        "--page-token=token",
      )

    val output = callCli(args)

    verifyProtoArgument(reportingSetsServiceMock, ReportingSetsCoroutineImplBase::listReportingSets)
      .isEqualTo(
        listReportingSetsRequest {
          parent = MEASUREMENT_CONSUMER_NAME
          pageSize = 50
          pageToken = "token"
        }
      )
    assertThat(parseTextProto(output.reader(), ListReportingSetsResponse.getDefaultInstance()))
      .isEqualTo(listReportingSetsResponse { reportingSets += REPORTING_SET })
  }

  @Test
  fun `create report with timeIntervalInput calls api with valid request`() {
    val textFormatReportingMetricEntryFile =
      TEXTPROTO_DIR.resolve("reporting_metric_entry.textproto").toFile()
    val startTime = "2017-01-15T01:30:15.01Z"
    val endTime = "2018-01-15T01:30:15.01Z"
    val startTime2 = "2019-01-15T01:30:15.01Z"
    val endTime2 = "2020-01-15T01:30:15.01Z"

    val args =
      arrayOf(
        "--tls-cert-file=$SECRETS_DIR/mc_tls.pem",
        "--tls-key-file=$SECRETS_DIR/mc_tls.key",
        "--cert-collection-file=$SECRETS_DIR/reporting_root.pem",
        "--reporting-server-api-target=$HOST:${server.port}",
        "reports",
        "create",
        "--parent=$MEASUREMENT_CONSUMER_NAME",
        "--interval-start-time=$startTime",
        "--interval-end-time=$endTime",
        "--interval-start-time=$startTime2",
        "--interval-end-time=$endTime2",
        "--id=$REPORT_ID",
        "--request-id=$REPORT_REQUEST_ID",
        "--reporting-metric-entry=${textFormatReportingMetricEntryFile.readText()}",
      )

    val output = callCli(args)

    verifyProtoArgument(reportsServiceMock, ReportsCoroutineImplBase::createReport)
      .isEqualTo(
        createReportRequest {
          parent = MEASUREMENT_CONSUMER_NAME
          reportId = REPORT_ID
          requestId = REPORT_REQUEST_ID
          report = report {
            reportingMetricEntries +=
              parseTextProto(
                textFormatReportingMetricEntryFile,
                Report.ReportingMetricEntry.getDefaultInstance()
              )
            timeIntervals = timeIntervals {
              timeIntervals += interval {
                this.startTime = Instant.parse(startTime).toProtoTime()
                this.endTime = Instant.parse(endTime).toProtoTime()
              }
              timeIntervals += interval {
                this.startTime = Instant.parse(startTime2).toProtoTime()
                this.endTime = Instant.parse(endTime2).toProtoTime()
              }
            }
          }
        }
      )

    assertThat(parseTextProto(output.reader(), Report.getDefaultInstance())).isEqualTo(REPORT)
  }

  @Test
  fun `create report with periodicTimeIntervalInput calls api with valid request`() {
    val textFormatReportingMetricEntryFile =
      TEXTPROTO_DIR.resolve("reporting_metric_entry.textproto").toFile()
    val startTime = "2017-01-15T01:30:15.01Z"
    val increment = "P1DT3H5M12.99S"

    val args =
      arrayOf(
        "--tls-cert-file=$SECRETS_DIR/mc_tls.pem",
        "--tls-key-file=$SECRETS_DIR/mc_tls.key",
        "--cert-collection-file=$SECRETS_DIR/reporting_root.pem",
        "--reporting-server-api-target=$HOST:${server.port}",
        "reports",
        "create",
        "--parent=$MEASUREMENT_CONSUMER_NAME",
        "--periodic-interval-start-time=$startTime",
        "--periodic-interval-increment=$increment",
        "--periodic-interval-count=3",
        "--id=$REPORT_ID",
        "--request-id=$REPORT_REQUEST_ID",
        "--reporting-metric-entry=${textFormatReportingMetricEntryFile.readText()}",
      )

    val output = callCli(args)

    verifyProtoArgument(reportsServiceMock, ReportsCoroutineImplBase::createReport)
      .isEqualTo(
        createReportRequest {
          parent = MEASUREMENT_CONSUMER_NAME
          reportId = REPORT_ID
          requestId = REPORT_REQUEST_ID
          report = report {
            reportingMetricEntries +=
              parseTextProto(
                textFormatReportingMetricEntryFile,
                Report.ReportingMetricEntry.getDefaultInstance()
              )
            periodicTimeInterval = periodicTimeInterval {
              this.startTime = Instant.parse(startTime).toProtoTime()
              this.increment = Duration.parse(increment).toProtoDuration()
              intervalCount = 3
            }
          }
        }
      )

    assertThat(parseTextProto(output.reader(), Report.getDefaultInstance())).isEqualTo(REPORT)
  }

  @Test
  fun `create report with both periodicTimeIntervalInput and timeIntervalInput fails`() {
    val textFormatReportingMetricEntryFile =
      TEXTPROTO_DIR.resolve("reporting_metric_entry.textproto").toFile()
    val increment = "P1DT3H5M12.99S"
    val startTime = "2017-01-15T01:30:15.01Z"
    val endTime = "2018-01-15T01:30:15.01Z"
    val startTime2 = "2019-01-15T01:30:15.01Z"
    val endTime2 = "2020-01-15T01:30:15.01Z"

    val args =
      arrayOf(
        "--tls-cert-file=$SECRETS_DIR/mc_tls.pem",
        "--tls-key-file=$SECRETS_DIR/mc_tls.key",
        "--cert-collection-file=$SECRETS_DIR/reporting_root.pem",
        "--reporting-server-api-target=$HOST:${server.port}",
        "reports",
        "create",
        "--parent=$MEASUREMENT_CONSUMER_NAME",
        "--periodic-interval-start-time=$startTime",
        "--periodic-interval-increment=$increment",
        "--periodic-interval-count=3",
        "--interval-start-time=$startTime",
        "--interval-end-time=$endTime",
        "--interval-start-time=$startTime2",
        "--interval-end-time=$endTime2",
        "--id=$REPORT_ID",
        "--request-id=$REPORT_REQUEST_ID",
        "--reporting-metric-entry=${textFormatReportingMetricEntryFile.readText()}",
      )

    CommandLineTesting.assertExitsWith(2) { Reporting.main(args) }
  }

  @Test
  fun `list reports calls api with valid request`() {
    val args =
      arrayOf(
        "--tls-cert-file=$SECRETS_DIR/mc_tls.pem",
        "--tls-key-file=$SECRETS_DIR/mc_tls.key",
        "--cert-collection-file=$SECRETS_DIR/reporting_root.pem",
        "--reporting-server-api-target=$HOST:${server.port}",
        "reports",
        "list",
        "--parent=$MEASUREMENT_CONSUMER_NAME",
      )
    callCli(args)

    verifyProtoArgument(reportsServiceMock, ReportsCoroutineImplBase::listReports)
      .isEqualTo(
        listReportsRequest {
          parent = MEASUREMENT_CONSUMER_NAME
          pageSize = 1000
        }
      )
  }

  @Test
  fun `get report calls api with valid request`() {
    val args =
      arrayOf(
        "--tls-cert-file=$SECRETS_DIR/mc_tls.pem",
        "--tls-key-file=$SECRETS_DIR/mc_tls.key",
        "--cert-collection-file=$SECRETS_DIR/reporting_root.pem",
        "--reporting-server-api-target=$HOST:${server.port}",
        "reports",
        "get",
        REPORT_NAME,
      )
    val output = callCli(args)

    verifyProtoArgument(reportsServiceMock, ReportsCoroutineImplBase::getReport)
      .isEqualTo(getReportRequest { name = REPORT_NAME })
    assertThat(parseTextProto(output.reader(), Report.getDefaultInstance())).isEqualTo(REPORT)
  }

  @Test
  fun `list event groups calls api with valid request`() {
    val args =
      arrayOf(
        "--tls-cert-file=$SECRETS_DIR/mc_tls.pem",
        "--tls-key-file=$SECRETS_DIR/mc_tls.key",
        "--cert-collection-file=$SECRETS_DIR/reporting_root.pem",
        "--reporting-server-api-target=$HOST:${server.port}",
        "event-groups",
        "list",
        "--parent=$MEASUREMENT_CONSUMER_NAME",
        "--filter=event_group_reference_id == 'abc'",
      )
    val output = callCli(args)

    verifyProtoArgument(eventGroupsServiceMock, EventGroupsCoroutineImplBase::listEventGroups)
      .isEqualTo(
        listEventGroupsRequest {
          parent = MEASUREMENT_CONSUMER_NAME
          filter = "event_group_reference_id == 'abc'"
          pageSize = 1000
        }
      )
    assertThat(parseTextProto(output.reader(), ListEventGroupsResponse.getDefaultInstance()))
      .isEqualTo(listEventGroupsResponse { eventGroups += EVENT_GROUP })
  }

  companion object {
    private const val HOST = "localhost"
    private val SECRETS_DIR: Path =
      getRuntimePath(
        Paths.get("wfa_measurement_system", "src", "main", "k8s", "testing", "secretfiles")
      )!!

    private val TEXTPROTO_DIR: Path =
      getRuntimePath(
        Paths.get(
          "wfa_measurement_system",
          "src",
          "test",
          "kotlin",
          "org",
          "wfanet",
          "measurement",
          "reporting",
          "service",
          "api",
          "v2alpha",
          "tools"
        )
      )!!

    private const val MEASUREMENT_CONSUMER_NAME = "measurementConsumers/1"
    private const val DATA_PROVIDER_NAME = "dataProviders/1"
    private const val CMMS_EVENT_GROUP_NAME_1 = "$DATA_PROVIDER_NAME/eventGroups/1"
    private const val CMMS_EVENT_GROUP_NAME_2 = "$DATA_PROVIDER_NAME/eventGroups/2"

    private const val REPORTING_SET_ID = "abc"
    private const val REPORTING_SET_NAME = "reportingSet/$REPORTING_SET_ID"

    private val REPORTING_SET = reportingSet { name = REPORTING_SET_NAME }

    private const val REPORT_REQUEST_ID = "def"
    private const val REPORT_ID = "abc"
    private const val REPORT_NAME = "$MEASUREMENT_CONSUMER_NAME/reports/$REPORT_ID"
    private val REPORT = report { name = REPORT_NAME }

    private const val EVENT_GROUP_NAME = "$MEASUREMENT_CONSUMER_NAME/eventGroups/1"
    private val EVENT_GROUP = eventGroup { name = EVENT_GROUP_NAME }
  }
}
