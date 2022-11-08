// Copyright 2022 The Cross-Media Measurement Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.wfanet.measurement.reporting.service.api.v1alpha.tools

import com.google.common.truth.Truth.assertThat
import io.grpc.Server
import io.grpc.ServerServiceDefinition
import io.grpc.netty.NettyServerBuilder
import java.nio.file.Path
import java.nio.file.Paths
import java.time.Duration
import java.time.Instant
import java.time.LocalDate
import java.time.ZoneOffset
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
import org.wfanet.measurement.reporting.v1alpha.EventGroupsGrpcKt.EventGroupsCoroutineImplBase
import org.wfanet.measurement.reporting.v1alpha.ListEventGroupsResponse
import org.wfanet.measurement.reporting.v1alpha.ListReportingSetsResponse
import org.wfanet.measurement.reporting.v1alpha.Metric
import org.wfanet.measurement.reporting.v1alpha.MetricKt.SetOperationKt.operand
import org.wfanet.measurement.reporting.v1alpha.MetricKt.namedSetOperation
import org.wfanet.measurement.reporting.v1alpha.MetricKt.reachParams
import org.wfanet.measurement.reporting.v1alpha.MetricKt.setOperation
import org.wfanet.measurement.reporting.v1alpha.Report
import org.wfanet.measurement.reporting.v1alpha.ReportKt.EventGroupUniverseKt.eventGroupEntry
import org.wfanet.measurement.reporting.v1alpha.ReportKt.eventGroupUniverse
import org.wfanet.measurement.reporting.v1alpha.ReportingSet
import org.wfanet.measurement.reporting.v1alpha.ReportingSetsGrpcKt.ReportingSetsCoroutineImplBase
import org.wfanet.measurement.reporting.v1alpha.ReportsGrpcKt.ReportsCoroutineImplBase
import org.wfanet.measurement.reporting.v1alpha.createReportRequest
import org.wfanet.measurement.reporting.v1alpha.createReportingSetRequest
import org.wfanet.measurement.reporting.v1alpha.eventGroup
import org.wfanet.measurement.reporting.v1alpha.getReportRequest
import org.wfanet.measurement.reporting.v1alpha.listEventGroupsRequest
import org.wfanet.measurement.reporting.v1alpha.listEventGroupsResponse
import org.wfanet.measurement.reporting.v1alpha.listReportingSetsRequest
import org.wfanet.measurement.reporting.v1alpha.listReportingSetsResponse
import org.wfanet.measurement.reporting.v1alpha.listReportsRequest
import org.wfanet.measurement.reporting.v1alpha.listReportsResponse
import org.wfanet.measurement.reporting.v1alpha.metric
import org.wfanet.measurement.reporting.v1alpha.periodicTimeInterval
import org.wfanet.measurement.reporting.v1alpha.report
import org.wfanet.measurement.reporting.v1alpha.reportingSet
import org.wfanet.measurement.reporting.v1alpha.timeInterval
import org.wfanet.measurement.reporting.v1alpha.timeIntervals

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
      "v1alpha",
      "tools"
    )
  )!!

private const val REPORT_IDEMPOTENCY_KEY = "report001"
private const val MEASUREMENT_CONSUMER_NAME = "measurementConsumers/1"
private const val EVENT_GROUP_NAME_1 = "$MEASUREMENT_CONSUMER_NAME/dataProviders/1/eventGroups/1"
private const val EVENT_GROUP_NAME_2 = "$MEASUREMENT_CONSUMER_NAME/dataProviders/1/eventGroups/2"
private const val EVENT_GROUP_NAME_3 = "$MEASUREMENT_CONSUMER_NAME/dataProviders/2/eventGroups/1"

private val REPORTING_SET = reportingSet { name = "$MEASUREMENT_CONSUMER_NAME/reportingSets/1" }
private val LIST_REPORTING_SETS_RESPONSE = listReportingSetsResponse {
  reportingSets += reportingSet {
    name = "$MEASUREMENT_CONSUMER_NAME/reportingSets/1"
    eventGroups += listOf(EVENT_GROUP_NAME_1, EVENT_GROUP_NAME_2, EVENT_GROUP_NAME_3)
    filter = "some.filter1"
    displayName = "test-reporting-set1"
  }
  reportingSets += reportingSet {
    name = "$MEASUREMENT_CONSUMER_NAME/reportingSets/2"
    eventGroups += listOf(EVENT_GROUP_NAME_1)
    filter = "some.filter2"
    displayName = "test-reporting-set2"
  }
  nextPageToken = "TokenToGetTheNextPage"
}

private const val REPORT_NAME = "$MEASUREMENT_CONSUMER_NAME/reports/1"
private val REPORT = report {
  name = REPORT_NAME
  measurementConsumer = MEASUREMENT_CONSUMER_NAME
  eventGroupUniverse = eventGroupUniverse {
    eventGroupEntries += eventGroupEntry {
      key = "measurementConsumers/1/dataProviders/1/eventGroups/1"
      value = ""
    }
    eventGroupEntries += eventGroupEntry {
      key = "measurementConsumers/1/dataProviders/2/eventGroups/3"
      value = "partner=abc"
    }
  }
  timeIntervals = timeIntervals {
    timeIntervals += timeInterval {
      startTime =
        LocalDate.now().minusDays(1).atStartOfDay().toInstant(ZoneOffset.UTC).toProtoTime()
      endTime = LocalDate.now().atStartOfDay().toInstant(ZoneOffset.UTC).toProtoTime()
    }
  }
}

private val LIST_REPORTS_RESPONSE = listReportsResponse {
  reports +=
    listOf(
      report {
        name = "$MEASUREMENT_CONSUMER_NAME/reports/1"
        measurementConsumer = MEASUREMENT_CONSUMER_NAME
        state = Report.State.RUNNING
      },
      report {
        name = "$MEASUREMENT_CONSUMER_NAME/reports/2"
        measurementConsumer = MEASUREMENT_CONSUMER_NAME
        state = Report.State.SUCCEEDED
      },
      report {
        name = "$MEASUREMENT_CONSUMER_NAME/reports/3"
        measurementConsumer = MEASUREMENT_CONSUMER_NAME
        state = Report.State.FAILED
      }
    )
}

private const val DATA_PROVIDER_NAME = "dataProviders/1"

private val LIST_EVENT_GROUPS_RESPONSE = listEventGroupsResponse { eventGroups += eventGroup {} }

@RunWith(JUnit4::class)
class ReportingTest {
  private val reportingSetsServiceMock: ReportingSetsCoroutineImplBase =
    mockService() {
      onBlocking { createReportingSet(any()) }.thenReturn(REPORTING_SET)
      onBlocking { listReportingSets(any()) }.thenReturn(LIST_REPORTING_SETS_RESPONSE)
    }
  private val reportsServiceMock: ReportsCoroutineImplBase =
    mockService() {
      onBlocking { createReport(any()) }.thenReturn(REPORT)
      onBlocking { listReports(any()) }.thenReturn(LIST_REPORTS_RESPONSE)
      onBlocking { getReport(any()) }.thenReturn(REPORT)
    }
  private val eventGroupsServiceMock: EventGroupsCoroutineImplBase =
    mockService() { onBlocking { listEventGroups(any()) }.thenReturn(LIST_EVENT_GROUPS_RESPONSE) }

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
  fun `reporting_sets create calls api with valid request`() {
    val args =
      arrayOf(
        "--tls-cert-file=$SECRETS_DIR/mc_tls.pem",
        "--tls-key-file=$SECRETS_DIR/mc_tls.key",
        "--cert-collection-file=$SECRETS_DIR/reporting_root.pem",
        "--reporting-server-api-target=$HOST:${server.port}",
        "reporting-sets",
        "create",
        "--parent=$MEASUREMENT_CONSUMER_NAME",
        "--event-group=$EVENT_GROUP_NAME_1",
        "--event-group=$EVENT_GROUP_NAME_2",
        "--event-group=$EVENT_GROUP_NAME_3",
        "--filter=some.filter",
        "--display-name=test-reporting-set",
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
            eventGroups += listOf(EVENT_GROUP_NAME_1, EVENT_GROUP_NAME_2, EVENT_GROUP_NAME_3)
            filter = "some.filter"
            displayName = "test-reporting-set"
          }
        }
      )
    assertThat(parseTextProto(output.reader(), ReportingSet.getDefaultInstance()))
      .isEqualTo(REPORTING_SET)
  }

  @Test
  fun `reporting_sets list calls api with valid request`() {
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
      )

    val output = callCli(args)

    verifyProtoArgument(reportingSetsServiceMock, ReportingSetsCoroutineImplBase::listReportingSets)
      .isEqualTo(
        listReportingSetsRequest {
          parent = MEASUREMENT_CONSUMER_NAME
          pageSize = 50
        }
      )
    assertThat(parseTextProto(output.reader(), ListReportingSetsResponse.getDefaultInstance()))
      .isEqualTo(LIST_REPORTING_SETS_RESPONSE)
  }

  @Test
  fun `Reports create calls api with valid request`() {
    val metric =
      """
    reach { }
    set_operations {
      unique_name: "operation1"
      set_operation {
        type: 1
        lhs {
          reporting_set: "measurementConsumers/1/reportingSets/1"
        }
        rhs {
          reporting_set: "measurementConsumers/1/reportingSets/2"
        }
      }
    }
    """
        .trimIndent()
    val args =
      arrayOf(
        "--tls-cert-file=$SECRETS_DIR/mc_tls.pem",
        "--tls-key-file=$SECRETS_DIR/mc_tls.key",
        "--cert-collection-file=$SECRETS_DIR/reporting_root.pem",
        "--reporting-server-api-target=$HOST:${server.port}",
        "reports",
        "create",
        "--idempotency-key=$REPORT_IDEMPOTENCY_KEY",
        "--parent=$MEASUREMENT_CONSUMER_NAME",
        "--event-group-key=$EVENT_GROUP_NAME_1",
        "--event-group-value=",
        "--event-group-key=$EVENT_GROUP_NAME_2",
        "--event-group-value=partner=abc",
        "--periodic-interval-start-time=2017-01-15T01:30:15.01Z",
        "--periodic-interval-increment=P1DT3H5M12.99S",
        "--periodic-interval-count=3",
        "--metric=$metric",
      )

    val output = callCli(args)

    verifyProtoArgument(reportsServiceMock, ReportsCoroutineImplBase::createReport)
      .isEqualTo(
        createReportRequest {
          parent = MEASUREMENT_CONSUMER_NAME
          report = report {
            reportIdempotencyKey = REPORT_IDEMPOTENCY_KEY
            measurementConsumer = MEASUREMENT_CONSUMER_NAME
            eventGroupUniverse = eventGroupUniverse {
              eventGroupEntries += eventGroupEntry { key = EVENT_GROUP_NAME_1 }
              eventGroupEntries += eventGroupEntry {
                key = EVENT_GROUP_NAME_2
                value = "partner=abc"
              }
            }
            periodicTimeInterval = periodicTimeInterval {
              startTime = Instant.parse("2017-01-15T01:30:15.01Z").toProtoTime()
              increment = Duration.parse("P1DT3H5M12.99S").toProtoDuration()
              intervalCount = 3
            }
            metrics += metric {
              reach = reachParams {}
              setOperations += namedSetOperation {
                uniqueName = "operation1"
                setOperation = setOperation {
                  type = Metric.SetOperation.Type.UNION
                  lhs = operand { reportingSet = "measurementConsumers/1/reportingSets/1" }
                  rhs = operand { reportingSet = "measurementConsumers/1/reportingSets/2" }
                }
              }
            }
          }
        }
      )
    assertThat(parseTextProto(output.reader(), Report.getDefaultInstance())).isEqualTo(REPORT)
  }

  @Test
  fun `Reports create calls api with correct time intervals params`() {
    val textFormatMetric =
      """
    reach { }
    set_operations {
      unique_name: "operation1"
      set_operation {
        type: 1
        lhs {
          reporting_set: "measurementConsumers/1/reportingSets/1"
        }
        rhs {
          reporting_set: "measurementConsumers/1/reportingSets/2"
        }
      }
    }
    """
        .trimIndent()

    val args =
      arrayOf(
        "--tls-cert-file=$SECRETS_DIR/mc_tls.pem",
        "--tls-key-file=$SECRETS_DIR/mc_tls.key",
        "--cert-collection-file=$SECRETS_DIR/reporting_root.pem",
        "--reporting-server-api-target=$HOST:${server.port}",
        "reports",
        "create",
        "--idempotency-key=$REPORT_IDEMPOTENCY_KEY",
        "--parent=$MEASUREMENT_CONSUMER_NAME",
        "--event-group-key=$EVENT_GROUP_NAME_1",
        "--event-group-value=",
        "--interval-start-time=2017-01-15T01:30:15.01Z",
        "--interval-end-time=2018-10-27T23:19:12.99Z",
        "--interval-start-time=2019-01-19T09:48:35.57Z",
        "--interval-end-time=2022-06-13T11:57:54.21Z",
        "--metric=$textFormatMetric",
      )
    val output = callCli(args)

    verifyProtoArgument(reportsServiceMock, ReportsCoroutineImplBase::createReport)
      .comparingExpectedFieldsOnly()
      .isEqualTo(
        createReportRequest {
          report = report {
            timeIntervals = timeIntervals {
              timeIntervals += timeInterval {
                startTime = Instant.parse("2017-01-15T01:30:15.01Z").toProtoTime()
                endTime = Instant.parse("2018-10-27T23:19:12.99Z").toProtoTime()
              }
              timeIntervals += timeInterval {
                startTime = Instant.parse("2019-01-19T09:48:35.57Z").toProtoTime()
                endTime = Instant.parse("2022-06-13T11:57:54.21Z").toProtoTime()
              }
            }
          }
        }
      )
    assertThat(parseTextProto(output.reader(), Report.getDefaultInstance())).isEqualTo(REPORT)
  }

  @Test
  fun `Reports create calls api with complex metric`() {
    val textFormatMetric = TEXTPROTO_DIR.resolve("metric2.textproto").toFile().readText()

    val args =
      arrayOf(
        "--tls-cert-file=$SECRETS_DIR/mc_tls.pem",
        "--tls-key-file=$SECRETS_DIR/mc_tls.key",
        "--cert-collection-file=$SECRETS_DIR/reporting_root.pem",
        "--reporting-server-api-target=$HOST:${server.port}",
        "reports",
        "create",
        "--idempotency-key=$REPORT_IDEMPOTENCY_KEY",
        "--parent=$MEASUREMENT_CONSUMER_NAME",
        "--event-group-key=$EVENT_GROUP_NAME_1",
        "--event-group-value=",
        "--periodic-interval-start-time=2017-01-15T01:30:15.01Z",
        "--periodic-interval-increment=P1DT3H5M12.99S",
        "--periodic-interval-count=3",
        "--metric=$textFormatMetric",
      )
    val output = callCli(args)

    verifyProtoArgument(reportsServiceMock, ReportsCoroutineImplBase::createReport)
      .comparingExpectedFieldsOnly()
      .isEqualTo(
        createReportRequest {
          report = report {
            metrics += metric {
              reach = reachParams {}
              cumulative = true
              setOperations += namedSetOperation {
                uniqueName = "operation1"
                setOperation = setOperation {
                  type = Metric.SetOperation.Type.UNION
                  lhs = operand { reportingSet = "measurementConsumers/1/reportingSets/1" }
                  rhs = operand { reportingSet = "measurementConsumers/1/reportingSets/2" }
                }
              }
              setOperations += namedSetOperation {
                uniqueName = "operation2"
                setOperation = setOperation {
                  type = Metric.SetOperation.Type.DIFFERENCE
                  lhs = operand { reportingSet = "measurementConsumers/1/reportingSets/3" }
                  rhs = operand {
                    operation = setOperation {
                      type = Metric.SetOperation.Type.INTERSECTION
                      lhs = operand { reportingSet = "measurementConsumers/1/reportingSets/4" }
                      rhs = operand { reportingSet = "measurementConsumers/1/reportingSets/5" }
                    }
                  }
                }
              }
            }
          }
        }
      )
    assertThat(parseTextProto(output.reader(), Report.getDefaultInstance())).isEqualTo(REPORT)
  }

  @Test
  fun `Reports list calls api with valid request`() {
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
  fun `Reports get calls api with valid request`() {
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
  fun `EventGroups list callls api with valid request`() {
    val args =
      arrayOf(
        "--tls-cert-file=$SECRETS_DIR/mc_tls.pem",
        "--tls-key-file=$SECRETS_DIR/mc_tls.key",
        "--cert-collection-file=$SECRETS_DIR/reporting_root.pem",
        "--reporting-server-api-target=$HOST:${server.port}",
        "event-groups",
        "list",
        "--parent=$DATA_PROVIDER_NAME",
        "--filter=abcd",
      )
    val output = callCli(args)

    verifyProtoArgument(eventGroupsServiceMock, EventGroupsCoroutineImplBase::listEventGroups)
      .isEqualTo(
        listEventGroupsRequest {
          parent = DATA_PROVIDER_NAME
          filter = "abcd"
          pageSize = 1000
        }
      )
    assertThat(parseTextProto(output.reader(), ListEventGroupsResponse.getDefaultInstance()))
      .isEqualTo(LIST_EVENT_GROUPS_RESPONSE)
  }
}
