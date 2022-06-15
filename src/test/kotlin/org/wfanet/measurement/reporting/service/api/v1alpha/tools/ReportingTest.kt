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

import com.google.protobuf.Timestamp
import com.google.protobuf.timestamp
import io.grpc.ServerServiceDefinition
import io.netty.handler.ssl.ClientAuth
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
import org.wfanet.measurement.common.grpc.CommonServer
import org.wfanet.measurement.common.grpc.testing.mockService
import org.wfanet.measurement.common.testing.verifyProtoArgument
import org.wfanet.measurement.common.toProtoTime
import org.wfanet.measurement.reporting.v1alpha.Metric
import org.wfanet.measurement.reporting.v1alpha.MetricKt
import org.wfanet.measurement.reporting.v1alpha.MetricKt.SetOperationKt.operand
import org.wfanet.measurement.reporting.v1alpha.MetricKt.namedSetOperation
import org.wfanet.measurement.reporting.v1alpha.MetricKt.reachParams
import org.wfanet.measurement.reporting.v1alpha.MetricKt.setOperation
import org.wfanet.measurement.reporting.v1alpha.Report
import org.wfanet.measurement.reporting.v1alpha.ReportKt.EventGroupUniverseKt.eventGroupEntry
import org.wfanet.measurement.reporting.v1alpha.ReportKt.eventGroupUniverse
import org.wfanet.measurement.reporting.v1alpha.ReportingSetsGrpcKt.ReportingSetsCoroutineImplBase
import org.wfanet.measurement.reporting.v1alpha.ReportsGrpcKt.ReportsCoroutineImplBase
import org.wfanet.measurement.reporting.v1alpha.createReportRequest
import org.wfanet.measurement.reporting.v1alpha.createReportingSetRequest
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
import picocli.CommandLine

private const val HOST = "localhost"
private const val PORT = 15789
private val SECRETS_DIR: Path =
  getRuntimePath(Paths.get("wfa_measurement_system/src/main/k8s/testing/secretfiles"))!!

private val TEXTPROTO_DIR: Path =
  getRuntimePath(
    Paths.get("wfa_measurement_system/src/test/kotlin/org/wfanet/measurement/reporting" +
                "/service/api/v1alpha/tools/textprotos")
  )!!

private const val MEASUREMENT_CONSUMER_NAME = "measurementConsumers/1"
private const val EVENT_GROUP_NAME_1 = "dataProviders/1/eventGroups/1"
private const val EVENT_GROUP_NAME_2 = "dataProviders/1/eventGroups/2"
private const val EVENT_GROUP_NAME_3 = "dataProviders/2/eventGroups/1"

private val REPORTING_SET = reportingSet {}
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
}

private val REPORT = report {
  measurementConsumer = MEASUREMENT_CONSUMER_NAME
  eventGroupUniverse = eventGroupUniverse {
    eventGroupEntries +=  eventGroupEntry {
      key = "measurementConsumers/1/dataProviders/1/eventGroups/1"
      value = ""
    }
    eventGroupEntries +=  eventGroupEntry {
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

private const val MAX_PAGE_SIZE = 1000

private val LIST_REPORTS_RESPONSE = listReportsResponse {
  reports += listOf(
    report {
      name = "$MEASUREMENT_CONSUMER_NAME/reports/1"
      measurementConsumer = MEASUREMENT_CONSUMER_NAME
      state = Report.State.RUNNING
    },
    report {
      name = "$MEASUREMENT_CONSUMER_NAME/reports/2"
      measurementConsumer = MEASUREMENT_CONSUMER_NAME
      state = Report.State.SUCCEEDED
      result = "CVS string"
    },
    report {
      name = "$MEASUREMENT_CONSUMER_NAME/reports/3"
      measurementConsumer = MEASUREMENT_CONSUMER_NAME
      state = Report.State.FAILED
    }
  )

}

private fun convertToTimestamp(instant: Instant): Timestamp {
  return timestamp {
    seconds = instant.epochSecond
    nanos = instant.nano
  }
}

private fun convertToProtoDuration(duration: Duration): com.google.protobuf.Duration {
  return com.google.protobuf.duration {
    seconds = duration.seconds
    nanos = duration.nano
  }
}

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
      onBlocking { listReports(any())}.thenReturn(LIST_REPORTS_RESPONSE)
    }

  private lateinit var server: CommonServer
  @Before
  fun initServer() {
    val services: List<ServerServiceDefinition> =
      listOf(
        reportingSetsServiceMock.bindService(),
        reportsServiceMock.bindService(),
      )

    // TODO(@renjiez): Use reporting server's credential
    val serverCerts =
      SigningCerts.fromPemFiles(
        certificateFile = SECRETS_DIR.resolve("kingdom_tls.pem").toFile(),
        privateKeyFile = SECRETS_DIR.resolve("kingdom_tls.key").toFile(),
        trustedCertCollectionFile = SECRETS_DIR.resolve("mc_root.pem").toFile(),
      )

    server =
      CommonServer.fromParameters(
        PORT,
        true,
        serverCerts,
        ClientAuth.REQUIRE,
        "kingdom-test",
        services
      )
    server.start()
  }

  @After
  fun shutdownServer() {
    server.server.shutdown()
    server.server.awaitTermination(1, SECONDS)
  }

  @Test
  fun `Create reporting_set call api with valid CreateReportingSetRequest`() {
    val args =
      arrayOf(
        "--tls-cert-file=$SECRETS_DIR/mc_tls.pem",
        "--tls-key-file=$SECRETS_DIR/mc_tls.key",
        "--cert-collection-file=$SECRETS_DIR/kingdom_root.pem",
        "--reporting-server-api-target=$HOST:$PORT",
        "create-reporting-set",
        "--measurement-consumer=$MEASUREMENT_CONSUMER_NAME",
        "--event-groups",
        "dataProviders/1/eventGroups/1",
        "dataProviders/1/eventGroups/2",
        "dataProviders/2/eventGroups/1",
        "--filter=some.filter",
        "--display-name=test-reporting-set",
      )

    CommandLine(Reporting()).execute(*args)

    verifyProtoArgument(
        reportingSetsServiceMock,
        ReportingSetsCoroutineImplBase::createReportingSet
      )
      .comparingExpectedFieldsOnly()
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
  }

  @Test
  fun `List reporting_sets call api with valid ListReportingSetRequest`() {
    val args =
      arrayOf(
        "--tls-cert-file=$SECRETS_DIR/mc_tls.pem",
        "--tls-key-file=$SECRETS_DIR/mc_tls.key",
        "--cert-collection-file=$SECRETS_DIR/kingdom_root.pem",
        "--reporting-server-api-target=$HOST:$PORT",
        "list-reporting-sets",
        "--measurement-consumer=$MEASUREMENT_CONSUMER_NAME",
      )
    CommandLine(Reporting()).execute(*args)

    verifyProtoArgument(reportingSetsServiceMock, ReportingSetsCoroutineImplBase::listReportingSets)
      .comparingExpectedFieldsOnly()
      .isEqualTo(listReportingSetsRequest { parent = MEASUREMENT_CONSUMER_NAME })
  }

  @Test
  fun `Report create calls api with valid request`() {
    val metric = """
    reach { }
    set_operations {
      display_name: "operation1"
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
    """.trimIndent()

    val args =
      arrayOf(
        "--tls-cert-file=$SECRETS_DIR/mc_tls.pem",
        "--tls-key-file=$SECRETS_DIR/mc_tls.key",
        "--cert-collection-file=$SECRETS_DIR/kingdom_root.pem",
        "--reporting-server-api-target=$HOST:$PORT",
        "create-report",
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
    CommandLine(Reporting()).execute(*args)

    verifyProtoArgument(reportsServiceMock, ReportsCoroutineImplBase::createReport)
      .isEqualTo(createReportRequest {
        parent = MEASUREMENT_CONSUMER_NAME
        report = report{
          measurementConsumer = MEASUREMENT_CONSUMER_NAME
          eventGroupUniverse = eventGroupUniverse {
            eventGroupEntries += eventGroupEntry {
              key = EVENT_GROUP_NAME_1
            }
            eventGroupEntries += eventGroupEntry {
              key = EVENT_GROUP_NAME_2
              value = "partner=abc"
            }
          }
          periodicTimeInterval = periodicTimeInterval {
            startTime = convertToTimestamp(Instant.parse("2017-01-15T01:30:15.01Z"))
            increment = convertToProtoDuration(Duration.parse("P1DT3H5M12.99S"))
            intervalCount = 3
          }
          metrics += metric {
            reach = reachParams {}
            setOperations += namedSetOperation {
              displayName = "operation1"
              setOperation = setOperation {
                type = Metric.SetOperation.Type.UNION
                lhs = operand {
                  reportingSet = "measurementConsumers/1/reportingSets/1"
                }
                rhs = operand {
                  reportingSet = "measurementConsumers/1/reportingSets/2"
                }
              }
            }
          }
        }
      })
  }

  @Test
  fun `Report create calls api with complex metric`() {
    val serializedMetrics = TEXTPROTO_DIR.resolve("metric.textproto").toFile().readText()

    val args =
      arrayOf(
        "--tls-cert-file=$SECRETS_DIR/mc_tls.pem",
        "--tls-key-file=$SECRETS_DIR/mc_tls.key",
        "--cert-collection-file=$SECRETS_DIR/kingdom_root.pem",
        "--reporting-server-api-target=$HOST:$PORT",
        "create-report",
        "--parent=$MEASUREMENT_CONSUMER_NAME",
        "--event-group-key=$EVENT_GROUP_NAME_1",
        "--event-group-value=",
        "--periodic-interval-start-time=2017-01-15T01:30:15.01Z",
        "--periodic-interval-increment=P1DT3H5M12.99S",
        "--periodic-interval-count=3",
        "--metric=$serializedMetrics",
      )
    CommandLine(Reporting()).execute(*args)

    verifyProtoArgument(reportsServiceMock, ReportsCoroutineImplBase::createReport)
      .comparingExpectedFieldsOnly()
      .isEqualTo(createReportRequest {
        report = report {
          metrics += metric {
            reach = reachParams {}
            cumulative = true
            setOperations += namedSetOperation {
              displayName = "operation1"
              setOperation = setOperation {
                type = Metric.SetOperation.Type.UNION
                lhs = operand {
                  reportingSet = "measurementConsumers/1/reportingSets/1"
                }
                rhs = operand {
                  reportingSet = "measurementConsumers/1/reportingSets/2"
                }
              }
            }
            setOperations += namedSetOperation {
              displayName = "operation2"
              setOperation = setOperation {
                type = Metric.SetOperation.Type.DIFFERENCE
                lhs = operand {
                  reportingSet = "measurementConsumers/1/reportingSets/3"
                }
                rhs = operand {
                  operation = setOperation {
                    type = Metric.SetOperation.Type.INTERSECTION
                    lhs = operand {
                      reportingSet = "measurementConsumers/1/reportingSets/4"
                    }
                    rhs = operand {
                      reportingSet = "measurementConsumers/1/reportingSets/5"
                    }
                  }
                }
              }
            }
          }
        }
      })
  }

  @Test
  fun `Report list calls api with valid request`() {
    val args =
      arrayOf(
        "--tls-cert-file=$SECRETS_DIR/mc_tls.pem",
        "--tls-key-file=$SECRETS_DIR/mc_tls.key",
        "--cert-collection-file=$SECRETS_DIR/kingdom_root.pem",
        "--reporting-server-api-target=$HOST:$PORT",
        "list-reports",
        "--parent=$MEASUREMENT_CONSUMER_NAME",
      )
    CommandLine(Reporting()).execute(*args)

    verifyProtoArgument(reportsServiceMock, ReportsCoroutineImplBase::listReports)
      .isEqualTo(listReportsRequest {
          parent = MEASUREMENT_CONSUMER_NAME
          pageSize = MAX_PAGE_SIZE
        }
      )

  }
}
