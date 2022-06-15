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

import io.grpc.Server
import io.grpc.ServerServiceDefinition
import io.grpc.netty.NettyServerBuilder
import java.nio.file.Path
import java.nio.file.Paths
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
import org.wfanet.measurement.common.testing.verifyProtoArgument
import org.wfanet.measurement.reporting.v1alpha.ReportingSetsGrpcKt.ReportingSetsCoroutineImplBase
import org.wfanet.measurement.reporting.v1alpha.createReportingSetRequest
import org.wfanet.measurement.reporting.v1alpha.listReportingSetsRequest
import org.wfanet.measurement.reporting.v1alpha.listReportingSetsResponse
import org.wfanet.measurement.reporting.v1alpha.reportingSet
import picocli.CommandLine

private const val HOST = "localhost"
private val SECRETS_DIR: Path =
  getRuntimePath(
    Paths.get(
      "wfa_measurement_system",
      "src",
      "main",
      "k8s",
      "testing",
      "secretfiles",
    )
  )!!

private const val MEASUREMENT_CONSUMER_NAME = "measurementConsumers/1"
private const val EVENT_GROUP_NAME_1 = "dataProviders/1/eventGroups/1"
private const val EVENT_GROUP_NAME_2 = "dataProviders/1/eventGroups/2"
private const val EVENT_GROUP_NAME_3 = "dataProviders/2/eventGroups/1"

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

@RunWith(JUnit4::class)
class ReportingTest {
  private val reportingSetsServiceMock: ReportingSetsCoroutineImplBase =
    mockService() {
      onBlocking { createReportingSet(any()) }.thenReturn(REPORTING_SET)
      onBlocking { listReportingSets(any()) }.thenReturn(LIST_REPORTING_SETS_RESPONSE)
    }

  // TODO(@renjiez): Use reporting server's credential
  private val serverCerts =
    SigningCerts.fromPemFiles(
      certificateFile = SECRETS_DIR.resolve("kingdom_tls.pem").toFile(),
      privateKeyFile = SECRETS_DIR.resolve("kingdom_tls.key").toFile(),
      trustedCertCollectionFile = SECRETS_DIR.resolve("mc_root.pem").toFile(),
    )

  private val services: List<ServerServiceDefinition> =
    listOf(
      reportingSetsServiceMock.bindService(),
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

  @Test
  fun `reporting_sets create calls api with valid request`() {
    val args =
      arrayOf(
        "--tls-cert-file=$SECRETS_DIR/mc_tls.pem",
        "--tls-key-file=$SECRETS_DIR/mc_tls.key",
        "--cert-collection-file=$SECRETS_DIR/kingdom_root.pem",
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

    CommandLine(Reporting()).execute(*args)

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
  }

  @Test
  fun `reporting_sets list calls api with valid request`() {
    val args =
      arrayOf(
        "--tls-cert-file=$SECRETS_DIR/mc_tls.pem",
        "--tls-key-file=$SECRETS_DIR/mc_tls.key",
        "--cert-collection-file=$SECRETS_DIR/kingdom_root.pem",
        "--reporting-server-api-target=$HOST:${server.port}",
        "reporting-sets",
        "list",
        "--parent=$MEASUREMENT_CONSUMER_NAME",
        "--page-size=50",
      )
    CommandLine(Reporting()).execute(*args)

    verifyProtoArgument(reportingSetsServiceMock, ReportingSetsCoroutineImplBase::listReportingSets)
      .isEqualTo(
        listReportingSetsRequest {
          parent = MEASUREMENT_CONSUMER_NAME
          pageSize = 50
        }
      )
  }
}
