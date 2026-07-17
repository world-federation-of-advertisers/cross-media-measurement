/*
 * Copyright 2026 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.reporting.mcp.tools

import com.google.common.truth.Truth.assertThat
import io.grpc.Metadata
import io.grpc.ServerCall
import io.grpc.ServerCallHandler
import io.grpc.ServerInterceptor
import io.grpc.Status
import io.grpc.StatusException
import io.grpc.inprocess.InProcessChannelBuilder
import io.grpc.inprocess.InProcessServerBuilder
import kotlinx.coroutines.runBlocking
import org.junit.After
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.reporting.mcp.grpc.ReportingPublicApiClient
import org.wfanet.measurement.reporting.v2alpha.BasicReport
import org.wfanet.measurement.reporting.v2alpha.BasicReportsGrpcKt
import org.wfanet.measurement.reporting.v2alpha.CreateBasicReportRequest
import org.wfanet.measurement.reporting.v2alpha.CreateReportingSetRequest
import org.wfanet.measurement.reporting.v2alpha.EventGroup
import org.wfanet.measurement.reporting.v2alpha.EventGroupsGrpcKt
import org.wfanet.measurement.reporting.v2alpha.GetBasicReportRequest
import org.wfanet.measurement.reporting.v2alpha.GetEventGroupRequest
import org.wfanet.measurement.reporting.v2alpha.GetImpressionQualificationFilterRequest
import org.wfanet.measurement.reporting.v2alpha.GetReportingSetRequest
import org.wfanet.measurement.reporting.v2alpha.ImpressionQualificationFilter
import org.wfanet.measurement.reporting.v2alpha.ImpressionQualificationFiltersGrpcKt
import org.wfanet.measurement.reporting.v2alpha.ListBasicReportsRequest
import org.wfanet.measurement.reporting.v2alpha.ListBasicReportsResponse
import org.wfanet.measurement.reporting.v2alpha.ListEventGroupsRequest
import org.wfanet.measurement.reporting.v2alpha.ListEventGroupsRequestKt
import org.wfanet.measurement.reporting.v2alpha.ListEventGroupsResponse
import org.wfanet.measurement.reporting.v2alpha.ListImpressionQualificationFiltersRequest
import org.wfanet.measurement.reporting.v2alpha.ListImpressionQualificationFiltersResponse
import org.wfanet.measurement.reporting.v2alpha.ListReportingSetsRequest
import org.wfanet.measurement.reporting.v2alpha.ListReportingSetsResponse
import org.wfanet.measurement.reporting.v2alpha.ReportingSet
import org.wfanet.measurement.reporting.v2alpha.ReportingSetsGrpcKt
import org.wfanet.measurement.reporting.v2alpha.basicReport
import org.wfanet.measurement.reporting.v2alpha.createBasicReportRequest
import org.wfanet.measurement.reporting.v2alpha.createReportingSetRequest
import org.wfanet.measurement.reporting.v2alpha.eventGroup
import org.wfanet.measurement.reporting.v2alpha.getBasicReportRequest
import org.wfanet.measurement.reporting.v2alpha.impressionQualificationFilter
import org.wfanet.measurement.reporting.v2alpha.listEventGroupsRequest
import org.wfanet.measurement.reporting.v2alpha.reportingSet

private val AUTH_KEY: Metadata.Key<String> =
  Metadata.Key.of("authorization", Metadata.ASCII_STRING_MARSHALLER)

@RunWith(JUnit4::class)
class ToolsIntegrationTest {

  private lateinit var grpcServer: io.grpc.Server
  private lateinit var apiClient: ReportingPublicApiClient
  private var capturedAuthHeader: String? = null

  @Before
  fun setUp() {
    val authInterceptor =
      object : ServerInterceptor {
        override fun <ReqT, RespT> interceptCall(
          call: ServerCall<ReqT, RespT>,
          headers: Metadata,
          next: ServerCallHandler<ReqT, RespT>,
        ): ServerCall.Listener<ReqT> {
          capturedAuthHeader = headers.get(AUTH_KEY)
          return next.startCall(call, headers)
        }
      }

    val serverName = InProcessServerBuilder.generateName()
    grpcServer =
      InProcessServerBuilder.forName(serverName)
        .directExecutor()
        .intercept(authInterceptor)
        .addService(FakeBasicReportsService())
        .addService(FakeEventGroupsService())
        .addService(FakeReportingSetsService())
        .addService(FakeIqfService())
        .build()
        .start()

    val channel = InProcessChannelBuilder.forName(serverName).directExecutor().build()
    apiClient =
      ReportingPublicApiClient(
        basicReports = BasicReportsGrpcKt.BasicReportsCoroutineStub(channel),
        eventGroups = EventGroupsGrpcKt.EventGroupsCoroutineStub(channel),
        reportingSets = ReportingSetsGrpcKt.ReportingSetsCoroutineStub(channel),
        impressionQualificationFilters =
          ImpressionQualificationFiltersGrpcKt.ImpressionQualificationFiltersCoroutineStub(channel),
      )
    capturedAuthHeader = null
  }

  @After
  fun tearDown() {
    grpcServer.shutdownNow()
  }

  @Test
  fun listEventGroupsReturnsProtoJson() = runBlocking {
    val stubs = apiClient.withBearerToken("test-token")
    val result =
      stubs.eventGroups.listEventGroups(
        listEventGroupsRequest { parent = "measurementConsumers/mc1" }
      )
    assertThat(result).isNotNull()
  }

  @Test
  fun getBasicReportReturnsReport() = runBlocking {
    val stubs = apiClient.withBearerToken("test-token")
    val result =
      stubs.basicReports.getBasicReport(
        getBasicReportRequest { name = "measurementConsumers/mc1/basicReports/br1" }
      )
    assertThat(result.name).isEqualTo("measurementConsumers/mc1/basicReports/br1")
  }

  @Test
  fun createBasicReportForwardsBearerToken() = runBlocking {
    val stubs = apiClient.withBearerToken("my-secret-bearer")
    stubs.basicReports.createBasicReport(
      createBasicReportRequest {
        parent = "measurementConsumers/mc1"
        basicReportId = "test-report"
      }
    )
    assertThat(capturedAuthHeader).isEqualTo("Bearer my-secret-bearer")
  }

  @Test
  fun getBasicReportNotFoundReturnsError() = runBlocking {
    val stubs = apiClient.withBearerToken("test-token")
    val result =
      ToolSupport.handleToolCall {
        ToolSupport.PROTO_JSON_PRINTER.print(
          stubs.basicReports.getBasicReport(
            getBasicReportRequest { name = "measurementConsumers/mc1/basicReports/nonexistent" }
          )
        )
      }
    assertThat(result.isError).isTrue()
    assertThat((result.content[0] as io.modelcontextprotocol.kotlin.sdk.types.TextContent).text)
      .contains("NOT_FOUND")
  }

  @Test
  fun handleToolCallCatchesParseException() = runBlocking {
    val result =
      ToolSupport.handleToolCall {
        com.google.protobuf.util.Timestamps.parse("not-a-timestamp")
        "should not reach here"
      }
    assertThat(result.isError).isTrue()
    assertThat((result.content[0] as io.modelcontextprotocol.kotlin.sdk.types.TextContent).text)
      .contains("timestamp")
  }

  @Test
  fun handleToolCallCatchesIllegalArgumentException() = runBlocking {
    val result =
      ToolSupport.handleToolCall {
        org.wfanet.measurement.reporting.v2alpha.EventGroup.View.valueOf("INVALID_VIEW")
        "should not reach here"
      }
    assertThat(result.isError).isTrue()
    assertThat((result.content[0] as io.modelcontextprotocol.kotlin.sdk.types.TextContent).text)
      .contains("Invalid argument")
  }

  @Test
  fun createReportingSetWithInvalidArgumentReturnsError() = runBlocking {
    val stubs = apiClient.withBearerToken("test-token")
    val result =
      ToolSupport.handleToolCall {
        ToolSupport.PROTO_JSON_PRINTER.print(
          stubs.reportingSets.createReportingSet(
            createReportingSetRequest {
              parent = ""
              reportingSetId = "test-rs"
            }
          )
        )
      }
    assertThat(result.isError).isTrue()
    assertThat((result.content[0] as io.modelcontextprotocol.kotlin.sdk.types.TextContent).text)
      .contains("INVALID_ARGUMENT")
  }

  @Test
  fun listEventGroupsWithStructuredFilterMergesIntoProto() = runBlocking {
    val stubs = apiClient.withBearerToken("test-token")
    val filter = ListEventGroupsRequestKt.filter { cmmsDataProviderIn += "dataProviders/dp1" }
    val result =
      stubs.eventGroups.listEventGroups(
        listEventGroupsRequest {
          parent = "measurementConsumers/mc1"
          structuredFilter = filter
        }
      )
    assertThat(result).isNotNull()
  }

  @Test
  fun listIqfWithNoArgumentsSucceeds() = runBlocking {
    val stubs = apiClient.withBearerToken("test-token")
    val result =
      stubs.impressionQualificationFilters.listImpressionQualificationFilters(
        ListImpressionQualificationFiltersRequest.getDefaultInstance()
      )
    assertThat(result).isNotNull()
  }

  private class FakeBasicReportsService : BasicReportsGrpcKt.BasicReportsCoroutineImplBase() {
    override suspend fun createBasicReport(request: CreateBasicReportRequest): BasicReport =
      basicReport {
        name = "${request.parent}/basicReports/${request.basicReportId}"
      }

    override suspend fun getBasicReport(request: GetBasicReportRequest): BasicReport {
      if (request.name.contains("nonexistent")) {
        throw StatusException(Status.NOT_FOUND.withDescription("Report not found"))
      }
      return basicReport { name = request.name }
    }

    override suspend fun listBasicReports(
      request: ListBasicReportsRequest
    ): ListBasicReportsResponse = ListBasicReportsResponse.getDefaultInstance()
  }

  private class FakeEventGroupsService : EventGroupsGrpcKt.EventGroupsCoroutineImplBase() {
    override suspend fun getEventGroup(request: GetEventGroupRequest): EventGroup = eventGroup {
      name = request.name
    }

    override suspend fun listEventGroups(request: ListEventGroupsRequest): ListEventGroupsResponse =
      ListEventGroupsResponse.getDefaultInstance()
  }

  private class FakeReportingSetsService : ReportingSetsGrpcKt.ReportingSetsCoroutineImplBase() {
    override suspend fun createReportingSet(request: CreateReportingSetRequest): ReportingSet {
      if (request.parent.isEmpty()) {
        throw StatusException(Status.INVALID_ARGUMENT.withDescription("parent must not be empty"))
      }
      return reportingSet { name = "${request.parent}/reportingSets/${request.reportingSetId}" }
    }

    override suspend fun getReportingSet(request: GetReportingSetRequest): ReportingSet =
      reportingSet {
        name = request.name
      }

    override suspend fun listReportingSets(
      request: ListReportingSetsRequest
    ): ListReportingSetsResponse = ListReportingSetsResponse.getDefaultInstance()
  }

  private class FakeIqfService :
    ImpressionQualificationFiltersGrpcKt.ImpressionQualificationFiltersCoroutineImplBase() {
    override suspend fun getImpressionQualificationFilter(
      request: GetImpressionQualificationFilterRequest
    ): ImpressionQualificationFilter = impressionQualificationFilter { name = request.name }

    override suspend fun listImpressionQualificationFilters(
      request: ListImpressionQualificationFiltersRequest
    ): ListImpressionQualificationFiltersResponse =
      ListImpressionQualificationFiltersResponse.getDefaultInstance()
  }
}
