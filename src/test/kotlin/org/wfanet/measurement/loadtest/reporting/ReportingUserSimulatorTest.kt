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

package org.wfanet.measurement.loadtest.reporting

import com.google.common.truth.Truth.assertThat
import com.google.protobuf.util.JsonFormat
import io.grpc.Status
import io.grpc.StatusException
import java.time.LocalDate
import kotlin.test.assertFailsWith
import kotlinx.coroutines.runBlocking
import okhttp3.Interceptor
import okhttp3.OkHttpClient
import okhttp3.Protocol
import okhttp3.Response
import okhttp3.ResponseBody.Companion.toResponseBody
import okio.Buffer
import org.junit.Rule
import org.junit.Test
import org.wfanet.measurement.api.v2alpha.DataProvidersGrpcKt.DataProvidersCoroutineStub
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.reporting.v2alpha.BasicReport
import org.wfanet.measurement.reporting.v2alpha.CreateReportingSetRequest
import org.wfanet.measurement.reporting.v2alpha.EventGroupsGrpcKt.EventGroupsCoroutineImplBase
import org.wfanet.measurement.reporting.v2alpha.EventGroupsGrpcKt.EventGroupsCoroutineStub
import org.wfanet.measurement.reporting.v2alpha.ListEventGroupsRequest
import org.wfanet.measurement.reporting.v2alpha.ListEventGroupsResponse
import org.wfanet.measurement.reporting.v2alpha.ReportingSet
import org.wfanet.measurement.reporting.v2alpha.ReportingSetsGrpcKt.ReportingSetsCoroutineImplBase
import org.wfanet.measurement.reporting.v2alpha.ReportingSetsGrpcKt.ReportingSetsCoroutineStub
import org.wfanet.measurement.reporting.v2alpha.ResultGroupMetricSpecKt
import org.wfanet.measurement.reporting.v2alpha.ResultGroupSpec
import org.wfanet.measurement.reporting.v2alpha.copy
import org.wfanet.measurement.reporting.v2alpha.dimensionSpec
import org.wfanet.measurement.reporting.v2alpha.eventGroup
import org.wfanet.measurement.reporting.v2alpha.listEventGroupsResponse
import org.wfanet.measurement.reporting.v2alpha.metricFrequencySpec
import org.wfanet.measurement.reporting.v2alpha.reportingUnit
import org.wfanet.measurement.reporting.v2alpha.resultGroupMetricSpec
import org.wfanet.measurement.reporting.v2alpha.resultGroupSpec

class ReportingUserSimulatorTest {
  private val reportingSetRequests = mutableListOf<CreateReportingSetRequest>()
  private val postedReports = mutableListOf<BasicReport>()
  private var failReportingSetCreation = false

  @get:Rule
  val grpcServer = GrpcTestServerRule {
    addService(
      object : EventGroupsCoroutineImplBase() {
        override suspend fun listEventGroups(
          request: ListEventGroupsRequest
        ): ListEventGroupsResponse = listEventGroupsResponse { eventGroups += EVENT_GROUPS }
      }
    )
    addService(
      object : ReportingSetsCoroutineImplBase() {
        override suspend fun createReportingSet(request: CreateReportingSetRequest): ReportingSet {
          if (failReportingSetCreation) throw Status.UNAVAILABLE.asRuntimeException()
          reportingSetRequests += request
          return request.reportingSet.copy {
            name = "${request.parent}/reportingSets/${request.reportingSetId}"
          }
        }
      }
    )
  }

  @Test
  fun `filtered groups preserve provider membership and reuse matching predicates`(): Unit =
    runBlocking {
      val simulator = simulator()
      val report =
        simulator.createBasicReportOverFilteredEventGroups(
          runId = "filtered",
          reportTitle = "Device cuts",
          campaignGroupDisplayName = "High overlap",
          eventGroupReferenceIds = REFERENCE_IDS,
          eventGroupEntityTypes = setOf("ad_group"),
          reportStart = START,
          reportEnd = END,
          qualificationFilters = ReportingUserSimulator.mediaTypeQualificationFilters(),
          componentFiltersByGroupTitle =
            linkedMapOf(
              "weekly device" to DEVICE_FILTER,
              "total device" to DEVICE_FILTER,
              "all" to "",
            ),
          buildResultGroupSpecs = { groups, components ->
            assertThat(groups).containsExactlyElementsIn(EVENT_GROUPS)
            components.map { (title, names) -> groupSpec(title, names) }
          },
        )

      assertThat(report.state).isEqualTo(BasicReport.State.SUCCEEDED)
      assertThat(reportingSetRequests).hasSize(4)
      for (predicate in listOf(DEVICE_FILTER, "")) {
        val requests = reportingSetRequests.filter { it.reportingSet.filter == predicate }
      assertThat(requests.map { it.reportingSetId }).isInOrder()
        assertThat(requests.map { it.reportingSet.primitive.cmmsEventGroupsList })
          .containsExactly(
            listOf("dataProviders/one/eventGroups/one", "dataProviders/one/eventGroups/two"),
            listOf("dataProviders/two/eventGroups/three"),
          )
          .inOrder()
      }
      val posted = postedReports.single()
      assertThat(posted.campaignGroup).isEmpty()
      assertThat(posted.modelLine).isEqualTo(MODEL_LINE)
      assertThat(posted.impressionQualificationFiltersList).hasSize(3)
      assertThat(posted.reportingInterval.reportEnd.day).isEqualTo(END.dayOfMonth)
      val groups = posted.resultGroupSpecsList.associateBy { it.title }
      assertThat(groups.getValue("weekly device").reportingUnit.componentsList)
        .containsExactlyElementsIn(groups.getValue("total device").reportingUnit.componentsList)
        .inOrder()
      val names =
        reportingSetRequests.map { "${it.parent}/reportingSets/${it.reportingSetId}" }.toSet()
      assertThat(posted.resultGroupSpecsList.flatMap { it.reportingUnit.componentsList }.toSet())
        .containsExactlyElementsIn(names)
    }

  @Test
  fun `campaign group report retains DataProvider components`(): Unit = runBlocking {
    val report =
      simulator()
        .createBasicReportOverEventGroups(
          runId = "campaign",
          reportTitle = "Existing report",
          campaignGroupDisplayName = "High overlap",
          eventGroupReferenceIds = REFERENCE_IDS,
          eventGroupEntityTypes = setOf("ad_group"),
          reportStart = START,
          reportEnd = END,
          qualificationFilters = ReportingUserSimulator.mediaTypeQualificationFilters(),
          buildResultGroupSpecs = { groups ->
            listOf(groupSpec("all", groups.map { it.cmmsDataProvider }.distinct()))
          },
        )

    assertThat(report.state).isEqualTo(BasicReport.State.SUCCEEDED)
    val campaign = reportingSetRequests.single()
    val posted = postedReports.single()
    assertThat(posted.campaignGroup)
      .isEqualTo("${campaign.parent}/reportingSets/${campaign.reportingSetId}")
    assertThat(posted.campaignGroupDisplayName).isEqualTo("High overlap")
    assertThat(campaign.reportingSet.primitive.cmmsEventGroupsList)
      .containsExactlyElementsIn(EVENT_GROUPS.map { it.cmmsEventGroup })
    assertThat(posted.resultGroupSpecsList.single().reportingUnit.componentsList)
      .containsExactly("dataProviders/one", "dataProviders/two")
  }

  @Test
  fun `failed component creation prevents creating the report`(): Unit = runBlocking {
    failReportingSetCreation = true
    assertFailsWith<StatusException> {
      simulator()
        .createBasicReportOverFilteredEventGroups(
          runId = "failure",
          reportTitle = "Device cuts",
          campaignGroupDisplayName = "High overlap",
          eventGroupReferenceIds = REFERENCE_IDS,
          eventGroupEntityTypes = setOf("ad_group"),
          reportStart = START,
          reportEnd = END,
          qualificationFilters = ReportingUserSimulator.mediaTypeQualificationFilters(),
          componentFiltersByGroupTitle = mapOf("device" to DEVICE_FILTER),
          buildResultGroupSpecs = { _, _ ->
            error("Must not build a report after component failure")
          },
        )
    }
    assertThat(postedReports).isEmpty()
  }

  private fun simulator(): ReportingUserSimulator {
    val interceptor = Interceptor { chain ->
      val request = chain.request()
      if (request.method == "POST") {
        val buffer = Buffer()
        requireNotNull(request.body).writeTo(buffer)
        postedReports +=
          BasicReport.newBuilder().also { JsonFormat.parser().merge(buffer.readUtf8(), it) }.build()
      }
      Response.Builder()
        .request(request)
        .protocol(Protocol.HTTP_1_1)
        .code(200)
        .message("OK")
        .body("""{"state":"SUCCEEDED"}""".toResponseBody())
        .build()
    }
    return ReportingUserSimulator(
      measurementConsumerName = PARENT,
      dataProvidersClient = DataProvidersCoroutineStub(grpcServer.channel),
      eventGroupsClient = EventGroupsCoroutineStub(grpcServer.channel),
      reportingSetsClient = ReportingSetsCoroutineStub(grpcServer.channel),
      okHttpReportingClient = OkHttpClient.Builder().addInterceptor(interceptor).build(),
      reportingGatewayHost = "example.test",
      getReportingAccessToken = { "test-token" },
      modelLineName = MODEL_LINE,
    )
  }

  private fun groupSpec(title: String, components: List<String>): ResultGroupSpec =
    resultGroupSpec {
      this.title = title
      reportingUnit = reportingUnit { this.components += components }
      dimensionSpec = dimensionSpec {}
      metricFrequency = metricFrequencySpec { total = true }
      resultGroupMetricSpec = resultGroupMetricSpec {
        reportingUnit =
          ResultGroupMetricSpecKt.reportingUnitMetricSetSpec {
            cumulative = ResultGroupMetricSpecKt.basicMetricSetSpec { reach = true }
          }
      }
    }

  companion object {
    private const val PARENT = "measurementConsumers/consumer"
    private const val MODEL_LINE = "modelProviders/provider/modelSuites/suite/modelLines/line"
    private const val DEVICE_FILTER = "common.device_type == 1 && common.media_type == 1"
    private val START = LocalDate.of(2026, 4, 1)
    private val END = START.plusDays(90)
    private val EVENT_GROUPS =
      listOf(
        eventGroup {
          cmmsDataProvider = "dataProviders/one"
          cmmsEventGroup = "dataProviders/one/eventGroups/one"
          eventGroupReferenceId = "ad_group-one"
        },
        eventGroup {
          cmmsDataProvider = "dataProviders/one"
          cmmsEventGroup = "dataProviders/one/eventGroups/two"
          eventGroupReferenceId = "ad_group-two"
        },
        eventGroup {
          cmmsDataProvider = "dataProviders/two"
          cmmsEventGroup = "dataProviders/two/eventGroups/three"
          eventGroupReferenceId = "ad_group-three"
        },
      )
    private val REFERENCE_IDS = EVENT_GROUPS.map { it.eventGroupReferenceId }.toSet()
  }
}
