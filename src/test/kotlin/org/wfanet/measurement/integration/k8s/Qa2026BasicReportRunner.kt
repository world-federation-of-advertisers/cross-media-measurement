// Copyright 2026 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.integration.k8s

import com.google.common.truth.Truth.assertThat
import com.google.protobuf.util.JsonFormat
import com.google.type.DayOfWeek
import com.google.type.date
import com.google.type.dateTime
import com.google.type.timeZone
import io.grpc.StatusException
import java.time.Duration
import java.time.LocalDate
import java.util.logging.Logger
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.flattenConcat
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.withTimeoutOrNull
import okhttp3.HttpUrl
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody.Companion.toRequestBody
import org.wfanet.measurement.api.v2alpha.EventGroupKey
import org.wfanet.measurement.api.v2alpha.MeasurementConsumerKey
import org.wfanet.measurement.common.ExponentialBackoff
import org.wfanet.measurement.common.api.grpc.ResourceList
import org.wfanet.measurement.common.api.grpc.listResources
import org.wfanet.measurement.common.coerceAtMost
import org.wfanet.measurement.reporting.service.api.v2alpha.BasicReportKey
import org.wfanet.measurement.reporting.service.api.v2alpha.ImpressionQualificationFilterKey
import org.wfanet.measurement.reporting.service.api.v2alpha.ReportingSetKey
import org.wfanet.measurement.reporting.v2alpha.BasicReport
import org.wfanet.measurement.reporting.v2alpha.EventGroupsGrpcKt.EventGroupsCoroutineStub
import org.wfanet.measurement.reporting.v2alpha.MediaType
import org.wfanet.measurement.reporting.v2alpha.ReportingImpressionQualificationFilterKt
import org.wfanet.measurement.reporting.v2alpha.ReportingSet
import org.wfanet.measurement.reporting.v2alpha.ReportingSetKt
import org.wfanet.measurement.reporting.v2alpha.ReportingSetsGrpcKt.ReportingSetsCoroutineStub
import org.wfanet.measurement.reporting.v2alpha.ResultGroup
import org.wfanet.measurement.reporting.v2alpha.ResultGroupMetricSpecKt
import org.wfanet.measurement.reporting.v2alpha.basicReport
import org.wfanet.measurement.reporting.v2alpha.createReportingSetRequest
import org.wfanet.measurement.reporting.v2alpha.dimensionSpec
import org.wfanet.measurement.reporting.v2alpha.impressionQualificationFilterSpec
import org.wfanet.measurement.reporting.v2alpha.listEventGroupsRequest
import org.wfanet.measurement.reporting.v2alpha.metricFrequencySpec
import org.wfanet.measurement.reporting.v2alpha.reportingImpressionQualificationFilter
import org.wfanet.measurement.reporting.v2alpha.reportingInterval
import org.wfanet.measurement.reporting.v2alpha.reportingSet
import org.wfanet.measurement.reporting.v2alpha.reportingUnit
import org.wfanet.measurement.reporting.v2alpha.resultGroupMetricSpec
import org.wfanet.measurement.reporting.v2alpha.resultGroupSpec

/**
 * Creates a [BasicReport] over the QA 2026 dataset broken down by media type and impression
 * qualification filter, and validates its results.
 *
 * The reporting unit is derived from the resolved EventGroups' parent DataProviders, so it always
 * matches the data the campaign group enumerates.
 *
 * @property eventGroupReferenceIds EventGroups the campaign group enumerates
 * @property modelLineName ModelLine to pin the report to
 */
class Qa2026BasicReportRunner(
  private val measurementConsumerName: String,
  private val reportingSetsClient: ReportingSetsCoroutineStub,
  private val eventGroupsClient: EventGroupsCoroutineStub,
  private val eventGroupReferenceIds: Set<String>,
  private val modelLineName: String,
  private val okHttpReportingClient: OkHttpClient,
  private val reportingGatewayScheme: String,
  private val reportingGatewayHost: String,
  private val reportingGatewayPort: Int,
  private val getReportingAccessToken: () -> String,
  private val reportStart: LocalDate,
  private val reportEnd: LocalDate,
  private val initialResultPollingDelay: Duration = Duration.ofSeconds(5),
  private val maximumResultPollingDelay: Duration = Duration.ofMinutes(1),
  private val completionTimeout: Duration = Duration.ofMinutes(30),
) {

  suspend fun run(runId: String) {
    require(eventGroupReferenceIds.isNotEmpty()) { "No QA 2026 EventGroups to report on" }

    val measurementConsumerKey =
      checkNotNull(MeasurementConsumerKey.fromName(measurementConsumerName))
    val basicReportKey =
      BasicReportKey(
        cmmsMeasurementConsumerId = measurementConsumerKey.measurementConsumerId,
        basicReportId = "qa2026-media-iqf-$runId",
      )

    val cmmsEventGroupNames = resolveEventGroups()
    val dataProviderNames =
      cmmsEventGroupNames
        .map { checkNotNull(EventGroupKey.fromName(it)) { "Unparseable EventGroup $it" }.parentKey }
        .distinct()
        .map { it.toName() }
        .sorted()
    check(dataProviderNames.size >= 2) {
      "The cross-publisher result group needs at least two DataProviders, got $dataProviderNames"
    }

    val campaignGroup = createCampaignGroup(measurementConsumerKey, runId, cmmsEventGroupNames)
    val request = buildBasicReport(campaignGroup.name, dataProviderNames)
    val created = createBasicReport(basicReportKey, request)
    logger.info("Created ${created.name}; polling for completion")
    val completed = pollForCompletedBasicReport(basicReportKey)

    assertThat(completed.state).isEqualTo(BasicReport.State.SUCCEEDED)
    assertThat(completed.effectiveModelLine).isEqualTo(modelLineName)
    assertResultGroups(completed)
    logger.info("${completed.name} validated")
  }

  /**
   * Resolves [eventGroupReferenceIds] to CMMS EventGroup resource names.
   *
   * @throws IllegalStateException if any reference ID has no EventGroup
   */
  @OptIn(ExperimentalCoroutinesApi::class) // For `flattenConcat`.
  private suspend fun resolveEventGroups(): List<String> {
    val namesByReferenceId: Map<String, String> =
      eventGroupsClient
        .listResources { pageToken: String ->
          val response =
            try {
              eventGroupsClient.listEventGroups(
                listEventGroupsRequest {
                  parent = measurementConsumerName
                  this.pageToken = pageToken
                  pageSize = EVENT_GROUP_PAGE_SIZE
                }
              )
            } catch (e: StatusException) {
              throw Exception("Error listing EventGroups for $measurementConsumerName", e)
            }
          ResourceList(response.eventGroupsList, response.nextPageToken)
        }
        .flattenConcat()
        .toList()
        .filter { it.eventGroupReferenceId in eventGroupReferenceIds }
        .associate { it.eventGroupReferenceId to it.cmmsEventGroup }

    val missing = eventGroupReferenceIds - namesByReferenceId.keys
    check(missing.isEmpty()) { "QA 2026 EventGroups not found for reference IDs $missing" }
    return namesByReferenceId.values.sorted()
  }

  /** Creates the campaign group enumerating every QA 2026 EventGroup being reported on. */
  private suspend fun createCampaignGroup(
    measurementConsumerKey: MeasurementConsumerKey,
    runId: String,
    cmmsEventGroupNames: List<String>,
  ): ReportingSet {
    val reportingSetId = "qa2026-$runId"
    val request = createReportingSetRequest {
      parent = measurementConsumerName
      this.reportingSetId = reportingSetId
      reportingSet = reportingSet {
        displayName = CAMPAIGN_GROUP_DISPLAY_NAME
        primitive = ReportingSetKt.primitive { cmmsEventGroups += cmmsEventGroupNames }
        campaignGroup = ReportingSetKey(measurementConsumerKey, reportingSetId).toName()
      }
    }
    return try {
      reportingSetsClient.createReportingSet(request)
    } catch (e: StatusException) {
      throw Exception("Error creating QA 2026 campaign group", e)
    }
  }

  private fun buildBasicReport(campaignGroupName: String, dataProviderNames: List<String>) =
    basicReport {
    title = "QA 2026 media type and IQF breakdown"
    campaignGroup = campaignGroupName
    campaignGroupDisplayName = CAMPAIGN_GROUP_DISPLAY_NAME
    modelLine = modelLineName
    reportingInterval = reportingInterval {
      this.reportStart = dateTime {
        year = this@Qa2026BasicReportRunner.reportStart.year
        month = this@Qa2026BasicReportRunner.reportStart.monthValue
        day = this@Qa2026BasicReportRunner.reportStart.dayOfMonth
        timeZone = timeZone { id = ZONE_ID }
      }
      this.reportEnd = date {
        year = this@Qa2026BasicReportRunner.reportEnd.year
        month = this@Qa2026BasicReportRunner.reportEnd.monthValue
        day = this@Qa2026BasicReportRunner.reportEnd.dayOfMonth
      }
    }

    // Media type is neither groupable nor filterable, so results split by media type only through
    // one Result per filter: `mrc` covers display, the custom filter video, and `ami` the total.
    // Only one custom filter is permitted per report.
    impressionQualificationFilters += reportingImpressionQualificationFilter {
      impressionQualificationFilter = ImpressionQualificationFilterKey(AMI_FILTER_ID).toName()
    }
    impressionQualificationFilters += reportingImpressionQualificationFilter {
      impressionQualificationFilter = ImpressionQualificationFilterKey(MRC_FILTER_ID).toName()
    }
    impressionQualificationFilters += reportingImpressionQualificationFilter {
      custom =
        ReportingImpressionQualificationFilterKt.customImpressionQualificationFilterSpec {
          filterSpec += impressionQualificationFilterSpec { mediaType = MediaType.VIDEO }
        }
    }

    // Single EDP: the media-type split comes from the report-level filters above.
    resultGroupSpecs += resultGroupSpec {
      title = SINGLE_EDP_GROUP_TITLE
      reportingUnit = reportingUnit { components += dataProviderNames.first() }
      metricFrequency = metricFrequencySpec { weekly = DayOfWeek.MONDAY }
      dimensionSpec = dimensionSpec {}
      resultGroupMetricSpec = resultGroupMetricSpec {
        populationSize = true
        component =
          ResultGroupMetricSpecKt.componentMetricSetSpec {
            nonCumulative =
              ResultGroupMetricSpecKt.basicMetricSetSpec {
                reach = true
                impressions = true
                averageFrequency = true
                kPlusReach = K_PLUS_REACH
              }
          }
      }
    }

    // Every provisioned EDP, so the filtered results are exercised across a union.
    resultGroupSpecs += resultGroupSpec {
      title = CROSS_PUB_GROUP_TITLE
      reportingUnit = reportingUnit { components += dataProviderNames }
      metricFrequency = metricFrequencySpec { weekly = DayOfWeek.MONDAY }
      dimensionSpec = dimensionSpec {}
      resultGroupMetricSpec = resultGroupMetricSpec {
        populationSize = true
        reportingUnit =
          ResultGroupMetricSpecKt.reportingUnitMetricSetSpec {
            nonCumulative =
              ResultGroupMetricSpecKt.basicMetricSetSpec {
                reach = true
                impressions = true
                averageFrequency = true
                kPlusReach = K_PLUS_REACH
              }
          }
        component =
          ResultGroupMetricSpecKt.componentMetricSetSpec {
            nonCumulative =
              ResultGroupMetricSpecKt.basicMetricSetSpec {
                reach = true
                impressions = true
                averageFrequency = true
              }
          }
      }
    }
  }

  /** Checks that every line item carries data and that filtered reach is bounded by unfiltered. */
  private fun assertResultGroups(report: BasicReport) {
    assertThat(report.resultGroupsList.map { it.title })
      .containsExactly(SINGLE_EDP_GROUP_TITLE, CROSS_PUB_GROUP_TITLE)

    for (resultGroup in report.resultGroupsList) {
      assertWithGroup(resultGroup, "has a result per impression qualification filter") {
        assertThat(resultGroup.resultsList).hasSize(EXPECTED_FILTER_COUNT)
      }

      val reachByFilter: Map<String, Long> =
        resultGroup.resultsList.associate { result ->
          filterLabel(result) to result.metricSet.reachOf(resultGroup.title)
        }

      for ((label, reach) in reachByFilter) {
        assertWithGroup(resultGroup, "$label reach is non-zero") {
          assertThat(reach).isGreaterThan(0L)
        }
      }

      val amiReach = reachByFilter.getValue(AMI_FILTER_ID)
      for (label in listOf(MRC_FILTER_ID, CUSTOM_FILTER_LABEL)) {
        assertWithGroup(resultGroup, "$label reach does not exceed $AMI_FILTER_ID reach") {
          assertThat(reachByFilter.getValue(label)).isAtMost(amiReach)
        }
      }
    }

    // The union over every EDP reaches at least as many people as the first EDP alone.
    val singleEdpAmi = amiReachOf(report, SINGLE_EDP_GROUP_TITLE)
    val crossPubAmi = amiReachOf(report, CROSS_PUB_GROUP_TITLE)
    assertThat(crossPubAmi).isAtLeast(singleEdpAmi)
  }

  private fun amiReachOf(report: BasicReport, groupTitle: String): Long {
    val resultGroup = report.resultGroupsList.single { it.title == groupTitle }
    val result = resultGroup.resultsList.single { filterLabel(it) == AMI_FILTER_ID }
    return result.metricSet.reachOf(groupTitle)
  }

  /**
   * The single-EDP group requests component metrics and the cross-publisher group requests
   * reporting-unit metrics, so the reach lives in a different field for each.
   */
  private fun ResultGroup.MetricSet.reachOf(groupTitle: String): Long =
    if (groupTitle == CROSS_PUB_GROUP_TITLE) {
      reportingUnit.nonCumulative.reach
    } else {
      componentsList.single().value.nonCumulative.reach
    }

  private fun filterLabel(result: ResultGroup.Result): String {
    val filter = result.metadata.filter
    return if (filter.hasCustom()) {
      CUSTOM_FILTER_LABEL
    } else {
      checkNotNull(ImpressionQualificationFilterKey.fromName(filter.impressionQualificationFilter)) {
          "Unparseable impression qualification filter ${filter.impressionQualificationFilter}"
        }
        .impressionQualificationFilterId
    }
  }

  private fun assertWithGroup(resultGroup: ResultGroup, what: String, block: () -> Unit) {
    try {
      block()
    } catch (e: AssertionError) {
      throw AssertionError("Result group '${resultGroup.title}': $what", e)
    }
  }

  private fun createBasicReport(key: BasicReportKey, report: BasicReport): BasicReport {
    val url =
      HttpUrl.Builder()
        .scheme(reportingGatewayScheme)
        .host(reportingGatewayHost)
        .port(reportingGatewayPort)
        .addPathSegments("v2alpha/$measurementConsumerName/basicReports")
        .addQueryParameter("basic_report_id", key.basicReportId)
        .build()
    val request =
      Request.Builder()
        .url(url)
        .post(JsonFormat.printer().print(report).toRequestBody())
        .header("Content-Type", "application/json; charset=utf-8")
        .header("Authorization", "Bearer ${getReportingAccessToken()}")
        .build()

    val response = okHttpReportingClient.newCall(request).execute()
    val body = response.body!!.string()
    if (!response.isSuccessful) {
      throw Exception("Error creating BasicReport: ${response.code} ${response.message} $body")
    }
    return BasicReport.newBuilder()
      .also { JsonFormat.parser().ignoringUnknownFields().merge(body, it) }
      .build()
  }

  private suspend fun pollForCompletedBasicReport(key: BasicReportKey): BasicReport =
    withTimeoutOrNull(completionTimeout) { pollUntilCompleted(key) }
      ?: throw Exception("BasicReport ${key.toName()} still RUNNING after $completionTimeout")

  private suspend fun pollUntilCompleted(key: BasicReportKey): BasicReport {
    val url =
      HttpUrl.Builder()
        .scheme(reportingGatewayScheme)
        .host(reportingGatewayHost)
        .port(reportingGatewayPort)
        .addPathSegments("v2alpha/${key.toName()}")
        .build()

    val backoff =
      ExponentialBackoff(initialDelay = initialResultPollingDelay, randomnessFactor = 0.0)
    var attempt = 1
    while (true) {
      val request =
        Request.Builder()
          .url(url)
          .get()
          .header("Authorization", "Bearer ${getReportingAccessToken()}")
          .build()
      val response = okHttpReportingClient.newCall(request).execute()
      val body = response.body!!.string()
      if (!response.isSuccessful) {
        throw Exception("Error getting BasicReport: ${response.code} ${response.message} $body")
      }
      val report =
        BasicReport.newBuilder()
          .also { JsonFormat.parser().ignoringUnknownFields().merge(body, it) }
          .build()

      @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA") // Proto enum fields are never null.
      when (report.state) {
        BasicReport.State.SUCCEEDED,
        BasicReport.State.FAILED,
        BasicReport.State.INVALID -> return report
        BasicReport.State.RUNNING -> {
          val resultPollingDelay =
            backoff.durationForAttempt(attempt).coerceAtMost(maximumResultPollingDelay)
          logger.info("BasicReport ${key.toName()} not completed. Waiting for $resultPollingDelay.")
          delay(resultPollingDelay)
          attempt++
        }
        BasicReport.State.UNRECOGNIZED,
        BasicReport.State.STATE_UNSPECIFIED -> throw Exception("Unknown BasicReport state")
      }
    }
  }

  companion object {
    private val logger: Logger = Logger.getLogger(this::class.java.name)

    private const val ZONE_ID = "UTC"
    private const val AMI_FILTER_ID = "ami"
    private const val MRC_FILTER_ID = "mrc"
    private const val CUSTOM_FILTER_LABEL = "custom-video"
    private const val EXPECTED_FILTER_COUNT = 3
    private const val K_PLUS_REACH = 5
    private const val EVENT_GROUP_PAGE_SIZE = 500

    private const val SINGLE_EDP_GROUP_TITLE = "Single EDP by media type"
    private const val CROSS_PUB_GROUP_TITLE = "Cross-publisher filtered"
    private const val CAMPAIGN_GROUP_DISPLAY_NAME = "QA 2026 campaign group"
  }
}
