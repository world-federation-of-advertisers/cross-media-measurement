/*
 * Copyright 2024 The Cross-Media Measurement Authors
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

import com.google.common.truth.extensions.proto.ProtoTruth.assertThat
import com.google.protobuf.util.JsonFormat
import com.google.type.DayOfWeek
import com.google.type.date
import com.google.type.dateTime
import com.google.type.timeZone
import io.grpc.StatusException
import java.time.Duration
import java.util.logging.Logger
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.filter
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.time.delay
import okhttp3.HttpUrl
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody.Companion.toRequestBody
import org.wfanet.measurement.api.v2alpha.DataProvider
import org.wfanet.measurement.api.v2alpha.DataProvidersGrpcKt
import org.wfanet.measurement.api.v2alpha.MeasurementConsumerKey
import org.wfanet.measurement.api.v2alpha.getDataProviderRequest
import org.wfanet.measurement.common.ExponentialBackoff
import org.wfanet.measurement.common.api.grpc.ResourceList
import org.wfanet.measurement.common.api.grpc.listResources
import org.wfanet.measurement.common.coerceAtMost
import org.wfanet.measurement.loadtest.config.TestIdentifiers
import org.wfanet.measurement.reporting.service.api.v2alpha.BasicReportKey
import org.wfanet.measurement.reporting.service.api.v2alpha.ReportingSetKey
import org.wfanet.measurement.reporting.v2alpha.BasicReport
import org.wfanet.measurement.reporting.v2alpha.EventGroup
import org.wfanet.measurement.reporting.v2alpha.EventGroupsGrpcKt
import org.wfanet.measurement.reporting.v2alpha.ReportingSet
import org.wfanet.measurement.reporting.v2alpha.ReportingSetKt
import org.wfanet.measurement.reporting.v2alpha.ReportingSetsGrpcKt
import org.wfanet.measurement.reporting.v2alpha.ResultGroupMetricSpecKt
import org.wfanet.measurement.reporting.v2alpha.basicReport
import org.wfanet.measurement.reporting.v2alpha.copy
import org.wfanet.measurement.reporting.v2alpha.createReportingSetRequest
import org.wfanet.measurement.reporting.v2alpha.dimensionSpec
import org.wfanet.measurement.reporting.v2alpha.listEventGroupsRequest
import org.wfanet.measurement.reporting.v2alpha.metricFrequencySpec
import org.wfanet.measurement.reporting.v2alpha.reportingInterval
import org.wfanet.measurement.reporting.v2alpha.reportingSet
import org.wfanet.measurement.reporting.v2alpha.reportingUnit
import org.wfanet.measurement.reporting.v2alpha.resultGroupMetricSpec
import org.wfanet.measurement.reporting.v2alpha.resultGroupSpec

/** Simulator for Reporting operations on the Reporting public API. */
class ReportingUserSimulator(
  private val measurementConsumerName: String,
  private val dataProvidersClient: DataProvidersGrpcKt.DataProvidersCoroutineStub,
  private val eventGroupsClient: EventGroupsGrpcKt.EventGroupsCoroutineStub,
  private val reportingSetsClient: ReportingSetsGrpcKt.ReportingSetsCoroutineStub,
  private val okHttpReportingClient: OkHttpClient,
  private val reportingGatewayScheme: String = "https",
  private val reportingGatewayHost: String,
  private val reportingGatewayPort: Int = 443,
  private val getReportingAccessToken: () -> String,
  private val modelLineName: String,
  private val initialResultPollingDelay: Duration = Duration.ofSeconds(1),
  private val maximumResultPollingDelay: Duration = Duration.ofMinutes(1),
) {
  private val dataProviderByName: MutableMap<String, DataProvider> = mutableMapOf()

  suspend fun testBasicReport(runId: String) {
    logger.info("Creating Basic Report...")

    val eventGroup = getEventGroup()
    val campaignGroup = createPrimitiveReportingSet(eventGroup, runId, isCampaignGroup = true)

    val basicReportKey =
      BasicReportKey(
        cmmsMeasurementConsumerId =
          MeasurementConsumerKey.fromName(measurementConsumerName)!!.measurementConsumerId,
        basicReportId = "basic-report-$runId",
      )

    val basicReport = basicReport {
      title = "title"
      this.campaignGroup = campaignGroup.name
      campaignGroupDisplayName = campaignGroup.displayName
      modelLine = modelLineName
      reportingInterval = reportingInterval {
        reportStart = dateTime {
          year = 2021
          month = 3
          day = 14
          hours = 17
          timeZone = timeZone { id = "America/Los_Angeles" }
        }
        reportEnd = date {
          year = 2021
          month = 3
          day = 15
        }
      }
      resultGroupSpecs += resultGroupSpec {
        title = "title"
        reportingUnit = reportingUnit { components += eventGroup.cmmsDataProvider }
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
                  kPlusReach = 5
                }
            }
        }
      }
    }

    val createBasicReportUrl =
      HttpUrl.Builder()
        .scheme(reportingGatewayScheme)
        .host(reportingGatewayHost)
        .port(reportingGatewayPort)
        .addPathSegments("v2alpha/${measurementConsumerName}/basicReports")
        .addQueryParameter("basic_report_id", basicReportKey.basicReportId)
        .build()

    val accessToken = getReportingAccessToken()

    val createBasicReportRequest =
      Request.Builder()
        .url(createBasicReportUrl)
        .post(JsonFormat.printer().print(basicReport).toRequestBody())
        .header("Content-Type", "application/json; charset=utf-8")
        .header("Authorization", "Bearer $accessToken")
        .build()

    val createdBasicReportJson: String =
      try {
        val response = okHttpReportingClient.newCall(createBasicReportRequest).execute()

        val responseBody = response.body!!.string()
        if (!response.isSuccessful) {
          throw Exception(
            "Error creating Basic Report: ${response.code} ${response.message} $responseBody"
          )
        }

        responseBody
      } catch (e: StatusException) {
        throw Exception("Error creating Basic Report", e)
      }

    logger.info("Basic Report created")

    val createdBasicReport =
      BasicReport.newBuilder()
        .also { JsonFormat.parser().ignoringUnknownFields().merge(createdBasicReportJson, it) }
        .build()

    val getBasicReportUrl =
      HttpUrl.Builder()
        .scheme("https")
        .host(reportingGatewayHost)
        .port(reportingGatewayPort)
        .addPathSegments("v2alpha/${basicReportKey.toName()}")
        .build()

    val getBasicReportRequest =
      Request.Builder()
        .url(getBasicReportUrl)
        .get()
        .header("Authorization", "Bearer $accessToken")
        .build()

    val retrievedCompletedBasicReport = pollForCompletedBasicReport(getBasicReportRequest)

    assertThat(retrievedCompletedBasicReport)
      .ignoringFields(
        BasicReport.CREATE_TIME_FIELD_NUMBER,
        BasicReport.EFFECTIVE_IMPRESSION_QUALIFICATION_FILTERS_FIELD_NUMBER,
        BasicReport.RESULT_GROUPS_FIELD_NUMBER,
      )
      .isEqualTo(
        basicReport.copy {
          name = basicReportKey.toName()
          state = BasicReport.State.SUCCEEDED
          effectiveModelLine = retrievedCompletedBasicReport.effectiveModelLine
        }
      )
    assertThat(retrievedCompletedBasicReport.createTime).isEqualTo(createdBasicReport.createTime)
    assertThat(retrievedCompletedBasicReport.effectiveImpressionQualificationFiltersList)
      .isNotEmpty()
    assertThat(retrievedCompletedBasicReport.resultGroupsList).isNotEmpty()

    logger.info("BasicReport ${retrievedCompletedBasicReport.name} is Completed.")
  }

  private suspend fun getEventGroup(): EventGroup {
    val resourceLists: Flow<ResourceList<EventGroup, String>> =
      eventGroupsClient.listResources(1, "") { pageToken: String, remaining: Int ->
        val listEventGroupsResponse =
          eventGroupsClient.listEventGroups(
            listEventGroupsRequest {
              parent = measurementConsumerName
              pageSize = remaining
              this.pageToken = pageToken
            }
          )

        val validEventGroup: EventGroup? =
          listEventGroupsResponse.eventGroupsList.firstOrNull { eventGroup ->
            eventGroup.eventGroupReferenceId.startsWith(
              TestIdentifiers.SIMULATOR_EVENT_GROUP_REFERENCE_ID_PREFIX
            ) &&
              dataProviderByName
                .getOrPut(eventGroup.cmmsDataProvider) {
                  getDataProvider(eventGroup.cmmsDataProvider)
                }
                .capabilities
                .honestMajorityShareShuffleSupported
          }

        val resources = validEventGroup?.let { listOf(it) } ?: emptyList()

        ResourceList(resources, listEventGroupsResponse.nextPageToken)
      }

    return resourceLists.filter { it.resources.isNotEmpty() }.first().resources.first()
  }

  private suspend fun getDataProvider(dataProviderName: String): DataProvider {
    try {
      return dataProvidersClient.getDataProvider(getDataProviderRequest { name = dataProviderName })
    } catch (e: StatusException) {
      throw Exception("Error getting DataProvider $dataProviderName", e)
    }
  }

  private suspend fun createPrimitiveReportingSet(
    eventGroup: EventGroup,
    runId: String,
    isCampaignGroup: Boolean = false,
  ): ReportingSet {
    val reportingSetId = "a-$runId"

    val primitiveReportingSet = reportingSet {
      primitive = ReportingSetKt.primitive { cmmsEventGroups += eventGroup.cmmsEventGroup }
      if (isCampaignGroup) {
        campaignGroup =
          ReportingSetKey(
              MeasurementConsumerKey.fromName(measurementConsumerName)!!,
              reportingSetId,
            )
            .toName()
      }
    }

    try {
      return reportingSetsClient.createReportingSet(
        createReportingSetRequest {
          parent = measurementConsumerName
          reportingSet = primitiveReportingSet
          this.reportingSetId = "a-$runId"
        }
      )
    } catch (e: StatusException) {
      throw Exception("Error creating ReportingSet", e)
    }
  }

  private suspend fun pollForCompletedBasicReport(getBasicReportRequest: Request): BasicReport {
    val backoff =
      ExponentialBackoff(initialDelay = initialResultPollingDelay, randomnessFactor = 0.0)
    var attempt = 1
    while (true) {
      val retrievedBasicReport =
        try {
          val retrievedBasicReportJson: String =
            try {
              val response = okHttpReportingClient.newCall(getBasicReportRequest).execute()

              val responseBody = response.body!!.string()
              if (!response.isSuccessful) {
                throw Exception(
                  "Error retrieving Basic Report: ${response.code} ${response.message} $responseBody"
                )
              }

              responseBody
            } catch (e: StatusException) {
              throw Exception("Error retrieving Basic Report", e)
            }

          BasicReport.newBuilder()
            .also {
              JsonFormat.parser().ignoringUnknownFields().merge(retrievedBasicReportJson, it)
            }
            .build()
        } catch (e: StatusException) {
          throw Exception("Error getting BasicReport", e)
        }

      @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA") // Proto enum fields are never null.
      when (retrievedBasicReport.state) {
        BasicReport.State.SUCCEEDED,
        BasicReport.State.FAILED,
        BasicReport.State.INVALID -> return retrievedBasicReport
        BasicReport.State.RUNNING -> {
          val resultPollingDelay =
            backoff.durationForAttempt(attempt).coerceAtMost(maximumResultPollingDelay)
          logger.info {
            "BasicReport not completed yet. Waiting for ${resultPollingDelay.seconds} seconds."
          }
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
  }
}
