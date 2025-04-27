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

import com.google.common.truth.Truth.assertThat
import com.google.common.truth.extensions.proto.ProtoTruth
import com.google.protobuf.timestamp
import com.google.protobuf.util.JsonFormat
import com.google.type.DayOfWeek
import com.google.type.date
import com.google.type.dateTime
import com.google.type.timeZone
import io.grpc.StatusException
import java.time.Duration
import java.util.logging.Logger
import kotlinx.coroutines.time.delay
import okhttp3.HttpUrl
import okhttp3.OkHttpClient
import okhttp3.Request
import org.wfanet.measurement.api.v2alpha.DataProvider
import org.wfanet.measurement.api.v2alpha.DataProviderKey
import org.wfanet.measurement.api.v2alpha.DataProvidersGrpcKt
import org.wfanet.measurement.api.v2alpha.MeasurementConsumerKey
import org.wfanet.measurement.api.v2alpha.getDataProviderRequest
import org.wfanet.measurement.common.ExponentialBackoff
import org.wfanet.measurement.common.coerceAtMost
import org.wfanet.measurement.internal.reporting.v2.BasicReportsGrpcKt as InternalBasicReportsGrpcKt
import org.wfanet.measurement.internal.reporting.v2.EventTemplateFieldKt as InternalEventTemplateFieldKt
import org.wfanet.measurement.internal.reporting.v2.ImpressionQualificationFilterSpec as InternalImpressionQualificationFilterSpec
import org.wfanet.measurement.internal.reporting.v2.ResultGroupKt as InternalResultGroupKt
import org.wfanet.measurement.internal.reporting.v2.basicReport as internalBasicReport
import org.wfanet.measurement.internal.reporting.v2.basicReportDetails
import org.wfanet.measurement.internal.reporting.v2.basicReportResultDetails
import org.wfanet.measurement.internal.reporting.v2.eventFilter as internalEventFilter
import org.wfanet.measurement.internal.reporting.v2.eventTemplateField as internalEventTemplateField
import org.wfanet.measurement.internal.reporting.v2.impressionQualificationFilterSpec as internalImpressionQualificationFilterSpec
import org.wfanet.measurement.internal.reporting.v2.insertBasicReportRequest
import org.wfanet.measurement.internal.reporting.v2.metricFrequencySpec as internalMetricFrequencySpec
import org.wfanet.measurement.internal.reporting.v2.reportingImpressionQualificationFilter as internalReportingImpressionQualificationFilter
import org.wfanet.measurement.internal.reporting.v2.reportingInterval as internalReportingInterval
import org.wfanet.measurement.internal.reporting.v2.resultGroup as internalResultGroup
import org.wfanet.measurement.loadtest.config.TestIdentifiers
import org.wfanet.measurement.reporting.service.api.v2alpha.BasicReportKey
import org.wfanet.measurement.reporting.service.api.v2alpha.EventGroupKey
import org.wfanet.measurement.reporting.service.api.v2alpha.ReportingSetKey
import org.wfanet.measurement.reporting.v2alpha.BasicReport
import org.wfanet.measurement.reporting.v2alpha.EventGroup
import org.wfanet.measurement.reporting.v2alpha.EventGroupsGrpcKt
import org.wfanet.measurement.reporting.v2alpha.EventTemplateFieldKt
import org.wfanet.measurement.reporting.v2alpha.ListEventGroupsResponse
import org.wfanet.measurement.reporting.v2alpha.MediaType
import org.wfanet.measurement.reporting.v2alpha.MetricCalculationSpec
import org.wfanet.measurement.reporting.v2alpha.MetricCalculationSpecKt
import org.wfanet.measurement.reporting.v2alpha.MetricCalculationSpecsGrpcKt
import org.wfanet.measurement.reporting.v2alpha.MetricSpecKt
import org.wfanet.measurement.reporting.v2alpha.Report
import org.wfanet.measurement.reporting.v2alpha.ReportKt
import org.wfanet.measurement.reporting.v2alpha.ReportingImpressionQualificationFilterKt
import org.wfanet.measurement.reporting.v2alpha.ReportingSet
import org.wfanet.measurement.reporting.v2alpha.ReportingSetKt
import org.wfanet.measurement.reporting.v2alpha.ReportingSetsGrpcKt
import org.wfanet.measurement.reporting.v2alpha.ReportsGrpcKt
import org.wfanet.measurement.reporting.v2alpha.ResultGroupKt
import org.wfanet.measurement.reporting.v2alpha.basicReport
import org.wfanet.measurement.reporting.v2alpha.createMetricCalculationSpecRequest
import org.wfanet.measurement.reporting.v2alpha.createReportRequest
import org.wfanet.measurement.reporting.v2alpha.createReportingSetRequest
import org.wfanet.measurement.reporting.v2alpha.eventFilter
import org.wfanet.measurement.reporting.v2alpha.eventTemplateField
import org.wfanet.measurement.reporting.v2alpha.getReportRequest
import org.wfanet.measurement.reporting.v2alpha.impressionQualificationFilterSpec
import org.wfanet.measurement.reporting.v2alpha.listEventGroupsRequest
import org.wfanet.measurement.reporting.v2alpha.metricCalculationSpec
import org.wfanet.measurement.reporting.v2alpha.metricFrequencySpec
import org.wfanet.measurement.reporting.v2alpha.metricSpec
import org.wfanet.measurement.reporting.v2alpha.report
import org.wfanet.measurement.reporting.v2alpha.reportingImpressionQualificationFilter
import org.wfanet.measurement.reporting.v2alpha.reportingInterval
import org.wfanet.measurement.reporting.v2alpha.reportingSet
import org.wfanet.measurement.reporting.v2alpha.resultGroup

/** Simulator for Reporting operations on the Reporting public API. */
class ReportingUserSimulator(
  private val measurementConsumerName: String,
  private val dataProvidersClient: DataProvidersGrpcKt.DataProvidersCoroutineStub,
  private val eventGroupsClient: EventGroupsGrpcKt.EventGroupsCoroutineStub,
  private val reportingSetsClient: ReportingSetsGrpcKt.ReportingSetsCoroutineStub,
  private val metricCalculationSpecsClient:
    MetricCalculationSpecsGrpcKt.MetricCalculationSpecsCoroutineStub,
  private val reportsClient: ReportsGrpcKt.ReportsCoroutineStub,
  private val okHttpReportingClient: OkHttpClient,
  private val reportingGatewayHost: String,
  private val reportingGatewayPort: Int,
  private val reportingAccessToken: String,
  private val internalBasicReportsClient: InternalBasicReportsGrpcKt.BasicReportsCoroutineStub,
  private val initialResultPollingDelay: Duration = Duration.ofSeconds(1),
  private val maximumResultPollingDelay: Duration = Duration.ofMinutes(1),
) {
  suspend fun testCreateReport(runId: String) {
    logger.info("Creating report...")

    val eventGroup =
      listEventGroups()
        .filter {
          it.eventGroupReferenceId.startsWith(
            TestIdentifiers.SIMULATOR_EVENT_GROUP_REFERENCE_ID_PREFIX
          )
        }
        .firstOrNull {
          getDataProvider(it.cmmsDataProvider).capabilities.honestMajorityShareShuffleSupported
        } ?: listEventGroups().first()
    val createdPrimitiveReportingSet = createPrimitiveReportingSet(eventGroup, runId)
    val createdMetricCalculationSpec = createMetricCalculationSpec(runId)

    val report = report {
      reportingMetricEntries +=
        ReportKt.reportingMetricEntry {
          key = createdPrimitiveReportingSet.name
          value =
            ReportKt.reportingMetricCalculationSpec {
              metricCalculationSpecs += createdMetricCalculationSpec.name
            }
        }
      reportingInterval =
        ReportKt.reportingInterval {
          reportStart = dateTime {
            year = 2024
            month = 1
            day = 3
            timeZone = timeZone { id = "America/Los_Angeles" }
          }
          reportEnd = date {
            year = 2024
            month = 1
            day = 4
          }
        }
    }

    val createdReport =
      try {
        reportsClient.createReport(
          createReportRequest {
            parent = measurementConsumerName
            this.report = report
            reportId = "a-$runId"
          }
        )
      } catch (e: StatusException) {
        throw Exception("Error creating Report", e)
      }

    val completedReport = pollForCompletedReport(createdReport.name)

    assertThat(completedReport.state).isEqualTo(Report.State.SUCCEEDED)
    logger.info("Report creation succeeded")
  }

  suspend fun testGetBasicReport(runId: String) {
    logger.info("Retrieving Basic Report...")

    val eventGroup =
      listEventGroups()
        .filter {
          it.eventGroupReferenceId.startsWith(
            TestIdentifiers.SIMULATOR_EVENT_GROUP_REFERENCE_ID_PREFIX
          )
        }
        .firstOrNull {
          getDataProvider(it.cmmsDataProvider).capabilities.honestMajorityShareShuffleSupported
        } ?: listEventGroups().first()

    val dataProvider =
      dataProvidersClient.getDataProvider(
        getDataProviderRequest { name = eventGroup.cmmsDataProvider }
      )

    val createdPrimitiveReportingSet = createPrimitiveReportingSet(eventGroup, runId)

    val dataProviderKey = DataProviderKey.fromName(dataProvider.name)
    val eventGroupKey = EventGroupKey.fromName(eventGroup.name)
    val reportingSetKey = ReportingSetKey.fromName(createdPrimitiveReportingSet.name)

    val basicReportKey =
      BasicReportKey(
        cmmsMeasurementConsumerId =
          MeasurementConsumerKey.fromName(measurementConsumerName)!!.measurementConsumerId,
        basicReportId = "basicReport-$runId",
      )

    val internalBasicReport = internalBasicReport {
      this.cmmsMeasurementConsumerId = basicReportKey.cmmsMeasurementConsumerId
      externalBasicReportId = basicReportKey.basicReportId
      externalCampaignGroupId = reportingSetKey!!.reportingSetId
      campaignGroupDisplayName = createdPrimitiveReportingSet.displayName
      details = basicReportDetails {
        title = "title"
        reportingInterval = internalReportingInterval {
          reportStart = dateTime { day = 3 }
          reportEnd = date { day = 5 }
        }
        impressionQualificationFilters += internalReportingImpressionQualificationFilter {
          filterSpecs += internalImpressionQualificationFilterSpec {
            mediaType = InternalImpressionQualificationFilterSpec.MediaType.VIDEO
            filters += internalEventFilter {
              terms += internalEventTemplateField {
                path = "common.age_group"
                value = InternalEventTemplateFieldKt.fieldValue { enumValue = "18_TO_35" }
              }
            }
          }
        }
      }

      resultDetails = basicReportResultDetails {
        resultGroups += internalResultGroup {
          title = "title"
          results +=
            InternalResultGroupKt.result {
              metadata =
                InternalResultGroupKt.metricMetadata {
                  reportingUnitSummary =
                    InternalResultGroupKt.MetricMetadataKt.reportingUnitSummary {
                      reportingUnitComponentSummary +=
                        InternalResultGroupKt.MetricMetadataKt.reportingUnitComponentSummary {
                          this.cmmsDataProviderId = dataProviderKey!!.dataProviderId
                          cmmsDataProviderDisplayName = dataProvider.displayName
                          eventGroupSummaries +=
                            InternalResultGroupKt.MetricMetadataKt.ReportingUnitComponentSummaryKt
                              .eventGroupSummary {
                                this.cmmsMeasurementConsumerId =
                                  basicReportKey.cmmsMeasurementConsumerId
                                cmmsEventGroupId = eventGroupKey!!.cmmsEventGroupId
                              }
                        }
                    }
                  nonCumulativeMetricStartTime = timestamp { seconds = 10 }
                  cumulativeMetricStartTime = timestamp { seconds = 12 }
                  metricEndTime = timestamp { seconds = 20 }
                  metricFrequencySpec = internalMetricFrequencySpec { weekly = DayOfWeek.MONDAY }
                  dimensionSpecSummary =
                    InternalResultGroupKt.MetricMetadataKt.dimensionSpecSummary {
                      grouping = internalEventTemplateField {
                        path = "common.gender"
                        value = InternalEventTemplateFieldKt.fieldValue { enumValue = "MALE" }
                      }
                      filters += internalEventFilter {
                        terms += internalEventTemplateField {
                          path = "common.age_group"
                          value = InternalEventTemplateFieldKt.fieldValue { enumValue = "18_TO_35" }
                        }
                      }
                    }
                  filter = internalReportingImpressionQualificationFilter {
                    filterSpecs += internalImpressionQualificationFilterSpec {
                      mediaType = InternalImpressionQualificationFilterSpec.MediaType.VIDEO
                      filters += internalEventFilter {
                        terms += internalEventTemplateField {
                          path = "common.age_group"
                          value = InternalEventTemplateFieldKt.fieldValue { enumValue = "18_TO_35" }
                        }
                      }
                    }
                  }
                }

              // Numbers below aren't actually calculated.
              metricSet =
                InternalResultGroupKt.metricSet {
                  populationSize = 1000
                  reportingUnit =
                    InternalResultGroupKt.MetricSetKt.reportingUnitMetricSet {
                      nonCumulative =
                        InternalResultGroupKt.MetricSetKt.basicMetricSet {
                          reach = 1
                          percentReach = 0.1f
                          kPlusReach += 1
                          kPlusReach += 2
                          percentKPlusReach += 0.1f
                          percentKPlusReach += 0.2f
                          averageFrequency = 0.1f
                          impressions = 1
                          grps = 0.1f
                        }
                      cumulative =
                        InternalResultGroupKt.MetricSetKt.basicMetricSet {
                          reach = 2
                          percentReach = 0.2f
                          kPlusReach += 2
                          kPlusReach += 4
                          percentKPlusReach += 0.2f
                          percentKPlusReach += 0.4f
                          averageFrequency = 0.2f
                          impressions = 2
                          grps = 0.2f
                        }
                      stackedIncrementalReach += 10
                      stackedIncrementalReach += 15
                    }
                  components +=
                    InternalResultGroupKt.MetricSetKt.dataProviderComponentMetricSetMapEntry {
                      key = dataProviderKey!!.dataProviderId
                      value =
                        InternalResultGroupKt.MetricSetKt.componentMetricSet {
                          nonCumulative =
                            InternalResultGroupKt.MetricSetKt.basicMetricSet {
                              reach = 1
                              percentReach = 0.1f
                              kPlusReach += 1
                              kPlusReach += 2
                              percentKPlusReach += 0.1f
                              percentKPlusReach += 0.2f
                              averageFrequency = 0.1f
                              impressions = 1
                              grps = 0.1f
                            }
                          cumulative =
                            InternalResultGroupKt.MetricSetKt.basicMetricSet {
                              reach = 2
                              percentReach = 0.2f
                              kPlusReach += 2
                              kPlusReach += 4
                              percentKPlusReach += 0.2f
                              percentKPlusReach += 0.4f
                              averageFrequency = 0.2f
                              impressions = 2
                              grps = 0.2f
                            }
                          uniqueReach = 5
                        }
                    }
                  componentIntersections +=
                    InternalResultGroupKt.MetricSetKt.dataProviderComponentIntersectionMetricSet {
                      nonCumulative =
                        InternalResultGroupKt.MetricSetKt.basicMetricSet {
                          reach = 15
                          percentReach = 0.1f
                          kPlusReach += 1
                          kPlusReach += 2
                          percentKPlusReach += 0.1f
                          percentKPlusReach += 0.2f
                          averageFrequency = 0.1f
                          impressions = 1
                          grps = 0.1f
                        }
                      cumulative =
                        InternalResultGroupKt.MetricSetKt.basicMetricSet {
                          reach = 20
                          percentReach = 0.2f
                          kPlusReach += 2
                          kPlusReach += 4
                          percentKPlusReach += 0.2f
                          percentKPlusReach += 0.4f
                          averageFrequency = 0.2f
                          impressions = 2
                          grps = 0.2f
                        }
                      cmmsDataProviderIds += dataProviderKey!!.dataProviderId
                    }
                }
            }
        }
      }
    }

    val createdInternalBasicReport =
      internalBasicReportsClient.insertBasicReport(
        insertBasicReportRequest { basicReport = internalBasicReport }
      )

    val url =
      HttpUrl.Builder()
        .scheme("http")
        .host(reportingGatewayHost)
        .port(reportingGatewayPort)
        .addPathSegments(
          "v2alpha/${basicReportKey.toName()}"
        )
        .build()

    val getBasicReportRequest = Request.Builder()
      .url(url)
      .get()
      .header("Authorization", "Bearer $reportingAccessToken")
      .build()

    val retrievedBasicReportJson: String =
      try {
        val response = okHttpReportingClient
          .newCall(getBasicReportRequest)
          .execute()

        response.body!!
          .bytes()
          .decodeToString()
      } catch (e: StatusException) {
        throw Exception("Error retrieving Basic Report", e)
      }

    val retrievedBasicReportBuilder = BasicReport.newBuilder()
    JsonFormat.parser()
      .ignoringUnknownFields()
      .merge(retrievedBasicReportJson, retrievedBasicReportBuilder)

    ProtoTruth.assertThat(retrievedBasicReportBuilder.build())
      .isEqualTo(
        basicReport {
          name = basicReportKey.toName()
          title = internalBasicReport.details.title
          campaignGroup = createdPrimitiveReportingSet.name
          campaignGroupDisplayName = createdPrimitiveReportingSet.displayName
          reportingInterval = reportingInterval {
            reportStart = dateTime { day = 3 }
            reportEnd = date { day = 5 }
          }

          impressionQualificationFilters += reportingImpressionQualificationFilter {
            custom =
              ReportingImpressionQualificationFilterKt.customImpressionQualificationFilterSpec {
                filterSpec += impressionQualificationFilterSpec {
                  mediaType = MediaType.VIDEO
                  filters += eventFilter {
                    terms += eventTemplateField {
                      path = "common.age_group"
                      value = EventTemplateFieldKt.fieldValue { enumValue = "18_TO_35" }
                    }
                  }
                }
              }
          }

          resultGroups += resultGroup {
            title = "title"
            results +=
              ResultGroupKt.result {
                metadata =
                  ResultGroupKt.metricMetadata {
                    reportingUnitSummary =
                      ResultGroupKt.MetricMetadataKt.reportingUnitSummary {
                        reportingUnitComponentSummary +=
                          ResultGroupKt.MetricMetadataKt.reportingUnitComponentSummary {
                            component = dataProvider.name
                            displayName = dataProvider.displayName
                            eventGroupSummaries +=
                              ResultGroupKt.MetricMetadataKt.ReportingUnitComponentSummaryKt
                                .eventGroupSummary { this.eventGroup = eventGroup.name }
                          }
                      }
                    nonCumulativeMetricStartTime = timestamp { seconds = 10 }
                    cumulativeMetricStartTime = timestamp { seconds = 12 }
                    metricEndTime = timestamp { seconds = 20 }
                    metricFrequency = metricFrequencySpec { weekly = DayOfWeek.MONDAY }
                    dimensionSpecSummary =
                      ResultGroupKt.MetricMetadataKt.dimensionSpecSummary {
                        grouping = eventTemplateField {
                          path = "common.gender"
                          value = EventTemplateFieldKt.fieldValue { enumValue = "MALE" }
                        }

                        filters += eventFilter {
                          terms += eventTemplateField {
                            path = "common.age_group"
                            value = EventTemplateFieldKt.fieldValue { enumValue = "18_TO_35" }
                          }
                        }
                      }
                    filter = reportingImpressionQualificationFilter {
                      custom =
                        ReportingImpressionQualificationFilterKt
                          .customImpressionQualificationFilterSpec {
                            filterSpec += impressionQualificationFilterSpec {
                              mediaType = MediaType.VIDEO
                              filters += eventFilter {
                                terms += eventTemplateField {
                                  path = "common.age_group"
                                  value = EventTemplateFieldKt.fieldValue { enumValue = "18_TO_35" }
                                }
                              }
                            }
                          }
                    }
                  }

                // Numbers below aren't actually calculated.
                metricSet =
                  ResultGroupKt.metricSet {
                    populationSize = 1000
                    reportingUnit =
                      ResultGroupKt.MetricSetKt.reportingUnitMetricSet {
                        nonCumulative =
                          ResultGroupKt.MetricSetKt.basicMetricSet {
                            reach = 1
                            percentReach = 0.1f
                            kPlusReach += 1
                            kPlusReach += 2
                            percentKPlusReach += 0.1f
                            percentKPlusReach += 0.2f
                            averageFrequency = 0.1f
                            impressions = 1
                            grps = 0.1f
                          }
                        cumulative =
                          ResultGroupKt.MetricSetKt.basicMetricSet {
                            reach = 2
                            percentReach = 0.2f
                            kPlusReach += 2
                            kPlusReach += 4
                            percentKPlusReach += 0.2f
                            percentKPlusReach += 0.4f
                            averageFrequency = 0.2f
                            impressions = 2
                            grps = 0.2f
                          }
                        stackedIncrementalReach += 10
                        stackedIncrementalReach += 15
                      }
                    components +=
                      ResultGroupKt.MetricSetKt.componentMetricSetMapEntry {
                        key = dataProvider.name
                        value =
                          ResultGroupKt.MetricSetKt.componentMetricSet {
                            nonCumulative =
                              ResultGroupKt.MetricSetKt.basicMetricSet {
                                reach = 1
                                percentReach = 0.1f
                                kPlusReach += 1
                                kPlusReach += 2
                                percentKPlusReach += 0.1f
                                percentKPlusReach += 0.2f
                                averageFrequency = 0.1f
                                impressions = 1
                                grps = 0.1f
                              }
                            cumulative =
                              ResultGroupKt.MetricSetKt.basicMetricSet {
                                reach = 2
                                percentReach = 0.2f
                                kPlusReach += 2
                                kPlusReach += 4
                                percentKPlusReach += 0.2f
                                percentKPlusReach += 0.4f
                                averageFrequency = 0.2f
                                impressions = 2
                                grps = 0.2f
                              }
                            uniqueReach = 5
                          }
                      }
                    componentIntersections +=
                      ResultGroupKt.MetricSetKt.componentIntersectionMetricSet {
                        nonCumulative =
                          ResultGroupKt.MetricSetKt.basicMetricSet {
                            reach = 15
                            percentReach = 0.1f
                            kPlusReach += 1
                            kPlusReach += 2
                            percentKPlusReach += 0.1f
                            percentKPlusReach += 0.2f
                            averageFrequency = 0.1f
                            impressions = 1
                            grps = 0.1f
                          }
                        cumulative =
                          ResultGroupKt.MetricSetKt.basicMetricSet {
                            reach = 20
                            percentReach = 0.2f
                            kPlusReach += 2
                            kPlusReach += 4
                            percentKPlusReach += 0.2f
                            percentKPlusReach += 0.4f
                            averageFrequency = 0.2f
                            impressions = 2
                            grps = 0.2f
                          }
                        components += dataProvider.name
                      }
                  }
              }
          }

          createTime = createdInternalBasicReport.createTime
        }
      )

    logger.info("Basic Report retrieval succeeded")
  }

  private suspend fun listEventGroups(): List<EventGroup> {
    try {
      return buildList {
        var response: ListEventGroupsResponse = ListEventGroupsResponse.getDefaultInstance()
        do {
          response =
            eventGroupsClient.listEventGroups(
              listEventGroupsRequest {
                parent = measurementConsumerName
                pageToken = response.nextPageToken
              }
            )
          addAll(response.eventGroupsList)
        } while (response.nextPageToken.isNotEmpty())
      }
    } catch (e: StatusException) {
      throw Exception("Error listing EventGroups", e)
    }
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
  ): ReportingSet {
    val primitiveReportingSet = reportingSet {
      primitive = ReportingSetKt.primitive { cmmsEventGroups += eventGroup.cmmsEventGroup }
    }

    try {
      return reportingSetsClient.createReportingSet(
        createReportingSetRequest {
          parent = measurementConsumerName
          reportingSet = primitiveReportingSet
          reportingSetId = "a-$runId"
        }
      )
    } catch (e: StatusException) {
      throw Exception("Error creating ReportingSet", e)
    }
  }

  private suspend fun createMetricCalculationSpec(runId: String): MetricCalculationSpec {
    try {
      return metricCalculationSpecsClient.createMetricCalculationSpec(
        createMetricCalculationSpecRequest {
          parent = measurementConsumerName
          metricCalculationSpecId = "a-$runId"
          metricCalculationSpec = metricCalculationSpec {
            displayName = "union reach"
            metricSpecs += metricSpec {
              reach =
                MetricSpecKt.reachParams {
                  singleDataProviderParams =
                    MetricSpecKt.samplingAndPrivacyParams {
                      privacyParams = MetricSpecKt.differentialPrivacyParams {}
                    }
                  multipleDataProviderParams =
                    MetricSpecKt.samplingAndPrivacyParams {
                      privacyParams = MetricSpecKt.differentialPrivacyParams {}
                    }
                }
            }
            metricFrequencySpec =
              MetricCalculationSpecKt.metricFrequencySpec {
                weekly =
                  MetricCalculationSpecKt.MetricFrequencySpecKt.weekly {
                    dayOfWeek = DayOfWeek.WEDNESDAY
                  }
              }
            trailingWindow =
              MetricCalculationSpecKt.trailingWindow {
                count = 1
                increment = MetricCalculationSpec.TrailingWindow.Increment.WEEK
              }
          }
        }
      )
    } catch (e: StatusException) {
      throw Exception("Error creating MetricCalculationSpec", e)
    }
  }

  private suspend fun pollForCompletedReport(reportName: String): Report {
    val backoff =
      ExponentialBackoff(initialDelay = initialResultPollingDelay, randomnessFactor = 0.0)
    var attempt = 1
    while (true) {
      val retrievedReport =
        try {
          reportsClient.getReport(getReportRequest { name = reportName })
        } catch (e: StatusException) {
          throw Exception("Error getting Report", e)
        }

      @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA") // Proto enum fields are never null.
      when (retrievedReport.state) {
        Report.State.SUCCEEDED,
        Report.State.FAILED -> return retrievedReport
        Report.State.RUNNING,
        Report.State.UNRECOGNIZED,
        Report.State.STATE_UNSPECIFIED -> {
          val resultPollingDelay =
            backoff.durationForAttempt(attempt).coerceAtMost(maximumResultPollingDelay)
          logger.info {
            "Report not completed yet. Waiting for ${resultPollingDelay.seconds} seconds."
          }
          delay(resultPollingDelay)
          attempt++
        }
      }
    }
  }

  companion object {
    private val logger: Logger = Logger.getLogger(this::class.java.name)
  }
}
