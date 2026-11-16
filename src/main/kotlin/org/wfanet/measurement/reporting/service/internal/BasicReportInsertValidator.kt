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

package org.wfanet.measurement.reporting.service.internal

import org.wfanet.measurement.internal.reporting.v2.BasicReport
import org.wfanet.measurement.internal.reporting.v2.ReportingUnit

/** Validates the ReportingSet references and component identities of a completed report insert. */
class BasicReportInsertValidator(private val reportingSetValidator: ReportingSetValidator) {
  /**
   * Checks the Campaign Group, request, summaries, and metric keys before any report is written.
   *
   * Custom components retain their exact ReportingSet IDs and filters. Partial or reordered metric
   * entries are allowed; each entry must identify a component present in the request and summary.
   * Deprecated EventGroup evidence is checked when present, without requiring it on new reports.
   *
   * @throws InvalidFieldValueException if the identities or memberships are inconsistent.
   */
  suspend fun validate(basicReport: BasicReport) {
    val campaignGroup = reportingSetValidator.getReportingSet(basicReport.externalCampaignGroupId)
    val campaignUniverse = reportingSetValidator.validateCampaignGroup(campaignGroup)
    val specs = basicReport.details.resultGroupSpecsList
    if (specs.isNotEmpty()) {
      requireValid(
        specs.size == basicReport.resultDetails.resultGroupsCount,
        "Result groups do not match the stored request",
      )
    }

    for ((groupIndex, group) in basicReport.resultDetails.resultGroupsList.withIndex()) {
      val unit = specs.getOrNull(groupIndex)?.reportingUnit
      val customKeys =
        if (unit?.componentsCase == ReportingUnit.ComponentsCase.REPORTING_SET_KEYS) {
          unit.reportingSetKeys.reportingSetKeysList
        } else {
          null
        }
      val requestedProviders =
        if (unit?.componentsCase == ReportingUnit.ComponentsCase.DATA_PROVIDER_KEYS) {
          unit.dataProviderKeys.dataProviderKeysList.map { it.cmmsDataProviderId }
        } else {
          null
        }
      if (customKeys != null) {
        requireValid(customKeys.isNotEmpty(), "Requested custom components are empty")
        requireValid(
          customKeys.all {
            it.cmmsMeasurementConsumerId == basicReport.cmmsMeasurementConsumerId &&
              it.externalReportingSetId.isNotEmpty()
          },
          "Requested custom component identity is invalid",
        )
      }
      if (requestedProviders != null) {
        requireValid(
          requestedProviders.isNotEmpty() &&
            requestedProviders.none { it.isEmpty() } &&
            requestedProviders.distinct().size == requestedProviders.size,
          "Requested DataProvider identities are empty or repeated",
        )
      }
      if (customKeys != null || requestedProviders != null) {
        requireValid(group.resultsCount > 0, "Requested result group has no results")
      }
      for (result in group.resultsList) {
        val summaries = result.metadata.reportingUnitSummary.reportingUnitComponentSummaryList
        if (customKeys != null) {
          requireValid(summaries.size == customKeys.size, "Custom component summary count differs")
        }
        val customIds = mutableSetOf<String>()
        val providerIds = mutableSetOf<String>()
        for ((index, summary) in summaries.withIndex()) {
          val reportingSet = reportingSetValidator.getReportingSet(summary.externalReportingSetId)
          val membership = reportingSetValidator.validate(reportingSet)
          requireValid(
            campaignUniverse.containsAll(membership),
            "Component ReportingSet is outside the Campaign Group",
          )
          if (customKeys != null) {
            requireValid(
              summary.cmmsDataProviderId.isEmpty() &&
                summary.externalReportingSetId == customKeys[index].externalReportingSetId,
              "Custom component summary identity differs from the stored request",
            )
          }
          if (summary.cmmsDataProviderId.isEmpty()) {
            requireValid(
              requestedProviders == null,
              "DataProvider component summary is missing its DataProvider ID",
            )
            requireValid(
              customIds.add(summary.externalReportingSetId),
              "Custom summary is repeated",
            )
          } else {
            requireValid(customKeys == null, "Custom component summary identifies a DataProvider")
            requireValid(
              reportingSet.hasPrimitive() &&
                reportingSet.filter.isEmpty() &&
                membership ==
                  campaignUniverse
                    .filter { it.cmmsDataProviderId == summary.cmmsDataProviderId }
                    .toSet(),
              "DataProvider component must identify its unfiltered Campaign Group membership",
            )
            requireValid(
              providerIds.add(summary.cmmsDataProviderId),
              "DataProvider summary is repeated",
            )
          }
          @Suppress("DEPRECATION") val eventGroupSummaries = summary.eventGroupSummariesList
          if (eventGroupSummaries.isNotEmpty()) {
            requireValid(
              eventGroupSummaries.all {
                it.cmmsMeasurementConsumerId.isEmpty() ||
                  it.cmmsMeasurementConsumerId == basicReport.cmmsMeasurementConsumerId
              } &&
                eventGroupSummaries.groupingBy { it.cmmsEventGroupId }.eachCount() ==
                  membership.groupingBy { it.cmmsEventGroupId }.eachCount(),
              "Recorded EventGroup identities differ from the component ReportingSet",
            )
          }
        }
        if (requestedProviders != null) {
          requireValid(
            providerIds == requestedProviders.toSet(),
            "DataProvider summaries do not cover the stored request",
          )
        }
        val metrics = result.metricSet
        val metricCustomIds = metrics.reportingSetComponentsList.map { it.externalReportingSetId }
        requireValid(
          metricCustomIds.all { it in customIds } &&
            metricCustomIds.distinct().size == metricCustomIds.size,
          "ReportingSet metric keys do not match the component summaries",
        )
        val metricProviderIds = metrics.componentsList.map { it.key }
        requireValid(
          metricProviderIds.all { it in providerIds } &&
            metricProviderIds.distinct().size == metricProviderIds.size,
          "DataProvider metric keys do not match the component summaries",
        )
        for (intersection in metrics.componentIntersectionsList) {
          val ids = intersection.cmmsDataProviderIdsList
          requireValid(
            ids.isNotEmpty() && ids.all { it in providerIds } && ids.distinct().size == ids.size,
            "Intersection metric keys do not match the component summaries",
          )
        }
      }
    }
  }

  private fun requireValid(condition: Boolean, message: String) {
    if (!condition) {
      throw InvalidFieldValueException("basic_report.result_details") { message }
    }
  }
}
