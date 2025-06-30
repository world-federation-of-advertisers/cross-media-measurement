/*
 * Copyright 2025 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.reporting.service.api.v2alpha

import org.wfanet.measurement.api.v2alpha.DataProviderKey
import org.wfanet.measurement.api.v2alpha.MeasurementConsumerEventGroupKey
import org.wfanet.measurement.internal.reporting.v2.BasicReport as InternalBasicReport
import org.wfanet.measurement.internal.reporting.v2.EventFilter as InternalEventFilter
import org.wfanet.measurement.internal.reporting.v2.EventTemplateField as InternalEventTemplateField
import org.wfanet.measurement.internal.reporting.v2.ImpressionQualificationFilter as InternalImpressionQualificationFilter
import org.wfanet.measurement.internal.reporting.v2.ImpressionQualificationFilterSpec as InternalImpressionQualificationFilterSpec
import org.wfanet.measurement.internal.reporting.v2.ImpressionQualificationFilterSpec.MediaType as InternalMediaType
import org.wfanet.measurement.internal.reporting.v2.MetricFrequencySpec as InternalMetricFrequencySpec
import org.wfanet.measurement.internal.reporting.v2.ReportingImpressionQualificationFilter as InternalReportingImpressionQualificationFilter
import org.wfanet.measurement.internal.reporting.v2.ResultGroup as InternalResultGroup
import org.wfanet.measurement.internal.reporting.v2.ResultGroup.MetricMetadata as InternalMetricMetadata
import org.wfanet.measurement.internal.reporting.v2.ResultGroup.MetricSet as InternalMetricSet
import org.wfanet.measurement.internal.reporting.v2.ResultGroup.MetricSet.BasicMetricSet as InternalBasicMetricSet
import org.wfanet.measurement.reporting.v2alpha.BasicReport
import org.wfanet.measurement.reporting.v2alpha.EventFilter
import org.wfanet.measurement.reporting.v2alpha.EventTemplateField
import org.wfanet.measurement.reporting.v2alpha.EventTemplateFieldKt
import org.wfanet.measurement.reporting.v2alpha.ImpressionQualificationFilter
import org.wfanet.measurement.reporting.v2alpha.MediaType
import org.wfanet.measurement.reporting.v2alpha.MetricFrequencySpec
import org.wfanet.measurement.reporting.v2alpha.ReportingImpressionQualificationFilter
import org.wfanet.measurement.reporting.v2alpha.ReportingImpressionQualificationFilterKt.customImpressionQualificationFilterSpec
import org.wfanet.measurement.reporting.v2alpha.ResultGroup
import org.wfanet.measurement.reporting.v2alpha.ResultGroup.MetricMetadata
import org.wfanet.measurement.reporting.v2alpha.ResultGroup.MetricSet
import org.wfanet.measurement.reporting.v2alpha.ResultGroup.MetricSet.BasicMetricSet
import org.wfanet.measurement.reporting.v2alpha.ResultGroupKt
import org.wfanet.measurement.reporting.v2alpha.ResultGroupKt.MetricMetadataKt.dimensionSpecSummary
import org.wfanet.measurement.reporting.v2alpha.basicReport
import org.wfanet.measurement.reporting.v2alpha.eventFilter
import org.wfanet.measurement.reporting.v2alpha.eventTemplateField
import org.wfanet.measurement.reporting.v2alpha.impressionQualificationFilter
import org.wfanet.measurement.reporting.v2alpha.impressionQualificationFilterSpec
import org.wfanet.measurement.reporting.v2alpha.metricFrequencySpec
import org.wfanet.measurement.reporting.v2alpha.reportingImpressionQualificationFilter
import org.wfanet.measurement.reporting.v2alpha.reportingInterval
import org.wfanet.measurement.reporting.v2alpha.resultGroup

/** Converts the internal [InternalBasicReport] to the public [BasicReport]. */
fun InternalBasicReport.toBasicReport(): BasicReport {
  val source = this
  return basicReport {
    name = BasicReportKey(source.cmmsMeasurementConsumerId, source.externalBasicReportId).toName()
    title = source.details.title
    campaignGroup =
      ReportingSetKey(source.cmmsMeasurementConsumerId, source.externalCampaignGroupId).toName()
    campaignGroupDisplayName = source.campaignGroupDisplayName
    reportingInterval = reportingInterval {
      reportStart = source.details.reportingInterval.reportStart
      reportEnd = source.details.reportingInterval.reportEnd
    }
    for (internalImpressionQualificationFilter in
      source.details.impressionQualificationFiltersList) {
      impressionQualificationFilters +=
        internalImpressionQualificationFilter.toReportingImpressionQualificationFilter()
    }
    for (internalResultGroup in source.resultDetails.resultGroupsList) {
      resultGroups += internalResultGroup.toResultGroup()
    }
    createTime = source.createTime
  }
}

/**
 * Converts the internal [InternalReportingImpressionQualificationFilter] to the public
 * [ReportingImpressionQualificationFilter].
 */
fun InternalReportingImpressionQualificationFilter.toReportingImpressionQualificationFilter():
  ReportingImpressionQualificationFilter {
  val source = this
  return reportingImpressionQualificationFilter {
    if (source.externalImpressionQualificationFilterId.isNotEmpty()) {
      impressionQualificationFilter =
        ImpressionQualificationFilterKey(source.externalImpressionQualificationFilterId).toName()
    } else {
      custom = customImpressionQualificationFilterSpec {
        for (internalFilterSpec in source.filterSpecsList) {
          filterSpec += impressionQualificationFilterSpec {
            mediaType = internalFilterSpec.mediaType.toMediaType()
            for (internalEventFilter in internalFilterSpec.filtersList) {
              filters += internalEventFilter.toEventFilter()
            }
          }
        }
      }
    }
  }
}

/** Converts the internal [InternalMediaType] to the public [MediaType]. */
fun InternalMediaType.toMediaType(): MediaType {
  return when (this) {
    InternalImpressionQualificationFilterSpec.MediaType.MEDIA_TYPE_UNSPECIFIED ->
      MediaType.MEDIA_TYPE_UNSPECIFIED
    InternalImpressionQualificationFilterSpec.MediaType.VIDEO -> MediaType.VIDEO
    InternalImpressionQualificationFilterSpec.MediaType.DISPLAY -> MediaType.DISPLAY
    InternalImpressionQualificationFilterSpec.MediaType.OTHER -> MediaType.OTHER
    InternalImpressionQualificationFilterSpec.MediaType.UNRECOGNIZED -> MediaType.UNRECOGNIZED
  }
}

/** Converts the internal [InternalEventFilter] to the public [EventFilter]. */
fun InternalEventFilter.toEventFilter(): EventFilter {
  val source = this
  return eventFilter {
    for (internalEventTemplateField in source.termsList) {
      terms += internalEventTemplateField.toEventTemplateField()
    }
  }
}

/** Converts the internal [InternalEventTemplateField] to the public [EventTemplateField]. */
fun InternalEventTemplateField.toEventTemplateField(): EventTemplateField {
  val source = this
  return eventTemplateField {
    path = source.path
    value =
      EventTemplateFieldKt.fieldValue {
        @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA") // Proto enum fields are never null.
        when (source.value.selectorCase) {
          InternalEventTemplateField.FieldValue.SelectorCase.STRING_VALUE -> {
            stringValue = source.value.stringValue
          }
          InternalEventTemplateField.FieldValue.SelectorCase.ENUM_VALUE -> {
            enumValue = source.value.enumValue
          }
          InternalEventTemplateField.FieldValue.SelectorCase.BOOL_VALUE -> {
            boolValue = source.value.boolValue
          }
          InternalEventTemplateField.FieldValue.SelectorCase.FLOAT_VALUE -> {
            floatValue = source.value.floatValue
          }
          InternalEventTemplateField.FieldValue.SelectorCase.SELECTOR_NOT_SET -> {}
        }
      }
  }
}

/** Converts the internal [InternalResultGroup] to the public [ResultGroup]. */
fun InternalResultGroup.toResultGroup(): ResultGroup {
  val source = this
  return resultGroup {
    title = source.title
    for (internalResult in source.resultsList) {
      results +=
        ResultGroupKt.result {
          metadata = internalResult.metadata.toMetricMetadata()
          metricSet = internalResult.metricSet.toMetricSet()
        }
    }
  }
}

/** Converts the internal [InternalMetricFrequencySpec] to the public [MetricFrequencySpec]. */
fun InternalMetricFrequencySpec.toMetricFrequencySpec(): MetricFrequencySpec {
  val source = this
  return metricFrequencySpec {
    @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA") // Proto enum fields are never null.
    when (source.selectorCase) {
      InternalMetricFrequencySpec.SelectorCase.WEEKLY -> {
        weekly = source.weekly
      }
      InternalMetricFrequencySpec.SelectorCase.TOTAL -> {
        total = source.total
      }
      InternalMetricFrequencySpec.SelectorCase.SELECTOR_NOT_SET -> {}
    }
  }
}

/** Converts the internal [InternalMetricMetadata] to the public [MetricMetadata]. */
fun InternalMetricMetadata.toMetricMetadata(): MetricMetadata {
  val source = this
  return ResultGroupKt.metricMetadata {
    reportingUnitSummary =
      ResultGroupKt.MetricMetadataKt.reportingUnitSummary {
        for (internalReportingUnitComponentSummary in
          source.reportingUnitSummary.reportingUnitComponentSummaryList) {
          reportingUnitComponentSummary +=
            ResultGroupKt.MetricMetadataKt.reportingUnitComponentSummary {
              component =
                DataProviderKey(internalReportingUnitComponentSummary.cmmsDataProviderId).toName()
              displayName = internalReportingUnitComponentSummary.cmmsDataProviderDisplayName
              for (internalEventGroupSummary in
                internalReportingUnitComponentSummary.eventGroupSummariesList) {
                eventGroupSummaries +=
                  ResultGroupKt.MetricMetadataKt.ReportingUnitComponentSummaryKt.eventGroupSummary {
                    eventGroup =
                      MeasurementConsumerEventGroupKey(
                          internalEventGroupSummary.cmmsMeasurementConsumerId,
                          internalEventGroupSummary.cmmsEventGroupId,
                        )
                        .toName()
                  }
              }
            }
        }
      }
    nonCumulativeMetricStartTime = source.nonCumulativeMetricStartTime
    cumulativeMetricStartTime = source.cumulativeMetricStartTime
    metricEndTime = source.metricEndTime
    metricFrequency = source.metricFrequencySpec.toMetricFrequencySpec()

    dimensionSpecSummary = dimensionSpecSummary {
      for (grouping in source.dimensionSpecSummary.groupingsList) {
        groupings += grouping.toEventTemplateField()
      }
      for (internalEventFilter in source.dimensionSpecSummary.filtersList) {
        filters += internalEventFilter.toEventFilter()
      }
    }
    filter = source.filter.toReportingImpressionQualificationFilter()
  }
}

/** Converts the internal [InternalMetricSet] to the public [MetricSet]. */
fun InternalMetricSet.toMetricSet(): MetricSet {
  val source = this
  return ResultGroupKt.metricSet {
    populationSize = source.populationSize
    reportingUnit =
      ResultGroupKt.MetricSetKt.reportingUnitMetricSet {
        if (source.reportingUnit.hasNonCumulative()) {
          nonCumulative = source.reportingUnit.nonCumulative.toBasicMetricSet()
        }
        if (source.reportingUnit.hasCumulative()) {
          cumulative = source.reportingUnit.cumulative.toBasicMetricSet()
        }
        stackedIncrementalReach += source.reportingUnit.stackedIncrementalReachList
      }
    for (internalDataProviderComponentMetricSetMapEntry in source.componentsList) {
      components +=
        ResultGroupKt.MetricSetKt.componentMetricSetMapEntry {
          key = DataProviderKey(internalDataProviderComponentMetricSetMapEntry.key).toName()
          value =
            ResultGroupKt.MetricSetKt.componentMetricSet {
              if (internalDataProviderComponentMetricSetMapEntry.value.hasNonCumulative()) {
                nonCumulative =
                  internalDataProviderComponentMetricSetMapEntry.value.nonCumulative
                    .toBasicMetricSet()
              }
              if (internalDataProviderComponentMetricSetMapEntry.value.hasCumulative()) {
                cumulative =
                  internalDataProviderComponentMetricSetMapEntry.value.cumulative.toBasicMetricSet()
              }
              uniqueReach = internalDataProviderComponentMetricSetMapEntry.value.uniqueReach
            }
        }
    }
    for (internalDataProviderComponentIntersectionMetricSet in source.componentIntersectionsList) {
      componentIntersections +=
        ResultGroupKt.MetricSetKt.componentIntersectionMetricSet {
          for (cmmsDataProviderId in
            internalDataProviderComponentIntersectionMetricSet.cmmsDataProviderIdsList) {
            components += DataProviderKey(cmmsDataProviderId).toName()
          }
          if (internalDataProviderComponentIntersectionMetricSet.hasNonCumulative()) {
            nonCumulative =
              internalDataProviderComponentIntersectionMetricSet.nonCumulative.toBasicMetricSet()
          }
          if (internalDataProviderComponentIntersectionMetricSet.hasCumulative()) {
            cumulative =
              internalDataProviderComponentIntersectionMetricSet.cumulative.toBasicMetricSet()
          }
        }
    }
  }
}

/** Converts the internal [InternalBasicMetricSet] to the public [BasicMetricSet]. */
fun InternalBasicMetricSet.toBasicMetricSet(): BasicMetricSet {
  val source = this
  return ResultGroupKt.MetricSetKt.basicMetricSet {
    reach = source.reach
    percentReach = source.percentReach
    kPlusReach += source.kPlusReachList
    percentKPlusReach += source.percentKPlusReachList
    averageFrequency = source.averageFrequency
    impressions = source.impressions
    grps = source.grps
  }
}

/**
 * Converts the internal [InternalImpressionQualificationFilter] to the public
 * [ImpressionQualificationFilter].
 */
fun InternalImpressionQualificationFilter.toImpressionQualificationFilter():
  ImpressionQualificationFilter {
  val source = this
  return impressionQualificationFilter {
    name = ImpressionQualificationFilterKey(source.externalImpressionQualificationFilterId).toName()
    displayName = externalImpressionQualificationFilterId
    filterSpecs +=
      source.filterSpecsList.map { it ->
        impressionQualificationFilterSpec {
          mediaType = it.mediaType.toMediaType()
          filters += it.filtersList.map { it -> it.toEventFilter() }
        }
      }
  }
}
