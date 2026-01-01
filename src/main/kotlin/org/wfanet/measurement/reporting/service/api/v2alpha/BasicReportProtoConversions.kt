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
import org.wfanet.measurement.api.v2alpha.ModelLineKey
import org.wfanet.measurement.internal.reporting.v2.BasicReport as InternalBasicReport
import org.wfanet.measurement.internal.reporting.v2.BasicReportKt as InternalBasicReportKt
import org.wfanet.measurement.internal.reporting.v2.DimensionSpec as InternalDimensionSpec
import org.wfanet.measurement.internal.reporting.v2.DimensionSpecKt as InternalDimensionSpecKt
import org.wfanet.measurement.internal.reporting.v2.EventFilter as InternalEventFilter
import org.wfanet.measurement.internal.reporting.v2.EventTemplateField as InternalEventTemplateField
import org.wfanet.measurement.internal.reporting.v2.EventTemplateFieldKt as InternalEventTemplateFieldKt
import org.wfanet.measurement.internal.reporting.v2.ImpressionQualificationFilter as InternalImpressionQualificationFilter
import org.wfanet.measurement.internal.reporting.v2.ImpressionQualificationFilterSpec as InternalImpressionQualificationFilterSpec
import org.wfanet.measurement.internal.reporting.v2.ImpressionQualificationFilterSpec.MediaType as InternalMediaType
import org.wfanet.measurement.internal.reporting.v2.MetricFrequencySpec as InternalMetricFrequencySpec
import org.wfanet.measurement.internal.reporting.v2.ReportingImpressionQualificationFilter as InternalReportingImpressionQualificationFilter
import org.wfanet.measurement.internal.reporting.v2.ReportingUnit as InternalReportingUnit
import org.wfanet.measurement.internal.reporting.v2.ReportingUnitKt as InternalReportingUnitKt
import org.wfanet.measurement.internal.reporting.v2.ResultGroup as InternalResultGroup
import org.wfanet.measurement.internal.reporting.v2.ResultGroup.MetricMetadata as InternalMetricMetadata
import org.wfanet.measurement.internal.reporting.v2.ResultGroup.MetricSet as InternalMetricSet
import org.wfanet.measurement.internal.reporting.v2.ResultGroup.MetricSet.BasicMetricSet as InternalBasicMetricSet
import org.wfanet.measurement.internal.reporting.v2.ResultGroupMetricSpec as InternalResultGroupMetricSpec
import org.wfanet.measurement.internal.reporting.v2.ResultGroupMetricSpecKt as InternalResultGroupMetricSpecKt
import org.wfanet.measurement.internal.reporting.v2.ResultGroupSpec as InternalResultGroupSpec
import org.wfanet.measurement.internal.reporting.v2.basicReport as internalBasicReport
import org.wfanet.measurement.internal.reporting.v2.basicReportDetails as internalBasicReportDetails
import org.wfanet.measurement.internal.reporting.v2.dataProviderKey as internalDataProviderKey
import org.wfanet.measurement.internal.reporting.v2.dimensionSpec as internalDimensionSpec
import org.wfanet.measurement.internal.reporting.v2.eventFilter as internalEventFilter
import org.wfanet.measurement.internal.reporting.v2.eventTemplateField as internalEventTemplateField
import org.wfanet.measurement.internal.reporting.v2.impressionQualificationFilterSpec as internalImpressionQualificationFilterSpec
import org.wfanet.measurement.internal.reporting.v2.metricFrequencySpec as internalMetricFrequencySpec
import org.wfanet.measurement.internal.reporting.v2.reportingImpressionQualificationFilter as internalReportingImpressionQualificationFilter
import org.wfanet.measurement.internal.reporting.v2.reportingInterval as internalReportingInterval
import org.wfanet.measurement.internal.reporting.v2.reportingUnit as internalReportingUnit
import org.wfanet.measurement.internal.reporting.v2.resultGroupMetricSpec as internalResultGroupMetricSpec
import org.wfanet.measurement.internal.reporting.v2.resultGroupSpec as internalResultGroupSpec
import org.wfanet.measurement.reporting.v2alpha.BasicReport
import org.wfanet.measurement.reporting.v2alpha.DimensionSpec
import org.wfanet.measurement.reporting.v2alpha.DimensionSpecKt
import org.wfanet.measurement.reporting.v2alpha.EventFilter
import org.wfanet.measurement.reporting.v2alpha.EventTemplateField
import org.wfanet.measurement.reporting.v2alpha.EventTemplateFieldKt
import org.wfanet.measurement.reporting.v2alpha.ImpressionQualificationFilter
import org.wfanet.measurement.reporting.v2alpha.ImpressionQualificationFilterSpec
import org.wfanet.measurement.reporting.v2alpha.MediaType
import org.wfanet.measurement.reporting.v2alpha.MetricFrequencySpec
import org.wfanet.measurement.reporting.v2alpha.ReportingImpressionQualificationFilter
import org.wfanet.measurement.reporting.v2alpha.ReportingImpressionQualificationFilterKt.customImpressionQualificationFilterSpec
import org.wfanet.measurement.reporting.v2alpha.ReportingUnit
import org.wfanet.measurement.reporting.v2alpha.ResultGroup
import org.wfanet.measurement.reporting.v2alpha.ResultGroup.MetricMetadata
import org.wfanet.measurement.reporting.v2alpha.ResultGroup.MetricSet
import org.wfanet.measurement.reporting.v2alpha.ResultGroup.MetricSet.BasicMetricSet
import org.wfanet.measurement.reporting.v2alpha.ResultGroupKt
import org.wfanet.measurement.reporting.v2alpha.ResultGroupKt.MetricMetadataKt.dimensionSpecSummary
import org.wfanet.measurement.reporting.v2alpha.ResultGroupMetricSpec
import org.wfanet.measurement.reporting.v2alpha.ResultGroupMetricSpecKt
import org.wfanet.measurement.reporting.v2alpha.ResultGroupSpec
import org.wfanet.measurement.reporting.v2alpha.basicReport
import org.wfanet.measurement.reporting.v2alpha.dimensionSpec
import org.wfanet.measurement.reporting.v2alpha.eventFilter
import org.wfanet.measurement.reporting.v2alpha.eventTemplateField
import org.wfanet.measurement.reporting.v2alpha.impressionQualificationFilter
import org.wfanet.measurement.reporting.v2alpha.impressionQualificationFilterSpec
import org.wfanet.measurement.reporting.v2alpha.metricFrequencySpec
import org.wfanet.measurement.reporting.v2alpha.reportingImpressionQualificationFilter
import org.wfanet.measurement.reporting.v2alpha.reportingInterval
import org.wfanet.measurement.reporting.v2alpha.reportingUnit
import org.wfanet.measurement.reporting.v2alpha.resultGroup
import org.wfanet.measurement.reporting.v2alpha.resultGroupMetricSpec
import org.wfanet.measurement.reporting.v2alpha.resultGroupSpec

/** Converts the public [BasicReport] to the internal [InternalBasicReport]. */
fun BasicReport.toInternal(
  cmmsMeasurementConsumerId: String,
  basicReportId: String,
  campaignGroupId: String,
  createReportRequestId: String,
  reportingImpressionQualificationFilters: Iterable<ReportingImpressionQualificationFilter>,
  effectiveReportingImpressionQualificationFilters:
    Iterable<ReportingImpressionQualificationFilter>,
  impressionQualificationFilterSpecsByName: Map<String, List<ImpressionQualificationFilterSpec>>,
  effectiveModelLine: String,
): InternalBasicReport {
  val source = this
  return internalBasicReport {
    this.cmmsMeasurementConsumerId = cmmsMeasurementConsumerId
    externalBasicReportId = basicReportId
    externalCampaignGroupId = campaignGroupId

    details = internalBasicReportDetails {
      title = source.title
      impressionQualificationFilters +=
        reportingImpressionQualificationFilters.map {
          it.toInternal(impressionQualificationFilterSpecsByName)
        }
      effectiveImpressionQualificationFilters +=
        effectiveReportingImpressionQualificationFilters.map {
          it.toInternal(impressionQualificationFilterSpecsByName)
        }
      reportingInterval = internalReportingInterval {
        reportStart = source.reportingInterval.reportStart
        reportEnd = source.reportingInterval.reportEnd
      }
      for (resultGroupSpec in source.resultGroupSpecsList) {
        resultGroupSpecs += resultGroupSpec.toInternal()
      }
    }

    this.createReportRequestId = createReportRequestId
    if (effectiveModelLine.isNotEmpty()) {
      val modelLineKey = ModelLineKey.fromName(effectiveModelLine)
      this.modelLineKey =
        InternalBasicReportKt.modelLineKey {
          cmmsModelProviderId = modelLineKey!!.modelProviderId
          cmmsModelSuiteId = modelLineKey.modelSuiteId
          cmmsModelLineId = modelLineKey.modelLineId
        }

      modelLineSystemSpecified = source.modelLine.isEmpty()
    }
  }
}

/**
 * Converts the public [ReportingImpressionQualificationFilter] to the internal
 * [InternalReportingImpressionQualificationFilter].
 */
fun ReportingImpressionQualificationFilter.toInternal(
  impressionQualificationFilterSpecsByName: Map<String, List<ImpressionQualificationFilterSpec>>
): InternalReportingImpressionQualificationFilter {
  val source = this
  return internalReportingImpressionQualificationFilter {
    @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA") // Proto enum fields are never null.
    when (source.selectorCase) {
      ReportingImpressionQualificationFilter.SelectorCase.IMPRESSION_QUALIFICATION_FILTER -> {
        externalImpressionQualificationFilterId =
          ImpressionQualificationFilterKey.fromName(source.impressionQualificationFilter)!!
            .impressionQualificationFilterId
        val filterSpecs =
          impressionQualificationFilterSpecsByName.getValue(source.impressionQualificationFilter)
        for (filterSpec in filterSpecs) {
          this.filterSpecs += filterSpec.toInternal()
        }
      }
      ReportingImpressionQualificationFilter.SelectorCase.CUSTOM -> {
        for (filterSpec in source.custom.filterSpecList) {
          filterSpecs += filterSpec.toInternal()
        }
      }
      ReportingImpressionQualificationFilter.SelectorCase.SELECTOR_NOT_SET -> {}
    }
  }
}

/**
 * Converts the public [ImpressionQualificationFilterSpec] to the internal
 * [InternalImpressionQualificationFilterSpec].
 */
fun ImpressionQualificationFilterSpec.toInternal(): InternalImpressionQualificationFilterSpec {
  val source = this
  return internalImpressionQualificationFilterSpec {
    mediaType = source.mediaType.toInternal()
    filters += source.filtersList.map { it.toInternal() }
  }
}

/** Converts the public [MediaType] to the internal [InternalMediaType]. */
fun MediaType.toInternal(): InternalMediaType {
  return when (this) {
    MediaType.VIDEO -> InternalMediaType.VIDEO
    MediaType.DISPLAY -> InternalMediaType.DISPLAY
    MediaType.OTHER -> InternalMediaType.OTHER
    MediaType.MEDIA_TYPE_UNSPECIFIED,
    MediaType.UNRECOGNIZED -> InternalMediaType.MEDIA_TYPE_UNSPECIFIED
  }
}

/** Converts the public [ResultGroupSpec] to the internal [InternalResultGroupSpec]. */
fun ResultGroupSpec.toInternal(): InternalResultGroupSpec {
  val source = this
  return internalResultGroupSpec {
    title = source.title
    reportingUnit = source.reportingUnit.toInternal()
    metricFrequency = source.metricFrequency.toInternal()
    dimensionSpec = source.dimensionSpec.toInternal()
    resultGroupMetricSpec = source.resultGroupMetricSpec.toInternal()
  }
}

/** Converts the public [ReportingUnit] to the internal [InternalReportingUnit]. */
fun ReportingUnit.toInternal(): InternalReportingUnit {
  val source = this
  return internalReportingUnit {
    // Only BasicReports with DataProvider components will be converted to internal BasicReports
    dataProviderKeys =
      InternalReportingUnitKt.dataProviderKeys {
        for (reportingUnitComponent in source.componentsList) {
          dataProviderKeys += internalDataProviderKey {
            cmmsDataProviderId = DataProviderKey.fromName(reportingUnitComponent)!!.dataProviderId
          }
        }
      }
  }
}

/** Converts the public [MetricFrequencySpec] to the internal [InternalMetricFrequencySpec]. */
fun MetricFrequencySpec.toInternal(): InternalMetricFrequencySpec {
  val source = this
  return internalMetricFrequencySpec {
    @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA") // Proto enum fields are never null.
    when (source.selectorCase) {
      MetricFrequencySpec.SelectorCase.WEEKLY -> {
        weekly = source.weekly
      }
      MetricFrequencySpec.SelectorCase.TOTAL -> {
        total = source.total
      }
      MetricFrequencySpec.SelectorCase.SELECTOR_NOT_SET -> {}
    }
  }
}

fun EventFilter.toInternal(): InternalEventFilter {
  return internalEventFilter {
    for (term in termsList) {
      terms += internalEventTemplateField {
        path = term.path
        value =
          InternalEventTemplateFieldKt.fieldValue {
            @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA") // Proto enum fields are never null.
            when (term.value.selectorCase) {
              EventTemplateField.FieldValue.SelectorCase.STRING_VALUE -> {
                stringValue = term.value.stringValue
              }
              EventTemplateField.FieldValue.SelectorCase.ENUM_VALUE -> {
                enumValue = term.value.enumValue
              }
              EventTemplateField.FieldValue.SelectorCase.BOOL_VALUE -> {
                boolValue = term.value.boolValue
              }
              EventTemplateField.FieldValue.SelectorCase.FLOAT_VALUE -> {
                floatValue = term.value.floatValue
              }
              EventTemplateField.FieldValue.SelectorCase.SELECTOR_NOT_SET -> {}
            }
          }
      }
    }
  }
}

/** Converts the public [DimensionSpec] to the internal [InternalDimensionSpec]. */
fun DimensionSpec.toInternal(): InternalDimensionSpec {
  val source = this
  return internalDimensionSpec {
    if (source.hasGrouping()) {
      grouping =
        InternalDimensionSpecKt.grouping {
          eventTemplateFields += source.grouping.eventTemplateFieldsList
        }
    }
    filters += source.filtersList.map { it.toInternal() }
  }
}

/** Converts the public [ResultGroupMetricSpec] to the internal [InternalResultGroupMetricSpec]. */
fun ResultGroupMetricSpec.toInternal(): InternalResultGroupMetricSpec {
  val source = this
  return internalResultGroupMetricSpec {
    populationSize = source.populationSize
    if (source.hasReportingUnit()) {
      reportingUnit =
        InternalResultGroupMetricSpecKt.reportingUnitMetricSetSpec {
          if (source.reportingUnit.hasNonCumulative()) {
            nonCumulative =
              InternalResultGroupMetricSpecKt.basicMetricSetSpec {
                reach = source.reportingUnit.nonCumulative.reach
                percentReach = source.reportingUnit.nonCumulative.percentReach
                kPlusReach = source.reportingUnit.nonCumulative.kPlusReach
                percentKPlusReach = source.reportingUnit.nonCumulative.percentKPlusReach
                averageFrequency = source.reportingUnit.nonCumulative.averageFrequency
                impressions = source.reportingUnit.nonCumulative.impressions
                grps = source.reportingUnit.nonCumulative.grps
              }
          }
          if (source.reportingUnit.hasCumulative()) {
            cumulative =
              InternalResultGroupMetricSpecKt.basicMetricSetSpec {
                reach = source.reportingUnit.cumulative.reach
                percentReach = source.reportingUnit.cumulative.percentReach
                kPlusReach = source.reportingUnit.cumulative.kPlusReach
                percentKPlusReach = source.reportingUnit.cumulative.percentKPlusReach
                averageFrequency = source.reportingUnit.cumulative.averageFrequency
                impressions = source.reportingUnit.cumulative.impressions
                grps = source.reportingUnit.cumulative.grps
              }
          }
          stackedIncrementalReach = source.reportingUnit.stackedIncrementalReach
        }
    }

    if (source.hasComponent()) {
      component =
        InternalResultGroupMetricSpecKt.componentMetricSetSpec {
          if (source.component.hasNonCumulative()) {
            nonCumulative =
              InternalResultGroupMetricSpecKt.basicMetricSetSpec {
                reach = source.component.nonCumulative.reach
                percentReach = source.component.nonCumulative.percentReach
                kPlusReach = source.component.nonCumulative.kPlusReach
                percentKPlusReach = source.component.nonCumulative.percentKPlusReach
                averageFrequency = source.component.nonCumulative.averageFrequency
                impressions = source.component.nonCumulative.impressions
                grps = source.component.nonCumulative.grps
              }
          }
          if (source.component.hasCumulative()) {
            cumulative =
              InternalResultGroupMetricSpecKt.basicMetricSetSpec {
                reach = source.component.cumulative.reach
                percentReach = source.component.cumulative.percentReach
                kPlusReach = source.component.cumulative.kPlusReach
                percentKPlusReach = source.component.cumulative.percentKPlusReach
                averageFrequency = source.component.cumulative.averageFrequency
                impressions = source.component.cumulative.impressions
                grps = source.component.cumulative.grps
              }
          }
          if (source.component.hasNonCumulativeUnique()) {
            nonCumulativeUnique =
              InternalResultGroupMetricSpecKt.uniqueMetricSetSpec {
                reach = source.component.nonCumulativeUnique.reach
              }
          }
          if (source.component.hasCumulativeUnique()) {
            cumulativeUnique =
              InternalResultGroupMetricSpecKt.uniqueMetricSetSpec {
                reach = source.component.cumulativeUnique.reach
              }
          }
        }
    }

    if (source.hasComponentIntersection()) {
      componentIntersection =
        InternalResultGroupMetricSpecKt.componentIntersectionMetricSetSpec {
          contributorCount += source.componentIntersection.contributorCountList
          if (source.componentIntersection.hasNonCumulative()) {
            nonCumulative =
              InternalResultGroupMetricSpecKt.basicMetricSetSpec {
                reach = source.componentIntersection.nonCumulative.reach
                percentReach = source.componentIntersection.nonCumulative.percentReach
                kPlusReach = source.componentIntersection.nonCumulative.kPlusReach
                percentKPlusReach = source.componentIntersection.nonCumulative.percentKPlusReach
                averageFrequency = source.componentIntersection.nonCumulative.averageFrequency
                impressions = source.componentIntersection.nonCumulative.impressions
                grps = source.componentIntersection.nonCumulative.grps
              }
          }
          if (source.componentIntersection.hasCumulative()) {
            cumulative =
              InternalResultGroupMetricSpecKt.basicMetricSetSpec {
                reach = source.componentIntersection.cumulative.reach
                percentReach = source.componentIntersection.cumulative.percentReach
                kPlusReach = source.componentIntersection.cumulative.kPlusReach
                percentKPlusReach = source.componentIntersection.cumulative.percentKPlusReach
                averageFrequency = source.componentIntersection.cumulative.averageFrequency
                impressions = source.componentIntersection.cumulative.impressions
                grps = source.componentIntersection.cumulative.grps
              }
          }
        }
    }
  }
}

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
    for (internalImpressionQualificationFilter in
      source.details.effectiveImpressionQualificationFiltersList) {
      effectiveImpressionQualificationFilters +=
        internalImpressionQualificationFilter.toReportingImpressionQualificationFilter()
    }
    for (internalResultGroupSpec in source.details.resultGroupSpecsList) {
      resultGroupSpecs += internalResultGroupSpec.toResultGroupSpec()
    }
    for (internalResultGroup in source.resultDetails.resultGroupsList) {
      resultGroups += internalResultGroup.toResultGroup()
    }
    createTime = source.createTime

    state =
      @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA") // Proto enum fields are never null.
      when (source.state) {
        InternalBasicReport.State.CREATED,
        InternalBasicReport.State.REPORT_CREATED,
        InternalBasicReport.State.UNPROCESSED_RESULTS_READY -> BasicReport.State.RUNNING
        InternalBasicReport.State.SUCCEEDED -> BasicReport.State.SUCCEEDED
        InternalBasicReport.State.FAILED -> BasicReport.State.FAILED
        InternalBasicReport.State.INVALID -> BasicReport.State.INVALID
        InternalBasicReport.State.STATE_UNSPECIFIED -> BasicReport.State.STATE_UNSPECIFIED
        InternalBasicReport.State.UNRECOGNIZED -> BasicReport.State.UNRECOGNIZED
      }

    if (modelLineKey.cmmsModelProviderId.isNotEmpty()) {
      val modelLineName =
        ModelLineKey(
            modelLineKey.cmmsModelProviderId,
            modelLineKey.cmmsModelSuiteId,
            modelLineKey.cmmsModelLineId,
          )
          .toName()

      effectiveModelLine = modelLineName
      if (!modelLineSystemSpecified) {
        modelLine = modelLineName
      }
    }
  }
}

/** Converts the internal [InternalResultGroupSpec] to the public [ResultGroupSpec]. */
fun InternalResultGroupSpec.toResultGroupSpec(): ResultGroupSpec {
  val source = this
  return resultGroupSpec {
    title = source.title
    reportingUnit = source.reportingUnit.toReportingUnit()
    metricFrequency = source.metricFrequency.toMetricFrequencySpec()
    dimensionSpec = source.dimensionSpec.toDimensionSpec()
    resultGroupMetricSpec = source.resultGroupMetricSpec.toResultGroupMetricSpec()
  }
}

/** Converts the internal [InternalReportingUnit] to the public [ReportingUnit]. */
fun InternalReportingUnit.toReportingUnit(): ReportingUnit {
  val source = this
  return reportingUnit {
    @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA") // Proto enum fields are never null.
    when (source.componentsCase) {
      InternalReportingUnit.ComponentsCase.DATA_PROVIDER_KEYS -> {
        for (internalDataProviderKey in source.dataProviderKeys.dataProviderKeysList) {
          components += DataProviderKey(internalDataProviderKey.cmmsDataProviderId).toName()
        }
      }
      InternalReportingUnit.ComponentsCase.REPORTING_SET_KEYS -> {
        for (internalReportingSetKey in source.reportingSetKeys.reportingSetKeysList) {
          components +=
            ReportingSetKey(
                internalReportingSetKey.cmmsMeasurementConsumerId,
                internalReportingSetKey.externalReportingSetId,
              )
              .toName()
        }
      }
      InternalReportingUnit.ComponentsCase.COMPONENTS_NOT_SET -> {}
    }
  }
}

/** Converts the internal [InternalDimensionSpec] to the public [DimensionSpec]. */
fun InternalDimensionSpec.toDimensionSpec(): DimensionSpec {
  val source = this
  return dimensionSpec {
    if (source.hasGrouping()) {
      grouping =
        DimensionSpecKt.grouping { eventTemplateFields += source.grouping.eventTemplateFieldsList }
    }
    filters += source.filtersList.map { it.toEventFilter() }
  }
}

/** Converts the internal [InternalResultGroupMetricSpec] to the public [ResultGroupMetricSpec]. */
fun InternalResultGroupMetricSpec.toResultGroupMetricSpec(): ResultGroupMetricSpec {
  val source = this
  return resultGroupMetricSpec {
    populationSize = source.populationSize
    if (source.hasReportingUnit()) {
      reportingUnit =
        ResultGroupMetricSpecKt.reportingUnitMetricSetSpec {
          if (source.reportingUnit.hasNonCumulative()) {
            nonCumulative =
              ResultGroupMetricSpecKt.basicMetricSetSpec {
                reach = source.reportingUnit.nonCumulative.reach
                percentReach = source.reportingUnit.nonCumulative.percentReach
                kPlusReach = source.reportingUnit.nonCumulative.kPlusReach
                percentKPlusReach = source.reportingUnit.nonCumulative.percentKPlusReach
                averageFrequency = source.reportingUnit.nonCumulative.averageFrequency
                impressions = source.reportingUnit.nonCumulative.impressions
                grps = source.reportingUnit.nonCumulative.grps
              }
          }
          if (source.reportingUnit.hasCumulative()) {
            cumulative =
              ResultGroupMetricSpecKt.basicMetricSetSpec {
                reach = source.reportingUnit.cumulative.reach
                percentReach = source.reportingUnit.cumulative.percentReach
                kPlusReach = source.reportingUnit.cumulative.kPlusReach
                percentKPlusReach = source.reportingUnit.cumulative.percentKPlusReach
                averageFrequency = source.reportingUnit.cumulative.averageFrequency
                impressions = source.reportingUnit.cumulative.impressions
                grps = source.reportingUnit.cumulative.grps
              }
          }
          stackedIncrementalReach = source.reportingUnit.stackedIncrementalReach
        }
    }

    if (source.hasComponent()) {
      component =
        ResultGroupMetricSpecKt.componentMetricSetSpec {
          if (source.component.hasNonCumulative()) {
            nonCumulative =
              ResultGroupMetricSpecKt.basicMetricSetSpec {
                reach = source.component.nonCumulative.reach
                percentReach = source.component.nonCumulative.percentReach
                kPlusReach = source.component.nonCumulative.kPlusReach
                percentKPlusReach = source.component.nonCumulative.percentKPlusReach
                averageFrequency = source.component.nonCumulative.averageFrequency
                impressions = source.component.nonCumulative.impressions
                grps = source.component.nonCumulative.grps
              }
          }
          if (source.component.hasCumulative()) {
            cumulative =
              ResultGroupMetricSpecKt.basicMetricSetSpec {
                reach = source.component.cumulative.reach
                percentReach = source.component.cumulative.percentReach
                kPlusReach = source.component.cumulative.kPlusReach
                percentKPlusReach = source.component.cumulative.percentKPlusReach
                averageFrequency = source.component.cumulative.averageFrequency
                impressions = source.component.cumulative.impressions
                grps = source.component.cumulative.grps
              }
          }
          if (source.component.hasNonCumulativeUnique()) {
            nonCumulativeUnique =
              ResultGroupMetricSpecKt.uniqueMetricSetSpec {
                reach = source.component.nonCumulativeUnique.reach
              }
          }
          if (source.component.hasCumulativeUnique()) {
            cumulativeUnique =
              ResultGroupMetricSpecKt.uniqueMetricSetSpec {
                reach = source.component.cumulativeUnique.reach
              }
          }
        }
    }

    if (source.hasComponentIntersection()) {
      componentIntersection =
        ResultGroupMetricSpecKt.componentIntersectionMetricSetSpec {
          contributorCount += source.componentIntersection.contributorCountList
          if (source.componentIntersection.hasNonCumulative()) {
            nonCumulative =
              ResultGroupMetricSpecKt.basicMetricSetSpec {
                reach = source.componentIntersection.nonCumulative.reach
                percentReach = source.componentIntersection.nonCumulative.percentReach
                kPlusReach = source.componentIntersection.nonCumulative.kPlusReach
                percentKPlusReach = source.componentIntersection.nonCumulative.percentKPlusReach
                averageFrequency = source.componentIntersection.nonCumulative.averageFrequency
                impressions = source.componentIntersection.nonCumulative.impressions
                grps = source.componentIntersection.nonCumulative.grps
              }
          }
          if (source.componentIntersection.hasCumulative()) {
            cumulative =
              ResultGroupMetricSpecKt.basicMetricSetSpec {
                reach = source.componentIntersection.cumulative.reach
                percentReach = source.componentIntersection.cumulative.percentReach
                kPlusReach = source.componentIntersection.cumulative.kPlusReach
                percentKPlusReach = source.componentIntersection.cumulative.percentKPlusReach
                averageFrequency = source.componentIntersection.cumulative.averageFrequency
                impressions = source.componentIntersection.cumulative.impressions
                grps = source.componentIntersection.cumulative.grps
              }
          }
        }
    }
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
          filterSpec += internalFilterSpec.toImpressionQualificationFilterSpec()
        }
      }
    }
  }
}

/**
 * Converts the internal [InternalImpressionQualificationFilterSpec] to the public
 * [ImpressionQualificationFilterSpec].
 */
fun InternalImpressionQualificationFilterSpec.toImpressionQualificationFilterSpec():
  ImpressionQualificationFilterSpec {
  val source = this
  return impressionQualificationFilterSpec {
    mediaType = source.mediaType.toMediaType()
    for (internalEventFilter in source.filtersList) {
      filters += internalEventFilter.toEventFilter()
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
              if (internalDataProviderComponentMetricSetMapEntry.value.hasNonCumulativeUnique()) {
                nonCumulativeUnique =
                  ResultGroupKt.MetricSetKt.uniqueMetricSet {
                    reach =
                      internalDataProviderComponentMetricSetMapEntry.value.nonCumulativeUnique.reach
                  }
              }
              if (internalDataProviderComponentMetricSetMapEntry.value.hasCumulativeUnique()) {
                cumulativeUnique =
                  ResultGroupKt.MetricSetKt.uniqueMetricSet {
                    reach =
                      internalDataProviderComponentMetricSetMapEntry.value.cumulativeUnique.reach
                  }
              }
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
          filters += it.filtersList.map { it.toEventFilter() }
        }
      }
  }
}
