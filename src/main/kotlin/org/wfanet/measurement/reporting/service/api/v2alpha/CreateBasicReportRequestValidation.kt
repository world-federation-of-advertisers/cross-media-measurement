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

import com.google.protobuf.Descriptors
import com.google.protobuf.Duration
import com.google.protobuf.Timestamp
import java.util.UUID
import kotlin.collections.List
import kotlin.collections.Set
import org.wfanet.measurement.api.v2alpha.DataProvider
import org.wfanet.measurement.api.v2alpha.DataProviderKey
import org.wfanet.measurement.api.v2alpha.EventGroupKey
import org.wfanet.measurement.common.api.ResourceIds
import org.wfanet.measurement.reporting.service.api.FieldUnimplementedException
import org.wfanet.measurement.reporting.service.api.InvalidFieldValueException
import org.wfanet.measurement.reporting.service.api.RequiredFieldNotSetException
import org.wfanet.measurement.reporting.v2alpha.CreateBasicReportRequest
import org.wfanet.measurement.reporting.v2alpha.DimensionSpec
import org.wfanet.measurement.reporting.v2alpha.EventTemplateField
import org.wfanet.measurement.reporting.v2alpha.MediaType
import org.wfanet.measurement.reporting.v2alpha.MetricFrequencySpec
import org.wfanet.measurement.reporting.v2alpha.ReportingImpressionQualificationFilter
import org.wfanet.measurement.reporting.v2alpha.ReportingImpressionQualificationFilter.CustomImpressionQualificationFilterSpec
import org.wfanet.measurement.reporting.v2alpha.ReportingInterval
import org.wfanet.measurement.reporting.v2alpha.ReportingSet
import org.wfanet.measurement.reporting.v2alpha.ReportingUnit
import org.wfanet.measurement.reporting.v2alpha.ResultGroupMetricSpec
import org.wfanet.measurement.reporting.v2alpha.ResultGroupSpec

private val RESOURCE_ID_REGEX = ResourceIds.AIP_122_REGEX

/**
 * Validates a [CreateBasicReportRequest]. Only supports validation that does not requires gRPC
 * calls
 *
 * @param request
 * @param campaignGroup
 * @param eventTemplateFieldsMap for validating [EventTemplateField]
 * @throws [RequiredFieldNotSetException] when validation fails
 * @throws [InvalidFieldValueException] when validation fails
 */
fun validateCreateBasicReportRequest(
  request: CreateBasicReportRequest,
  campaignGroup: ReportingSet,
  eventTemplateFieldsMap: Map<String, EventDescriptor.EventTemplateFieldInfo>,
) {
  if (request.basicReportId.isEmpty()) {
    throw RequiredFieldNotSetException("basic_report_id")
  }

  if (!request.basicReportId.matches(RESOURCE_ID_REGEX)) {
    throw InvalidFieldValueException("basic_report_id")
  }

  if (request.requestId.isNotEmpty()) {
    try {
      UUID.fromString(request.requestId)
    } catch (e: IllegalArgumentException) {
      throw InvalidFieldValueException("request_id") { "Not a UUID" }
    }
  }

  if (campaignGroup.campaignGroup != campaignGroup.name) {
    throw InvalidFieldValueException("basic_report.campaign_group") { "Not a Campaign Group" }
  }

  if (request.basicReport.hasReportingInterval()) {
    validateReportingInterval(request.basicReport.reportingInterval)
  } else {
    throw RequiredFieldNotSetException("basic_report.reporting_interval")
  }

  validateReportingImpressionQualificationFilters(
    request.basicReport.impressionQualificationFiltersList
  )
  validateResultGroupSpecs(
    request.basicReport.resultGroupSpecsList,
    campaignGroup,
    eventTemplateFieldsMap,
  )
}

/**
 * Validates a [List] of [ResultGroupSpec]
 *
 * @param resultGroupSpecs [List] of [ResultGroupSpec] to validate
 * @param campaignGroup [ReportingSet] to validate against
 * @param eventTemplateFieldsMap for validating [EventTemplateField]
 * @throws [RequiredFieldNotSetException] when validation fails
 * @throws [InvalidFieldValueException] when validation fails
 */
fun validateResultGroupSpecs(
  resultGroupSpecs: List<ResultGroupSpec>,
  campaignGroup: ReportingSet,
  eventTemplateFieldsMap: Map<String, EventDescriptor.EventTemplateFieldInfo>,
) {
  if (resultGroupSpecs.isEmpty()) {
    throw RequiredFieldNotSetException("basic_report.result_group_specs")
  }

  val dataProviderNameSet: Set<String> = buildSet {
    for (cmmsEventGroup in campaignGroup.primitive.cmmsEventGroupsList) {
      val eventGroupKey = EventGroupKey.fromName(cmmsEventGroup)
      add(eventGroupKey!!.parentKey.toName())
    }
  }

  for (resultGroupSpec in resultGroupSpecs) {
    if (
      resultGroupSpec.metricFrequency.selectorCase ==
        MetricFrequencySpec.SelectorCase.SELECTOR_NOT_SET
    ) {
      throw RequiredFieldNotSetException("basic_report.result_group_specs.metric_frequency")
    }

    if (resultGroupSpec.hasReportingUnit()) {
      validateReportingUnit(resultGroupSpec.reportingUnit, dataProviderNameSet)
    } else {
      throw RequiredFieldNotSetException("basic_report.result_group_specs.reporting_unit")
    }

    if (resultGroupSpec.hasDimensionSpec()) {
      validateDimensionSpec(resultGroupSpec.dimensionSpec, eventTemplateFieldsMap)
    } else {
      throw RequiredFieldNotSetException("basic_report.result_group_specs.dimension_spec")
    }

    if (resultGroupSpec.hasResultGroupMetricSpec()) {
      validateResultGroupMetricSpec(
        resultGroupSpec.resultGroupMetricSpec,
        resultGroupSpec.metricFrequency.selectorCase,
      )
    } else {
      throw RequiredFieldNotSetException("basic_report.result_group_specs.result_group_metric_spec")
    }
  }
}

/**
 * Validates a [ReportingUnit]
 *
 * @param reportingUnit [ReportingUnit] to validate
 * @param dataProviderNameSet [Set] of [DataProvider] names that CampaignGroup is associated with
 * @throws [InvalidFieldValueException] when validation fails
 */
fun validateReportingUnit(reportingUnit: ReportingUnit, dataProviderNameSet: Set<String>) {
  if (reportingUnit.componentsList.isEmpty()) {
    throw InvalidFieldValueException("basic_report.result_group_specs.reporting_unit.components")
  }

  for (component in reportingUnit.componentsList) {
    DataProviderKey.fromName(component)
      ?: throw InvalidFieldValueException(
        "basic_report.result_group_specs.reporting_unit.components"
      ) { fieldName ->
        "$component in $fieldName is not a valid data provider name"
      }

    if (!dataProviderNameSet.contains(component)) {
      throw InvalidFieldValueException(
        "basic_report.result_group_specs.reporting_unit.components"
      ) { fieldName ->
        "$component in $fieldName does not have any cmms_event_groups in campaign_group"
      }
    }
  }
}

/**
 * Validates a [DimensionSpec]
 *
 * @param dimensionSpec [DimensionSpec] to validate
 * @param eventTemplateFieldsMap for validating [EventTemplateField]
 * @throws [RequiredFieldNotSetException] when validation fails
 * @throws [InvalidFieldValueException] when validation fails
 */
fun validateDimensionSpec(
  dimensionSpec: DimensionSpec,
  eventTemplateFieldsMap: Map<String, EventDescriptor.EventTemplateFieldInfo>,
) {
  val groupingEventTemplateFieldsSet: Set<String> = buildSet {
    if (dimensionSpec.hasGrouping()) {
      validateDimensionSpecGrouping(dimensionSpec.grouping, eventTemplateFieldsMap)
      for (eventTemplateFieldPath in dimensionSpec.grouping.eventTemplateFieldsList) {
        add(eventTemplateFieldPath)
      }
    }
  }

  for (eventFilter in dimensionSpec.filtersList) {
    if (eventFilter.termsList.size != 1) {
      throw InvalidFieldValueException(
        "basic_report.result_group_specs.dimension_spec.filters.terms"
      )
    }

    for (eventTemplateField in eventFilter.termsList) {
      validateEventTemplateField(eventTemplateField, eventTemplateFieldsMap)
      if (groupingEventTemplateFieldsSet.contains(eventTemplateField.path)) {
        throw InvalidFieldValueException(
          "basic_report.result_group_specs.dimension_spec.filters.terms.path"
        ) { fieldName ->
          "$fieldName is already in basic_report.result_group_specs.dimension_spec.grouping.event_template_fields for the same dimension_spec"
        }
      }
    }
  }
}

/**
 * Validates a [DimensionSpec.Grouping]
 *
 * @param grouping [DimensionSpec.Grouping] to validate
 * @param eventTemplateFieldsMap for validating [EventTemplateField]
 * @throws [RequiredFieldNotSetException] when validation fails
 * @throws [InvalidFieldValueException] when validation fails
 */
fun validateDimensionSpecGrouping(
  grouping: DimensionSpec.Grouping,
  eventTemplateFieldsMap: Map<String, EventDescriptor.EventTemplateFieldInfo>,
) {
  if (grouping.eventTemplateFieldsList.isEmpty()) {
    throw RequiredFieldNotSetException(
      "basic_report.result_group_specs.dimension_spec.grouping.event_template_fields"
    )
  }

  for (eventTemplateFieldPath in grouping.eventTemplateFieldsList) {
    val eventTemplateFieldInfo =
      eventTemplateFieldsMap[eventTemplateFieldPath]
        ?: throw InvalidFieldValueException(
          "basic_report.result_group_specs.dimension_spec.grouping.event_template_fields"
        ) { fieldName ->
          "$fieldName contains event template field that doesn't exist"
        }

    if (!eventTemplateFieldInfo.supportedReportingFeatures.groupable) {
      throw InvalidFieldValueException(
        "basic_report.result_group_specs.dimension_spec.grouping.event_template_fields"
      ) { fieldName ->
        "$fieldName contains event template field that is not groupable"
      }
    }

    if (!eventTemplateFieldInfo.isPopulationAttribute) {
      throw InvalidFieldValueException(
        "basic_report.result_group_specs.dimension_spec.grouping.event_template_fields"
      ) { fieldName ->
        "$fieldName contains event template field that is not a population attribute"
      }
    }
  }
}

/**
 * Validates an [EventTemplateField]
 *
 * @param eventTemplateField [EventTemplateField] to validate
 * @param eventTemplateFieldsMap for validating [EventTemplateField]
 * @throws [RequiredFieldNotSetException] when validation fails
 * @throws [InvalidFieldValueException] when validation fails
 */
fun validateEventTemplateField(
  eventTemplateField: EventTemplateField,
  eventTemplateFieldsMap: Map<String, EventDescriptor.EventTemplateFieldInfo>,
) {
  if (eventTemplateField.path.isEmpty()) {
    throw RequiredFieldNotSetException(
      "basic_report.result_group_specs.dimension_spec.filters.terms.path"
    )
  } else {
    val eventTemplateFieldInfo =
      eventTemplateFieldsMap[eventTemplateField.path]
        ?: throw InvalidFieldValueException(
          "basic_report.result_group_specs.dimension_spec.filters.terms.path"
        ) { fieldName ->
          "$fieldName doesn't exist"
        }

    if (!eventTemplateFieldInfo.supportedReportingFeatures.filterable) {
      throw InvalidFieldValueException(
        "basic_report.result_group_specs.dimension_spec.filters.terms.path"
      ) { fieldName ->
        "$fieldName is not filterable"
      }
    }

    @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA") // Protobuf enum fields cannot be null.
    when (eventTemplateField.value.selectorCase) {
      EventTemplateField.FieldValue.SelectorCase.STRING_VALUE -> {
        if (eventTemplateFieldInfo.type != Descriptors.FieldDescriptor.Type.STRING) {
          if (
            eventTemplateFieldInfo.type != Descriptors.FieldDescriptor.Type.MESSAGE ||
              eventTemplateFieldInfo.messageTypeFullName != Timestamp.getDescriptor().fullName
          ) {
            throw InvalidFieldValueException(
              "basic_report.result_group_specs.dimension_spec.filters.terms.value.string_value"
            ) { fieldName ->
              "$fieldName is invalid for ${eventTemplateField.path}"
            }
          }
        }
      }
      EventTemplateField.FieldValue.SelectorCase.ENUM_VALUE -> {
        if (
          eventTemplateFieldInfo.type != Descriptors.FieldDescriptor.Type.ENUM ||
            !eventTemplateFieldInfo.enumValuesMap.containsKey(eventTemplateField.value.enumValue)
        ) {
          throw InvalidFieldValueException(
            "basic_report.result_group_specs.dimension_spec.filters.terms.value.enum_value"
          ) { fieldName ->
            "$fieldName is invalid for for ${eventTemplateField.path}"
          }
        }
      }
      EventTemplateField.FieldValue.SelectorCase.BOOL_VALUE ->
        if (eventTemplateFieldInfo.type != Descriptors.FieldDescriptor.Type.BOOL) {
          throw InvalidFieldValueException(
            "basic_report.result_group_specs.dimension_spec.filters.terms.value.bool_value"
          ) { fieldName ->
            "$fieldName is invalid for ${eventTemplateField.path}"
          }
        }
      EventTemplateField.FieldValue.SelectorCase.FLOAT_VALUE -> {
        if (
          eventTemplateFieldInfo.type == Descriptors.FieldDescriptor.Type.ENUM ||
            eventTemplateFieldInfo.type == Descriptors.FieldDescriptor.Type.STRING ||
            eventTemplateFieldInfo.type == Descriptors.FieldDescriptor.Type.BOOL ||
            (eventTemplateFieldInfo.type == Descriptors.FieldDescriptor.Type.MESSAGE &&
              eventTemplateFieldInfo.messageTypeFullName != Duration.getDescriptor().fullName)
        ) {
          throw InvalidFieldValueException(
            "basic_report.result_group_specs.dimension_spec.filters.terms.value.float_value"
          ) { fieldName ->
            "$fieldName is invalid for ${eventTemplateField.path}"
          }
        }
      }
      EventTemplateField.FieldValue.SelectorCase.SELECTOR_NOT_SET -> {
        throw RequiredFieldNotSetException(
          "basic_report.result_group_specs.dimension_spec.filters.terms.value"
        )
      }
    }

    if (!eventTemplateFieldInfo.isPopulationAttribute) {
      throw InvalidFieldValueException(
        "basic_report.result_group_specs.dimension_spec.filters.terms.path"
      ) { fieldName ->
        "$fieldName is not a population attribute"
      }
    }
  }
}

/**
 * Validates a [ResultGroupMetricSpec]
 *
 * @param resultGroupMetricSpec [ResultGroupMetricSpec] to validate
 * @param metricFrequencySelectorCase [MetricFrequencySpec.SelectorCase] to validate against
 * @throws [RequiredFieldNotSetException] when validation fails
 * @throws [InvalidFieldValueException] when validation fails
 * @throws [FieldUnimplementedException] when validation fails
 */
fun validateResultGroupMetricSpec(
  resultGroupMetricSpec: ResultGroupMetricSpec,
  metricFrequencySelectorCase: MetricFrequencySpec.SelectorCase,
) {
  if (resultGroupMetricSpec.hasComponentIntersection()) {
    throw FieldUnimplementedException(
      "basic_report.result_group_specs.result_group_metric_spec.component_intersection"
    )
  }

  if (metricFrequencySelectorCase == MetricFrequencySpec.SelectorCase.TOTAL) {
    if (resultGroupMetricSpec.reportingUnit.hasNonCumulative()) {
      throw InvalidFieldValueException(
        "basic_report.result_group_specs.result_group_metric_spec.reporting_unit.non_cumulative"
      ) { fieldName ->
        "$fieldName cannot be specified when metric_frequency is total"
      }
    }

    if (resultGroupMetricSpec.component.hasNonCumulative()) {
      throw InvalidFieldValueException(
        "basic_report.result_group_specs.result_group_metric_spec.component.non_cumulative"
      ) { fieldName ->
        "$fieldName cannot be specified when metric_frequency is total"
      }
    }

    if (resultGroupMetricSpec.component.hasNonCumulativeUnique()) {
      throw InvalidFieldValueException(
        "basic_report.result_group_specs.result_group_metric_spec.component.non_cumulative_unique"
      ) { fieldName ->
        "$fieldName cannot be specified when metric_frequency is total"
      }
    }
  }

  validateBasicMetricSetSpec(
    resultGroupMetricSpec.reportingUnit.cumulative,
    "basic_report.result_group_specs.result_group_metric_spec.reporting_unit.cumulative.k_plus_reach",
  )
  validateBasicMetricSetSpec(
    resultGroupMetricSpec.reportingUnit.nonCumulative,
    "basic_report.result_group_specs.result_group_metric_spec.reporting_unit.non_cumulative.k_plus_reach",
  )
  validateBasicMetricSetSpec(
    resultGroupMetricSpec.component.nonCumulative,
    "basic_report.result_group_specs.result_group_metric_spec.component.non_cumulative.k_plus_reach",
  )
  validateBasicMetricSetSpec(
    resultGroupMetricSpec.component.cumulative,
    "basic_report.result_group_specs.result_group_metric_spec.component.cumulative.k_plus_reach",
  )

  if (
    resultGroupMetricSpec.reportingUnit.stackedIncrementalReach &&
      metricFrequencySelectorCase != MetricFrequencySpec.SelectorCase.TOTAL
  ) {
    throw InvalidFieldValueException(
      "basic_report.result_group_specs.result_group_metric_spec.reporting_unit.stacked_incremental_reach"
    ) { fieldName ->
      "$fieldName requires metric_frequency to be total"
    }
  }
}

/**
 * Validates a [ReportingInterval]
 *
 * @param reportingInterval [ReportingInterval] to validate
 * @throws [RequiredFieldNotSetException] when validation fails
 * @throws [InvalidFieldValueException] when validation fails
 */
fun validateReportingInterval(reportingInterval: ReportingInterval) {
  if (!reportingInterval.hasReportStart()) {
    throw RequiredFieldNotSetException("basic_report.reporting_interval.report_start")
  }

  if (!reportingInterval.hasReportEnd()) {
    throw RequiredFieldNotSetException("basic_report.reporting_interval.report_end")
  }

  if (
    reportingInterval.reportStart.year == 0 ||
      reportingInterval.reportStart.month == 0 ||
      reportingInterval.reportStart.day == 0 ||
      !(reportingInterval.reportStart.hasTimeZone() || reportingInterval.reportStart.hasUtcOffset())
  ) {
    throw InvalidFieldValueException("basic_report.reporting_interval.report_start") { fieldName ->
      "$fieldName requires year, month, and day to all be set, as well as either time_zone or utc_offset"
    }
  }

  if (
    reportingInterval.reportEnd.year == 0 ||
      reportingInterval.reportEnd.month == 0 ||
      reportingInterval.reportEnd.day == 0
  ) {
    throw InvalidFieldValueException("basic_report.reporting_interval.report_end") { fieldName ->
      "$fieldName requires year, month, and day to be set"
    }
  }
}

/**
 * Validates a [List] of [ReportingImpressionQualificationFilter]
 *
 * @param reportingImpressionQualificationFilters [List] of [ReportingImpressionQualificationFilter]
 *   to validate
 * @throws [RequiredFieldNotSetException] when validation fails
 * @throws [InvalidFieldValueException] when validation fails
 */
fun validateReportingImpressionQualificationFilters(
  reportingImpressionQualificationFilters: List<ReportingImpressionQualificationFilter>
) {
  if (reportingImpressionQualificationFilters.isEmpty()) {
    throw RequiredFieldNotSetException("basic_report.impression_qualification_filters")
  }

  for (impressionQualificationFilter in reportingImpressionQualificationFilters) {
    if (impressionQualificationFilter.hasImpressionQualificationFilter()) {
      ImpressionQualificationFilterKey.fromName(
        impressionQualificationFilter.impressionQualificationFilter
      )
        ?: throw InvalidFieldValueException(
          "basic_report.impression_qualification_filters.impression_qualification_filter"
        )
    } else if (impressionQualificationFilter.hasCustom()) {
      validateCustomImpressionQualificationFilterSpec(impressionQualificationFilter.custom)
    } else {
      throw InvalidFieldValueException("basic_report.impression_qualification_filters.selector")
    }
  }
}

/**
 * Validates a [CustomImpressionQualificationFilterSpec]
 *
 * @param customImpressionQualificationFilterSpec [CustomImpressionQualificationFilterSpec] to
 *   validate
 * @throws [RequiredFieldNotSetException] when validation fails
 * @throws [InvalidFieldValueException] when validation fails
 */
fun validateCustomImpressionQualificationFilterSpec(
  customImpressionQualificationFilterSpec: CustomImpressionQualificationFilterSpec
) {
  if (customImpressionQualificationFilterSpec.filterSpecList.isEmpty()) {
    throw RequiredFieldNotSetException(
      "basic_report.impression_qualification_filters.custom.filter_spec"
    )
  }

  // No more than 1 filter_spec per MediaType
  buildSet<MediaType> {
    for (filterSpec in customImpressionQualificationFilterSpec.filterSpecList) {
      if (this.contains(filterSpec.mediaType)) {
        throw InvalidFieldValueException(
          "basic_report.impression_qualification_filters.custom.filter_spec"
        ) { fieldName ->
          "$fieldName cannot have more than 1 filter_spec for MediaType ${filterSpec.mediaType}. Only 1 filter_spec per MediaType allowed"
        }
      }
      add(filterSpec.mediaType)

      if (filterSpec.filtersList.isEmpty()) {
        throw RequiredFieldNotSetException(
          "basic_report.impression_qualification_filters.custom.filter_spec.filters"
        )
      }

      for (eventFilter in filterSpec.filtersList) {
        if (eventFilter.termsList.size != 1) {
          throw RequiredFieldNotSetException(
            "basic_report.impression_qualification_filters.custom.filter_spec.filters.terms"
          )
        }

        // TODO(@tristanvuong2021): Need to verify against eventTemplate descriptor
        for (eventTemplateField in eventFilter.termsList) {
          if (eventTemplateField.path.isEmpty()) {
            throw RequiredFieldNotSetException(
              "basic_report.impression_qualification_filters.custom.filter_spec.filters.terms.path"
            )
          }

          if (
            eventTemplateField.value.selectorCase ==
              EventTemplateField.FieldValue.SelectorCase.SELECTOR_NOT_SET
          ) {
            throw RequiredFieldNotSetException(
              "basic_report.impression_qualification_filters.custom.filter_spec.filters.terms.value"
            )
          }
        }
      }
    }
  }
}

/**
 * Validates a [ResultGroupMetricSpec.BasicMetricSetSpec]
 *
 * @param basicMetricSetSpec [ResultGroupMetricSpec.BasicMetricSetSpec] to validate
 * @param kPlusReachFieldName field name to use in error
 * @throws [InvalidFieldValueException] when validation fails
 */
fun validateBasicMetricSetSpec(
  basicMetricSetSpec: ResultGroupMetricSpec.BasicMetricSetSpec,
  kPlusReachFieldName: String,
) {
  if (basicMetricSetSpec.percentKPlusReach) {
    if (basicMetricSetSpec.kPlusReach <= 0) {
      throw InvalidFieldValueException(kPlusReachFieldName) { fieldName ->
        "$fieldName must have a positive value"
      }
    }
  }
}
