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
import io.grpc.Status
import java.util.UUID
import kotlin.collections.Set
import org.wfanet.measurement.api.v2alpha.DataProviderKey
import org.wfanet.measurement.api.v2alpha.EventGroupKey
import org.wfanet.measurement.api.v2alpha.EventMessageDescriptor
import org.wfanet.measurement.api.v2alpha.MeasurementConsumerKey
import org.wfanet.measurement.api.v2alpha.ModelLineKey
import org.wfanet.measurement.common.api.ResourceIds
import org.wfanet.measurement.common.mediatype.toEventAnnotationMediaType
import org.wfanet.measurement.reporting.service.api.DataProviderNotFoundForCampaignGroupException
import org.wfanet.measurement.reporting.service.api.EventTemplateFieldInvalidException
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

object CreateBasicReportRequestValidation {
  private val RESOURCE_ID_REGEX = ResourceIds.AIP_122_REGEX

  data class ParsedFields(
    val parentKey: MeasurementConsumerKey,
    val impressionQualificationFilterKeys: Set<ImpressionQualificationFilterKey>,
  )

  private data class CampaignGroupInfo(val name: String, val dataProviderNames: Set<String>) {
    constructor(
      campaignGroup: ReportingSet
    ) : this(
      campaignGroup.name,
      buildSet {
        for (eventGroupName in campaignGroup.primitive.cmmsEventGroupsList) {
          val eventGroupKey = requireNotNull(EventGroupKey.fromName(eventGroupName))
          add(eventGroupKey.parentKey.toName())
        }
      },
    )
  }

  /**
   * Validates [request].
   *
   * This does not validate the
   * [request.basicReport.campaignGroup][org.wfanet.measurement.reporting.v2alpha.BasicReport.campaignGroup]
   * field.
   *
   * @throws InvalidFieldValueException
   * @throws RequiredFieldNotSetException
   * @throws DataProviderNotFoundForCampaignGroupException
   * @throws EventTemplateFieldInvalidException
   * @throws FieldUnimplementedException
   */
  fun validateRequest(
    request: CreateBasicReportRequest,
    campaignGroup: ReportingSet,
    eventTemplateFieldsByPath: Map<String, EventMessageDescriptor.EventTemplateFieldInfo>,
  ): ParsedFields {
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
        throw InvalidFieldValueException("request_id", e) { fieldPath ->
          "$fieldPath is not a valid UUID"
        }
      }
    }

    if (request.parent.isEmpty()) {
      throw RequiredFieldNotSetException("parent")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }
    val parentKey =
      MeasurementConsumerKey.fromName(request.parent)
        ?: throw InvalidFieldValueException("parent") { fieldPath ->
          "$fieldPath is not a valid MeasurmentConsumer resource name"
        }

    if (!request.hasBasicReport()) {
      throw RequiredFieldNotSetException("basic_report")
    }
    if (request.basicReport.hasReportingInterval()) {
      validateReportingInterval(request.basicReport.reportingInterval)
    } else {
      throw RequiredFieldNotSetException("basic_report.reporting_interval")
    }
    if (request.basicReport.modelLine.isNotEmpty()) {
      ModelLineKey.fromName(request.basicReport.modelLine)
        ?: throw InvalidFieldValueException("basic_report.model_line")
    }
    val impressionQualificationFilterKeyByName =
      validateReportingImpressionQualificationFilters(
        request.basicReport.impressionQualificationFiltersList,
        eventTemplateFieldsByPath,
      )

    validateResultGroupSpecs(
      request.basicReport.resultGroupSpecsList,
      eventTemplateFieldsByPath,
      CampaignGroupInfo(campaignGroup),
    )

    return ParsedFields(parentKey, impressionQualificationFilterKeyByName)
  }

  /**
   * Validates [resultGroupSpecs] within the context of a [CreateBasicReportRequest].
   *
   * @param resultGroupSpecs [List] of [ResultGroupSpec] to validate
   * @param eventTemplateFieldsByPath Map of EventTemplate field path with respect to Event message
   *   to info for the field. Used for validating [EventTemplateField]
   * @throws [RequiredFieldNotSetException] when validation fails
   * @throws [InvalidFieldValueException] when validation fails
   * @throws FieldUnimplementedException
   */
  private fun validateResultGroupSpecs(
    resultGroupSpecs: List<ResultGroupSpec>,
    eventTemplateFieldsByPath: Map<String, EventMessageDescriptor.EventTemplateFieldInfo>,
    campaignGroupInfo: CampaignGroupInfo,
  ) {
    val fieldPath = "basic_report.result_group_specs"
    if (resultGroupSpecs.isEmpty()) {
      throw RequiredFieldNotSetException(fieldPath)
    }

    resultGroupSpecs.forEachIndexed { index, resultGroupSpec ->
      val resultGroupSpecFieldPath = "$fieldPath[$index]"
      if (
        resultGroupSpec.metricFrequency.selectorCase ==
          MetricFrequencySpec.SelectorCase.SELECTOR_NOT_SET
      ) {
        throw RequiredFieldNotSetException("$resultGroupSpecFieldPath.metric_frequency")
      }

      if (resultGroupSpec.hasReportingUnit()) {
        validateReportingUnit(
          resultGroupSpec.reportingUnit,
          "$resultGroupSpecFieldPath.reporting_unit",
          campaignGroupInfo,
        )
      } else {
        throw RequiredFieldNotSetException("$resultGroupSpecFieldPath.reporting_unit")
      }

      if (resultGroupSpec.hasDimensionSpec()) {
        validateDimensionSpec(
          resultGroupSpec.dimensionSpec,
          "$resultGroupSpecFieldPath.dimension_spec",
          eventTemplateFieldsByPath,
        )
      } else {
        throw RequiredFieldNotSetException("$resultGroupSpecFieldPath.dimension_spec")
      }

      if (resultGroupSpec.hasResultGroupMetricSpec()) {
        validateResultGroupMetricSpec(
          resultGroupSpec.resultGroupMetricSpec,
          "$resultGroupSpecFieldPath.result_group_metric_spec",
          resultGroupSpec.metricFrequency.selectorCase,
        )
      } else {
        throw RequiredFieldNotSetException("$resultGroupSpecFieldPath.result_group_metric_spec")
      }
    }
  }

  /**
   * Validates [reportingUnit] within the context of a [CreateBasicReportRequest].
   *
   * @param fieldPath Path of [reportingUnit] relative to the request message
   * @param campaignGroupInfo Information about the Campaign Group for the BasicReport
   */
  private fun validateReportingUnit(
    reportingUnit: ReportingUnit,
    fieldPath: String,
    campaignGroupInfo: CampaignGroupInfo,
  ) {
    if (reportingUnit.componentsList.isEmpty()) {
      throw InvalidFieldValueException("$fieldPath.components")
    }

    reportingUnit.componentsList.forEachIndexed { index, component ->
      DataProviderKey.fromName(component)
        ?: throw InvalidFieldValueException("$fieldPath.components[$index]") { fieldPath ->
          "$fieldPath is not a valid data provider name"
        }

      if (!campaignGroupInfo.dataProviderNames.contains(component)) {
        throw DataProviderNotFoundForCampaignGroupException(component, campaignGroupInfo.name)
      }
    }
  }

  /**
   * Validates [dimensionSpec] within the context of a [CreateBasicReportRequest].
   *
   * @param dimensionSpec [DimensionSpec] to validate
   * @param fieldPath Path of [dimensionSpec] relative to the request
   * @param eventTemplateFieldsByPath Map of EventTemplate field path with respect to Event message
   *   to info for the field. Used for validating [EventTemplateField]
   * @throws RequiredFieldNotSetException
   * @throws InvalidFieldValueException
   * @throws EventTemplateFieldInvalidException
   */
  private fun validateDimensionSpec(
    dimensionSpec: DimensionSpec,
    fieldPath: String,
    eventTemplateFieldsByPath: Map<String, EventMessageDescriptor.EventTemplateFieldInfo>,
  ) {
    val groupingEventTemplateFieldsSet: Set<String> = buildSet {
      if (dimensionSpec.hasGrouping()) {
        validateDimensionSpecGrouping(
          dimensionSpec.grouping,
          "$fieldPath.grouping",
          eventTemplateFieldsByPath,
        )
        for (eventTemplateFieldPath in dimensionSpec.grouping.eventTemplateFieldsList) {
          add(eventTemplateFieldPath)
        }
      }
    }

    dimensionSpec.filtersList.forEachIndexed { index, eventFilter ->
      val filterFieldPath = "$fieldPath.filters[$index]"
      if (eventFilter.termsList.isEmpty()) {
        throw RequiredFieldNotSetException("$filterFieldPath.terms")
      }
      if (eventFilter.termsList.size > 1) {
        throw InvalidFieldValueException("$filterFieldPath.terms") { fieldName ->
          "$fieldName can only have a size of 1"
        }
      }

      eventFilter.termsList.forEachIndexed { index, term ->
        val termFieldPath = "$filterFieldPath.terms[$index]"
        validateDimensionSpecEventTemplateField(term, termFieldPath, eventTemplateFieldsByPath)
        if (groupingEventTemplateFieldsSet.contains(term.path)) {
          throw InvalidFieldValueException(termFieldPath) { fieldName ->
            "$fieldName is already in $fieldPath.grouping.event_template_fields for the same dimension_spec"
          }
        }
      }
    }
  }

  /**
   * Validates [grouping] within the context of a [CreateBasicReportRequest].
   *
   * @param grouping [DimensionSpec.Grouping] to validate
   * @param fieldPath Path of [grouping] relative to the request
   * @param eventTemplateFieldsByPath Map of EventTemplate field path with respect to Event message
   *   to info for the field. Used for validating [EventTemplateField]
   * @throws RequiredFieldNotSetException
   * @throws EventTemplateFieldInvalidException
   */
  private fun validateDimensionSpecGrouping(
    grouping: DimensionSpec.Grouping,
    fieldPath: String,
    eventTemplateFieldsByPath: Map<String, EventMessageDescriptor.EventTemplateFieldInfo>,
  ) {
    if (grouping.eventTemplateFieldsList.isEmpty()) {
      throw RequiredFieldNotSetException("$fieldPath.event_template_fields")
    }

    for (eventTemplateFieldPath in grouping.eventTemplateFieldsList) {
      val eventTemplateFieldInfo =
        eventTemplateFieldsByPath[eventTemplateFieldPath]
          ?: throw EventTemplateFieldInvalidException(eventTemplateFieldPath) {
            eventTemplateFieldPath ->
            "$eventTemplateFieldPath does not exist in event message"
          }

      if (!eventTemplateFieldInfo.supportedReportingFeatures.groupable) {
        throw EventTemplateFieldInvalidException(eventTemplateFieldPath) { eventTemplateFieldPath ->
          "$eventTemplateFieldPath is not groupable"
        }
      }

      if (!eventTemplateFieldInfo.isPopulationAttribute) {
        throw EventTemplateFieldInvalidException(eventTemplateFieldPath) { eventTemplateFieldPath ->
          "$eventTemplateFieldPath is not a population attribute"
        }
      }
    }
  }

  /**
   * Validates [eventTemplateField] within the context of a [CreateBasicReportRequest].
   *
   * @param eventTemplateField [EventTemplateField] to validate
   * @param fieldPath Path of [eventTemplateField] relative to the request
   * @param eventTemplateFieldsByPath Map of EventTemplate field path with respect to Event message
   *   to info for the field. Used for validating [EventTemplateField]
   * @throws EventTemplateFieldInvalidException
   * @throws RequiredFieldNotSetException
   * @throws InvalidFieldValueException
   */
  private fun validateDimensionSpecEventTemplateField(
    eventTemplateField: EventTemplateField,
    fieldPath: String,
    eventTemplateFieldsByPath: Map<String, EventMessageDescriptor.EventTemplateFieldInfo>,
  ) {
    if (eventTemplateField.path.isEmpty()) {
      throw RequiredFieldNotSetException("$fieldPath.path")
    }
    if (!eventTemplateField.hasValue()) {
      throw RequiredFieldNotSetException("$fieldPath.value")
    }

    val eventTemplateFieldInfo =
      eventTemplateFieldsByPath[eventTemplateField.path]
        ?: throw EventTemplateFieldInvalidException(eventTemplateField.path)

    if (!eventTemplateFieldInfo.supportedReportingFeatures.filterable) {
      throw EventTemplateFieldInvalidException(eventTemplateField.path) { eventTemplateFieldPath ->
        "$eventTemplateFieldPath is not filterable"
      }
    }
    if (!eventTemplateFieldInfo.isPopulationAttribute) {
      throw EventTemplateFieldInvalidException(eventTemplateField.path) { eventTemplateFieldPath ->
        "$eventTemplateFieldPath is not a population attribute"
      }
    }

    validateEventTemplateFieldValue(eventTemplateField, fieldPath, eventTemplateFieldInfo)
  }

  /**
   * Validates [eventTemplateField] in the context of a [CreateBasicReportRequest].
   *
   * @param fieldPath Path of [eventTemplateField] relative to the request
   * @throws EventTemplateFieldInvalidException
   * @throws RequiredFieldNotSetException
   */
  private fun validateEventTemplateFieldValue(
    eventTemplateField: EventTemplateField,
    fieldPath: String,
    eventTemplateFieldInfo: EventMessageDescriptor.EventTemplateFieldInfo,
  ) {
    when (eventTemplateField.value.selectorCase) {
      EventTemplateField.FieldValue.SelectorCase.STRING_VALUE -> {
        if (eventTemplateFieldInfo.type != Descriptors.FieldDescriptor.Type.STRING) {
          throw EventTemplateFieldInvalidException(eventTemplateField.path) { eventTemplateFieldPath
            ->
            "Incorrect value type specified for template field $eventTemplateFieldPath"
          }
        }
      }

      EventTemplateField.FieldValue.SelectorCase.ENUM_VALUE -> {
        if (eventTemplateFieldInfo.type != Descriptors.FieldDescriptor.Type.ENUM) {
          throw EventTemplateFieldInvalidException(eventTemplateField.path) { eventTemplateFieldPath
            ->
            "Incorrect value type specified for template field $eventTemplateFieldPath"
          }
        }
        if (
          eventTemplateFieldInfo.enumType!!.findValueByName(eventTemplateField.value.enumValue) ==
            null
        ) {
          throw EventTemplateFieldInvalidException(eventTemplateField.path) { eventTemplateFieldPath
            ->
            "Invalid enum value specified for template field $eventTemplateFieldPath"
          }
        }
      }

      EventTemplateField.FieldValue.SelectorCase.BOOL_VALUE ->
        if (eventTemplateFieldInfo.type != Descriptors.FieldDescriptor.Type.BOOL) {
          throw EventTemplateFieldInvalidException(eventTemplateField.path) { eventTemplateFieldPath
            ->
            "Incorrect value type specified for template field $eventTemplateFieldPath"
          }
        }

      EventTemplateField.FieldValue.SelectorCase.FLOAT_VALUE -> {
        throw EventTemplateFieldInvalidException(eventTemplateField.path) { eventTemplateFieldPath
          ->
          "Incorrect value type specified for template field $eventTemplateFieldPath"
        }
      }

      EventTemplateField.FieldValue.SelectorCase.SELECTOR_NOT_SET -> {
        throw RequiredFieldNotSetException("$fieldPath.value.selector")
      }
    }
  }

  /**
   * Validates [term] of an Impression Qualification Filter (IQF) spec in the context of a
   * [CreateBasicReportRequest].
   *
   * @param term [EventTemplateField] to validate
   * @param fieldPath Path of [term] relative to the request
   * @param eventTemplateFieldsByPath Map of EventTemplate field path with respect to Event message
   *   to info for the field. Used for validating [EventTemplateField]
   * @param mediaType [MediaType] to check against if set
   * @throws EventTemplateFieldInvalidException
   */
  private fun validateIqfTerm(
    term: EventTemplateField,
    fieldPath: String,
    eventTemplateFieldsByPath: Map<String, EventMessageDescriptor.EventTemplateFieldInfo>,
    mediaType: MediaType,
  ) {
    if (term.path.isEmpty()) {
      throw RequiredFieldNotSetException("$fieldPath.path")
    }
    if (!term.hasValue()) {
      throw RequiredFieldNotSetException("$fieldPath.value")
    }

    val eventTemplateFieldInfo =
      eventTemplateFieldsByPath[term.path]
        ?: throw EventTemplateFieldInvalidException(term.path) { eventTemplateFieldPath ->
          "$eventTemplateFieldPath does not exist in event message"
        }

    if (!eventTemplateFieldInfo.supportedReportingFeatures.impressionQualification) {
      throw EventTemplateFieldInvalidException(term.path) { eventTemplateFieldPath ->
        "$eventTemplateFieldPath cannot be used for impression qualification"
      }
    }

    if (eventTemplateFieldInfo.mediaType != mediaType.toEventAnnotationMediaType()) {
      throw InvalidFieldValueException("$fieldPath.path") { fieldName ->
        "$fieldName does not have same media_type as parent ImpressionQualificationFilterSpec"
      }
    }

    validateEventTemplateFieldValue(term, fieldPath, eventTemplateFieldInfo)
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
  private fun validateResultGroupMetricSpec(
    resultGroupMetricSpec: ResultGroupMetricSpec,
    fieldPath: String,
    metricFrequencySelectorCase: MetricFrequencySpec.SelectorCase,
  ) {
    if (resultGroupMetricSpec.hasComponentIntersection()) {
      throw FieldUnimplementedException("$fieldPath.component_intersection")
    }

    if (metricFrequencySelectorCase == MetricFrequencySpec.SelectorCase.TOTAL) {
      if (resultGroupMetricSpec.reportingUnit.hasNonCumulative()) {
        throw InvalidFieldValueException("$fieldPath.reporting_unit.non_cumulative") { fieldName ->
          "$fieldName cannot be specified when metric_frequency is total"
        }
      }

      if (resultGroupMetricSpec.component.hasNonCumulative()) {
        throw InvalidFieldValueException("$fieldPath.component.non_cumulative") { fieldName ->
          "$fieldName cannot be specified when metric_frequency is total"
        }
      }

      if (resultGroupMetricSpec.component.hasNonCumulativeUnique()) {
        throw InvalidFieldValueException("$fieldPath.component.non_cumulative_unique") { fieldName
          ->
          "$fieldName cannot be specified when metric_frequency is total"
        }
      }
    }

    validateBasicMetricSetSpec(
      basicMetricSetSpec = resultGroupMetricSpec.reportingUnit.cumulative,
      isWeeklyCumulative = metricFrequencySelectorCase == MetricFrequencySpec.SelectorCase.WEEKLY,
      fieldPath = "$fieldPath.reporting_unit.cumulative",
    )
    validateBasicMetricSetSpec(
      basicMetricSetSpec = resultGroupMetricSpec.reportingUnit.nonCumulative,
      isWeeklyCumulative = false,
      fieldPath = "$fieldPath.reporting_unit.non_cumulative",
    )
    validateBasicMetricSetSpec(
      basicMetricSetSpec = resultGroupMetricSpec.component.nonCumulative,
      isWeeklyCumulative = false,
      fieldPath = "$fieldPath.component.non_cumulative",
    )
    validateBasicMetricSetSpec(
      basicMetricSetSpec = resultGroupMetricSpec.component.cumulative,
      isWeeklyCumulative = metricFrequencySelectorCase == MetricFrequencySpec.SelectorCase.WEEKLY,
      fieldPath = "$fieldPath.component.cumulative",
    )

    if (
      resultGroupMetricSpec.reportingUnit.stackedIncrementalReach &&
        metricFrequencySelectorCase != MetricFrequencySpec.SelectorCase.TOTAL
    ) {
      throw InvalidFieldValueException("$fieldPath.reporting_unit.stacked_incremental_reach") {
        fieldName ->
        "$fieldName requires metric_frequency to be total"
      }
    }
  }

  /**
   * Validates [reportingInterval] within the context of a [CreateBasicReportRequest].
   *
   * @param reportingInterval [ReportingInterval] to validate
   * @throws RequiredFieldNotSetException when validation fails
   * @throws InvalidFieldValueException when validation fails
   */
  private fun validateReportingInterval(reportingInterval: ReportingInterval) {
    val fieldPath = "basic_report.reporting_interval"
    if (!reportingInterval.hasReportStart()) {
      throw RequiredFieldNotSetException("$fieldPath.report_start")
    }

    if (!reportingInterval.hasReportEnd()) {
      throw RequiredFieldNotSetException("$fieldPath.report_end")
    }

    if (
      reportingInterval.reportStart.year == 0 ||
        reportingInterval.reportStart.month == 0 ||
        reportingInterval.reportStart.day == 0 ||
        !(reportingInterval.reportStart.hasTimeZone() ||
          reportingInterval.reportStart.hasUtcOffset())
    ) {
      throw InvalidFieldValueException("$fieldPath.report_start") { fieldName ->
        "$fieldName requires year, month, and day to all be set, as well as either time_zone or utc_offset"
      }
    }

    if (
      reportingInterval.reportEnd.year == 0 ||
        reportingInterval.reportEnd.month == 0 ||
        reportingInterval.reportEnd.day == 0
    ) {
      throw InvalidFieldValueException("$fieldPath.report_end") { fieldName ->
        "$fieldName requires year, month, and day to be set"
      }
    }
  }

  /**
   * Validates [reportingImpressionQualificationFilters] within the context of a
   * [CreateBasicReportRequest].
   *
   * @param reportingImpressionQualificationFilters [List] of
   *   [ReportingImpressionQualificationFilter] to validate
   * @param eventTemplateFieldsByPath Map of EventTemplate field path with respect to Event message
   *   to info for the field. Used for validating [EventTemplateField]
   * @throws [RequiredFieldNotSetException] when validation fails
   * @throws [InvalidFieldValueException] when validation fails
   */
  private fun validateReportingImpressionQualificationFilters(
    reportingImpressionQualificationFilters: List<ReportingImpressionQualificationFilter>,
    eventTemplateFieldsByPath: Map<String, EventMessageDescriptor.EventTemplateFieldInfo>,
  ): Set<ImpressionQualificationFilterKey> {
    val fieldPath = "basic_report.impression_qualification_filters"

    return buildSet {
      var customImpressionQualificationFilterUsed = false
      reportingImpressionQualificationFilters.forEachIndexed { index, reportingIqf ->
        val reportingIqfFieldPath = "$fieldPath[$index]"
        when (reportingIqf.selectorCase) {
          ReportingImpressionQualificationFilter.SelectorCase.IMPRESSION_QUALIFICATION_FILTER -> {
            val key =
              ImpressionQualificationFilterKey.fromName(reportingIqf.impressionQualificationFilter)
                ?: throw InvalidFieldValueException(
                  "$reportingIqfFieldPath.impression_qualification_filter"
                )
            add(key)
          }

          ReportingImpressionQualificationFilter.SelectorCase.CUSTOM -> {
            if (customImpressionQualificationFilterUsed) {
              throw InvalidFieldValueException(fieldPath) { fieldPath ->
                "$fieldPath may have at most one entry with a custom filter"
              }
            }
            validateCustomImpressionQualificationFilterSpec(
              reportingIqf.custom,
              "$reportingIqfFieldPath.custom",
              eventTemplateFieldsByPath,
            )

            customImpressionQualificationFilterUsed = true
          }

          ReportingImpressionQualificationFilter.SelectorCase.SELECTOR_NOT_SET ->
            throw RequiredFieldNotSetException("$reportingIqfFieldPath.selector")
        }
      }
    }
  }

  /**
   * Validates a [CustomImpressionQualificationFilterSpec]
   *
   * @param customImpressionQualificationFilterSpec [CustomImpressionQualificationFilterSpec] to
   *   validate
   * @param eventTemplateFieldsByPath Map of EventTemplate field path with respect to Event message
   *   to info for the field. Used for validating [EventTemplateField]
   * @throws [RequiredFieldNotSetException] when validation fails
   * @throws [InvalidFieldValueException] when validation fails
   */
  private fun validateCustomImpressionQualificationFilterSpec(
    customImpressionQualificationFilterSpec: CustomImpressionQualificationFilterSpec,
    fieldPath: String,
    eventTemplateFieldsByPath: Map<String, EventMessageDescriptor.EventTemplateFieldInfo>,
  ) {
    if (customImpressionQualificationFilterSpec.filterSpecList.isEmpty()) {
      throw RequiredFieldNotSetException("$fieldPath.filter_spec")
    }

    // No more than 1 filter_spec per MediaType
    buildSet<MediaType> {
      customImpressionQualificationFilterSpec.filterSpecList.forEachIndexed { index, filterSpec ->
        if (this.contains(filterSpec.mediaType)) {
          throw InvalidFieldValueException("$fieldPath.filter_specs") { fieldName ->
            "$fieldName cannot have more than 1 filter_spec for MediaType ${filterSpec.mediaType}. Only 1 filter_spec per MediaType allowed"
          }
        }
        add(filterSpec.mediaType)

        val filterSpecFieldPath = "$fieldPath.filter_specs[$index]"
        if (filterSpec.filtersList.isEmpty()) {
          throw RequiredFieldNotSetException("$filterSpecFieldPath.filters")
        }

        filterSpec.filtersList.forEachIndexed { filterIndex, filter ->
          val filterFieldPath = "$filterSpecFieldPath.filters[$filterIndex]"
          if (filter.termsList.isEmpty()) {
            throw RequiredFieldNotSetException("$filterFieldPath.terms")
          }
          if (filter.termsList.size > 1) {
            throw InvalidFieldValueException("$filterFieldPath.terms") { fieldName ->
              "$fieldName can only have a size of 1"
            }
          }

          filter.termsList.forEachIndexed { termIndex, term ->
            validateIqfTerm(
              term,
              "$filterFieldPath.terms[$termIndex]",
              eventTemplateFieldsByPath,
              filterSpec.mediaType,
            )
          }
        }
      }
    }
  }

  /**
   * Validates [basicMetricSetSpec] within the context of a [CreateBasicReportRequest],
   *
   * @param basicMetricSetSpec [ResultGroupMetricSpec.BasicMetricSetSpec] to validate
   * @param fieldPath Path of [basicMetricSetSpec] relative to the request
   * @throws InvalidFieldValueException
   * @throws FieldUnimplementedException
   */
  private fun validateBasicMetricSetSpec(
    basicMetricSetSpec: ResultGroupMetricSpec.BasicMetricSetSpec,
    isWeeklyCumulative: Boolean,
    fieldPath: String,
  ) {
    if (isWeeklyCumulative) {
      if (
        basicMetricSetSpec.impressions ||
          basicMetricSetSpec.grps ||
          basicMetricSetSpec.averageFrequency ||
          basicMetricSetSpec.kPlusReach > 0 ||
          basicMetricSetSpec.percentKPlusReach
      ) {
        throw FieldUnimplementedException(fieldPath) { fieldName ->
          "$fieldName only supports reach and percent_reach when metric_frequency weekly"
        }
      }
    }

    if (basicMetricSetSpec.percentKPlusReach) {
      if (basicMetricSetSpec.kPlusReach <= 0) {
        throw InvalidFieldValueException("$fieldPath.k_plus_reach") { fieldName ->
          "$fieldName must have a positive value"
        }
      }
    }
  }
}
