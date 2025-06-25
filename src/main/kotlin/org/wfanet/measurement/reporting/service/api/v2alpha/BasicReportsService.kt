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

import com.google.longrunning.Operation
import com.google.protobuf.InvalidProtocolBufferException
import io.grpc.Status
import io.grpc.StatusException
import io.grpc.StatusRuntimeException
import java.util.UUID
import kotlin.coroutines.CoroutineContext
import kotlin.coroutines.EmptyCoroutineContext
import org.wfanet.measurement.access.client.v1alpha.Authorization
import org.wfanet.measurement.access.client.v1alpha.check
import org.wfanet.measurement.api.v2alpha.DataProviderKey
import org.wfanet.measurement.api.v2alpha.EventGroupKey
import org.wfanet.measurement.api.v2alpha.MeasurementConsumerKey
import org.wfanet.measurement.common.api.ResourceIds
import org.wfanet.measurement.common.base64UrlDecode
import org.wfanet.measurement.common.base64UrlEncode
import org.wfanet.measurement.internal.reporting.v2.BasicReport as InternalBasicReport
import org.wfanet.measurement.internal.reporting.v2.BasicReportsGrpcKt.BasicReportsCoroutineStub
import org.wfanet.measurement.internal.reporting.v2.ImpressionQualificationFiltersGrpcKt.ImpressionQualificationFiltersCoroutineStub
import org.wfanet.measurement.internal.reporting.v2.ListBasicReportsPageToken
import org.wfanet.measurement.internal.reporting.v2.ListBasicReportsPageTokenKt
import org.wfanet.measurement.internal.reporting.v2.ListBasicReportsRequest as InternalListBasicReportsRequest
import org.wfanet.measurement.internal.reporting.v2.ListBasicReportsRequestKt as InternalListBasicReportsRequestKt
import org.wfanet.measurement.internal.reporting.v2.ReportingSetsGrpcKt.ReportingSetsCoroutineStub
import org.wfanet.measurement.internal.reporting.v2.batchGetReportingSetsRequest
import org.wfanet.measurement.internal.reporting.v2.getBasicReportRequest as internalGetBasicReportRequest
import org.wfanet.measurement.internal.reporting.v2.getImpressionQualificationFilterRequest
import org.wfanet.measurement.internal.reporting.v2.listBasicReportsPageToken
import org.wfanet.measurement.internal.reporting.v2.listBasicReportsRequest as internalListBasicReportsRequest
import org.wfanet.measurement.reporting.service.api.ArgumentChangedInRequestForNextPageException
import org.wfanet.measurement.reporting.service.api.BasicReportNotFoundException
import org.wfanet.measurement.reporting.service.api.InvalidFieldValueException
import org.wfanet.measurement.reporting.service.api.RequiredFieldNotSetException
import org.wfanet.measurement.reporting.service.internal.Errors as InternalErrors
import org.wfanet.measurement.reporting.v2alpha.BasicReport
import org.wfanet.measurement.reporting.v2alpha.BasicReportsGrpcKt.BasicReportsCoroutineImplBase
import org.wfanet.measurement.reporting.v2alpha.CreateBasicReportRequest
import org.wfanet.measurement.reporting.v2alpha.EventTemplateField
import org.wfanet.measurement.reporting.v2alpha.GetBasicReportRequest
import org.wfanet.measurement.reporting.v2alpha.ImpressionQualificationFilterSpec
import org.wfanet.measurement.reporting.v2alpha.ListBasicReportsRequest
import org.wfanet.measurement.reporting.v2alpha.ListBasicReportsResponse
import org.wfanet.measurement.reporting.v2alpha.MediaType
import org.wfanet.measurement.reporting.v2alpha.MetricFrequencySpec
import org.wfanet.measurement.reporting.v2alpha.ReportingSet
import org.wfanet.measurement.reporting.v2alpha.listBasicReportsResponse

class BasicReportsService(
  private val internalBasicReportsStub: BasicReportsCoroutineStub,
  private val internalImpressionQualificationFiltersStub:
    ImpressionQualificationFiltersCoroutineStub,
  private val internalReportingSetsStub: ReportingSetsCoroutineStub,
  private val authorization: Authorization,
  coroutineContext: CoroutineContext = EmptyCoroutineContext,
) : BasicReportsCoroutineImplBase(coroutineContext) {

  override suspend fun createBasicReport(request: CreateBasicReportRequest): Operation {
    if (request.parent.isEmpty()) {
      throw RequiredFieldNotSetException("parent")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }

    val measurementConsumerKey =
      MeasurementConsumerKey.fromName(request.parent)
        ?: throw InvalidFieldValueException("parent")
          .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)

    authorization.check(listOf(request.parent, measurementConsumerKey.toName()), Permission.CREATE)

    if (request.basicReport.campaignGroup.isEmpty()) {
      throw RequiredFieldNotSetException("basic_report.campaign_group")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }

    val campaignGroupKey =
      ReportingSetKey.fromName(request.basicReport.campaignGroup)
        ?: throw InvalidFieldValueException("basic_report.campaign_group")
          .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)

    // Required for creating Report
    val campaignGroup: ReportingSet =
      try {
        internalReportingSetsStub
          .batchGetReportingSets(
            batchGetReportingSetsRequest {
              cmmsMeasurementConsumerId = campaignGroupKey.cmmsMeasurementConsumerId
              externalReportingSetIds += campaignGroupKey.reportingSetId
            }
          )
          .reportingSetsList
          .first()
          .toReportingSet()
      } catch (e: StatusException) {
        throw when (e.status.code) {
          Status.Code.NOT_FOUND ->
            throw InvalidFieldValueException("basic_report.campaign_group") { "Not found" }
              .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
          Status.Code.DEADLINE_EXCEEDED -> Status.DEADLINE_EXCEEDED
          Status.Code.INTERNAL -> Status.INTERNAL
          Status.Code.CANCELLED -> Status.CANCELLED
          else -> Status.UNKNOWN
        }.asRuntimeException()
      }

    validateCreateBasicReportRequest(request, campaignGroup)

    // Validates that IQFs exist, but also constructs a List required for creating Report
    val impressionQualificationFilterSpecsLists: List<List<ImpressionQualificationFilterSpec>> =
      buildList {
        for (impressionQualificationFilter in
          request.basicReport.impressionQualificationFiltersList) {
          if (impressionQualificationFilter.hasImpressionQualificationFilter()) {
            val key =
              ImpressionQualificationFilterKey.fromName(
                impressionQualificationFilter.impressionQualificationFilter
              )
            try {
              add(
                internalImpressionQualificationFiltersStub
                  .getImpressionQualificationFilter(
                    getImpressionQualificationFilterRequest {
                      externalImpressionQualificationFilterId =
                        key!!.impressionQualificationFilterId
                    }
                  )
                  .toImpressionQualificationFilter()
                  .filterSpecsList
              )
            } catch (e: StatusException) {
              throw when (InternalErrors.getReason(e)) {
                InternalErrors.Reason.IMPRESSION_QUALIFICATION_FILTER_NOT_FOUND ->
                  InvalidFieldValueException(
                      "basic_report.impression_qualification_filters.impression_qualification_filter",
                      e,
                    ) {
                      "ImpressionQualificationFilter ${impressionQualificationFilter.impressionQualificationFilter} not found"
                    }
                    .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
                InternalErrors.Reason.BASIC_REPORT_NOT_FOUND,
                InternalErrors.Reason.MEASUREMENT_CONSUMER_NOT_FOUND,
                InternalErrors.Reason.BASIC_REPORT_ALREADY_EXISTS,
                InternalErrors.Reason.REQUIRED_FIELD_NOT_SET,
                InternalErrors.Reason.INVALID_FIELD_VALUE,
                InternalErrors.Reason.METRIC_NOT_FOUND,
                InternalErrors.Reason.INVALID_METRIC_STATE_TRANSITION,
                null -> Status.INTERNAL.withCause(e).asRuntimeException()
              }
            }
          } else if (impressionQualificationFilter.hasCustom()) {
            add(impressionQualificationFilter.custom.filterSpecList)
          }
        }
      }

    // TODO(@tristanvuong2021): Will be implemented for phase 2
    return super.createBasicReport(request)
  }

  override suspend fun getBasicReport(request: GetBasicReportRequest): BasicReport {
    if (request.name.isEmpty()) {
      throw RequiredFieldNotSetException("name")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }

    val (measurementConsumerKey, basicReportId) =
      BasicReportKey.fromName(request.name)
        ?: throw InvalidFieldValueException("name")
          .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)

    authorization.check(listOf(request.name, measurementConsumerKey.toName()), Permission.GET)

    val internalBasicReport: InternalBasicReport =
      try {
        internalBasicReportsStub.getBasicReport(
          internalGetBasicReportRequest {
            cmmsMeasurementConsumerId = measurementConsumerKey.measurementConsumerId
            externalBasicReportId = basicReportId
          }
        )
      } catch (e: StatusException) {
        throw when (InternalErrors.getReason(e)) {
          InternalErrors.Reason.BASIC_REPORT_NOT_FOUND ->
            BasicReportNotFoundException(request.name, e)
              .asStatusRuntimeException(Status.Code.NOT_FOUND)
          InternalErrors.Reason.MEASUREMENT_CONSUMER_NOT_FOUND,
          InternalErrors.Reason.BASIC_REPORT_ALREADY_EXISTS,
          InternalErrors.Reason.REQUIRED_FIELD_NOT_SET,
          InternalErrors.Reason.INVALID_FIELD_VALUE,
          InternalErrors.Reason.METRIC_NOT_FOUND,
          InternalErrors.Reason.INVALID_METRIC_STATE_TRANSITION,
          InternalErrors.Reason.IMPRESSION_QUALIFICATION_FILTER_NOT_FOUND,
          null -> Status.INTERNAL.withCause(e).asRuntimeException()
        }
      }

    return internalBasicReport.toBasicReport()
  }

  override suspend fun listBasicReports(
    request: ListBasicReportsRequest
  ): ListBasicReportsResponse {
    if (request.parent.isEmpty()) {
      throw RequiredFieldNotSetException("parent")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }

    if (request.pageSize < 0) {
      throw InvalidFieldValueException("page_size")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }

    authorization.check(request.parent, Permission.LIST)

    val internalListBasicReportsResponse =
      try {
        val internalRequest: InternalListBasicReportsRequest = request.toInternal()
        internalBasicReportsStub.listBasicReports(internalRequest)
      } catch (e: InvalidFieldValueException) {
        throw e.asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
      } catch (e: ArgumentChangedInRequestForNextPageException) {
        throw e.asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
      } catch (e: StatusException) {
        throw when (InternalErrors.getReason(e)) {
          InternalErrors.Reason.INVALID_FIELD_VALUE ->
            Status.INVALID_ARGUMENT.withCause(e).asRuntimeException()
          InternalErrors.Reason.BASIC_REPORT_NOT_FOUND,
          InternalErrors.Reason.MEASUREMENT_CONSUMER_NOT_FOUND,
          InternalErrors.Reason.BASIC_REPORT_ALREADY_EXISTS,
          InternalErrors.Reason.REQUIRED_FIELD_NOT_SET,
          InternalErrors.Reason.METRIC_NOT_FOUND,
          InternalErrors.Reason.INVALID_METRIC_STATE_TRANSITION,
          InternalErrors.Reason.IMPRESSION_QUALIFICATION_FILTER_NOT_FOUND,
          null -> Status.INTERNAL.withCause(e).asRuntimeException()
        }
      }

    if (internalListBasicReportsResponse.basicReportsList.isEmpty()) {
      return ListBasicReportsResponse.getDefaultInstance()
    }

    return listBasicReportsResponse {
      this.basicReports +=
        internalListBasicReportsResponse.basicReportsList.map { it.toBasicReport() }.toList()
      if (internalListBasicReportsResponse.hasNextPageToken()) {
        nextPageToken =
          internalListBasicReportsResponse.nextPageToken.toByteString().base64UrlEncode()
      }
    }
  }

  /**
   * Validates a [CreateBasicReportRequest].
   *
   * @param request
   * @throws [StatusRuntimeException] when validation fails
   */
  private fun validateCreateBasicReportRequest(
    request: CreateBasicReportRequest,
    campaignGroup: ReportingSet,
  ) {
    if (request.basicReportId.isEmpty()) {
      throw RequiredFieldNotSetException("basic_report_id")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }

    if (!request.basicReportId.matches(RESOURCE_ID_REGEX)) {
      throw InvalidFieldValueException("basic_report_id")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }

    if (request.requestId.isNotEmpty()) {
      try {
        UUID.fromString(request.requestId)
      } catch (e: IllegalArgumentException) {
        throw InvalidFieldValueException("request_id") { "Not a UUID" }
          .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
      }
    }

    if (campaignGroup.campaignGroup != campaignGroup.name) {
      throw InvalidFieldValueException("basic_report.campaign_group") { "Not a Campaign Group" }
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }

    if (!request.basicReport.hasReportingInterval()) {
      throw RequiredFieldNotSetException("basic_report.reporting_interval")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }

    if (!request.basicReport.reportingInterval.hasReportStart()) {
      throw RequiredFieldNotSetException("basic_report.reporting_interval.report_start")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }

    if (!request.basicReport.reportingInterval.hasReportEnd()) {
      throw RequiredFieldNotSetException("basic_report.reporting_interval.report_end")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }

    if (
      request.basicReport.reportingInterval.reportStart.year == 0 ||
        request.basicReport.reportingInterval.reportStart.month == 0 ||
        request.basicReport.reportingInterval.reportStart.day == 0 ||
        !(request.basicReport.reportingInterval.reportStart.hasTimeZone() ||
          request.basicReport.reportingInterval.reportStart.hasUtcOffset())
    ) {
      throw InvalidFieldValueException("basic_report.reporting_interval.report_start")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }

    if (
      request.basicReport.reportingInterval.reportEnd.year == 0 ||
        request.basicReport.reportingInterval.reportEnd.month == 0 ||
        request.basicReport.reportingInterval.reportEnd.day == 0
    ) {
      throw InvalidFieldValueException("basic_report.reporting_interval.report_end")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }

    if (request.basicReport.impressionQualificationFiltersList.isEmpty()) {
      throw RequiredFieldNotSetException("basic_report.impression_qualification_filters")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }

    for (impressionQualificationFilter in request.basicReport.impressionQualificationFiltersList) {
      if (impressionQualificationFilter.hasImpressionQualificationFilter()) {
        ImpressionQualificationFilterKey.fromName(
          impressionQualificationFilter.impressionQualificationFilter
        )
          ?: throw InvalidFieldValueException(
              "basic_report.impression_qualification_filters.impression_qualification_filter"
            )
            .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
      } else if (impressionQualificationFilter.hasCustom()) {
        if (impressionQualificationFilter.custom.filterSpecList.isEmpty()) {
          throw RequiredFieldNotSetException(
              "basic_report.impression_qualification_filters.custom.filter_spec"
            )
            .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
        }

        // No more than 1 filter_spec per MediaType
        buildSet<MediaType> {
          for (filterSpec in impressionQualificationFilter.custom.filterSpecList) {
            if (this.contains(filterSpec.mediaType)) {
              throw InvalidFieldValueException(
                  "basic_report.impression_qualification_filters.custom.filter_spec"
                ) {
                  "More than 1 filter_spec for a MediaType"
                }
                .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
            }
            add(filterSpec.mediaType)

            if (filterSpec.filtersList.isEmpty()) {
              throw RequiredFieldNotSetException(
                  "basic_report.impression_qualification_filters.custom.filter_spec.filters"
                )
                .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
            }

            for (eventFilter in filterSpec.filtersList) {
              if (eventFilter.termsList.size != 1) {
                throw RequiredFieldNotSetException(
                    "basic_report.impression_qualification_filters.custom.filter_spec.filters.terms"
                  )
                  .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
              }

              for (eventTemplateField in eventFilter.termsList) {
                if (eventTemplateField.path.isEmpty()) {
                  throw RequiredFieldNotSetException(
                      "basic_report.impression_qualification_filters.custom.filter_spec.filters.terms.path"
                    )
                    .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
                }

                if (
                  eventTemplateField.value.selectorCase ==
                    EventTemplateField.FieldValue.SelectorCase.SELECTOR_NOT_SET
                ) {
                  throw RequiredFieldNotSetException(
                      "basic_report.impression_qualification_filters.custom.filter_spec.filters.terms.value"
                    )
                    .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
                }
              }
            }
          }
        }
      } else {
        throw InvalidFieldValueException("basic_report.impression_qualification_filters.selector")
          .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
      }
    }

    if (request.basicReport.resultGroupSpecsList.isEmpty()) {
      throw RequiredFieldNotSetException("basic_report.result_group_specs")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }

    val dataProviderNameSet: Set<String> = buildSet {
      for (cmmsEventGroup in campaignGroup.primitive.cmmsEventGroupsList) {
        val eventGroupKey = EventGroupKey.fromName(cmmsEventGroup)
        add(eventGroupKey!!.parentKey.toName())
      }
    }

    for (resultGroupSpec in request.basicReport.resultGroupSpecsList) {
      if (!resultGroupSpec.hasReportingUnit()) {
        throw RequiredFieldNotSetException("basic_report.result_group_specs.reporting_unit")
          .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
      }

      if (resultGroupSpec.reportingUnit.componentsList.isEmpty()) {
        throw InvalidFieldValueException(
            "basic_report.result_group_specs.reporting_unit.components"
          )
          .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
      }

      for (component in resultGroupSpec.reportingUnit.componentsList) {
        DataProviderKey.fromName(component)
          ?: throw InvalidFieldValueException(
              "basic_report.result_group_specs.reporting_unit.components"
            )
            .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)

        if (!dataProviderNameSet.contains(component)) {
          throw InvalidFieldValueException(
              "basic_report.result_group_specs.reporting_unit.components"
            ) {
              "reporting_unit component does not have any cmms_event_groups in campaign_group"
            }
            .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
        }
      }

      if (!resultGroupSpec.hasDimensionSpec()) {
        throw RequiredFieldNotSetException("basic_report.result_group_specs.dimension_spec")
          .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
      }

      if (
        resultGroupSpec.dimensionSpec.hasGrouping() &&
          resultGroupSpec.dimensionSpec.grouping.eventTemplateFieldsList.isEmpty()
      ) {
        throw RequiredFieldNotSetException(
            "basic_report.result_group_specs.dimension_spec.grouping.event_template_fields"
          )
          .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
      }

      for (eventFilter in resultGroupSpec.dimensionSpec.filtersList) {
        if (eventFilter.termsList.size != 1) {
          throw InvalidFieldValueException(
              "basic_report.result_group_specs.dimension_spec.filters.terms"
            )
            .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
        }

        for (eventTemplateField in eventFilter.termsList) {
          if (eventTemplateField.path.isEmpty()) {
            throw RequiredFieldNotSetException(
                "basic_report.result_group_specs.dimension_spec.filters.terms.path"
              )
              .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
          }

          if (
            eventTemplateField.value.selectorCase ==
              EventTemplateField.FieldValue.SelectorCase.SELECTOR_NOT_SET
          ) {
            throw RequiredFieldNotSetException(
                "basic_report.result_group_specs.dimension_spec.filters.terms.value"
              )
              .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
          }
        }
      }

      if (!resultGroupSpec.hasResultGroupMetricSpec()) {
        throw RequiredFieldNotSetException(
            "basic_report.result_group_specs.result_group_metric_spec"
          )
          .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
      }

      if (resultGroupSpec.resultGroupMetricSpec.hasComponentIntersection()) {
        throw InvalidFieldValueException(
            "basic_report.result_group_specs.result_group_metric_spec.component_intersection"
          ) {
            "Not supported at this time"
          }
          .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
      }

      if (
        resultGroupSpec.metricFrequency.selectorCase ==
          MetricFrequencySpec.SelectorCase.SELECTOR_NOT_SET
      ) {
        throw RequiredFieldNotSetException("basic_report.result_group_specs.metric_frequency")
          .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
      }

      if (resultGroupSpec.metricFrequency.selectorCase == MetricFrequencySpec.SelectorCase.TOTAL) {
        if (resultGroupSpec.resultGroupMetricSpec.reportingUnit.hasNonCumulative()) {
          throw InvalidFieldValueException(
              "basic_report.result_group_specs.result_group_metric_spec.reporting_unit.non_cumulative"
            ) {
              "non_cumulative cannot be specified when metric_frequency is total"
            }
            .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
        }

        if (resultGroupSpec.resultGroupMetricSpec.component.hasNonCumulative()) {
          throw InvalidFieldValueException(
              "basic_report.result_group_specs.result_group_metric_spec.component.non_cumulative"
            ) {
              "non_cumulative cannot be specified when metric_frequency is total"
            }
            .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
        }
      }

      if (resultGroupSpec.resultGroupMetricSpec.reportingUnit.nonCumulative.percentKPlusReach) {
        if (resultGroupSpec.resultGroupMetricSpec.reportingUnit.nonCumulative.kPlusReach <= 0) {
          throw InvalidFieldValueException(
              "basic_report.result_group_specs.result_group_metric_spec.reporting_unit.non_cumulative.k_plus_reach"
            ) {
              "percent_k_plus_reach requires k_plus_reach to be positive"
            }
            .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
        }
      }

      if (resultGroupSpec.resultGroupMetricSpec.reportingUnit.cumulative.percentKPlusReach) {
        if (resultGroupSpec.resultGroupMetricSpec.reportingUnit.cumulative.kPlusReach <= 0) {
          throw InvalidFieldValueException(
              "basic_report.result_group_specs.result_group_metric_spec.reporting_unit.cumulative.k_plus_reach"
            ) {
              "percent_k_plus_reach requires k_plus_reach to be positive"
            }
            .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
        }
      }

      if (
        resultGroupSpec.resultGroupMetricSpec.reportingUnit.stackedIncrementalReach &&
          resultGroupSpec.metricFrequency.selectorCase != MetricFrequencySpec.SelectorCase.TOTAL
      ) {
        throw InvalidFieldValueException(
            "basic_report.result_group_specs.result_group_metric_spec.reporting_unit.stacked_incremental_reach"
          ) {
            "stacked_incremental_reach requires metric_frequency to be total"
          }
          .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
      }

      if (resultGroupSpec.resultGroupMetricSpec.component.nonCumulative.percentKPlusReach) {
        if (resultGroupSpec.resultGroupMetricSpec.component.nonCumulative.kPlusReach <= 0) {
          throw InvalidFieldValueException(
              "basic_report.result_group_specs.result_group_metric_spec.component.non_cumulative.k_plus_reach"
            ) {
              "percent_k_plus_reach requires k_plus_reach to be positive"
            }
            .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
        }
      }

      if (resultGroupSpec.resultGroupMetricSpec.component.cumulative.percentKPlusReach) {
        if (resultGroupSpec.resultGroupMetricSpec.component.cumulative.kPlusReach <= 0) {
          throw InvalidFieldValueException(
              "basic_report.result_group_specs.result_group_metric_spec.component.cumulative.k_plus_reach"
            ) {
              "percent_k_plus_reach requires k_plus_reach to be positive"
            }
            .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
        }
      }
    }
  }

  private fun ListBasicReportsRequest.toInternal(): InternalListBasicReportsRequest {
    val source = this

    val measurementConsumerKey =
      MeasurementConsumerKey.fromName(source.parent) ?: throw InvalidFieldValueException("parent")

    val cmmsMeasurementConsumerId = measurementConsumerKey.measurementConsumerId

    return if (source.pageToken.isNotBlank()) {
      val decodedPageToken =
        try {
          ListBasicReportsPageToken.parseFrom(source.pageToken.base64UrlDecode())
        } catch (e: InvalidProtocolBufferException) {
          throw InvalidFieldValueException("page_token")
        }

      if (!decodedPageToken.filter.createTimeAfter.equals(source.filter.createTimeAfter)) {
        throw ArgumentChangedInRequestForNextPageException("filter.create_time_after")
      }

      val finalPageSize =
        if (source.pageSize in 1..MAX_PAGE_SIZE) {
          source.pageSize
        } else if (source.pageSize > MAX_PAGE_SIZE) {
          MAX_PAGE_SIZE
        } else {
          DEFAULT_PAGE_SIZE
        }

      internalListBasicReportsRequest {
        this.filter =
          InternalListBasicReportsRequestKt.filter {
            this.cmmsMeasurementConsumerId = cmmsMeasurementConsumerId
          }
        pageSize = finalPageSize
        pageToken = listBasicReportsPageToken {
          lastBasicReport =
            ListBasicReportsPageTokenKt.previousPageEnd {
              createTime = decodedPageToken.lastBasicReport.createTime
              externalBasicReportId = decodedPageToken.lastBasicReport.externalBasicReportId
            }
        }
      }
    } else {
      val finalPageSize =
        if (source.pageSize in 1..MAX_PAGE_SIZE) {
          source.pageSize
        } else if (source.pageSize > MAX_PAGE_SIZE) {
          MAX_PAGE_SIZE
        } else DEFAULT_PAGE_SIZE

      internalListBasicReportsRequest {
        this.filter =
          InternalListBasicReportsRequestKt.filter {
            this.cmmsMeasurementConsumerId = cmmsMeasurementConsumerId
            if (source.filter.hasCreateTimeAfter()) {
              createTimeAfter = source.filter.createTimeAfter
            }
          }
        pageSize = finalPageSize
      }
    }
  }

  object Permission {
    private const val TYPE = "reporting.basicReports"
    const val CREATE = "$TYPE.create"
    const val GET = "$TYPE.get"
    const val LIST = "$TYPE.list"
  }

  companion object {
    private val RESOURCE_ID_REGEX = ResourceIds.AIP_122_REGEX
    private const val DEFAULT_PAGE_SIZE = 10
    private const val MAX_PAGE_SIZE = 25
  }
}
