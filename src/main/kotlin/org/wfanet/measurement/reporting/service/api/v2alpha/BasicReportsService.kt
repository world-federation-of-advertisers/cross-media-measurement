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

import com.google.protobuf.InvalidProtocolBufferException
import io.grpc.Status
import io.grpc.StatusException
import java.util.UUID
import kotlin.collections.List
import kotlin.coroutines.CoroutineContext
import kotlin.coroutines.EmptyCoroutineContext
import kotlin.random.Random
import kotlinx.coroutines.flow.filter
import org.wfanet.measurement.access.client.v1alpha.Authorization
import org.wfanet.measurement.access.client.v1alpha.check
import org.wfanet.measurement.access.client.v1alpha.withForwardedTrustedCredentials
import org.wfanet.measurement.api.v2alpha.EventGroupKey
import org.wfanet.measurement.api.v2alpha.MeasurementConsumerKey
import org.wfanet.measurement.common.base64UrlDecode
import org.wfanet.measurement.common.base64UrlEncode
import org.wfanet.measurement.config.reporting.MetricSpecConfig
import org.wfanet.measurement.internal.reporting.ErrorCode
import org.wfanet.measurement.internal.reporting.v2.BasicReport as InternalBasicReport
import org.wfanet.measurement.internal.reporting.v2.BasicReportsGrpcKt.BasicReportsCoroutineStub
import org.wfanet.measurement.internal.reporting.v2.ImpressionQualificationFiltersGrpcKt.ImpressionQualificationFiltersCoroutineStub
import org.wfanet.measurement.internal.reporting.v2.ListBasicReportsPageToken
import org.wfanet.measurement.internal.reporting.v2.ListBasicReportsPageTokenKt
import org.wfanet.measurement.internal.reporting.v2.ListBasicReportsRequest as InternalListBasicReportsRequest
import org.wfanet.measurement.internal.reporting.v2.ListBasicReportsRequestKt as InternalListBasicReportsRequestKt
import org.wfanet.measurement.internal.reporting.v2.ListMetricCalculationSpecsRequestKt
import org.wfanet.measurement.internal.reporting.v2.MetricCalculationSpec as InternalMetricCalculationSpec
import org.wfanet.measurement.internal.reporting.v2.MetricCalculationSpec
import org.wfanet.measurement.internal.reporting.v2.MetricCalculationSpecsGrpcKt.MetricCalculationSpecsCoroutineStub as InternalMetricCalculationSpecsCoroutineStub
import org.wfanet.measurement.internal.reporting.v2.MetricSpec
import org.wfanet.measurement.internal.reporting.v2.MetricSpecKt
import org.wfanet.measurement.internal.reporting.v2.ReportingImpressionQualificationFilter as InternalReportingImpressionQualificationFilter
import org.wfanet.measurement.internal.reporting.v2.ReportingSet as InternalReportingSet
import org.wfanet.measurement.internal.reporting.v2.ReportingSetsGrpcKt.ReportingSetsCoroutineStub as InternalReportingSetsCoroutineStub
import org.wfanet.measurement.internal.reporting.v2.StreamReportingSetsRequestKt
import org.wfanet.measurement.internal.reporting.v2.batchGetReportingSetsRequest
import org.wfanet.measurement.internal.reporting.v2.copy
import org.wfanet.measurement.internal.reporting.v2.createBasicReportRequest
import org.wfanet.measurement.internal.reporting.v2.createMetricCalculationSpecRequest
import org.wfanet.measurement.internal.reporting.v2.createReportingSetRequest
import org.wfanet.measurement.internal.reporting.v2.getBasicReportRequest as internalGetBasicReportRequest
import org.wfanet.measurement.internal.reporting.v2.getImpressionQualificationFilterRequest
import org.wfanet.measurement.internal.reporting.v2.listBasicReportsPageToken
import org.wfanet.measurement.internal.reporting.v2.listBasicReportsRequest as internalListBasicReportsRequest
import org.wfanet.measurement.internal.reporting.v2.listMetricCalculationSpecsRequest
import org.wfanet.measurement.internal.reporting.v2.metricCalculationSpec
import org.wfanet.measurement.internal.reporting.v2.metricSpec
import org.wfanet.measurement.internal.reporting.v2.reportingImpressionQualificationFilter
import org.wfanet.measurement.internal.reporting.v2.setExternalReportIdRequest
import org.wfanet.measurement.internal.reporting.v2.streamReportingSetsRequest
import org.wfanet.measurement.reporting.service.api.ArgumentChangedInRequestForNextPageException
import org.wfanet.measurement.reporting.service.api.BasicReportAlreadyExistsException
import org.wfanet.measurement.reporting.service.api.BasicReportNotFoundException
import org.wfanet.measurement.reporting.service.api.Errors
import org.wfanet.measurement.reporting.service.api.ImpressionQualificationFilterNotFoundException
import org.wfanet.measurement.reporting.service.api.InvalidFieldValueException
import org.wfanet.measurement.reporting.service.api.ReportingSetNotFoundException
import org.wfanet.measurement.reporting.service.api.RequiredFieldNotSetException
import org.wfanet.measurement.reporting.service.api.ServiceException
import org.wfanet.measurement.reporting.service.internal.Errors as InternalErrors
import org.wfanet.measurement.reporting.service.internal.ReportingInternalException
import org.wfanet.measurement.reporting.v2alpha.BasicReport
import org.wfanet.measurement.reporting.v2alpha.BasicReportsGrpcKt.BasicReportsCoroutineImplBase
import org.wfanet.measurement.reporting.v2alpha.CreateBasicReportRequest
import org.wfanet.measurement.reporting.v2alpha.GetBasicReportRequest
import org.wfanet.measurement.reporting.v2alpha.ImpressionQualificationFilterSpec
import org.wfanet.measurement.reporting.v2alpha.ListBasicReportsRequest
import org.wfanet.measurement.reporting.v2alpha.ListBasicReportsResponse
import org.wfanet.measurement.reporting.v2alpha.Report
import org.wfanet.measurement.reporting.v2alpha.ReportKt
import org.wfanet.measurement.reporting.v2alpha.ReportingSet
import org.wfanet.measurement.reporting.v2alpha.ReportingSetKt
import org.wfanet.measurement.reporting.v2alpha.ReportsGrpcKt.ReportsCoroutineStub
import org.wfanet.measurement.reporting.v2alpha.createReportRequest
import org.wfanet.measurement.reporting.v2alpha.listBasicReportsResponse
import org.wfanet.measurement.reporting.v2alpha.report
import org.wfanet.measurement.reporting.v2alpha.reportingSet

class BasicReportsService(
  private val internalBasicReportsStub: BasicReportsCoroutineStub,
  private val internalImpressionQualificationFiltersStub:
    ImpressionQualificationFiltersCoroutineStub,
  private val internalReportingSetsStub: InternalReportingSetsCoroutineStub,
  private val internalMetricCalculationSpecsStub: InternalMetricCalculationSpecsCoroutineStub,
  private val reportsStub: ReportsCoroutineStub,
  private val eventDescriptor: EventDescriptor?,
  private val metricSpecConfig: MetricSpecConfig,
  private val secureRandom: Random,
  private val authorization: Authorization,
  coroutineContext: CoroutineContext = EmptyCoroutineContext,
) : BasicReportsCoroutineImplBase(coroutineContext) {
  private sealed class ReportingSetMapKey {
    data class Composite(val composite: ReportingSet.Composite) : ReportingSetMapKey()

    data class Primitive(val cmmsEventGroups: Set<String>) : ReportingSetMapKey()
  }

  private data class ReportingSetMaps(
    // Map of DataProvider resource name to Primitive ReportingSet
    val primitiveReportingSetsByDataProvider: Map<String, ReportingSet>,
    // Map of ReportingSet composite to ReportingSet resource name
    val nameByReportingSetComposite: Map<ReportingSet.Composite, String>,
  )

  override suspend fun createBasicReport(request: CreateBasicReportRequest): BasicReport {
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
    val campaignGroup: ReportingSet = getReportingSet(request.basicReport.campaignGroup)

    val eventTemplateFieldsByPath = eventDescriptor?.eventTemplateFieldsByPath ?: emptyMap()

    try {
      validateCreateBasicReportRequest(request, campaignGroup, eventTemplateFieldsByPath)
    } catch (e: ServiceException) {
      throw when (e.reason) {
        Errors.Reason.REQUIRED_FIELD_NOT_SET,
        Errors.Reason.INVALID_FIELD_VALUE ->
          e.asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
        Errors.Reason.FIELD_UNIMPLEMENTED -> e.asStatusRuntimeException(Status.Code.UNIMPLEMENTED)
        Errors.Reason.BASIC_REPORT_NOT_FOUND,
        Errors.Reason.BASIC_REPORT_ALREADY_EXISTS,
        Errors.Reason.REPORTING_SET_NOT_FOUND,
        Errors.Reason.METRIC_NOT_FOUND,
        Errors.Reason.CAMPAIGN_GROUP_INVALID,
        Errors.Reason.INVALID_METRIC_STATE_TRANSITION,
        Errors.Reason.ARGUMENT_CHANGED_IN_REQUEST_FOR_NEXT_PAGE,
        Errors.Reason.IMPRESSION_QUALIFICATION_FILTER_NOT_FOUND ->
          e.asStatusRuntimeException(Status.Code.INTERNAL) // Shouldn't be reached
      }
    }

    // Validates that IQFs exist, but also constructs a List required for creating Report
    val impressionQualificationFilterSpecsLists:
      MutableList<List<ImpressionQualificationFilterSpec>> =
      mutableListOf()
    val internalReportingImpressionQualificationFilters:
      List<InternalReportingImpressionQualificationFilter> =
      buildList {
        for (impressionQualificationFilter in
          request.basicReport.impressionQualificationFiltersList) {
          if (impressionQualificationFilter.hasImpressionQualificationFilter()) {
            val key =
              ImpressionQualificationFilterKey.fromName(
                impressionQualificationFilter.impressionQualificationFilter
              )
            try {
              val internalImpressionQualificationFilter =
                internalImpressionQualificationFiltersStub.getImpressionQualificationFilter(
                  getImpressionQualificationFilterRequest {
                    externalImpressionQualificationFilterId = key!!.impressionQualificationFilterId
                  }
                )

              add(
                reportingImpressionQualificationFilter {
                  externalImpressionQualificationFilterId =
                    internalImpressionQualificationFilter.externalImpressionQualificationFilterId
                  filterSpecs += internalImpressionQualificationFilter.filterSpecsList
                }
              )

              impressionQualificationFilterSpecsLists.add(
                internalImpressionQualificationFilter
                  .toImpressionQualificationFilter()
                  .filterSpecsList
              )
            } catch (e: StatusException) {
              throw when (InternalErrors.getReason(e)) {
                InternalErrors.Reason.IMPRESSION_QUALIFICATION_FILTER_NOT_FOUND ->
                  ImpressionQualificationFilterNotFoundException(
                      impressionQualificationFilter.impressionQualificationFilter
                    )
                    .asStatusRuntimeException(Status.Code.FAILED_PRECONDITION)
                InternalErrors.Reason.BASIC_REPORT_NOT_FOUND,
                InternalErrors.Reason.MEASUREMENT_CONSUMER_NOT_FOUND,
                InternalErrors.Reason.BASIC_REPORT_ALREADY_EXISTS,
                InternalErrors.Reason.REQUIRED_FIELD_NOT_SET,
                InternalErrors.Reason.INVALID_FIELD_VALUE,
                InternalErrors.Reason.METRIC_NOT_FOUND,
                InternalErrors.Reason.INVALID_METRIC_STATE_TRANSITION,
                InternalErrors.Reason.REPORT_RESULT_NOT_FOUND,
                InternalErrors.Reason.REPORTING_SET_RESULT_NOT_FOUND,
                InternalErrors.Reason.REPORTING_WINDOW_RESULT_NOT_FOUND,
                InternalErrors.Reason.BASIC_REPORT_STATE_INVALID,
                null -> Status.INTERNAL.withCause(e).asRuntimeException()
              }
            }
          } else if (impressionQualificationFilter.hasCustom()) {
            impressionQualificationFilterSpecsLists.add(
              impressionQualificationFilter.custom.filterSpecList
            )
          }
        }
      }

    val createReportRequestId = UUID.randomUUID().toString()

    val createdInternalBasicReport =
      try {
        internalBasicReportsStub.createBasicReport(
          createBasicReportRequest {
            basicReport =
              request.basicReport.toInternal(
                cmmsMeasurementConsumerId = measurementConsumerKey.measurementConsumerId,
                basicReportId = request.basicReportId,
                campaignGroupId = campaignGroupKey.reportingSetId,
                createReportRequestId = createReportRequestId,
                internalReportingImpressionQualificationFilters =
                  internalReportingImpressionQualificationFilters,
              )
            requestId = request.requestId
          }
        )
      } catch (e: StatusException) {
        throw when (InternalErrors.getReason(e)) {
          InternalErrors.Reason.BASIC_REPORT_ALREADY_EXISTS ->
            BasicReportAlreadyExistsException(
                BasicReportKey(
                    cmmsMeasurementConsumerId = measurementConsumerKey.measurementConsumerId,
                    basicReportId = request.basicReportId,
                  )
                  .toName()
              )
              .asStatusRuntimeException(Status.Code.ALREADY_EXISTS)
          InternalErrors.Reason.IMPRESSION_QUALIFICATION_FILTER_NOT_FOUND,
          InternalErrors.Reason.BASIC_REPORT_NOT_FOUND,
          InternalErrors.Reason.MEASUREMENT_CONSUMER_NOT_FOUND,
          InternalErrors.Reason.REQUIRED_FIELD_NOT_SET,
          InternalErrors.Reason.INVALID_FIELD_VALUE,
          InternalErrors.Reason.METRIC_NOT_FOUND,
          InternalErrors.Reason.INVALID_METRIC_STATE_TRANSITION,
          InternalErrors.Reason.REPORT_RESULT_NOT_FOUND,
          InternalErrors.Reason.REPORTING_SET_RESULT_NOT_FOUND,
          InternalErrors.Reason.REPORTING_WINDOW_RESULT_NOT_FOUND,
          InternalErrors.Reason.BASIC_REPORT_STATE_INVALID,
          null -> Status.INTERNAL.withCause(e).asRuntimeException()
        }
      }

    val reportingSetMaps: ReportingSetMaps = buildReportingSetMaps(campaignGroup, campaignGroupKey)

    val reportingSetsMetricCalculationSpecDetailsMap:
      Map<ReportingSet, List<InternalMetricCalculationSpec.Details>> =
      buildReportingSetMetricCalculationSpecDetailsMap(
        campaignGroupName = request.basicReport.campaignGroup,
        impressionQualificationFilterSpecsLists = impressionQualificationFilterSpecsLists,
        dataProviderPrimitiveReportingSetMap =
          reportingSetMaps.primitiveReportingSetsByDataProvider,
        resultGroupSpecs = request.basicReport.resultGroupSpecsList,
        eventTemplateFieldsByPath = eventTemplateFieldsByPath,
      )

    val report: Report =
      buildReport(
        request.basicReport,
        campaignGroupKey,
        reportingSetMaps.nameByReportingSetComposite,
        reportingSetsMetricCalculationSpecDetailsMap,
      )

    val createReportRequest = createReportRequest {
      parent = request.parent
      this.report = report
      requestId = createdInternalBasicReport.createReportRequestId
      reportId = "a${UUID.randomUUID()}"
    }

    reportsStub.withForwardedTrustedCredentials().createReport(createReportRequest)

    internalBasicReportsStub.setExternalReportId(
      setExternalReportIdRequest {
        cmmsMeasurementConsumerId = measurementConsumerKey.measurementConsumerId
        externalBasicReportId = request.basicReportId
        externalReportId = createReportRequest.reportId
      }
    )

    return createdInternalBasicReport.toBasicReport()
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
          InternalErrors.Reason.REPORT_RESULT_NOT_FOUND,
          InternalErrors.Reason.REPORTING_SET_RESULT_NOT_FOUND,
          InternalErrors.Reason.REPORTING_WINDOW_RESULT_NOT_FOUND,
          InternalErrors.Reason.BASIC_REPORT_STATE_INVALID,
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
          InternalErrors.Reason.REPORT_RESULT_NOT_FOUND,
          InternalErrors.Reason.REPORTING_SET_RESULT_NOT_FOUND,
          InternalErrors.Reason.REPORTING_WINDOW_RESULT_NOT_FOUND,
          InternalErrors.Reason.BASIC_REPORT_STATE_INVALID,
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

  /** Get a single [ReportingSet] */
  private suspend fun getReportingSet(name: String): ReportingSet {
    val reportingSetKey = ReportingSetKey.fromName(name)!!
    return try {
      internalReportingSetsStub
        .batchGetReportingSets(
          batchGetReportingSetsRequest {
            cmmsMeasurementConsumerId = reportingSetKey.cmmsMeasurementConsumerId
            externalReportingSetIds += reportingSetKey.reportingSetId
          }
        )
        .reportingSetsList
        .first()
        .toReportingSet()
    } catch (e: StatusException) {
      throw when (ReportingInternalException.getErrorCode(e)) {
        ErrorCode.REPORTING_SET_NOT_FOUND ->
          throw ReportingSetNotFoundException(name)
            .asStatusRuntimeException(Status.Code.FAILED_PRECONDITION)
        ErrorCode.MEASUREMENT_CONSUMER_NOT_FOUND,
        ErrorCode.REPORTING_SET_ALREADY_EXISTS,
        ErrorCode.CAMPAIGN_GROUP_INVALID,
        ErrorCode.UNKNOWN_ERROR,
        ErrorCode.MEASUREMENT_ALREADY_EXISTS,
        ErrorCode.MEASUREMENT_NOT_FOUND,
        ErrorCode.MEASUREMENT_CALCULATION_TIME_INTERVAL_NOT_FOUND,
        ErrorCode.REPORT_NOT_FOUND,
        ErrorCode.MEASUREMENT_STATE_INVALID,
        ErrorCode.MEASUREMENT_CONSUMER_ALREADY_EXISTS,
        ErrorCode.METRIC_ALREADY_EXISTS,
        ErrorCode.REPORT_ALREADY_EXISTS,
        ErrorCode.REPORT_SCHEDULE_ALREADY_EXISTS,
        ErrorCode.REPORT_SCHEDULE_NOT_FOUND,
        ErrorCode.REPORT_SCHEDULE_STATE_INVALID,
        ErrorCode.REPORT_SCHEDULE_ITERATION_NOT_FOUND,
        ErrorCode.REPORT_SCHEDULE_ITERATION_STATE_INVALID,
        ErrorCode.METRIC_CALCULATION_SPEC_NOT_FOUND,
        ErrorCode.METRIC_CALCULATION_SPEC_ALREADY_EXISTS,
        ErrorCode.UNRECOGNIZED,
        null -> Status.INTERNAL.withCause(e).asRuntimeException()
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
        } catch (_: InvalidProtocolBufferException) {
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
            createTimeAfter = decodedPageToken.filter.createTimeAfter
          }
        pageSize = finalPageSize
        pageToken = listBasicReportsPageToken {
          filter =
            ListBasicReportsPageTokenKt.filter {
              createTimeAfter = decodedPageToken.filter.createTimeAfter
            }
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

  /**
   * Builds two different maps from ReportingSets for the specified CampaignGroup: one for
   * DataProvider resource name to Primitive ReportingSet and the other for ReportingSet composite
   * to ReportingSet resource name.
   */
  private suspend fun buildReportingSetMaps(
    campaignGroup: ReportingSet,
    campaignGroupKey: ReportingSetKey,
  ): ReportingSetMaps {
    val dataProviderEventGroupsMap: Map<String, List<String>> =
      campaignGroup.primitive.cmmsEventGroupsList.groupBy {
        EventGroupKey.fromName(it)!!.parentKey.toName()
      }

    // Map of ReportingSetMapKey to ReportingSet. For determining whether a
    // ReportingSet already exists.
    val campaignGroupReportingSetMap: Map<ReportingSetMapKey, ReportingSet> = buildMap {
      internalReportingSetsStub
        .streamReportingSets(
          streamReportingSetsRequest {
            filter =
              StreamReportingSetsRequestKt.filter {
                cmmsMeasurementConsumerId = campaignGroupKey.cmmsMeasurementConsumerId
                externalCampaignGroupId = campaignGroupKey.reportingSetId
              }
            limit = 1000
          }
        )
        .filter { it.filter.isEmpty() }
        .collect {
          val reportingSet = it.toReportingSet()
          if (reportingSet.hasComposite()) {
            put(ReportingSetMapKey.Composite(composite = reportingSet.composite), reportingSet)
          } else {
            put(
              ReportingSetMapKey.Primitive(
                cmmsEventGroups = reportingSet.primitive.cmmsEventGroupsList.toSet()
              ),
              reportingSet,
            )
          }
        }
    }

    // Map of DataProvider resource names to primitive Reporting Sets.
    val dataProviderPrimitiveReportingSetMap: Map<String, ReportingSet> = buildMap {
      for (dataProviderName in dataProviderEventGroupsMap.keys) {
        val reportingSetMapKey =
          ReportingSetMapKey.Primitive(
            cmmsEventGroups = dataProviderEventGroupsMap.getValue(dataProviderName).toSet()
          )

        if (campaignGroupReportingSetMap.containsKey(reportingSetMapKey)) {
          put(dataProviderName, campaignGroupReportingSetMap.getValue(reportingSetMapKey))
        } else {
          val uuid = UUID.randomUUID()
          val id = "a$uuid"

          val reportingSet = reportingSet {
            this.campaignGroup = campaignGroup.name
            primitive =
              ReportingSetKt.primitive {
                cmmsEventGroups += dataProviderEventGroupsMap.getValue(dataProviderName)
              }
          }

          val createdReportingSet =
            try {
              internalReportingSetsStub
                .createReportingSet(
                  createReportingSetRequest {
                    externalReportingSetId = "a$uuid"
                    this.reportingSet =
                      reportingSet.toInternal(
                        reportingSetId = id,
                        cmmsMeasurementConsumerId = campaignGroupKey.cmmsMeasurementConsumerId,
                        internalReportingSetsStub = internalReportingSetsStub,
                      )
                  }
                )
                .toReportingSet()
            } catch (e: StatusException) {
              throw Status.INTERNAL.withCause(e).asRuntimeException()
            }

          put(dataProviderName, createdReportingSet)
        }
      }
    }

    // Map of ReportingSet Composite to ReportingSet resource name.
    val reportingSetCompositeToNameMap: Map<ReportingSet.Composite, String> = buildMap {
      campaignGroupReportingSetMap
        .filter { it.key is ReportingSetMapKey.Composite }
        .forEach { put((it.key as ReportingSetMapKey.Composite).composite, it.value.name) }
    }

    return ReportingSetMaps(dataProviderPrimitiveReportingSetMap, reportingSetCompositeToNameMap)
  }

  /**
   * Build map of MetricCalculationSpec to MetricCalculationSpec resource name using CampaignGroup
   * resource name as filter.
   */
  private suspend fun buildMetricCalculationSpecToNameMap(
    campaignGroupKey: ReportingSetKey
  ): Map<InternalMetricCalculationSpec, String> {
    // Map of MetricCalculationSpec to MetricCalculationSpec resource name for
    // MetricCalculationSpecs that already exist for the campaign group.
    return internalMetricCalculationSpecsStub
      .listMetricCalculationSpecs(
        listMetricCalculationSpecsRequest {
          cmmsMeasurementConsumerId = campaignGroupKey.cmmsMeasurementConsumerId
          filter =
            ListMetricCalculationSpecsRequestKt.filter {
              externalCampaignGroupId = campaignGroupKey.reportingSetId
            }
          limit = 1000
        }
      )
      .metricCalculationSpecsList
      .associate {
        it.copy {
          clearExternalMetricCalculationSpecId()
          details =
            details.copy {
              val realMetricSpecs = metricSpecs
              metricSpecs.clear()
              for (realMetricSpec in realMetricSpecs) {
                @Suppress(
                  "WHEN_ENUM_CAN_BE_NULL_IN_JAVA"
                ) // Protobuf oneof case enums cannot be null.
                when (realMetricSpec.typeCase) {
                  MetricSpec.TypeCase.REACH_AND_FREQUENCY -> {
                    metricSpecs += metricSpec {
                      reachAndFrequency = MetricSpecKt.reachAndFrequencyParams {}
                    }
                  }

                  MetricSpec.TypeCase.REACH -> {
                    metricSpecs += metricSpec { reach = MetricSpecKt.reachParams {} }
                  }

                  MetricSpec.TypeCase.IMPRESSION_COUNT -> {
                    metricSpecs += metricSpec {
                      impressionCount = MetricSpecKt.impressionCountParams {}
                    }
                  }

                  MetricSpec.TypeCase.POPULATION_COUNT -> {
                    metricSpecs += metricSpec {
                      populationCount = MetricSpecKt.populationCountParams {}
                    }
                  }

                  MetricSpec.TypeCase.WATCH_DURATION,
                  MetricSpec.TypeCase.TYPE_NOT_SET -> {}
                }
              }
            }
        } to
          MetricCalculationSpecKey(it.cmmsMeasurementConsumerId, it.externalMetricCalculationSpecId)
            .toName()
      }
  }

  /**
   * Builds a [Report]
   *
   * @param basicReport [BasicReport]
   * @param campaignGroupKey [ReportingSetKey] representing CampaignGroup
   * @param nameByReportingSetComposite Map of [ReportingSet.Composite] to ReportingSet resource
   *   name
   * @param reportingSetMetricCalculationSpecDetailsMap Map of [ReportingSet] to List of
   *   [InternalMetricCalculationSpec.Details]
   */
  private suspend fun buildReport(
    basicReport: BasicReport,
    campaignGroupKey: ReportingSetKey,
    nameByReportingSetComposite: Map<ReportingSet.Composite, String>,
    reportingSetMetricCalculationSpecDetailsMap:
      Map<ReportingSet, List<InternalMetricCalculationSpec.Details>>,
  ): Report {
    val existingReportingSetCompositesMap = nameByReportingSetComposite.toMutableMap()
    val existingMetricCalculationSpecsMap =
      buildMetricCalculationSpecToNameMap(campaignGroupKey).toMutableMap()

    return report {
      for (reportingSetMetricCalculationSpecDetailsEntry in
        reportingSetMetricCalculationSpecDetailsMap.entries) {
        reportingMetricEntries +=
          ReportKt.reportingMetricEntry {
            // Reuse ReportingSet or create a new one if it doesn't exist
            key =
              // All required Primitive ReportingSets have already been created so the name exists
              if (reportingSetMetricCalculationSpecDetailsEntry.key.hasPrimitive()) {
                reportingSetMetricCalculationSpecDetailsEntry.key.name
              } else {
                existingReportingSetCompositesMap.getOrPut(
                  reportingSetMetricCalculationSpecDetailsEntry.key.composite
                ) {
                  val createdReportingSet =
                    createCompositeReportingSet(
                      reportingSetMetricCalculationSpecDetailsEntry.key,
                      campaignGroupKey.cmmsMeasurementConsumerId,
                    )

                  ReportingSetKey(
                      createdReportingSet.cmmsMeasurementConsumerId,
                      createdReportingSet.externalReportingSetId,
                    )
                    .toName()
                }
              }

            value =
              ReportKt.reportingMetricCalculationSpec {
                // Reuse MetricCalculationSpec or create a new one if it doesn't exist
                for (metricCalculationSpecDetails in
                  reportingSetMetricCalculationSpecDetailsEntry.value) {
                  val metricCalculationSpec = metricCalculationSpec {
                    cmmsMeasurementConsumerId = campaignGroupKey.cmmsMeasurementConsumerId
                    externalCampaignGroupId = campaignGroupKey.reportingSetId
                    cmmsModelLine = basicReport.modelLine
                    details = metricCalculationSpecDetails
                  }
                  metricCalculationSpecs +=
                    existingMetricCalculationSpecsMap.getOrPut(metricCalculationSpec) {
                      val createdMetricCalculationSpec =
                        createMetricCalculationSpec(metricCalculationSpec)

                      MetricCalculationSpecKey(
                          createdMetricCalculationSpec.cmmsMeasurementConsumerId,
                          createdMetricCalculationSpec.externalMetricCalculationSpecId,
                        )
                        .toName()
                    }
                }
              }
          }
      }

      reportingInterval =
        ReportKt.reportingInterval {
          reportStart = basicReport.reportingInterval.reportStart
          reportEnd = basicReport.reportingInterval.reportEnd
        }
    }
  }

  private suspend fun createCompositeReportingSet(
    reportingSet: ReportingSet,
    cmmsMeasurementConsumerId: String,
  ): InternalReportingSet {
    return internalReportingSetsStub.createReportingSet(
      createReportingSetRequest {
        this.reportingSet =
          reportingSet.toInternal(
            externalReportingSetId,
            cmmsMeasurementConsumerId,
            internalReportingSetsStub,
          )
        externalReportingSetId = "a${UUID.randomUUID()}"
      }
    )
  }

  private suspend fun createMetricCalculationSpec(
    metricCalculationSpec: MetricCalculationSpec
  ): MetricCalculationSpec {
    return internalMetricCalculationSpecsStub.createMetricCalculationSpec(
      createMetricCalculationSpecRequest {
        this.metricCalculationSpec =
          metricCalculationSpec.copy {
            details =
              details.copy {
                val metricSpecsWithDefault =
                  metricSpecs.map { it.withDefaults(metricSpecConfig, secureRandom) }
                metricSpecs.clear()
                metricSpecs += metricSpecsWithDefault
              }
          }
        externalMetricCalculationSpecId = "a${UUID.randomUUID()}"
      }
    )
  }

  object Permission {
    private const val TYPE = "reporting.basicReports"
    const val CREATE = "$TYPE.create"
    const val GET = "$TYPE.get"
    const val LIST = "$TYPE.list"
  }

  companion object {
    private const val DEFAULT_PAGE_SIZE = 10
    private const val MAX_PAGE_SIZE = 25
    private const val SCALING_FACTOR = 10000

    /** Specifies default values using [MetricSpecConfig] */
    private fun MetricSpec.withDefaults(
      metricSpecConfig: MetricSpecConfig,
      secureRandom: Random,
      allowSamplingIntervalWrapping: Boolean = false,
    ): MetricSpec {
      return copy {
        when (typeCase) {
          MetricSpec.TypeCase.REACH -> {
            reach =
              defaultReachParams(metricSpecConfig, secureRandom, allowSamplingIntervalWrapping)
          }
          MetricSpec.TypeCase.REACH_AND_FREQUENCY -> {
            reachAndFrequency =
              defaultReachAndFrequencyParams(
                metricSpecConfig,
                secureRandom,
                allowSamplingIntervalWrapping,
              )
          }
          MetricSpec.TypeCase.IMPRESSION_COUNT -> {
            impressionCount = defaultImpressionCountParams(metricSpecConfig, secureRandom)
          }
          MetricSpec.TypeCase.POPULATION_COUNT,
          MetricSpec.TypeCase.WATCH_DURATION,
          MetricSpec.TypeCase.TYPE_NOT_SET -> {}
        }
      }
    }

    /** Specifies default values using [MetricSpecConfig] */
    private fun defaultReachParams(
      metricSpecConfig: MetricSpecConfig,
      secureRandom: Random,
      allowSamplingIntervalWrapping: Boolean,
    ): MetricSpec.ReachParams {
      return MetricSpecKt.reachParams {
        multipleDataProviderParams =
          MetricSpecKt.samplingAndPrivacyParams {
            privacyParams =
              defaultDifferentialPrivacyParams(
                defaultEpsilon =
                  metricSpecConfig.reachParams.multipleDataProviderParams.privacyParams.epsilon,
                defaultDelta =
                  metricSpecConfig.reachParams.multipleDataProviderParams.privacyParams.delta,
              )
            vidSamplingInterval =
              metricSpecConfig.reachParams.multipleDataProviderParams.vidSamplingInterval
                .toVidSamplingInterval(secureRandom, allowSamplingIntervalWrapping)
          }

        singleDataProviderParams =
          MetricSpecKt.samplingAndPrivacyParams {
            privacyParams =
              defaultDifferentialPrivacyParams(
                defaultEpsilon =
                  metricSpecConfig.reachParams.singleDataProviderParams.privacyParams.epsilon,
                defaultDelta =
                  metricSpecConfig.reachParams.singleDataProviderParams.privacyParams.delta,
              )
            vidSamplingInterval =
              metricSpecConfig.reachParams.singleDataProviderParams.vidSamplingInterval
                .toVidSamplingInterval(secureRandom)
          }
      }
    }

    /** Specifies default values using [MetricSpecConfig] */
    private fun defaultReachAndFrequencyParams(
      metricSpecConfig: MetricSpecConfig,
      secureRandom: Random,
      allowSamplingIntervalWrapping: Boolean,
    ): MetricSpec.ReachAndFrequencyParams {
      return MetricSpecKt.reachAndFrequencyParams {
        maximumFrequency = metricSpecConfig.reachAndFrequencyParams.maximumFrequency

        multipleDataProviderParams =
          MetricSpecKt.reachAndFrequencySamplingAndPrivacyParams {
            reachPrivacyParams =
              defaultDifferentialPrivacyParams(
                defaultEpsilon =
                  metricSpecConfig.reachAndFrequencyParams.multipleDataProviderParams
                    .reachPrivacyParams
                    .epsilon,
                defaultDelta =
                  metricSpecConfig.reachAndFrequencyParams.multipleDataProviderParams
                    .reachPrivacyParams
                    .delta,
              )
            frequencyPrivacyParams =
              defaultDifferentialPrivacyParams(
                defaultEpsilon =
                  metricSpecConfig.reachAndFrequencyParams.multipleDataProviderParams
                    .frequencyPrivacyParams
                    .epsilon,
                defaultDelta =
                  metricSpecConfig.reachAndFrequencyParams.multipleDataProviderParams
                    .frequencyPrivacyParams
                    .delta,
              )
            vidSamplingInterval =
              metricSpecConfig.reachAndFrequencyParams.multipleDataProviderParams
                .vidSamplingInterval
                .toVidSamplingInterval(secureRandom, allowSamplingIntervalWrapping)
          }

        singleDataProviderParams =
          MetricSpecKt.reachAndFrequencySamplingAndPrivacyParams {
            reachPrivacyParams =
              defaultDifferentialPrivacyParams(
                defaultEpsilon =
                  metricSpecConfig.reachAndFrequencyParams.singleDataProviderParams
                    .reachPrivacyParams
                    .epsilon,
                defaultDelta =
                  metricSpecConfig.reachAndFrequencyParams.singleDataProviderParams
                    .reachPrivacyParams
                    .delta,
              )
            frequencyPrivacyParams =
              defaultDifferentialPrivacyParams(
                defaultEpsilon =
                  metricSpecConfig.reachAndFrequencyParams.singleDataProviderParams
                    .frequencyPrivacyParams
                    .epsilon,
                defaultDelta =
                  metricSpecConfig.reachAndFrequencyParams.singleDataProviderParams
                    .frequencyPrivacyParams
                    .delta,
              )
            vidSamplingInterval =
              metricSpecConfig.reachAndFrequencyParams.singleDataProviderParams.vidSamplingInterval
                .toVidSamplingInterval(secureRandom)
          }
      }
    }

    /** Specifies default values using [MetricSpecConfig] */
    private fun defaultImpressionCountParams(
      metricSpecConfig: MetricSpecConfig,
      secureRandom: Random,
    ): MetricSpec.ImpressionCountParams {
      return MetricSpecKt.impressionCountParams {
        maximumFrequencyPerUser = metricSpecConfig.impressionCountParams.maximumFrequencyPerUser

        params =
          MetricSpecKt.samplingAndPrivacyParams {
            privacyParams =
              defaultDifferentialPrivacyParams(
                defaultEpsilon =
                  metricSpecConfig.impressionCountParams.params.privacyParams.epsilon,
                defaultDelta = metricSpecConfig.impressionCountParams.params.privacyParams.delta,
              )
            vidSamplingInterval =
              metricSpecConfig.impressionCountParams.params.vidSamplingInterval
                .toVidSamplingInterval(secureRandom)
          }
      }
    }

    /** Specifies the values in the optional fields of [MetricSpec.DifferentialPrivacyParams] */
    private fun defaultDifferentialPrivacyParams(
      defaultEpsilon: Double,
      defaultDelta: Double,
    ): MetricSpec.DifferentialPrivacyParams {
      return MetricSpecKt.differentialPrivacyParams {
        epsilon = defaultEpsilon
        delta = defaultDelta
      }
    }

    /** Converts an [MetricSpecConfig.VidSamplingInterval] to a [MetricSpec.VidSamplingInterval]. */
    private fun MetricSpecConfig.VidSamplingInterval.toVidSamplingInterval(
      secureRandom: Random,
      allowSamplingIntervalWrapping: Boolean = false,
    ): MetricSpec.VidSamplingInterval {
      val source = this
      if (source.hasFixedStart()) {
        return MetricSpecKt.vidSamplingInterval {
          start = source.fixedStart.start
          width = source.fixedStart.width
        }
      } else {
        // The SCALING_FACTOR is to help turn the float into an int without losing too much data.
        val maxStart =
          if (allowSamplingIntervalWrapping) {
            SCALING_FACTOR
          } else {
            SCALING_FACTOR - (source.randomStart.width * SCALING_FACTOR).toInt()
          }
        val randomStart = secureRandom.nextInt(maxStart) % maxStart
        return MetricSpecKt.vidSamplingInterval {
          start = randomStart.toFloat() / SCALING_FACTOR
          width = source.randomStart.width
        }
      }
    }
  }
}
