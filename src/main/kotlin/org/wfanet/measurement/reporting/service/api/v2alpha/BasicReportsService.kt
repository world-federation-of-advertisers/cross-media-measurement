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
import com.google.type.copy
import com.google.type.interval
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
import org.wfanet.measurement.api.v2alpha.EventMessageDescriptor
import org.wfanet.measurement.api.v2alpha.MeasurementConsumerKey
import org.wfanet.measurement.api.v2alpha.ModelLine
import org.wfanet.measurement.api.v2alpha.ModelLinesGrpcKt.ModelLinesCoroutineStub as KingdomModelLinesCoroutineStub
import org.wfanet.measurement.api.v2alpha.ModelSuiteKey
import org.wfanet.measurement.api.v2alpha.enumerateValidModelLinesRequest
import org.wfanet.measurement.api.withAuthenticationKey
import org.wfanet.measurement.common.api.ResourceKey
import org.wfanet.measurement.common.base64UrlDecode
import org.wfanet.measurement.common.base64UrlEncode
import org.wfanet.measurement.common.toTimestamp
import org.wfanet.measurement.config.reporting.MeasurementConsumerConfigs
import org.wfanet.measurement.config.reporting.MetricSpecConfig
import org.wfanet.measurement.internal.reporting.ErrorCode
import org.wfanet.measurement.internal.reporting.v2.BasicReport as InternalBasicReport
import org.wfanet.measurement.internal.reporting.v2.BasicReportsGrpcKt.BasicReportsCoroutineStub
import org.wfanet.measurement.internal.reporting.v2.ImpressionQualificationFilter as InternalImpressionQualificationFilter
import org.wfanet.measurement.internal.reporting.v2.ImpressionQualificationFiltersGrpcKt.ImpressionQualificationFiltersCoroutineStub
import org.wfanet.measurement.internal.reporting.v2.ListBasicReportsPageToken
import org.wfanet.measurement.internal.reporting.v2.ListBasicReportsRequest as InternalListBasicReportsRequest
import org.wfanet.measurement.internal.reporting.v2.ListBasicReportsRequestKt as InternalListBasicReportsRequestKt
import org.wfanet.measurement.internal.reporting.v2.ListMetricCalculationSpecsRequestKt
import org.wfanet.measurement.internal.reporting.v2.MetricCalculationSpec as InternalMetricCalculationSpec
import org.wfanet.measurement.internal.reporting.v2.MetricCalculationSpec
import org.wfanet.measurement.internal.reporting.v2.MetricCalculationSpecsGrpcKt.MetricCalculationSpecsCoroutineStub as InternalMetricCalculationSpecsCoroutineStub
import org.wfanet.measurement.internal.reporting.v2.MetricSpec
import org.wfanet.measurement.internal.reporting.v2.MetricSpecKt
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
import org.wfanet.measurement.internal.reporting.v2.listBasicReportsRequest as internalListBasicReportsRequest
import org.wfanet.measurement.internal.reporting.v2.listMetricCalculationSpecsRequest
import org.wfanet.measurement.internal.reporting.v2.metricCalculationSpec
import org.wfanet.measurement.internal.reporting.v2.metricSpec
import org.wfanet.measurement.internal.reporting.v2.setExternalReportIdRequest
import org.wfanet.measurement.internal.reporting.v2.streamReportingSetsRequest
import org.wfanet.measurement.reporting.service.api.ArgumentChangedInRequestForNextPageException
import org.wfanet.measurement.reporting.service.api.BasicReportAlreadyExistsException
import org.wfanet.measurement.reporting.service.api.BasicReportNotFoundException
import org.wfanet.measurement.reporting.service.api.CampaignGroupInvalidException
import org.wfanet.measurement.reporting.service.api.DataProviderNotFoundForCampaignGroupException
import org.wfanet.measurement.reporting.service.api.EventTemplateFieldInvalidException
import org.wfanet.measurement.reporting.service.api.FieldUnimplementedException
import org.wfanet.measurement.reporting.service.api.ImpressionQualificationFilterNotFoundException
import org.wfanet.measurement.reporting.service.api.InvalidFieldValueException
import org.wfanet.measurement.reporting.service.api.ModelLineNotActiveException
import org.wfanet.measurement.reporting.service.api.ReportingSetNotFoundException
import org.wfanet.measurement.reporting.service.api.RequiredFieldNotSetException
import org.wfanet.measurement.reporting.service.internal.Errors as InternalErrors
import org.wfanet.measurement.reporting.service.internal.Normalization
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
import org.wfanet.measurement.reporting.v2alpha.ReportingImpressionQualificationFilter
import org.wfanet.measurement.reporting.v2alpha.ReportingInterval
import org.wfanet.measurement.reporting.v2alpha.ReportingSet
import org.wfanet.measurement.reporting.v2alpha.ReportingSetKt
import org.wfanet.measurement.reporting.v2alpha.ReportsGrpcKt.ReportsCoroutineStub
import org.wfanet.measurement.reporting.v2alpha.copy
import org.wfanet.measurement.reporting.v2alpha.createReportRequest
import org.wfanet.measurement.reporting.v2alpha.listBasicReportsResponse
import org.wfanet.measurement.reporting.v2alpha.report
import org.wfanet.measurement.reporting.v2alpha.reportingImpressionQualificationFilter
import org.wfanet.measurement.reporting.v2alpha.reportingSet

class BasicReportsService(
  private val internalBasicReportsStub: BasicReportsCoroutineStub,
  private val internalImpressionQualificationFiltersStub:
    ImpressionQualificationFiltersCoroutineStub,
  private val internalReportingSetsStub: InternalReportingSetsCoroutineStub,
  private val internalMetricCalculationSpecsStub: InternalMetricCalculationSpecsCoroutineStub,
  private val reportsStub: ReportsCoroutineStub,
  private val kingdomModelLinesStub: KingdomModelLinesCoroutineStub,
  private val eventMessageDescriptor: EventMessageDescriptor?,
  private val metricSpecConfig: MetricSpecConfig,
  private val secureRandom: Random,
  private val authorization: Authorization,
  private val measurementConsumerConfigs: MeasurementConsumerConfigs,
  private val baseExternalImpressionQualificationFilterIds: Iterable<String>,
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
    val eventTemplateFieldsByPath = eventMessageDescriptor?.eventTemplateFieldsByPath ?: emptyMap()

    if (request.basicReport.campaignGroup.isEmpty()) {
      throw RequiredFieldNotSetException("basic_report.campaign_group")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }
    val campaignGroupKey =
      ReportingSetKey.fromName(request.basicReport.campaignGroup)
        ?: throw InvalidFieldValueException("basic_report.campaign_group") { fieldPath ->
            "$fieldPath is not a valid ReportingSet resource name"
          }
          .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    val campaignGroup: ReportingSet =
      try {
        getReportingSet(campaignGroupKey)
      } catch (e: ReportingSetNotFoundException) {
        throw e.asStatusRuntimeException(Status.Code.FAILED_PRECONDITION)
      }
    if (campaignGroup.campaignGroup != campaignGroup.name) {
      throw CampaignGroupInvalidException(request.basicReport.campaignGroup)
        .asStatusRuntimeException(Status.Code.FAILED_PRECONDITION)
    }

    val (parentKey: MeasurementConsumerKey, requestImpressionQualificationFilterKeys) =
      try {
        CreateBasicReportRequestValidation.validateRequest(
          request,
          campaignGroup,
          eventTemplateFieldsByPath,
        )
      } catch (e: RequiredFieldNotSetException) {
        throw e.asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
      } catch (e: InvalidFieldValueException) {
        throw e.asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
      } catch (e: FieldUnimplementedException) {
        throw e.asStatusRuntimeException(Status.Code.UNIMPLEMENTED)
      } catch (e: DataProviderNotFoundForCampaignGroupException) {
        throw e.asStatusRuntimeException(Status.Code.FAILED_PRECONDITION)
      } catch (e: EventTemplateFieldInvalidException) {
        throw e.asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
      }

    val reportingSetMaps: ReportingSetMaps = buildReportingSetMaps(campaignGroup, campaignGroupKey)
    val effectiveModelLine: ModelLine? =
      try {
        getEffectiveModelLine(
          request.basicReport.modelLine,
          request.basicReport.reportingInterval,
          reportingSetMaps.primitiveReportingSetsByDataProvider.keys,
          parentKey,
        )
      } catch (e: ModelLineNotActiveException) {
        throw e.asStatusRuntimeException(Status.Code.FAILED_PRECONDITION)
      }

    val requiredPermissionIds = buildSet {
      add(Permission.CREATE)

      if (effectiveModelLine?.type == ModelLine.Type.DEV) {
        add(Permission.CREATE_WITH_DEV_MODEL_LINE)
      }
    }

    authorization.check(request.parent, requiredPermissionIds)

    val baseImpressionQualificationFilterKeys: List<ImpressionQualificationFilterKey> =
      baseExternalImpressionQualificationFilterIds.map { ImpressionQualificationFilterKey(it) }

    val baseImpressionQualificationFilterNames =
      baseImpressionQualificationFilterKeys.map { it.toName() }

    val impressionQualificationFilterKeyByName: Map<String, ImpressionQualificationFilterKey> =
      (baseImpressionQualificationFilterKeys + requestImpressionQualificationFilterKeys)
        .associateBy { it.toName() }
    val effectiveReportingImpressionQualificationFilters:
      List<ReportingImpressionQualificationFilter> =
      baseImpressionQualificationFilterNames.map {
        reportingImpressionQualificationFilter { impressionQualificationFilter = it }
      } +
        request.basicReport.impressionQualificationFiltersList.filter {
          it.impressionQualificationFilter !in baseImpressionQualificationFilterNames
        }
    if (effectiveReportingImpressionQualificationFilters.isEmpty()) {
      throw RequiredFieldNotSetException("basic_report.impression_qualification_filters") {
          fieldPath ->
          "$fieldPath must be set when there are no base ImpressionQualificationFilters configured"
        }
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }

    // Validates that IQFs exist as in addition to creating the Map
    val impressionQualificationFilterSpecsByName:
      Map<String, List<ImpressionQualificationFilterSpec>> =
      effectiveReportingImpressionQualificationFilters
        .filter { it.hasImpressionQualificationFilter() }
        .associate {
          val impressionQualificationFilterKey =
            impressionQualificationFilterKeyByName.getValue(it.impressionQualificationFilter)
          val internalImpressionQualificationFilter: InternalImpressionQualificationFilter =
            try {
              getInternalImpressionQualificationFilter(impressionQualificationFilterKey)
            } catch (e: ImpressionQualificationFilterNotFoundException) {
              throw e.asStatusRuntimeException(Status.Code.FAILED_PRECONDITION)
            }
          val filterSpecs =
            internalImpressionQualificationFilter.filterSpecsList.map { internalFilterSpec ->
              internalFilterSpec.toImpressionQualificationFilterSpec()
            }

          it.impressionQualificationFilter to filterSpecs
        }

    val customFilterSpecs: List<List<ImpressionQualificationFilterSpec>> = buildList {
      addAll(
        effectiveReportingImpressionQualificationFilters
          .filter { it.hasCustom() }
          .map { customIqf ->
            val normalizedCustomSpecs: Iterable<ImpressionQualificationFilterSpec> =
              normalizeImpressionQualificationFilterSpecs(customIqf.custom.filterSpecList)

            // Check if custom ImpressionQualificationFilter already exists
            impressionQualificationFilterSpecsByName.forEach { (iqFName, existingIqfSpecs) ->
              val normalizedExistingIqfSpecs =
                normalizeImpressionQualificationFilterSpecs(existingIqfSpecs)

              if (normalizedCustomSpecs == normalizedExistingIqfSpecs) {
                throw InvalidFieldValueException("basic_report.impression_qualification_filters") {
                    fieldPath ->
                    "$fieldPath contains a custom ReportingImpressionQualificationFilter that matches $iqFName"
                  }
                  .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
              }
            }
            customIqf.custom.filterSpecList
          }
      )
    }

    val createReportRequestId = UUID.randomUUID().toString()

    val createdInternalBasicReport =
      try {
        internalBasicReportsStub.createBasicReport(
          createBasicReportRequest {
            basicReport =
              request.basicReport.toInternal(
                cmmsMeasurementConsumerId = parentKey.measurementConsumerId,
                basicReportId = request.basicReportId,
                campaignGroupId = campaignGroupKey.reportingSetId,
                createReportRequestId = createReportRequestId,
                reportingImpressionQualificationFilters =
                  request.basicReport.impressionQualificationFiltersList,
                effectiveReportingImpressionQualificationFilters =
                  effectiveReportingImpressionQualificationFilters,
                impressionQualificationFilterSpecsByName = impressionQualificationFilterSpecsByName,
                effectiveModelLine = effectiveModelLine?.name.orEmpty(),
              )
            requestId = request.requestId
          }
        )
      } catch (e: StatusException) {
        throw when (InternalErrors.getReason(e)) {
          InternalErrors.Reason.BASIC_REPORT_ALREADY_EXISTS ->
            BasicReportAlreadyExistsException(
                BasicReportKey(
                    cmmsMeasurementConsumerId = parentKey.measurementConsumerId,
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
          InternalErrors.Reason.INVALID_BASIC_REPORT,
          null -> Status.INTERNAL.withCause(e).asRuntimeException()
        }
      }

    val reportingSetsMetricCalculationSpecDetailsMap:
      Map<ReportingSet, List<InternalMetricCalculationSpec.Details>> =
      buildReportingSetMetricCalculationSpecDetailsMap(
        campaignGroupName = request.basicReport.campaignGroup,
        impressionQualificationFilterSpecsLists =
          impressionQualificationFilterSpecsByName.values + customFilterSpecs,
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
        effectiveModelLine?.name.orEmpty(),
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
        cmmsMeasurementConsumerId = parentKey.measurementConsumerId
        externalBasicReportId = request.basicReportId
        externalReportId = createReportRequest.reportId
      }
    )

    return createdInternalBasicReport.toBasicReport()
  }

  /**
   * Gets an [InternalImpressionQualificationFilter].
   *
   * @throws ImpressionQualificationFilterNotFoundException
   */
  private suspend fun getInternalImpressionQualificationFilter(
    key: ImpressionQualificationFilterKey
  ): InternalImpressionQualificationFilter {
    return try {
      internalImpressionQualificationFiltersStub.getImpressionQualificationFilter(
        getImpressionQualificationFilterRequest {
          externalImpressionQualificationFilterId = key.impressionQualificationFilterId
        }
      )
    } catch (e: StatusException) {
      throw when (InternalErrors.getReason(e)) {
        InternalErrors.Reason.IMPRESSION_QUALIFICATION_FILTER_NOT_FOUND ->
          ImpressionQualificationFilterNotFoundException(key.toName())
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
        InternalErrors.Reason.INVALID_BASIC_REPORT,
        null -> Exception("Error retrieving internal ImpressionQualificationFilter", e)
      }
    }
  }

  /**
   * Returns the effective [ModelLine], or `null` if there is none.
   *
   * @param requestModelLine resource name of the [ModelLine] from a request, which may be empty
   * @throws ModelLineNotActiveException if [requestModelLine] is not active
   */
  private suspend fun getEffectiveModelLine(
    requestModelLine: String,
    reportingInterval: ReportingInterval,
    dataProviderNames: Iterable<String>,
    measurementConsumerKey: MeasurementConsumerKey,
  ): ModelLine? {
    val measurementConsumerName = measurementConsumerKey.toName()
    val measurementConsumerConfig =
      checkNotNull(measurementConsumerConfigs.configsMap[measurementConsumerName]) {
        "Config not found for $measurementConsumerName"
      }
    val measurementConsumerCredentials =
      MeasurementConsumerCredentials.fromConfig(measurementConsumerKey, measurementConsumerConfig)
    val validModelLines: List<ModelLine> =
      try {
          kingdomModelLinesStub
            .withAuthenticationKey(
              measurementConsumerCredentials.callCredentials.apiAuthenticationKey
            )
            .enumerateValidModelLines(
              enumerateValidModelLinesRequest {
                parent = ModelSuiteKey(ResourceKey.WILDCARD_ID, ResourceKey.WILDCARD_ID).toName()
                timeInterval = interval {
                  startTime = reportingInterval.reportStart.toTimestamp()
                  endTime =
                    reportingInterval.reportStart
                      .copy {
                        day = reportingInterval.reportEnd.day
                        month = reportingInterval.reportEnd.month
                        year = reportingInterval.reportEnd.year
                      }
                      .toTimestamp()
                }
                dataProviders += dataProviderNames
              }
            )
        } catch (e: StatusException) {
          throw Exception("Error enumerating valid ModelLines", e)
        }
        .modelLinesList

    return if (requestModelLine.isEmpty()) {
      if (validModelLines.isEmpty()) {
        null
      } else {
        validModelLines.first()
      }
    } else {
      validModelLines.find { it.name == requestModelLine }
        ?: throw ModelLineNotActiveException(requestModelLine)
    }
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
          InternalErrors.Reason.INVALID_BASIC_REPORT,
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
          InternalErrors.Reason.INVALID_BASIC_REPORT,
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
   * Gets a single [ReportingSet].
   *
   * @throws ReportingSetNotFoundException
   */
  private suspend fun getReportingSet(key: ReportingSetKey): ReportingSet {
    return try {
      internalReportingSetsStub
        .batchGetReportingSets(
          batchGetReportingSetsRequest {
            cmmsMeasurementConsumerId = key.cmmsMeasurementConsumerId
            externalReportingSetIds += key.reportingSetId
          }
        )
        .reportingSetsList
        .first()
        .toReportingSet()
    } catch (e: StatusException) {
      throw when (ReportingInternalException.getErrorCode(e)) {
        ErrorCode.REPORTING_SET_NOT_FOUND -> ReportingSetNotFoundException(key.toName())
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
        null -> Exception("Internal error retrieving ReportingSet", e)
      }
    }
  }

  private fun ListBasicReportsRequest.toInternal(): InternalListBasicReportsRequest {
    val source = this

    val measurementConsumerKey =
      MeasurementConsumerKey.fromName(source.parent) ?: throw InvalidFieldValueException("parent")

    val cmmsMeasurementConsumerId = measurementConsumerKey.measurementConsumerId

    val pageSize =
      if (source.pageSize in 1..MAX_PAGE_SIZE) {
        source.pageSize
      } else if (source.pageSize > MAX_PAGE_SIZE) {
        MAX_PAGE_SIZE
      } else {
        DEFAULT_PAGE_SIZE
      }

    val decodedPageToken =
      if (source.pageToken.isNotEmpty()) {
        try {
          ListBasicReportsPageToken.parseFrom(source.pageToken.base64UrlDecode())
        } catch (e: InvalidProtocolBufferException) {
          throw InvalidFieldValueException("page_token", e)
        }
        // TODO(@SanjayVas): Check if filter changed since previous page or delegate the check to
        // the internal API. The former requires putting filter information into the public API's
        // page token, and the latter requires putting filter information into the internal API's
        // page token.
      } else {
        null
      }

    return internalListBasicReportsRequest {
      filter =
        InternalListBasicReportsRequestKt.filter {
          this.cmmsMeasurementConsumerId = cmmsMeasurementConsumerId
          if (source.filter.hasCreateTimeAfter()) {
            createTimeAfter = source.filter.createTimeAfter
          }
        }
      this.pageSize = pageSize
      if (decodedPageToken != null) {
        pageToken = decodedPageToken
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
   * @param modelLine The model line to use for the report.
   */
  private suspend fun buildReport(
    basicReport: BasicReport,
    campaignGroupKey: ReportingSetKey,
    nameByReportingSetComposite: Map<ReportingSet.Composite, String>,
    reportingSetMetricCalculationSpecDetailsMap:
      Map<ReportingSet, List<InternalMetricCalculationSpec.Details>>,
    modelLine: String,
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
                    cmmsModelLine = modelLine
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

  /** Normalize an iteration of [ImpressionQualificationFilterSpec]. */
  private fun normalizeImpressionQualificationFilterSpecs(
    impressionQualificationFilterSpecs: Iterable<ImpressionQualificationFilterSpec>
  ): Iterable<ImpressionQualificationFilterSpec> {
    return impressionQualificationFilterSpecs
      .sortedBy { it.mediaType }
      .map { impressionQualificationFilterSpec ->
        impressionQualificationFilterSpec.copy {
          filters.clear()
          val normalizedEventFilters =
            Normalization.normalizeEventFilters(
              impressionQualificationFilterSpec.filtersList.map { it.toInternal() }
            )
          filters += normalizedEventFilters.map { it.toEventFilter() }
        }
      }
  }

  object Permission {
    private const val TYPE = "reporting.basicReports"
    const val CREATE = "$TYPE.create"
    const val CREATE_WITH_DEV_MODEL_LINE = "$TYPE.createWithDevModelLine"
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
