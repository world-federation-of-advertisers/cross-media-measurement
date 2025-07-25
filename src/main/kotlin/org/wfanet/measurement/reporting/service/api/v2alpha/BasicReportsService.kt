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
import com.google.protobuf.ByteString
import com.google.protobuf.InvalidProtocolBufferException
import io.grpc.Status
import io.grpc.StatusException
import java.util.UUID
import kotlin.collections.List
import kotlin.coroutines.CoroutineContext
import kotlin.coroutines.EmptyCoroutineContext
import org.wfanet.measurement.access.client.v1alpha.Authorization
import org.wfanet.measurement.access.client.v1alpha.check
import org.wfanet.measurement.api.v2alpha.EventGroupKey
import org.wfanet.measurement.api.v2alpha.MeasurementConsumerKey
import org.wfanet.measurement.common.base64UrlDecode
import org.wfanet.measurement.common.base64UrlEncode
import org.wfanet.measurement.internal.reporting.ErrorCode
import org.wfanet.measurement.internal.reporting.v2.BasicReport as InternalBasicReport
import org.wfanet.measurement.internal.reporting.v2.BasicReportsGrpcKt.BasicReportsCoroutineStub
import org.wfanet.measurement.internal.reporting.v2.ImpressionQualificationFiltersGrpcKt.ImpressionQualificationFiltersCoroutineStub
import org.wfanet.measurement.internal.reporting.v2.ListBasicReportsPageToken
import org.wfanet.measurement.internal.reporting.v2.ListBasicReportsPageTokenKt
import org.wfanet.measurement.internal.reporting.v2.ListBasicReportsRequest as InternalListBasicReportsRequest
import org.wfanet.measurement.internal.reporting.v2.ListBasicReportsRequestKt as InternalListBasicReportsRequestKt
import org.wfanet.measurement.internal.reporting.v2.ReportingSetsGrpcKt.ReportingSetsCoroutineStub as InternalReportingSetsCoroutineStub
import org.wfanet.measurement.internal.reporting.v2.StreamReportingSetsRequestKt
import org.wfanet.measurement.internal.reporting.v2.batchGetReportingSetsRequest
import org.wfanet.measurement.internal.reporting.v2.createReportingSetRequest
import org.wfanet.measurement.internal.reporting.v2.getBasicReportRequest as internalGetBasicReportRequest
import org.wfanet.measurement.internal.reporting.v2.getImpressionQualificationFilterRequest
import org.wfanet.measurement.internal.reporting.v2.listBasicReportsPageToken
import org.wfanet.measurement.internal.reporting.v2.listBasicReportsRequest as internalListBasicReportsRequest
import org.wfanet.measurement.internal.reporting.v2.streamReportingSetsRequest
import org.wfanet.measurement.reporting.service.api.ArgumentChangedInRequestForNextPageException
import org.wfanet.measurement.reporting.service.api.BasicReportNotFoundException
import org.wfanet.measurement.reporting.service.api.Errors
import org.wfanet.measurement.reporting.service.api.ImpressionQualificationFilterNotFoundException
import org.wfanet.measurement.reporting.service.api.InvalidFieldValueException
import org.wfanet.measurement.reporting.service.api.ReportingSetNotFoundException
import org.wfanet.measurement.reporting.service.api.RequiredFieldNotSetException
import org.wfanet.measurement.reporting.service.api.ServiceException
import org.wfanet.measurement.reporting.service.internal.Errors as InternalErrors
import com.google.protobuf.Descriptors
import com.google.protobuf.Descriptors.FieldDescriptor
import org.wfanet.measurement.api.v2alpha.EventAnnotationsProto
import org.wfanet.measurement.api.v2alpha.EventFieldDescriptor
import org.wfanet.measurement.api.v2alpha.EventTemplateDescriptor
import org.wfanet.measurement.reporting.service.internal.ReportingInternalException
import org.wfanet.measurement.reporting.v2alpha.BasicReport
import org.wfanet.measurement.reporting.v2alpha.BasicReportsGrpcKt.BasicReportsCoroutineImplBase
import org.wfanet.measurement.reporting.v2alpha.CreateBasicReportRequest
import org.wfanet.measurement.reporting.v2alpha.GetBasicReportRequest
import org.wfanet.measurement.reporting.v2alpha.ImpressionQualificationFilterSpec
import org.wfanet.measurement.reporting.v2alpha.ListBasicReportsRequest
import org.wfanet.measurement.reporting.v2alpha.ListBasicReportsResponse
import org.wfanet.measurement.reporting.v2alpha.ReportingSet
import org.wfanet.measurement.reporting.v2alpha.ReportingSetKt
import org.wfanet.measurement.reporting.v2alpha.copy
import org.wfanet.measurement.reporting.v2alpha.listBasicReportsResponse
import org.wfanet.measurement.reporting.v2alpha.reportingSet

class BasicReportsService(
  private val internalBasicReportsStub: BasicReportsCoroutineStub,
  private val internalImpressionQualificationFiltersStub:
    ImpressionQualificationFiltersCoroutineStub,
  private val internalReportingSetsStub: InternalReportingSetsCoroutineStub,
  private val eventTemplateFieldsMap: Map<String, EventTemplateFieldInfo.EventTemplateFieldInfo>,
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
    val campaignGroup: ReportingSet = getReportingSet(request.basicReport.campaignGroup)

    try {
      validateCreateBasicReportRequest(request, campaignGroup, eventTemplateFieldsMap)
    } catch (e: ServiceException) {
      throw when (e.reason) {
        Errors.Reason.REQUIRED_FIELD_NOT_SET,
        Errors.Reason.INVALID_FIELD_VALUE ->
          e.asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
        Errors.Reason.FIELD_UNIMPLEMENTED -> e.asStatusRuntimeException(Status.Code.UNIMPLEMENTED)
        Errors.Reason.BASIC_REPORT_NOT_FOUND,
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
                null -> Status.INTERNAL.withCause(e).asRuntimeException()
              }
            }
          } else if (impressionQualificationFilter.hasCustom()) {
            add(impressionQualificationFilter.custom.filterSpecList)
          }
        }
      }

    val dataProviderPrimitiveReportingSetMap: Map<String, ReportingSet> =
      buildDataProviderPrimitiveReportingSetMap(campaignGroup, campaignGroupKey)

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

  private suspend fun buildDataProviderPrimitiveReportingSetMap(
    campaignGroup: ReportingSet,
    campaignGroupKey: ReportingSetKey,
  ): Map<String, ReportingSet> {
    val dataProviderEventGroupsMap: Map<String, MutableList<String>> = buildMap {
      for (eventGroupName in campaignGroup.primitive.cmmsEventGroupsList) {
        val eventGroupKey = EventGroupKey.fromName(eventGroupName)
        val eventGroupsList = getOrDefault(eventGroupKey!!.parentKey.toName(), mutableListOf())
        eventGroupsList.add(eventGroupName)
        put(eventGroupKey.parentKey.toName(), eventGroupsList)
      }
    }

    // For determining whether a ReportingSet already exists.
    val campaignGroupReportingSetMap =
      buildMap<ByteString, ReportingSet> {
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
          .collect {
            val reportingSet = it.toReportingSet()
            put(reportingSet.copy { clearName() }.toByteString(), reportingSet)
          }
      }

    // Map of DataProvider resource names to primitive Reporting Sets.
    val dataProviderPrimitiveReportingSetMap: Map<String, ReportingSet> = buildMap {
      for (dataProviderName in dataProviderEventGroupsMap.keys) {
        val reportingSet = reportingSet {
          this.campaignGroup = campaignGroup.name
          primitive =
            ReportingSetKt.primitive {
              cmmsEventGroups += dataProviderEventGroupsMap.getValue(dataProviderName)
            }
        }

        if (campaignGroupReportingSetMap.containsKey(reportingSet.toByteString())) {
          put(dataProviderName, campaignGroupReportingSetMap.getValue(reportingSet.toByteString()))
        } else {
          val uuid = UUID.randomUUID()
          val id = "a$uuid"

          val createdReportingSet =
            try {
              internalReportingSetsStub
                .createReportingSet(
                  createReportingSetRequest {
                    externalReportingSetId = "a$uuid"
                    this.reportingSet =
                      reportingSet {
                          this.campaignGroup = campaignGroup.name
                          primitive =
                            ReportingSetKt.primitive {
                              cmmsEventGroups +=
                                dataProviderEventGroupsMap.getValue(dataProviderName)
                            }
                        }
                        .toInternal(
                          reportingSetId = id,
                          cmmsMeasurementConsumerId = campaignGroupKey.cmmsMeasurementConsumerId,
                          internalReportingSetsStub = internalReportingSetsStub,
                        )
                  }
                )
                .toReportingSet()
            } catch (e: StatusException) {
              throw when (InternalErrors.getReason(e)) {
                InternalErrors.Reason.IMPRESSION_QUALIFICATION_FILTER_NOT_FOUND,
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

          put(dataProviderName, createdReportingSet)
        }
      }
    }

    return dataProviderPrimitiveReportingSetMap
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

    /**
     * Builds Map of EventTemplateField name with respect to Event message to object containing info relevant to [BasicReport]
     *
     * @param eventDescriptor [Descriptors.Descriptor] for Event message
     */
    fun buildEventTemplateFieldsMap(eventDescriptor: Descriptors.Descriptor): Map<String, EventTemplateFieldInfo.EventTemplateFieldInfo> {
      return buildMap {
        for (field in eventDescriptor.fields) {
          if (field.messageType.options.hasExtension(EventAnnotationsProto.eventTemplate)) {
            val templateAnnotation: EventTemplateDescriptor =
              field.messageType.options.getExtension(EventAnnotationsProto.eventTemplate)
            val mediaType = templateAnnotation.mediaType

            for (templateField in field.messageType.fields) {
              if (!templateField.options.hasExtension(EventAnnotationsProto.templateField)) {
                val eventTemplateFieldName = "${templateAnnotation.name}.${templateField.name}"

                val templateFieldAnnotation: EventFieldDescriptor =
                  templateField.options.getExtension(EventAnnotationsProto.templateField)

                val isPopulationAttribute = templateFieldAnnotation.populationAttribute

                val supportedReportingFeatures = EventTemplateFieldInfo.SupportedReportingFeatures()
                for (reportingFeature in templateFieldAnnotation.reportingFeaturesList) {
                  @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA") // Proto enum fields cannot be null.
                  when (reportingFeature) {
                    EventFieldDescriptor.ReportingFeature.GROUPABLE -> {
                      supportedReportingFeatures.groupable = true
                    }
                    EventFieldDescriptor.ReportingFeature.FILTERABLE -> {
                      supportedReportingFeatures.filterable = true
                    }
                    EventFieldDescriptor.ReportingFeature.IMPRESSION_QUALIFICATION -> {
                      supportedReportingFeatures.impressionQualification = true
                    }
                    EventFieldDescriptor.ReportingFeature.REPORTING_FEATURE_UNSPECIFIED,
                    EventFieldDescriptor.ReportingFeature.UNRECOGNIZED -> {}
                  }
                }

                val groupingValuesMap = buildMap {
                  if (templateField.type == FieldDescriptor.Type.ENUM) {
                    templateField.enumType.values.forEach {
                      put(it.name, it.number)
                    }
                  }
                }

                put(
                  eventTemplateFieldName,
                  EventTemplateFieldInfo.EventTemplateFieldInfo(
                    mediaType = mediaType,
                    isPopulationAttribute = isPopulationAttribute,
                    supportedReportingFeatures = supportedReportingFeatures,
                    type = templateField.type,
                    groupingValuesMap = groupingValuesMap,
                  )
                )
              }
            }
          }
        }
      }
    }
  }
}
