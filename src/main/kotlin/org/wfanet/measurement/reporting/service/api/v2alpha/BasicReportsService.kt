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
import com.google.protobuf.util.Durations
import com.google.type.DateTime
import com.google.type.copy
import com.google.type.dateTime
import com.google.type.interval
import com.google.type.timeZone
import io.grpc.Status
import io.grpc.StatusException
import java.time.ZoneId
import java.time.ZoneOffset
import java.util.UUID
import kotlin.collections.List
import kotlin.coroutines.CoroutineContext
import kotlin.coroutines.EmptyCoroutineContext
import kotlin.random.Random
import kotlinx.coroutines.flow.filter
import org.wfanet.measurement.access.client.v1alpha.Authorization
import org.wfanet.measurement.access.client.v1alpha.check
import org.wfanet.measurement.access.client.v1alpha.withForwardedTrustedCredentials
import org.wfanet.measurement.api.v2alpha.DataProviderKey
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
import org.wfanet.measurement.internal.reporting.v2.createReportingSetRequest as internalCreateReportingSetRequest
import org.wfanet.measurement.internal.reporting.v2.ensureSynthesizedCampaignGroupReportingSetRequest
import org.wfanet.measurement.internal.reporting.v2.getBasicReportRequest as internalGetBasicReportRequest
import org.wfanet.measurement.internal.reporting.v2.getImpressionQualificationFilterRequest
import org.wfanet.measurement.internal.reporting.v2.listBasicReportsRequest as internalListBasicReportsRequest
import org.wfanet.measurement.internal.reporting.v2.listMetricCalculationSpecsRequest
import org.wfanet.measurement.internal.reporting.v2.metricCalculationSpec
import org.wfanet.measurement.internal.reporting.v2.metricSpec
import org.wfanet.measurement.internal.reporting.v2.reportingSet as internalReportingSet
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
import org.wfanet.measurement.reporting.v2alpha.createReportingSetRequest
import org.wfanet.measurement.reporting.v2alpha.listBasicReportsResponse
import org.wfanet.measurement.reporting.v2alpha.report
import org.wfanet.measurement.reporting.v2alpha.reportingImpressionQualificationFilter

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
  private val defaultReportStartHour: ZonedHour? = null,
  private val baseExternalImpressionQualificationFilterIds: Iterable<String>,
  coroutineContext: CoroutineContext = EmptyCoroutineContext,
) : BasicReportsCoroutineImplBase(coroutineContext) {
  data class ZonedHour(val hour: Int, val zoneId: ZoneId)

  private sealed class ReportingSetMapKey {
    data class Composite(val composite: ReportingSet.Composite) : ReportingSetMapKey()

    data class Primitive(val cmmsEventGroups: Set<String>) : ReportingSetMapKey()
  }

  /**
   * The primitive [ReportingSet] to use for each ReportingUnit component (keyed by the component's
   * [ResourceKey]) plus the resource name of each existing composite [ReportingSet] under the
   * Campaign Group (for reuse).
   */
  private sealed interface ReportingSetMaps<T : ResourceKey> {
    /** Map of ReportingUnit component key to the Primitive [ReportingSet] used for it. */
    val primitiveReportingSetsByComponentKey: Map<T, ReportingSet>
    /** Map of composite [ReportingSet] to its resource name. */
    val nameByReportingSetComposite: Map<ReportingSet.Composite, String>
  }

  /** [ReportingSetMaps] where the ReportingUnit components are DataProviders. */
  private data class DataProviderReportingSetMaps(
    override val primitiveReportingSetsByComponentKey: Map<DataProviderKey, ReportingSet>,
    override val nameByReportingSetComposite: Map<ReportingSet.Composite, String>,
  ) : ReportingSetMaps<DataProviderKey>

  /** [ReportingSetMaps] where the ReportingUnit components are ReportingSets. */
  private data class ReportingSetComponentReportingSetMaps(
    override val primitiveReportingSetsByComponentKey: Map<ReportingSetKey, ReportingSet>,
    override val nameByReportingSetComposite: Map<ReportingSet.Composite, String>,
  ) : ReportingSetMaps<ReportingSetKey>

  /**
   * The effective Campaign Group for a [CreateBasicReportRequest] and the derived facts the request
   * flow needs, resolved from either a caller-supplied Campaign Group (when `campaign_group` is
   * specified) or a server-synthesized one (when it is not).
   */
  private data class CampaignGroupResolution(
    /** The effective Campaign Group [ReportingSet] (caller-supplied or server-synthesized). */
    val campaignGroup: ReportingSet,
    /** Key of [campaignGroup]. */
    val campaignGroupKey: ReportingSetKey,
    /** Resource names of the DataProviders spanned by [campaignGroup]'s EventGroup universe. */
    val dataProviderNames: Set<String>,
    /**
     * The ReportingUnit's ReportingSet components keyed by resource name. Empty when
     * `campaign_group` was specified (components are DataProviders); otherwise these
     * caller-supplied primitive ReportingSets are used directly as the per-component primitives (no
     * per-DataProvider auto-mint), and the Campaign Group is synthesized from them.
     */
    val reportingSetComponentsByName: Map<String, ReportingSet>,
  ) {
    /** Whether the Campaign Group was synthesized (i.e. `campaign_group` was not specified). */
    val synthesized: Boolean
      get() = reportingSetComponentsByName.isNotEmpty()
  }

  override suspend fun createBasicReport(request: CreateBasicReportRequest): BasicReport {
    val eventTemplateFieldsByPath = eventMessageDescriptor?.eventTemplateFieldsByPath ?: emptyMap()

    // The Campaign Group is either supplied by the caller (when campaign_group is specified) or,
    // when campaign_group is empty, synthesized by the server from the ReportingSet components.
    // Read the supplied Campaign Group (if any) up front so request validation can enforce the
    // corresponding component-type rules; synthesis runs after validation.
    val suppliedCampaignGroup: ReportingSet? =
      if (request.basicReport.campaignGroup.isEmpty()) {
        null
      } else {
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
          } catch (e: InternalReportingSetsException) {
            throw Status.INTERNAL.withDescription(e.message).withCause(e).asRuntimeException()
          }
        if (campaignGroup.campaignGroup != campaignGroup.name) {
          throw CampaignGroupInvalidException(request.basicReport.campaignGroup)
            .asStatusRuntimeException(Status.Code.FAILED_PRECONDITION)
        }
        campaignGroup
      }

    val (parentKey: MeasurementConsumerKey, requestImpressionQualificationFilterKeys) =
      try {
        CreateBasicReportRequestValidation.validateRequest(
          request,
          suppliedCampaignGroup,
          defaultReportStartHour != null,
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

    // When campaign_group is not specified, the effective Campaign Group is synthesized from the
    // (now validated) ReportingSet components. Otherwise it is the supplied Campaign Group.
    val campaignGroupResolution: CampaignGroupResolution =
      if (suppliedCampaignGroup != null) {
        resolveSuppliedCampaignGroup(suppliedCampaignGroup)
      } else {
        try {
          synthesizeCampaignGroup(request, parentKey)
        } catch (e: ReportingSetNotFoundException) {
          throw e.asStatusRuntimeException(Status.Code.FAILED_PRECONDITION)
        } catch (e: InvalidFieldValueException) {
          throw e.asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
        } catch (e: InternalReportingSetsException) {
          throw Status.INTERNAL.withDescription(e.message).withCause(e).asRuntimeException()
        }
      }

    val effectiveReportStart =
      if (defaultReportStartHour != null) {
        if (request.basicReport.reportingInterval.hasReportStartDate()) {
          dateTime {
            year = request.basicReport.reportingInterval.reportStartDate.year
            month = request.basicReport.reportingInterval.reportStartDate.month
            day = request.basicReport.reportingInterval.reportStartDate.day
            hours = defaultReportStartHour.hour
            val zoneId = defaultReportStartHour.zoneId
            if (zoneId is ZoneOffset) {
              utcOffset = Durations.fromSeconds(zoneId.totalSeconds.toLong())
            } else {
              timeZone = timeZone { id = zoneId.id }
            }
          }
        } else {
          request.basicReport.reportingInterval.reportStart
        }
      } else {
        request.basicReport.reportingInterval.reportStart
      }

    val reportingSetMaps: ReportingSetMaps<*> = buildReportingSetMaps(campaignGroupResolution)
    val effectiveModelLine: ModelLine? =
      try {
        getEffectiveModelLine(
          request.basicReport.modelLine,
          request.basicReport.reportingInterval,
          effectiveReportStart,
          campaignGroupResolution.dataProviderNames,
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
                campaignGroupId = campaignGroupResolution.campaignGroupKey.reportingSetId,
                campaignGroupSynthesized = campaignGroupResolution.synthesized,
                createReportRequestId = createReportRequestId,
                reportingImpressionQualificationFilters =
                  request.basicReport.impressionQualificationFiltersList,
                effectiveReportingImpressionQualificationFilters =
                  effectiveReportingImpressionQualificationFilters,
                impressionQualificationFilterSpecsByName = impressionQualificationFilterSpecsByName,
                effectiveModelLine = effectiveModelLine?.name.orEmpty(),
                effectiveReportStart = effectiveReportStart,
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
        campaignGroupName = campaignGroupResolution.campaignGroup.name,
        impressionQualificationFilterSpecsLists =
          impressionQualificationFilterSpecsByName.values + customFilterSpecs,
        dataProviderPrimitiveReportingSetMap =
          reportingSetMaps.primitiveReportingSetsByComponentKey.mapKeys { it.key.toName() },
        resultGroupSpecs = request.basicReport.resultGroupSpecsList,
        eventTemplateFieldsByPath = eventTemplateFieldsByPath,
      )

    val report: Report =
      try {
        buildReport(
          request.basicReport,
          campaignGroupResolution.campaignGroupKey,
          reportingSetMaps.nameByReportingSetComposite,
          reportingSetsMetricCalculationSpecDetailsMap,
          effectiveModelLine?.name.orEmpty(),
          effectiveReportStart,
        )
      } catch (e: ReportingSetNotFoundException) {
        throw e.asStatusRuntimeException(Status.Code.FAILED_PRECONDITION)
      } catch (e: InternalReportingSetsException) {
        throw Status.INTERNAL.withDescription(e.message).withCause(e).asRuntimeException()
      }

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

    return createdInternalBasicReport.toBasicReport(
      populateDeprecatedReportingUnitEventGroupSummaries = false
    )
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
    effectiveReportStart: DateTime,
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
                  startTime = effectiveReportStart.toTimestamp()
                  endTime =
                    effectiveReportStart
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

    return internalBasicReport.toBasicReport(!request.excludeDeprecatedEventGroupSummaries)
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
    if (
      internalListBasicReportsResponse.basicReportsList.isEmpty() &&
        internalListBasicReportsResponse.unreachableList.isEmpty()
    ) {
      return ListBasicReportsResponse.getDefaultInstance()
    }

    val populateDeprecated = !request.excludeDeprecatedEventGroupSummaries
    return listBasicReportsResponse {
      this.basicReports +=
        internalListBasicReportsResponse.basicReportsList
          .map { it.toBasicReport(populateDeprecated) }
          .toList()
      unreachable += internalListBasicReportsResponse.unreachableList
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
   * @throws InternalReportingSetsException
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
        null -> InternalReportingSetsException("Error retrieving ReportingSet", e)
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
   * Builds the [ReportingSetMaps] for the effective Campaign Group: the Primitive ReportingSet to
   * use for each ReportingUnit component, and a map of composite ReportingSet to resource name (for
   * reusing composites already created under the Campaign Group).
   *
   * When `campaign_group` was specified, a primitive is reused or auto-minted per DataProvider
   * spanned by the Campaign Group. Otherwise the caller-supplied ReportingSet components are the
   * per-component primitives directly, so no auto-mint occurs.
   */
  private suspend fun buildReportingSetMaps(
    campaignGroupResolution: CampaignGroupResolution
  ): ReportingSetMaps<*> {
    val campaignGroupKey = campaignGroupResolution.campaignGroupKey

    // Existing ReportingSets under the effective Campaign Group, indexed for reuse: composites by
    // their set expression, primitives by their EventGroup set.
    val campaignGroupReportingSetMap: Map<ReportingSetMapKey, ReportingSet> =
      try {
        buildMap {
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
      } catch (e: StatusException) {
        throw Status.INTERNAL.withCause(e).asRuntimeException()
      }

    // Map of composite ReportingSet to resource name.
    val reportingSetCompositeToNameMap: Map<ReportingSet.Composite, String> = buildMap {
      campaignGroupReportingSetMap
        .filter { it.key is ReportingSetMapKey.Composite }
        .forEach { put((it.key as ReportingSetMapKey.Composite).composite, it.value.name) }
    }

    if (campaignGroupResolution.synthesized) {
      // The caller-supplied ReportingSet components are the per-component primitives.
      val primitiveReportingSetsByComponentKey: Map<ReportingSetKey, ReportingSet> =
        campaignGroupResolution.reportingSetComponentsByName.mapKeys {
          checkNotNull(ReportingSetKey.fromName(it.key))
        }
      return ReportingSetComponentReportingSetMaps(
        primitiveReportingSetsByComponentKey,
        reportingSetCompositeToNameMap,
      )
    }

    // Reuse or auto-mint a primitive per DataProvider spanned by the Campaign Group's EventGroup
    // universe.
    val eventGroupsByDataProviderKey: Map<DataProviderKey, List<String>> =
      campaignGroupResolution.campaignGroup.primitive.cmmsEventGroupsList.groupBy {
        checkNotNull(EventGroupKey.fromName(it)).parentKey
      }

    val primitiveReportingSetsByComponentKey: Map<DataProviderKey, ReportingSet> = buildMap {
      for ((dataProviderKey, eventGroupNames) in eventGroupsByDataProviderKey) {
        val reportingSetMapKey =
          ReportingSetMapKey.Primitive(cmmsEventGroups = eventGroupNames.toSet())

        if (campaignGroupReportingSetMap.containsKey(reportingSetMapKey)) {
          put(dataProviderKey, campaignGroupReportingSetMap.getValue(reportingSetMapKey))
        } else {
          val id = "a${UUID.randomUUID()}"

          val primitiveReportingSet =
            try {
              internalReportingSetsStub
                .createReportingSet(
                  internalCreateReportingSetRequest {
                    externalReportingSetId = id
                    reportingSet = internalReportingSet {
                      cmmsMeasurementConsumerId = campaignGroupKey.parentKey.measurementConsumerId
                      this.externalCampaignGroupId = campaignGroupKey.reportingSetId
                      primitive =
                        ReportingSetKt.primitive { cmmsEventGroups += eventGroupNames }.toInternal()
                    }
                  }
                )
                .toReportingSet()
            } catch (e: StatusException) {
              throw Status.INTERNAL.withCause(e).asRuntimeException()
            }

          put(dataProviderKey, primitiveReportingSet)
        }
      }
    }

    return DataProviderReportingSetMaps(
      primitiveReportingSetsByComponentKey,
      reportingSetCompositeToNameMap,
    )
  }

  /**
   * Resolves a caller-supplied Campaign Group (when `campaign_group` is specified) into a
   * [CampaignGroupResolution].
   */
  private fun resolveSuppliedCampaignGroup(campaignGroup: ReportingSet): CampaignGroupResolution {
    val campaignGroupKey = checkNotNull(ReportingSetKey.fromName(campaignGroup.name))
    return CampaignGroupResolution(
      campaignGroup = campaignGroup,
      campaignGroupKey = campaignGroupKey,
      dataProviderNames = dataProviderNames(campaignGroup),
      reportingSetComponentsByName = emptyMap(),
    )
  }

  /**
   * Synthesizes the Campaign Group for a [request] whose `campaign_group` is not specified.
   *
   * Resolves the ReportingSet components across all ResultGroupSpecs, validates them (each must be
   * a primitive ReportingSet, and no two may resolve to the same DataProvider set), and
   * transactionally get-or-creates a primitive self-referencing Campaign Group whose EventGroup
   * universe is the union of the components' EventGroups. Concurrent and retried requests with the
   * same universe converge on one shared ReportingSet.
   *
   * @throws ReportingSetNotFoundException if a ReportingSet component does not exist
   * @throws InvalidFieldValueException if a component is not a primitive ReportingSet, or two
   *   components resolve to the same DataProvider set
   * @throws InternalReportingSetsException on other internal errors
   */
  private suspend fun synthesizeCampaignGroup(
    request: CreateBasicReportRequest,
    parentKey: MeasurementConsumerKey,
  ): CampaignGroupResolution {
    val componentsFieldPath = "basic_report.result_group_specs.reporting_unit.components"

    // Distinct ReportingSet components across all ResultGroupSpecs.
    val componentNames: Set<String> =
      request.basicReport.resultGroupSpecsList.flatMap { it.reportingUnit.componentsList }.toSet()

    val componentKeys: List<ReportingSetKey> =
      componentNames.map { name ->
        val key =
          ReportingSetKey.fromName(name)
            ?: throw InvalidFieldValueException(componentsFieldPath) { fieldPath ->
              "$fieldPath is not a valid ReportingSet resource name"
            }
        if (key.cmmsMeasurementConsumerId != parentKey.measurementConsumerId) {
          throw InvalidFieldValueException(componentsFieldPath) { fieldPath ->
            "$fieldPath must reference ReportingSets owned by the parent MeasurementConsumer"
          }
        }
        key
      }

    val reportingSetComponentsByName: Map<String, ReportingSet> =
      getReportingSets(parentKey, componentKeys)

    // Each component must be a primitive ReportingSet, and no two may resolve to the same
    // DataProvider set (which would silently collide in the post-processor).
    val nameByDataProviderSet = mutableMapOf<Set<String>, String>()
    for ((name, reportingSet) in reportingSetComponentsByName) {
      if (!reportingSet.hasPrimitive()) {
        throw InvalidFieldValueException(componentsFieldPath) { fieldPath ->
          "$fieldPath must each reference a primitive ReportingSet; $name is not primitive"
        }
      }
      val dataProviderSet = dataProviderNames(reportingSet)
      val collidingName = nameByDataProviderSet.put(dataProviderSet, name)
      if (collidingName != null) {
        throw InvalidFieldValueException(componentsFieldPath) { fieldPath ->
          "$fieldPath entries $name and $collidingName resolve to the same DataProvider set"
        }
      }
    }

    // Universe = union of all components' EventGroups.
    val eventGroupNames: List<String> =
      reportingSetComponentsByName.values.flatMap { it.primitive.cmmsEventGroupsList }.distinct()

    val synthesizedId = "a${UUID.randomUUID()}"
    val ensuredInternalReportingSet =
      try {
        internalReportingSetsStub.ensureSynthesizedCampaignGroupReportingSet(
          ensureSynthesizedCampaignGroupReportingSetRequest {
            externalReportingSetId = synthesizedId
            reportingSet = internalReportingSet {
              cmmsMeasurementConsumerId = parentKey.measurementConsumerId
              externalCampaignGroupId = synthesizedId
              primitive =
                ReportingSetKt.primitive { cmmsEventGroups += eventGroupNames }.toInternal()
            }
          }
        )
      } catch (e: StatusException) {
        throw InternalReportingSetsException("Error synthesizing campaign group ReportingSet", e)
      }

    val synthesizedCampaignGroup = ensuredInternalReportingSet.toReportingSet()
    return CampaignGroupResolution(
      campaignGroup = synthesizedCampaignGroup,
      campaignGroupKey =
        ReportingSetKey(
          ensuredInternalReportingSet.cmmsMeasurementConsumerId,
          ensuredInternalReportingSet.externalReportingSetId,
        ),
      dataProviderNames = dataProviderNames(synthesizedCampaignGroup),
      reportingSetComponentsByName = reportingSetComponentsByName,
    )
  }

  /** Resource names of the DataProviders spanned by [reportingSet]'s primitive EventGroups. */
  private fun dataProviderNames(reportingSet: ReportingSet): Set<String> = buildSet {
    for (eventGroupName in reportingSet.primitive.cmmsEventGroupsList) {
      add(checkNotNull(EventGroupKey.fromName(eventGroupName)).parentKey.toName())
    }
  }

  /**
   * Batch-retrieves the [ReportingSet]s for [keys] under [parentKey], returned keyed by resource
   * name.
   *
   * @throws ReportingSetNotFoundException if any ReportingSet does not exist
   * @throws InternalReportingSetsException on other internal errors
   */
  private suspend fun getReportingSets(
    parentKey: MeasurementConsumerKey,
    keys: Collection<ReportingSetKey>,
  ): Map<String, ReportingSet> {
    return try {
      internalReportingSetsStub
        .batchGetReportingSets(
          batchGetReportingSetsRequest {
            cmmsMeasurementConsumerId = parentKey.measurementConsumerId
            externalReportingSetIds += keys.map { it.reportingSetId }
          }
        )
        .reportingSetsList
        .associate {
          val reportingSet = it.toReportingSet()
          reportingSet.name to reportingSet
        }
    } catch (e: StatusException) {
      throw when (ReportingInternalException.getErrorCode(e)) {
        ErrorCode.REPORTING_SET_NOT_FOUND -> ReportingSetNotFoundException(keys.first().toName())
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
        null -> InternalReportingSetsException("Error retrieving ReportingSets", e)
      }
    }
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
   * @param effectiveReportStart The report start to use for the report.
   * @throws ReportingSetNotFoundException
   * @throws InternalReportingSetsException
   */
  private suspend fun buildReport(
    basicReport: BasicReport,
    campaignGroupKey: ReportingSetKey,
    nameByReportingSetComposite: Map<ReportingSet.Composite, String>,
    reportingSetMetricCalculationSpecDetailsMap:
      Map<ReportingSet, List<InternalMetricCalculationSpec.Details>>,
    modelLine: String,
    effectiveReportStart: DateTime,
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
                      campaignGroupKey.parentKey,
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
          reportStart = effectiveReportStart
          reportEnd = basicReport.reportingInterval.reportEnd
        }
    }
  }

  /**
   * Creates a composite [ReportingSet] via the internal API.
   *
   * @param [reportingSet] Request [ReportingSet] which has not been created
   * @param [parentKey] Key of the parent of the [ReportingSet] to create
   * @throws ReportingSetNotFoundException if a [ReportingSets] referenced in the set expression is
   *   not found
   * @throws InternalReportingSetsException if there was another error from the internal API
   */
  private suspend fun createCompositeReportingSet(
    reportingSet: ReportingSet,
    parentKey: MeasurementConsumerKey,
  ): InternalReportingSet {
    require(reportingSet.hasComposite())

    val referencedReportingSets: Map<ReportingSetKey, InternalReportingSet> =
      ReportingSets.getReferencedReportingSets(
        internalReportingSetsStub,
        ReportingSets.getReferencedReportingSetKeys(reportingSet.composite.expression),
      )
    val campaignGroupKey = checkNotNull(ReportingSetKey.fromName(reportingSet.campaignGroup))
    val reportingSetId = "a${UUID.randomUUID()}"
    val request =
      ReportingSets.buildInternalCreateReportingSetRequest(
        createReportingSetRequest {
          this.reportingSetId = reportingSetId
          this.reportingSet = reportingSet
        },
        parentKey,
        campaignGroupKey,
        referencedReportingSets,
      )

    return try {
      internalReportingSetsStub.createReportingSet(request)
    } catch (e: StatusException) {
      throw InternalReportingSetsException("Error creating composite ReportingSet", e)
    }
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
