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

package org.wfanet.measurement.reporting.deploy.v2.gcloud.spanner

import com.google.cloud.spanner.Options
import com.google.protobuf.Descriptors
import com.google.type.DateTime
import io.grpc.Status
import kotlin.coroutines.CoroutineContext
import kotlin.coroutines.EmptyCoroutineContext
import org.wfanet.measurement.api.v2alpha.EventTemplates
import org.wfanet.measurement.common.IdGenerator
import org.wfanet.measurement.common.RandomIdGenerator
import org.wfanet.measurement.common.generateNewId
import org.wfanet.measurement.common.toLocalDate
import org.wfanet.measurement.config.reporting.ImpressionQualificationFilterConfig
import org.wfanet.measurement.gcloud.spanner.AsyncDatabaseClient
import org.wfanet.measurement.internal.reporting.v2.CreateReportResultRequest
import org.wfanet.measurement.internal.reporting.v2.GetReportResultRequest
import org.wfanet.measurement.internal.reporting.v2.ReportResult
import org.wfanet.measurement.internal.reporting.v2.ReportResultView
import org.wfanet.measurement.internal.reporting.v2.ReportResultsGrpcKt
import org.wfanet.measurement.internal.reporting.v2.copy
import org.wfanet.measurement.internal.reporting.v2.nonCumulativeStartOrNull
import org.wfanet.measurement.reporting.deploy.v2.gcloud.spanner.db.ReportResultResult
import org.wfanet.measurement.reporting.deploy.v2.gcloud.spanner.db.getMeasurementConsumerByCmmsMeasurementConsumerId
import org.wfanet.measurement.reporting.deploy.v2.gcloud.spanner.db.insertNoisyReportResultValues
import org.wfanet.measurement.reporting.deploy.v2.gcloud.spanner.db.insertReportResult
import org.wfanet.measurement.reporting.deploy.v2.gcloud.spanner.db.insertReportingSetResult
import org.wfanet.measurement.reporting.deploy.v2.gcloud.spanner.db.insertReportingWindowResult
import org.wfanet.measurement.reporting.deploy.v2.gcloud.spanner.db.readNoisyReportResult
import org.wfanet.measurement.reporting.deploy.v2.gcloud.spanner.db.reportResultExists
import org.wfanet.measurement.reporting.deploy.v2.gcloud.spanner.db.reportResultExistsWithExternalId
import org.wfanet.measurement.reporting.deploy.v2.gcloud.spanner.db.reportingSetResultExists
import org.wfanet.measurement.reporting.deploy.v2.gcloud.spanner.db.reportingWindowResultExists
import org.wfanet.measurement.reporting.service.internal.GroupingDimensions
import org.wfanet.measurement.reporting.service.internal.ImpressionQualificationFilterMapping
import org.wfanet.measurement.reporting.service.internal.ImpressionQualificationFilterNotFoundException
import org.wfanet.measurement.reporting.service.internal.InvalidFieldValueException
import org.wfanet.measurement.reporting.service.internal.MeasurementConsumerNotFoundException
import org.wfanet.measurement.reporting.service.internal.Normalization
import org.wfanet.measurement.reporting.service.internal.RequiredFieldNotSetException

/** Spanner implementation of ReportResults gRPC service. */
class SpannerReportResultsService(
  private val spannerClient: AsyncDatabaseClient,
  private val impressionQualificationFilterMapping: ImpressionQualificationFilterMapping,
  eventMessageDescriptor: Descriptors.Descriptor,
  private val idGenerator: IdGenerator = RandomIdGenerator(),
  coroutineContext: CoroutineContext = EmptyCoroutineContext,
) : ReportResultsGrpcKt.ReportResultsCoroutineImplBase(coroutineContext) {
  private val eventMessageVersion =
    EventTemplates.getEventDescriptor(eventMessageDescriptor).currentVersion
  private val groupingDimensions = GroupingDimensions(eventMessageDescriptor)

  override suspend fun createReportResult(request: CreateReportResultRequest): ReportResult {
    try {
      validate(request)
    } catch (e: RequiredFieldNotSetException) {
      throw e.asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    } catch (e: InvalidFieldValueException) {
      throw e.asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }

    val reportingSetResults:
      Map<ReportResult.ReportingSetResultKey, ReportResult.ReportingSetResult> =
      request.reportResult.reportingSetResultsList.associate { entry -> entry.key to entry.value }
    if (reportingSetResults.size != request.reportResult.reportingSetResultsCount) {
      throw InvalidFieldValueException("report_result.reporting_set_results") { fieldPath ->
          "Duplicate key in $fieldPath"
        }
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }

    val externalImpressionQualificationFilterIds: List<String> =
      reportingSetResults.keys
        .filter { !it.custom }
        .map { it.externalImpressionQualificationFilterId }

    val impressionQualificationFiltersByExternalId:
      Map<String, ImpressionQualificationFilterConfig.ImpressionQualificationFilter> =
      externalImpressionQualificationFilterIds.associateWith {
        impressionQualificationFilterMapping.getImpressionQualificationByExternalId(it)
          ?: throw ImpressionQualificationFilterNotFoundException(it)
            .asStatusRuntimeException(Status.Code.FAILED_PRECONDITION)
      }

    val txnRunner: AsyncDatabaseClient.TransactionRunner =
      spannerClient.readWriteTransaction(Options.tag("action=createReportResult"))
    return txnRunner.run { txn: AsyncDatabaseClient.TransactionContext ->
      val measurementConsumerId: Long =
        try {
            txn.getMeasurementConsumerByCmmsMeasurementConsumerId(
              request.reportResult.cmmsMeasurementConsumerId
            )
          } catch (e: MeasurementConsumerNotFoundException) {
            throw e.asStatusRuntimeException(Status.Code.NOT_FOUND)
          }
          .measurementConsumerId
      val reportResultId: Long =
        idGenerator.generateNewId { id -> txn.reportResultExists(measurementConsumerId, id) }
      val externalReportResultId: Long =
        idGenerator.generateNewId { id ->
          txn.reportResultExistsWithExternalId(measurementConsumerId, id)
        }
      txn.insertReportResult(
        measurementConsumerId,
        reportResultId,
        externalReportResultId,
        request.reportResult.reportStart,
      )

      request.reportResult.copy {
        this.externalReportResultId = externalReportResultId
        this.reportingSetResults.mapIndexed { index, entry ->
          val key: ReportResult.ReportingSetResultKey = entry.key
          val value: ReportResult.ReportingSetResult = entry.value

          val reportingSetResultId =
            idGenerator.generateNewId {
              txn.reportingSetResultExists(measurementConsumerId, reportResultId, it)
            }
          val impressionQualificationFilterId: Long? =
            if (key.custom) {
              null
            } else {
              impressionQualificationFiltersByExternalId
                .getValue(key.externalImpressionQualificationFilterId)
                .impressionQualificationFilterId
            }
          val metricFrequencySpecFingerprint: Long =
            Normalization.computeFingerprint(key.metricFrequencySpec)
          val groupingDimensionFingerprint: Long =
            Normalization.computeFingerprint(eventMessageVersion, key.groupingsList)
          val filterFingerprint: Long? =
            if (key.eventFiltersList.isEmpty()) {
              null
            } else {
              Normalization.computeFingerprint(key.eventFiltersList)
            }
          txn.insertReportingSetResult(
            measurementConsumerId,
            reportResultId,
            reportingSetResultId,
            key.externalReportingSetId,
            key.vennDiagramRegionType,
            impressionQualificationFilterId,
            key.metricFrequencySpec,
            metricFrequencySpecFingerprint,
            groupingDimensionFingerprint,
            key.eventFiltersList,
            filterFingerprint,
            value.populationSize,
          )
          this.reportingSetResults[index] =
            entry.copy {
              this.value =
                value.copy {
                  this.metricFrequencySpecFingerprint = metricFrequencySpecFingerprint
                  this.groupingDimensionFingerprint = groupingDimensionFingerprint
                  if (filterFingerprint == null) {
                    clearFilterFingerprint()
                  } else {
                    this.filterFingerprint = filterFingerprint
                  }
                }
            }

          for (windowEntry in value.reportingWindowResultsList) {
            val reportingWindowResultId =
              idGenerator.generateNewId {
                txn.reportingWindowResultExists(
                  measurementConsumerId,
                  reportResultId,
                  reportingSetResultId,
                  it,
                )
              }
            txn.insertReportingWindowResult(
              measurementConsumerId,
              reportResultId,
              reportingSetResultId,
              reportingWindowResultId,
              windowEntry.key.nonCumulativeStartOrNull?.toLocalDate(),
              windowEntry.key.end.toLocalDate(),
            )

            txn.insertNoisyReportResultValues(
              measurementConsumerId,
              reportResultId,
              reportingSetResultId,
              reportingWindowResultId,
              windowEntry.value.noisyReportResultValues,
            )
          }
        }
      }
    }
  }

  private fun validate(request: CreateReportResultRequest) {
    if (!request.hasReportResult()) {
      throw RequiredFieldNotSetException("report_result")
    }
    if (request.reportResult.cmmsMeasurementConsumerId.isEmpty()) {
      throw RequiredFieldNotSetException("report_result.cmms_measurement_consumer_id")
    }
    if (!request.reportResult.hasReportStart()) {
      throw RequiredFieldNotSetException("report_result.report_start")
    }
    if (
      request.reportResult.reportStart.timeOffsetCase == DateTime.TimeOffsetCase.TIMEOFFSET_NOT_SET
    ) {
      throw RequiredFieldNotSetException("report_result.report_start.time_offset")
    }
    request.reportResult.reportingSetResultsList.mapIndexed {
      index,
      entry: ReportResult.ReportingSetResultEntry ->
      val resultKeyPath = "report_result.reporting_set_results[$index]"
      val key: ReportResult.ReportingSetResultKey = entry.key
      val keyFieldPath = "$resultKeyPath.key"
      if (key.externalReportingSetId.isEmpty()) {
        throw RequiredFieldNotSetException("$keyFieldPath.external_reporting_set_id")
      }
      if (
        key.vennDiagramRegionType ==
          ReportResult.VennDiagramRegionType.VENN_DIAGRAM_REGION_TYPE_UNSPECIFIED
      ) {
        throw RequiredFieldNotSetException("$keyFieldPath.venn_diagram_region_type")
      }
      if (key.vennDiagramRegionType == ReportResult.VennDiagramRegionType.UNRECOGNIZED) {
        throw InvalidFieldValueException("$keyFieldPath.venn_diagram_region_type")
      }
      if (!key.custom && key.externalImpressionQualificationFilterId.isEmpty()) {
        throw RequiredFieldNotSetException("$keyFieldPath.impression_qualification_filter")
      }
      if (!key.hasMetricFrequencySpec()) {
        throw RequiredFieldNotSetException("$keyFieldPath.metric_frequency_spec")
      }
      if (key.groupingsList != Normalization.sortGrouping(key.groupingsList)) {
        throw InvalidFieldValueException("$keyFieldPath.groupings") { fieldPath ->
          "$fieldPath not normalized"
        }
      }
      if (key.eventFiltersList != Normalization.normalizeEventFilters(key.eventFiltersList)) {
        throw InvalidFieldValueException("$keyFieldPath.event_filters") { fieldPath ->
          "$fieldPath not normalized"
        }
      }
    }
  }

  override suspend fun getReportResult(request: GetReportResultRequest): ReportResult {
    if (request.cmmsMeasurementConsumerId.isEmpty()) {
      throw RequiredFieldNotSetException("cmms_measurement_consumer_id")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }
    if (request.externalReportResultId == 0L) {
      throw RequiredFieldNotSetException("external_report_result_id")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }

    val txn = spannerClient.singleUse()
    val result: ReportResultResult =
      when (request.view) {
        ReportResultView.REPORT_RESULT_VIEW_UNSPECIFIED,
        ReportResultView.REPORT_RESULT_VIEW_NOISY ->
          txn.readNoisyReportResult(
            impressionQualificationFilterMapping,
            groupingDimensions,
            request.cmmsMeasurementConsumerId,
            request.externalReportResultId,
          )
        ReportResultView.REPORT_RESULT_VIEW_FULL ->
          throw Status.UNIMPLEMENTED.withDescription("REPORT_RESULT_VIEW_FULL not yet implemented")
            .asRuntimeException()
        ReportResultView.UNRECOGNIZED ->
          throw InvalidFieldValueException("view")
            .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
      }

    return result.reportResult
  }
}
