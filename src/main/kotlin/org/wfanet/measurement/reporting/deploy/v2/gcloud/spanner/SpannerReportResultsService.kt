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
import org.wfanet.measurement.internal.reporting.v2.BatchCreateReportingSetResultsRequest
import org.wfanet.measurement.internal.reporting.v2.CreateReportResultRequest
import org.wfanet.measurement.internal.reporting.v2.GetReportResultRequest
import org.wfanet.measurement.internal.reporting.v2.ReportResult
import org.wfanet.measurement.internal.reporting.v2.ReportResultsGrpcKt
import org.wfanet.measurement.internal.reporting.v2.ReportingSetResult
import org.wfanet.measurement.internal.reporting.v2.batchCreateReportingSetResultsResponse
import org.wfanet.measurement.internal.reporting.v2.copy
import org.wfanet.measurement.internal.reporting.v2.nonCumulativeStartOrNull
import org.wfanet.measurement.reporting.deploy.v2.gcloud.spanner.db.ReportResultResult
import org.wfanet.measurement.reporting.deploy.v2.gcloud.spanner.db.getMeasurementConsumerByCmmsMeasurementConsumerId
import org.wfanet.measurement.reporting.deploy.v2.gcloud.spanner.db.insertNoisyReportResultValues
import org.wfanet.measurement.reporting.deploy.v2.gcloud.spanner.db.insertReportResult
import org.wfanet.measurement.reporting.deploy.v2.gcloud.spanner.db.insertReportingSetResult
import org.wfanet.measurement.reporting.deploy.v2.gcloud.spanner.db.insertReportingWindowResult
import org.wfanet.measurement.reporting.deploy.v2.gcloud.spanner.db.reportResultExists
import org.wfanet.measurement.reporting.deploy.v2.gcloud.spanner.db.reportResultExistsWithExternalId
import org.wfanet.measurement.reporting.deploy.v2.gcloud.spanner.db.reportingSetResultExists
import org.wfanet.measurement.reporting.deploy.v2.gcloud.spanner.db.reportingSetResultExistsByExternalId
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

    val txnRunner: AsyncDatabaseClient.TransactionRunner =
      spannerClient.readWriteTransaction(Options.tag("action=createReportResult"))
    return txnRunner.run { txn: AsyncDatabaseClient.TransactionContext ->
      val measurementConsumerId: Long =
        try {
            txn.getMeasurementConsumerByCmmsMeasurementConsumerId(request.cmmsMeasurementConsumerId)
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
        cmmsMeasurementConsumerId = request.cmmsMeasurementConsumerId
        this.externalReportResultId = externalReportResultId
      }
    }
  }

  private suspend fun batchCreateReportingSetResults(
    request: BatchCreateReportingSetResultsRequest
  ) {

    val externalImpressionQualificationFilterIds: List<String> =
      request.requestsList
        .map { it.reportingSetResult.dimension }
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
            txn.getMeasurementConsumerByCmmsMeasurementConsumerId(request.cmmsMeasurementConsumerId)
          } catch (e: MeasurementConsumerNotFoundException) {
            throw e.asStatusRuntimeException(Status.Code.NOT_FOUND)
          }
          .measurementConsumerId

      val reportResultId: Long = TODO()

      batchCreateReportingSetResultsResponse {
        for (reportingSetResultRequest in request.requestsList) {
          val value = reportingSetResultRequest.reportingSetResult
          val dimension = value.dimension
          val reportingSetResultId =
            idGenerator.generateNewId {
              txn.reportingSetResultExists(measurementConsumerId, reportResultId, it)
            }
          val externalReportingSetResultId =
            idGenerator.generateNewId {
              txn.reportingSetResultExistsByExternalId(measurementConsumerId, reportResultId, it)
            }
          val impressionQualificationFilterId: Long? =
            if (dimension.custom) {
              null
            } else {
              impressionQualificationFiltersByExternalId
                .getValue(dimension.externalImpressionQualificationFilterId)
                .impressionQualificationFilterId
            }
          val metricFrequencySpecFingerprint: Long =
            Normalization.computeFingerprint(dimension.metricFrequencySpec)
          val groupingDimensionFingerprint: Long =
            Normalization.computeFingerprint(eventMessageVersion, dimension.grouping)
          val filterFingerprint: Long? =
            if (dimension.eventFiltersList.isEmpty()) {
              null
            } else {
              Normalization.computeFingerprint(dimension.eventFiltersList)
            }
          txn.insertReportingSetResult(
            measurementConsumerId,
            reportResultId,
            reportingSetResultId,
            externalReportingSetResultId,
            dimension,
            impressionQualificationFilterId,
            metricFrequencySpecFingerprint,
            groupingDimensionFingerprint,
            filterFingerprint,
            value.populationSize,
          )
          reportingSetResults +=
            value.copy {
              cmmsMeasurementConsumerId = request.cmmsMeasurementConsumerId
              this.externalReportResultId = externalReportResultId
              this.externalReportingSetResultId = externalReportingSetResultId
              this.metricFrequencySpecFingerprint = metricFrequencySpecFingerprint
              this.groupingDimensionFingerprint = groupingDimensionFingerprint
              if (filterFingerprint == null) {
                clearFilterFingerprint()
              } else {
                this.filterFingerprint = filterFingerprint
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
    if (request.cmmsMeasurementConsumerId.isEmpty()) {
      throw RequiredFieldNotSetException("cmms_measurement_consumer_id")
    }
    if (!request.hasReportResult()) {
      throw RequiredFieldNotSetException("report_result")
    }
    if (!request.reportResult.hasReportStart()) {
      throw RequiredFieldNotSetException("report_result.report_start")
    }
    if (
      request.reportResult.reportStart.timeOffsetCase == DateTime.TimeOffsetCase.TIMEOFFSET_NOT_SET
    ) {
      throw RequiredFieldNotSetException("report_result.report_start.time_offset")
    }
  }

  private fun validate(request: BatchCreateReportingSetResultsRequest) {
    val seenDimensions = mutableSetOf<ReportingSetResult.Dimension>()
    request.requestsList.mapIndexed { index, reportingSetRequest ->
      val requestPath = "requests[$index]"
      if (!reportingSetRequest.hasReportingSetResult()) {
        throw RequiredFieldNotSetException("$requestPath.reporting_set_result")
      }

      val dimension = reportingSetRequest.reportingSetResult.dimension
      val dimensionPath = "$requestPath.reporting_set_result.dimension"
      if (!seenDimensions.add(dimension)) {
        throw InvalidFieldValueException(dimensionPath) { fieldPath ->
          "$fieldPath is duplicated in request"
        }
      }
      if (dimension.externalReportingSetId.isEmpty()) {
        throw RequiredFieldNotSetException("$dimensionPath.external_reporting_set_id")
      }
      if (
        dimension.vennDiagramRegionType ==
          ReportingSetResult.Dimension.VennDiagramRegionType.VENN_DIAGRAM_REGION_TYPE_UNSPECIFIED
      ) {
        throw RequiredFieldNotSetException("$dimensionPath.venn_diagram_region_type")
      }
      if (
        dimension.vennDiagramRegionType ==
          ReportingSetResult.Dimension.VennDiagramRegionType.UNRECOGNIZED
      ) {
        throw InvalidFieldValueException("$dimensionPath.venn_diagram_region_type")
      }
      if (!dimension.custom && dimension.externalImpressionQualificationFilterId.isEmpty()) {
        throw RequiredFieldNotSetException("$dimensionPath.impression_qualification_filter")
      }
      if (!dimension.hasMetricFrequencySpec()) {
        throw RequiredFieldNotSetException("$dimensionPath.metric_frequency_spec")
      }
      if (
        dimension.eventFiltersList !=
          Normalization.normalizeEventFilters(dimension.eventFiltersList)
      ) {
        throw InvalidFieldValueException("$dimensionPath.event_filters") { fieldPath ->
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
    val result: ReportResultResult = TODO()

    return result.reportResult
  }
}
