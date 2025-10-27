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
import java.time.ZoneId
import java.time.ZoneOffset
import java.time.ZonedDateTime
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
import org.wfanet.measurement.internal.reporting.v2.ReportResult
import org.wfanet.measurement.internal.reporting.v2.ReportResultsGrpcKt
import org.wfanet.measurement.internal.reporting.v2.copy
import org.wfanet.measurement.internal.reporting.v2.nonCumulativeStartOrNull
import org.wfanet.measurement.reporting.deploy.v2.gcloud.spanner.db.getMeasurementConsumerByCmmsMeasurementConsumerId
import org.wfanet.measurement.reporting.deploy.v2.gcloud.spanner.db.insertReportResult
import org.wfanet.measurement.reporting.deploy.v2.gcloud.spanner.db.insertReportingSetResult
import org.wfanet.measurement.reporting.deploy.v2.gcloud.spanner.db.insertReportingWindowResult
import org.wfanet.measurement.reporting.deploy.v2.gcloud.spanner.db.reportResultExists
import org.wfanet.measurement.reporting.deploy.v2.gcloud.spanner.db.reportResultExistsWithExternalId
import org.wfanet.measurement.reporting.deploy.v2.gcloud.spanner.db.reportingSetResultExists
import org.wfanet.measurement.reporting.deploy.v2.gcloud.spanner.db.reportingWindowResultExists
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
        val iqf =
          impressionQualificationFilterMapping.getImpressionQualificationByExternalId(it)
            ?: throw ImpressionQualificationFilterNotFoundException(it)
              .asStatusRuntimeException(Status.Code.FAILED_PRECONDITION)
        iqf
      }

    val txnRunner: AsyncDatabaseClient.TransactionRunner =
      spannerClient.readWriteTransaction(Options.tag("action=createReportResult"))
    txnRunner.run { txn: AsyncDatabaseClient.TransactionContext ->
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
        request.reportResult.reportStart.toZonedDateTime().toInstant(),
      )

      for ((key, value) in reportingSetResults) {
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
        txn.insertReportingSetResult(
          measurementConsumerId,
          reportResultId,
          reportingSetResultId,
          key.externalReportingSetId,
          key.vennDiagramRegionType,
          impressionQualificationFilterId,
          key.metricFrequencySpec,
          eventMessageVersion,
          key.groupingsList,
          key.eventFiltersList,
          value.populationSize,
        )
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

          // TODO(@SanjayVas): Insert into NoisyReportResultValues
        }
      }
    }

    return request.reportResult.copy { this.externalReportResultId = externalReportResultId }
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
      if (key.groupingsList != Normalization.normalizeEventFilters(key.eventFiltersList)) {
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
}

// TODO(@SanjayVas): Use method from common once merged.
private fun DateTime.toZonedDateTime(): ZonedDateTime {
  val zoneId: ZoneId =
    when (timeOffsetCase) {
      DateTime.TimeOffsetCase.UTC_OFFSET -> ZoneOffset.ofTotalSeconds(utcOffset.seconds.toInt())
      DateTime.TimeOffsetCase.TIME_ZONE -> ZoneId.of(timeZone.id)
      DateTime.TimeOffsetCase.TIMEOFFSET_NOT_SET -> error("time_offset not set")
    }
  return ZonedDateTime.of(year, month, day, hours, minutes, seconds, nanos, zoneId)
}
