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
import com.google.protobuf.Empty
import com.google.type.DateTime
import io.grpc.Status
import java.time.Clock
import java.time.Duration
import java.util.UUID
import kotlin.coroutines.CoroutineContext
import kotlin.coroutines.EmptyCoroutineContext
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.firstOrNull
import kotlinx.coroutines.flow.toList
import org.wfanet.measurement.api.v2alpha.EventTemplates
import org.wfanet.measurement.common.IdGenerator
import org.wfanet.measurement.common.RandomIdGenerator
import org.wfanet.measurement.common.generateNewId
import org.wfanet.measurement.common.toLocalDate
import org.wfanet.measurement.common.toProtoDateTime
import org.wfanet.measurement.common.toZonedDateTime
import org.wfanet.measurement.config.reporting.ImpressionQualificationFilterConfig
import org.wfanet.measurement.gcloud.spanner.AsyncDatabaseClient
import org.wfanet.measurement.internal.reporting.v2.AddProcessedResultValuesRequest
import org.wfanet.measurement.internal.reporting.v2.BasicReport
import org.wfanet.measurement.internal.reporting.v2.BatchCreateReportResultAssemblyRequest
import org.wfanet.measurement.internal.reporting.v2.BatchCreateReportingSetResultsRequest
import org.wfanet.measurement.internal.reporting.v2.BatchCreateReportingSetResultsResponse
import org.wfanet.measurement.internal.reporting.v2.BeginReportResultAssemblyRequest
import org.wfanet.measurement.internal.reporting.v2.CleanupReportResultAssembliesRequest
import org.wfanet.measurement.internal.reporting.v2.CreateReportResultRequest
import org.wfanet.measurement.internal.reporting.v2.DiscardReportResultRequest
import org.wfanet.measurement.internal.reporting.v2.GetReportResultRequest
import org.wfanet.measurement.internal.reporting.v2.ListBasicReportsRequestKt
import org.wfanet.measurement.internal.reporting.v2.ListReportingSetResultsRequest
import org.wfanet.measurement.internal.reporting.v2.ListReportingSetResultsResponse
import org.wfanet.measurement.internal.reporting.v2.ReportResult
import org.wfanet.measurement.internal.reporting.v2.ReportResultsGrpcKt
import org.wfanet.measurement.internal.reporting.v2.ReportingSetResult
import org.wfanet.measurement.internal.reporting.v2.ReportingSetResultView
import org.wfanet.measurement.internal.reporting.v2.batchCreateReportingSetResultsRequest
import org.wfanet.measurement.internal.reporting.v2.batchCreateReportingSetResultsResponse
import org.wfanet.measurement.internal.reporting.v2.copy
import org.wfanet.measurement.internal.reporting.v2.createReportResultRequest
import org.wfanet.measurement.internal.reporting.v2.listReportingSetResultsResponse
import org.wfanet.measurement.internal.reporting.v2.nonCumulativeStartOrNull
import org.wfanet.measurement.reporting.deploy.v2.gcloud.spanner.db.BasicReportResult
import org.wfanet.measurement.reporting.deploy.v2.gcloud.spanner.db.ReportResultAssembly
import org.wfanet.measurement.reporting.deploy.v2.gcloud.spanner.db.ReportResultResult
import org.wfanet.measurement.reporting.deploy.v2.gcloud.spanner.db.ReportingSetResultResult
import org.wfanet.measurement.reporting.deploy.v2.gcloud.spanner.db.discardReportResult
import org.wfanet.measurement.reporting.deploy.v2.gcloud.spanner.db.getBasicReportByExternalId
import org.wfanet.measurement.reporting.deploy.v2.gcloud.spanner.db.getMeasurementConsumerByCmmsMeasurementConsumerId
import org.wfanet.measurement.reporting.deploy.v2.gcloud.spanner.db.insertNoisyReportResultValues
import org.wfanet.measurement.reporting.deploy.v2.gcloud.spanner.db.insertReportResult
import org.wfanet.measurement.reporting.deploy.v2.gcloud.spanner.db.insertReportResultValues
import org.wfanet.measurement.reporting.deploy.v2.gcloud.spanner.db.insertReportingSetResult
import org.wfanet.measurement.reporting.deploy.v2.gcloud.spanner.db.insertReportingWindowResult
import org.wfanet.measurement.reporting.deploy.v2.gcloud.spanner.db.readAbandonedReportResultAssemblies
import org.wfanet.measurement.reporting.deploy.v2.gcloud.spanner.db.readBasicReports
import org.wfanet.measurement.reporting.deploy.v2.gcloud.spanner.db.readExistingReportingSetResultIds
import org.wfanet.measurement.reporting.deploy.v2.gcloud.spanner.db.readFullReportingSetResults
import org.wfanet.measurement.reporting.deploy.v2.gcloud.spanner.db.readReportResult
import org.wfanet.measurement.reporting.deploy.v2.gcloud.spanner.db.readReportResultAssembly
import org.wfanet.measurement.reporting.deploy.v2.gcloud.spanner.db.readReportingSetResultIds
import org.wfanet.measurement.reporting.deploy.v2.gcloud.spanner.db.readReportingWindowResultIds
import org.wfanet.measurement.reporting.deploy.v2.gcloud.spanner.db.readUnprocessedReportingSetResults
import org.wfanet.measurement.reporting.deploy.v2.gcloud.spanner.db.renewReportResultAssembly
import org.wfanet.measurement.reporting.deploy.v2.gcloud.spanner.db.reportResultExists
import org.wfanet.measurement.reporting.deploy.v2.gcloud.spanner.db.reportResultExistsWithExternalId
import org.wfanet.measurement.reporting.deploy.v2.gcloud.spanner.db.reportingWindowResultExists
import org.wfanet.measurement.reporting.deploy.v2.gcloud.spanner.db.setBasicReportStateToSucceeded
import org.wfanet.measurement.reporting.deploy.v2.gcloud.spanner.db.setBasicReportStateToUnprocessedResultsReady
import org.wfanet.measurement.reporting.service.internal.BasicReportNotFoundException
import org.wfanet.measurement.reporting.service.internal.BasicReportStateInvalidException
import org.wfanet.measurement.reporting.service.internal.GroupingDimensions
import org.wfanet.measurement.reporting.service.internal.ImpressionQualificationFilterMapping
import org.wfanet.measurement.reporting.service.internal.ImpressionQualificationFilterNotFoundException
import org.wfanet.measurement.reporting.service.internal.InvalidFieldValueException
import org.wfanet.measurement.reporting.service.internal.MeasurementConsumerNotFoundException
import org.wfanet.measurement.reporting.service.internal.Normalization
import org.wfanet.measurement.reporting.service.internal.ReportResultNotFoundException
import org.wfanet.measurement.reporting.service.internal.ReportingSetResultNotFoundException
import org.wfanet.measurement.reporting.service.internal.ReportingWindowResultNotFoundException
import org.wfanet.measurement.reporting.service.internal.RequiredFieldNotSetException

/** Spanner implementation of ReportResults gRPC service. */
class SpannerReportResultsService(
  private val spannerClient: AsyncDatabaseClient,
  private val impressionQualificationFilterMapping: ImpressionQualificationFilterMapping,
  private val clock: Clock,
  private val idGenerator: IdGenerator = RandomIdGenerator(),
  coroutineContext: CoroutineContext = EmptyCoroutineContext,
) : ReportResultsGrpcKt.ReportResultsCoroutineImplBase(coroutineContext) {
  private val eventMessageVersion =
    EventTemplates.getEventDescriptor(impressionQualificationFilterMapping.eventMessageDescriptor)
      .currentVersion
  private val groupingDimensions =
    GroupingDimensions(impressionQualificationFilterMapping.eventMessageDescriptor)

  override suspend fun beginReportResultAssembly(
    request: BeginReportResultAssemblyRequest
  ): ReportResult {
    try {
      validate(
        createReportResultRequest {
          cmmsMeasurementConsumerId = request.cmmsMeasurementConsumerId
          reportResult = request.reportResult
        }
      )
      if (request.externalBasicReportId.isEmpty()) {
        throw RequiredFieldNotSetException("external_basic_report_id")
      }
      if (request.requestId.isEmpty()) {
        throw RequiredFieldNotSetException("request_id")
      }
      if (UUID.fromString(request.requestId).toString() != request.requestId) {
        throw IllegalArgumentException("request_id must be a UUID")
      }
    } catch (e: RequiredFieldNotSetException) {
      throw e.asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    } catch (e: IllegalArgumentException) {
      throw Status.INVALID_ARGUMENT.withDescription("request_id must be a UUID")
        .withCause(e)
        .asRuntimeException()
    }
    val normalizedReportStart = request.reportResult.reportStart.toZonedDateTime().toProtoDateTime()
    return spannerClient
      .readWriteTransaction(Options.tag("action=beginReportResultAssembly"))
      .run { txn ->
        val owner =
          getAssemblyOwner(txn, request.cmmsMeasurementConsumerId, request.externalBasicReportId)
        val existing =
          txn.readReportResultAssembly(
            request.cmmsMeasurementConsumerId,
            owner.measurementConsumerId,
            owner.basicReportId,
          )
        val existingAssembly = existing?.assembly
        if (existing != null && existingAssembly?.requestId == request.requestId) {
          if (existing.reportResult.reportStart != normalizedReportStart) {
            throw Status.INVALID_ARGUMENT.withDescription(
                "request_id reused with different metadata"
              )
              .asRuntimeException()
          }
          return@run existing.reportResult
        }
        requireAssemblyPending(owner)
        if (existing != null) {
          if (checkNotNull(existingAssembly).leaseExpirationTime > clock.instant()) {
            throw Status.ALREADY_EXISTS.withDescription("Report assembly has an active lease")
              .asRuntimeException()
          }
          if (
            !txn.discardReportResult(
              request.cmmsMeasurementConsumerId,
              existing.reportResult.externalReportResultId,
            )
          ) {
            throw Status.FAILED_PRECONDITION.withDescription("Assembly is already complete")
              .asRuntimeException()
          }
        }
        val reportResultId =
          idGenerator.generateNewId { txn.reportResultExists(owner.measurementConsumerId, it) }
        val externalReportResultId =
          idGenerator.generateNewId {
            txn.reportResultExistsWithExternalId(owner.measurementConsumerId, it)
          }
        txn.insertReportResult(
          owner.measurementConsumerId,
          reportResultId,
          externalReportResultId,
          normalizedReportStart,
          ReportResultAssembly(
            owner.basicReportId,
            request.requestId,
            clock.instant().plus(ASSEMBLY_LEASE_DURATION),
          ),
        )
        request.reportResult.copy {
          cmmsMeasurementConsumerId = request.cmmsMeasurementConsumerId
          this.externalReportResultId = externalReportResultId
          reportStart = normalizedReportStart
        }
      }
  }

  override suspend fun cleanupReportResultAssemblies(
    request: CleanupReportResultAssembliesRequest
  ): Empty {
    if (request.cmmsMeasurementConsumerId.isEmpty()) {
      throw RequiredFieldNotSetException("cmms_measurement_consumer_id")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }
    val candidates =
      spannerClient
        .singleUse()
        .readAbandonedReportResultAssemblies(
          request.cmmsMeasurementConsumerId,
          clock.instant(),
          ASSEMBLY_CLEANUP_BATCH_SIZE,
          externalReportResultId = null,
        )
        .toList()
    for (externalReportResultId in candidates) {
      spannerClient.readWriteTransaction(Options.tag("action=cleanupReportResultAssembly")).run {
        txn ->
        // Recheck eligibility transactionally against lease renewals and final publication.
        if (
          txn
            .readAbandonedReportResultAssemblies(
              request.cmmsMeasurementConsumerId,
              clock.instant(),
              1,
              externalReportResultId,
            )
            .firstOrNull() != null
        ) {
          txn.discardReportResult(request.cmmsMeasurementConsumerId, externalReportResultId)
        }
      }
    }
    return Empty.getDefaultInstance()
  }

  private suspend fun getAssemblyOwner(
    txn: AsyncDatabaseClient.ReadContext,
    cmmsMeasurementConsumerId: String,
    externalBasicReportId: String,
  ): BasicReportResult {
    return try {
      txn.getBasicReportByExternalId(cmmsMeasurementConsumerId, externalBasicReportId)
    } catch (e: BasicReportNotFoundException) {
      throw e.asStatusRuntimeException(Status.Code.NOT_FOUND)
    }
  }

  private fun requireAssemblyPending(owner: BasicReportResult) {
    if (owner.basicReport.state != BasicReport.State.REPORT_CREATED) {
      throw BasicReportStateInvalidException(
          owner.basicReport.cmmsMeasurementConsumerId,
          owner.basicReport.externalBasicReportId,
          owner.basicReport.state,
        )
        .asStatusRuntimeException(Status.Code.FAILED_PRECONDITION)
    }
  }

  override suspend fun discardReportResult(request: DiscardReportResultRequest): Empty {
    if (request.cmmsMeasurementConsumerId.isEmpty()) {
      throw RequiredFieldNotSetException("cmms_measurement_consumer_id")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }
    if (request.externalReportResultId == 0L) {
      throw RequiredFieldNotSetException("external_report_result_id")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }
    spannerClient.readWriteTransaction(Options.tag("action=discardReportResult")).run { txn ->
      txn.discardReportResult(request.cmmsMeasurementConsumerId, request.externalReportResultId)
    }
    return Empty.getDefaultInstance()
  }

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
        assembly = null,
      )

      request.reportResult.copy {
        cmmsMeasurementConsumerId = request.cmmsMeasurementConsumerId
        this.externalReportResultId = externalReportResultId
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
      try {
        txn.readReportResult(request.cmmsMeasurementConsumerId, request.externalReportResultId)
      } catch (e: ReportResultNotFoundException) {
        throw e.asStatusRuntimeException(Status.Code.NOT_FOUND)
      }

    return result.reportResult
  }

  override suspend fun batchCreateReportingSetResults(
    request: BatchCreateReportingSetResultsRequest
  ): BatchCreateReportingSetResultsResponse {
    return writeReportingSetResults(request, assemblyOwner = null)
  }

  override suspend fun batchCreateReportResultAssembly(
    request: BatchCreateReportResultAssemblyRequest
  ): BatchCreateReportingSetResultsResponse {
    if (request.externalBasicReportId.isEmpty()) {
      throw RequiredFieldNotSetException("external_basic_report_id")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }
    return writeReportingSetResults(
      batchCreateReportingSetResultsRequest {
        cmmsMeasurementConsumerId = request.cmmsMeasurementConsumerId
        externalReportResultId = request.externalReportResultId
        requests += request.requestsList
        if (request.finalBatch) {
          externalBasicReportId = request.externalBasicReportId
        }
      },
      assemblyOwner = request.externalBasicReportId,
    )
  }

  private suspend fun writeReportingSetResults(
    request: BatchCreateReportingSetResultsRequest,
    assemblyOwner: String?,
  ): BatchCreateReportingSetResultsResponse {
    try {
      validate(request)
    } catch (e: RequiredFieldNotSetException) {
      throw e.asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    } catch (e: InvalidFieldValueException) {
      throw e.asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }

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
      spannerClient.readWriteTransaction(Options.tag("action=batchCreateReportingSetResults"))
    return txnRunner.run { txn: AsyncDatabaseClient.TransactionContext ->
      val result =
        try {
          txn.readReportResult(request.cmmsMeasurementConsumerId, request.externalReportResultId)
        } catch (e: ReportResultNotFoundException) {
          throw e.asStatusRuntimeException(Status.Code.NOT_FOUND)
        }
      val (measurementConsumerId, reportResultId) = result
      if (assemblyOwner == null) {
        if (result.assembly != null) {
          throw Status.FAILED_PRECONDITION.withDescription(
              "Owned results require the assembly batch API"
            )
            .asRuntimeException()
        }
      } else {
        val owner = getAssemblyOwner(txn, request.cmmsMeasurementConsumerId, assemblyOwner)
        if (result.assembly?.basicReportId != owner.basicReportId) {
          throw Status.FAILED_PRECONDITION.withDescription("Assembly owner does not match")
            .asRuntimeException()
        }
        requireAssemblyPending(owner)
        val now = clock.instant()
        if (checkNotNull(result.assembly).leaseExpirationTime <= now) {
          throw Status.FAILED_PRECONDITION.withDescription("Report assembly lease has expired")
            .asRuntimeException()
        }
        txn.renewReportResultAssembly(result, now.plus(ASSEMBLY_LEASE_DURATION))
      }

      if (request.externalBasicReportId.isNotEmpty()) {
        associateBasicReport(
          txn,
          request.cmmsMeasurementConsumerId,
          request.externalBasicReportId,
          reportResultId,
        )
      }

      val ids =
        generateReportingSetResultIds(
          txn,
          measurementConsumerId,
          reportResultId,
          request.requestsCount,
        )
      batchCreateReportingSetResultsResponse {
        for ((index, reportingSetResultRequest) in request.requestsList.withIndex()) {
          val value = reportingSetResultRequest.reportingSetResult
          val dimension = value.dimension
          val (reportingSetResultId, externalReportingSetResultId) = ids[index]
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
              externalReportResultId = request.externalReportResultId
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
            addUnprocessedWindowResults(
              txn,
              measurementConsumerId,
              reportResultId,
              reportingSetResultId,
              windowEntry,
            )
          }
        }
      }
    }
  }

  private data class ReportingSetResultIds(
    val reportingSetResultId: Long,
    val externalReportingSetResultId: Long,
  )

  private suspend fun generateReportingSetResultIds(
    txn: AsyncDatabaseClient.TransactionContext,
    measurementConsumerId: Long,
    reportResultId: Long,
    count: Int,
  ): List<ReportingSetResultIds> {
    val allocatedInternalIds = mutableSetOf<Long>()
    val allocatedExternalIds = mutableSetOf<Long>()
    val ids =
      MutableList(count) {
        ReportingSetResultIds(
          idGenerator.generateNewId { !allocatedInternalIds.add(it) },
          idGenerator.generateNewId { !allocatedExternalIds.add(it) },
        )
      }
    var pendingIndices = ids.indices.toList()
    while (pendingIndices.isNotEmpty()) {
      // Check candidates in the same transaction as the inserts, so concurrent conflicts retry.
      val existingInternalIds =
        txn.readExistingReportingSetResultIds(
          measurementConsumerId,
          reportResultId,
          pendingIndices.map { ids[it].reportingSetResultId },
        )
      val existingExternalIds =
        txn
          .readReportingSetResultIds(
            measurementConsumerId,
            reportResultId,
            pendingIndices.map { ids[it].externalReportingSetResultId },
          )
          .keys
      pendingIndices =
        pendingIndices.filter { index ->
          val candidate = ids[index]
          val internalCollision = candidate.reportingSetResultId in existingInternalIds
          val externalCollision = candidate.externalReportingSetResultId in existingExternalIds
          if (internalCollision || externalCollision) {
            ids[index] =
              candidate.copy(
                reportingSetResultId =
                  if (internalCollision) {
                    idGenerator.generateNewId { !allocatedInternalIds.add(it) }
                  } else {
                    candidate.reportingSetResultId
                  },
                externalReportingSetResultId =
                  if (externalCollision) {
                    idGenerator.generateNewId { !allocatedExternalIds.add(it) }
                  } else {
                    candidate.externalReportingSetResultId
                  },
              )
          }
          internalCollision || externalCollision
        }
    }
    return ids
  }

  private fun validate(request: BatchCreateReportingSetResultsRequest) {
    if (request.cmmsMeasurementConsumerId.isEmpty()) {
      throw RequiredFieldNotSetException("cmms_measurement_consumer_id")
    }
    if (request.externalReportResultId == 0L) {
      throw RequiredFieldNotSetException("external_report_result_id")
    }
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

      reportingSetRequest.reportingSetResult.reportingWindowResultsList.mapIndexed {
        windowIndex,
        windowEntry ->
        val windowEntryPath =
          "$requestPath.reporting_set_result.reporting_window_results[$windowIndex]"
        val unprocessedReportResultValuesPath =
          "$windowEntryPath.value.unprocessed_report_result_values"
        val windowResult: ReportingSetResult.ReportingWindowResult = windowEntry.value
        if (!windowResult.hasUnprocessedReportResultValues()) {
          throw RequiredFieldNotSetException(unprocessedReportResultValuesPath)
        }
        val unprocessedReportResultValues = windowResult.unprocessedReportResultValues
        if (
          windowEntry.key.hasNonCumulativeStart() &&
            !unprocessedReportResultValues.hasNonCumulativeResults()
        ) {
          throw RequiredFieldNotSetException(
            "$unprocessedReportResultValuesPath.non_cumulative_results"
          ) { fieldPath ->
            "$fieldPath is required when window has non-cumulative start"
          }
        }
        if (
          unprocessedReportResultValues.hasNonCumulativeResults() &&
            !windowEntry.key.hasNonCumulativeStart()
        ) {
          throw RequiredFieldNotSetException("$windowEntryPath.key.non_cumulative_start") {
            fieldPath ->
            "$fieldPath is required when window has non-cumulative results"
          }
        }
        if (
          !unprocessedReportResultValues.hasCumulativeResults() &&
            !unprocessedReportResultValues.hasNonCumulativeResults()
        ) {
          throw InvalidFieldValueException(unprocessedReportResultValuesPath) { fieldPath ->
            "$fieldPath must have a result"
          }
        }
      }
    }
  }

  private suspend fun associateBasicReport(
    txn: AsyncDatabaseClient.TransactionContext,
    cmmsMeasurementConsumerId: String,
    externalBasicReportId: String,
    reportResultId: Long,
  ) {
    val basicReportResult =
      try {
        txn.getBasicReportByExternalId(cmmsMeasurementConsumerId, externalBasicReportId)
      } catch (e: BasicReportNotFoundException) {
        throw e.asStatusRuntimeException(Status.Code.FAILED_PRECONDITION)
      }
    val basicReport = basicReportResult.basicReport
    if (basicReport.state != BasicReport.State.REPORT_CREATED) {
      throw BasicReportStateInvalidException(
        basicReport.cmmsMeasurementConsumerId,
        basicReport.externalBasicReportId,
        basicReport.state,
      )
    }
    val assembly =
      txn.readReportResultAssembly(
        cmmsMeasurementConsumerId,
        basicReportResult.measurementConsumerId,
        basicReportResult.basicReportId,
      )
    if (assembly != null && assembly.reportResultId != reportResultId) {
      throw Status.FAILED_PRECONDITION.withDescription("Another assembly attempt owns this report")
        .asRuntimeException()
    }
    txn.setBasicReportStateToUnprocessedResultsReady(
      basicReportResult.measurementConsumerId,
      basicReportResult.basicReportId,
      reportResultId,
    )
  }

  private suspend fun addUnprocessedWindowResults(
    txn: AsyncDatabaseClient.TransactionContext,
    measurementConsumerId: Long,
    reportResultId: Long,
    reportingSetResultId: Long,
    windowEntry: ReportingSetResult.ReportingWindowEntry,
  ) {
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
      windowEntry.value.unprocessedReportResultValues,
    )
  }

  override suspend fun listReportingSetResults(
    request: ListReportingSetResultsRequest
  ): ListReportingSetResultsResponse {
    if (request.pageSize != 0 || request.hasPageToken()) {
      throw Status.UNIMPLEMENTED.withDescription("Pagination not yet implemented")
        .asRuntimeException()
    }
    if (request.cmmsMeasurementConsumerId.isEmpty()) {
      throw RequiredFieldNotSetException("cmms_measurement_consumer_id")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }
    if (request.externalReportResultId == 0L) {
      throw RequiredFieldNotSetException("external_report_result_id")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }

    spannerClient.readOnlyTransaction().use { txn ->
      val (measurementConsumerId, reportResultId, _) =
        try {
          txn.readReportResult(request.cmmsMeasurementConsumerId, request.externalReportResultId)
        } catch (e: ReportResultNotFoundException) {
          throw e.asStatusRuntimeException(Status.Code.NOT_FOUND)
        }
      val resultsFlow: Flow<ReportingSetResultResult> =
        when (request.view) {
          ReportingSetResultView.REPORTING_SET_RESULT_VIEW_UNSPECIFIED,
          ReportingSetResultView.REPORTING_SET_RESULT_VIEW_UNPROCESSED ->
            txn.readUnprocessedReportingSetResults(
              impressionQualificationFilterMapping,
              groupingDimensions,
              measurementConsumerId,
              reportResultId,
            )
          ReportingSetResultView.REPORTING_SET_RESULT_VIEW_FULL ->
            txn.readFullReportingSetResults(
              impressionQualificationFilterMapping,
              groupingDimensions,
              measurementConsumerId,
              reportResultId,
            )
          ReportingSetResultView.UNRECOGNIZED ->
            throw InvalidFieldValueException("view")
              .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
        }

      return listReportingSetResultsResponse {
        resultsFlow.collect { reportingSetResults += it.reportingSetResult }
      }
    }
  }

  override suspend fun addProcessedResultValues(request: AddProcessedResultValuesRequest): Empty {
    val cmmsMeasurementConsumerId = request.cmmsMeasurementConsumerId
    val externalReportResultId = request.externalReportResultId
    try {
      validate(request)
    } catch (e: RequiredFieldNotSetException) {
      throw e.asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    } catch (e: InvalidFieldValueException) {
      throw e.asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }

    val txnRunner =
      spannerClient.readWriteTransaction(Options.tag("action=addDenoisedResultValues"))
    txnRunner.run { txn ->
      val (measurementConsumerId, reportResultId, _) =
        try {
          txn.readReportResult(cmmsMeasurementConsumerId, externalReportResultId)
        } catch (e: ReportResultNotFoundException) {
          throw e.asStatusRuntimeException(Status.Code.NOT_FOUND)
        }
      val reportingSetResultIds: Map<Long, Long> =
        txn.readReportingSetResultIds(
          measurementConsumerId,
          reportResultId,
          request.reportingSetResultsMap.keys,
        )

      for ((externalReportingSetResultId, denoisedResult) in request.reportingSetResultsMap) {
        val reportingSetResultId: Long =
          reportingSetResultIds[externalReportingSetResultId]
            ?: throw ReportingSetResultNotFoundException(
                cmmsMeasurementConsumerId,
                externalReportResultId,
                externalReportingSetResultId,
              )
              .asStatusRuntimeException(Status.Code.FAILED_PRECONDITION)
        val reportingWindowResultIds: Map<ReportingSetResult.ReportingWindow, Long> =
          txn.readReportingWindowResultIds(
            measurementConsumerId,
            reportResultId,
            reportingSetResultId,
            denoisedResult.reportingWindowResultsList.map { it.key },
          )

        for (entry in denoisedResult.reportingWindowResultsList) {
          val reportingWindow: ReportingSetResult.ReportingWindow = entry.key
          val reportingWindowResultId: Long =
            reportingWindowResultIds[reportingWindow]
              ?: throw ReportingWindowResultNotFoundException(
                  cmmsMeasurementConsumerId,
                  externalReportResultId,
                  externalReportingSetResultId,
                  reportingWindow,
                )
                .asStatusRuntimeException(Status.Code.FAILED_PRECONDITION)
          txn.insertReportResultValues(
            measurementConsumerId,
            reportResultId,
            reportingSetResultId,
            reportingWindowResultId,
            entry.value,
          )
        }
      }

      txn
        .readBasicReports(
          ListBasicReportsRequestKt.filter {
            this.cmmsMeasurementConsumerId = cmmsMeasurementConsumerId
            this.externalReportResultId = externalReportResultId
          }
        )
        .collect { basicReportResult ->
          try {
            markBasicReportCompleted(txn, basicReportResult)
          } catch (e: BasicReportStateInvalidException) {
            throw e.asStatusRuntimeException(Status.Code.FAILED_PRECONDITION)
          }
        }
    }

    return Empty.getDefaultInstance()
  }

  private fun validate(request: AddProcessedResultValuesRequest) {
    if (request.cmmsMeasurementConsumerId.isEmpty()) {
      throw RequiredFieldNotSetException("cmms_measurement_consumer_id")
    }
    if (request.externalReportResultId == 0L) {
      throw RequiredFieldNotSetException("external_report_result_id")
    }

    for ((externalReportingSetResultId, denoisedResults) in request.reportingSetResultsMap) {
      val reportingSetResultPath = "reporting_set_results[$externalReportingSetResultId]"
      denoisedResults.reportingWindowResultsList.mapIndexed { index, entry ->
        val windowEntryPath = "$reportingSetResultPath.reporting_window_results[$index]"
        if (entry.key.hasNonCumulativeStart() && !entry.value.hasNonCumulativeResults()) {
          throw RequiredFieldNotSetException("$windowEntryPath.value.non_cumulative_results") {
            fieldPath ->
            "$fieldPath is required when window has non-cumulative start"
          }
        }
        if (entry.value.hasNonCumulativeResults() && !entry.key.hasNonCumulativeStart()) {
          throw RequiredFieldNotSetException("$windowEntryPath.key.non_cumulative_start") {
            fieldPath ->
            "$fieldPath is required when window has non-cumulative results"
          }
        }
        if (!entry.value.hasCumulativeResults() && !entry.value.hasNonCumulativeResults()) {
          throw InvalidFieldValueException("$windowEntryPath.value") { fieldPath ->
            "$fieldPath must have a result"
          }
        }
      }
    }
  }

  /**
   * Marks a [BasicReport] as completed.
   *
   * @throws BasicReportStateInvalidException if the [BasicReport] is not in a valid state for the
   *   operation
   */
  private fun markBasicReportCompleted(
    txn: AsyncDatabaseClient.TransactionContext,
    basicReportResult: BasicReportResult,
  ) {
    val basicReport = basicReportResult.basicReport
    if (basicReport.state != BasicReport.State.UNPROCESSED_RESULTS_READY) {
      throw BasicReportStateInvalidException(
        basicReport.cmmsMeasurementConsumerId,
        basicReport.externalBasicReportId,
        basicReport.state,
      )
    }

    txn.setBasicReportStateToSucceeded(
      basicReportResult.measurementConsumerId,
      basicReportResult.basicReportId,
    )
  }

  companion object {
    private val ASSEMBLY_LEASE_DURATION: Duration = Duration.ofMinutes(10)
    private const val ASSEMBLY_CLEANUP_BATCH_SIZE = 100
  }
}
