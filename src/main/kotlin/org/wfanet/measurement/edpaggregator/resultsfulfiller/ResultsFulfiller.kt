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

package org.wfanet.measurement.edpaggregator.resultsfulfiller

import com.google.crypto.tink.KmsClient
import com.google.protobuf.Message
import com.google.protobuf.kotlin.unpack
import io.grpc.StatusException
import io.opentelemetry.api.common.AttributeKey
import io.opentelemetry.api.common.Attributes
import io.opentelemetry.api.metrics.DoubleHistogram
import io.opentelemetry.api.trace.Span
import java.security.GeneralSecurityException
import java.time.Duration
import java.time.Instant
import java.util.logging.Logger
import kotlin.time.TimeSource
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.flatMapMerge
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.withContext
import org.wfanet.measurement.api.v2alpha.MeasurementSpec
import org.wfanet.measurement.api.v2alpha.PopulationSpec
import org.wfanet.measurement.api.v2alpha.Requisition
import org.wfanet.measurement.api.v2alpha.RequisitionSpec
import org.wfanet.measurement.api.v2alpha.RequisitionsGrpcKt.RequisitionsCoroutineStub
import org.wfanet.measurement.api.v2alpha.SignedMessage
import org.wfanet.measurement.api.v2alpha.getRequisitionRequest
import org.wfanet.measurement.api.v2alpha.unpack
import org.wfanet.measurement.common.api.grpc.ResourceList
import org.wfanet.measurement.common.api.grpc.flattenConcat
import org.wfanet.measurement.common.api.grpc.listResources
import org.wfanet.measurement.common.crypto.PrivateKeyHandle
import org.wfanet.measurement.common.toInstant
import org.wfanet.measurement.consent.client.dataprovider.decryptRequisitionSpec
import org.wfanet.measurement.edpaggregator.StorageConfig
import org.wfanet.measurement.edpaggregator.telemetry.Tracing
import org.wfanet.measurement.edpaggregator.v1alpha.GroupedRequisitions
import org.wfanet.measurement.edpaggregator.v1alpha.ListRequisitionMetadataRequestKt
import org.wfanet.measurement.edpaggregator.v1alpha.ListRequisitionMetadataResponse
import org.wfanet.measurement.edpaggregator.v1alpha.RequisitionMetadata
import org.wfanet.measurement.edpaggregator.v1alpha.RequisitionMetadataServiceGrpcKt.RequisitionMetadataServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.v1alpha.fulfillRequisitionMetadataRequest
import org.wfanet.measurement.edpaggregator.v1alpha.listRequisitionMetadataRequest
import org.wfanet.measurement.edpaggregator.v1alpha.markWithdrawnRequisitionMetadataRequest
import org.wfanet.measurement.edpaggregator.v1alpha.refuseRequisitionMetadataRequest
import org.wfanet.measurement.edpaggregator.v1alpha.startProcessingRequisitionMetadataRequest

/**
 * Fulfills event-level measurement requisitions using protocol-specific fulfillers.
 *
 * This orchestrates the lifecycle for a batch of requisitions: it processes grouped requisitions,
 * calculates frequency vectors via the event processing pipeline, and dispatches fulfillment using
 * a FulfillerSelector to choose the appropriate protocol implementation.
 *
 * Concurrency: supports concurrent fulfillment per batch via Kotlin coroutines. Long-running or
 * blocking operations (storage access, crypto, and RPC) execute on the IO dispatcher as
 * appropriate.
 *
 * @param dataProvider [DataProvider] resource name.
 * @param requisitionMetadataStub used to sync [Requisition]s with the RequisitionMetadataStorage
 * @param privateEncryptionKey Private key used to decrypt `RequisitionSpec`s.
 * @param groupedRequisitions The grouped requisitions to fulfill.
 * @param modelLineInfoMap Map of model line to [ModelLineInfo] providing descriptors and indexes.
 * @param pipelineConfiguration Configuration for the event processing pipeline.
 * @param impressionDataSourceProvider Service to resolve impression metadata and sources.
 * @param kmsClient KMS client for accessing encrypted resources in storage.
 * @param impressionsStorageConfig Storage configuration for impression/event ingestion.
 * @param fulfillerSelector Selector for choosing the appropriate fulfiller based on protocol.
 * @param responsePageSize
 * @param metrics Metrics recorder for telemetry.
 */
class ResultsFulfiller(
  private val dataProvider: String,
  private val requisitionMetadataStub: RequisitionMetadataServiceCoroutineStub,
  private val requisitionsStub: RequisitionsCoroutineStub,
  private val privateEncryptionKey: PrivateKeyHandle,
  private val groupedRequisitions: GroupedRequisitions,
  private val modelLineInfoMap: Map<String, ModelLineInfo>,
  private val pipelineConfiguration: PipelineConfiguration,
  private val impressionDataSourceProvider: ImpressionDataSourceProvider,
  private val kmsClient: KmsClient?,
  private val impressionsStorageConfig: StorageConfig,
  private val fulfillerSelector: FulfillerSelector,
  private val responsePageSize: Int? = null,
  private val metrics: ResultsFulfillerMetrics,
) {

  private val orchestrator: EventProcessingOrchestrator<Message> by lazy {
    EventProcessingOrchestrator<Message>(privateEncryptionKey)
  }

  /**
   * Processes and fulfills all requisitions in the grouped requisitions.
   *
   * Steps:
   * - Builds frequency vectors via the event-processing pipeline.
   * - Selects a protocol-specific fulfiller for each requisition and submits results.
   *
   * @param parallelism Maximum number of requisitions to fulfill concurrently.
   * @throws IllegalArgumentException If a requisition specifies an unsupported protocol.
   * @throws Exception If decryption or RPC fulfillment fails.
   */
  @OptIn(ExperimentalCoroutinesApi::class)
  suspend fun fulfillRequisitions(parallelism: Int = DEFAULT_FULFILLMENT_PARALLELISM) {
    val reportProcessingTimer = TimeSource.Monotonic.markNow()
    val requisitions =
      groupedRequisitions.requisitionsList.mapIndexed { index, entry ->
        entry.requisition.unpack(Requisition::class.java)
      }

    val eventGroupReferenceIdMap =
      groupedRequisitions.eventGroupMapList.associate {
        it.eventGroup to it.details.eventGroupReferenceId
      }

    // Filter requisitions that are not in the requisitions metadata storage or have been either
    // fulfilled or refused already
    val requisitionsMetadata: List<RequisitionMetadata> = listRequisitionMetadata()

    require(requisitionsMetadata.isNotEmpty()) {
      "No requisition metadata found for group id: ${groupedRequisitions.groupId}"
    }

    val requisitionMetadataByName: Map<String, RequisitionMetadata> =
      requisitionsMetadata.associateBy { it.cmmsRequisition }

    val filteredRequisitions =
      requisitions.filter { it.shouldBeProcessed(requisitionMetadataByName) }
    if (filteredRequisitions.isEmpty()) {
      return
    }

    val reportId: String =
      requireNotNull(requisitionsMetadata.first().report) {
        "Report ID is missing from requisition metadata for group id: ${groupedRequisitions.groupId}"
      }
    val earliestCreateTime: Instant =
      requireNotNull(requisitionsMetadata.mapNotNull { it.createTime?.toInstant() }.minOrNull()) {
        "Create time is missing from requisition metadata for group id: ${groupedRequisitions.groupId}"
      }
    val modelLine = groupedRequisitions.modelLine
    val modelInfo = modelLineInfoMap.getValue(modelLine)
    val eventDescriptor = modelInfo.eventDescriptor

    val populationSpec = modelInfo.populationSpec
    val vidIndexMap = modelInfo.vidIndexMap

    val eventSource =
      StorageEventSource(
        impressionDataSourceProvider = impressionDataSourceProvider,
        eventGroupDetailsList = groupedRequisitions.eventGroupMapList.map { it.details },
        modelLine = modelLine,
        kmsClient = kmsClient,
        impressionsStorageConfig = impressionsStorageConfig,
        descriptor = eventDescriptor,
        batchSize = pipelineConfiguration.batchSize,
      )

    Tracing.traceSuspending(spanName = SPAN_REPORT_FULFILLMENT, attributes = Attributes.empty()) {
      val span = Span.current()
      val frequencyVectorMap =
        metrics.frequencyVectorDuration.measureSuspending {
          orchestrator.run(
            eventSource = eventSource,
            vidIndexMap = vidIndexMap,
            populationSpec = populationSpec,
            requisitions = filteredRequisitions,
            eventGroupReferenceIdMap = eventGroupReferenceIdMap,
            config = pipelineConfiguration,
            eventDescriptor = eventDescriptor,
          )
        }
      span.addEvent(
        EVENT_FREQUENCY_VECTOR_FINISHED,
        Attributes.builder()
          .put(ATTR_REQUISITION_COUNT_KEY, filteredRequisitions.size.toLong())
          .put(ATTR_MODEL_LINE_KEY, modelLine)
          .put(ATTR_GROUP_ID_KEY, groupedRequisitions.groupId)
          .put(ATTR_REPORT_ID_KEY, reportId)
          .build(),
      )

      filteredRequisitions
        .asFlow()
        .map { req: Requisition -> req to frequencyVectorMap.getValue(req.name) }
        .flatMapMerge(concurrency = parallelism) { (req, frequencyVector) ->
          flow {
            fulfillSingleRequisition(
              requisition = req,
              frequencyVector = frequencyVector,
              populationSpec = populationSpec,
              requisitionsMetadata = requisitionMetadataByName,
            )
            emit(Unit)
          }
        }
        .collect()

      recordReportCompletion(
        span = span,
        requisitionCount = filteredRequisitions.size,
        modelLine = modelLine,
        processingTimer = reportProcessingTimer,
        earliestCreateTime = earliestCreateTime,
        groupId = groupedRequisitions.groupId,
        reportId = reportId,
      )
    }
  }

  private suspend fun Requisition.shouldBeProcessed(
    metadataByName: Map<String, RequisitionMetadata>
  ): Boolean {
    val metadata = metadataByName[name]
    requireNotNull(metadata) { "Requisition metadata not found for requisition: $name" }
    val requisition =
      requisitionsStub.getRequisition(getRequisitionRequest { name = metadata.cmmsRequisition })
    return when (requisition.state) {
      Requisition.State.FULFILLED -> {
        if (metadata.state !== RequisitionMetadata.State.FULFILLED) {
          signalRequisitionFulfilled(metadata)
        }
        false
      }
      Requisition.State.UNFULFILLED -> {
        true
      }
      Requisition.State.WITHDRAWN -> {
        if (metadata.state !== RequisitionMetadata.State.WITHDRAWN) {
          signalRequisitionWithdrawn(metadata)
        }
        false
      }
      Requisition.State.REFUSED -> {
        if (metadata.state !== RequisitionMetadata.State.REFUSED) {
          val refusalMessage = "Requisition in invalid cmms state: ${requisition.state}"
          signalRequisitionRefused(metadata, refusalMessage)
        }
        false
      }
      else -> {
        throw IllegalStateException(
          "Requisition is in unsupported cmms state: ${requisition.state}"
        )
      }
    }
  }

  /**
   * Decrypts inputs, selects a protocol implementation, and fulfills a single requisition.
   *
   * @param requisition The `Requisition` to fulfill.
   * @param frequencyVector Pre-computed per-VID frequency vector for this requisition.
   * @param populationSpec Population specification associated with the model line.
   * @throws Exception If the requisition spec cannot be decrypted or fulfillment fails.
   */
  private suspend fun fulfillSingleRequisition(
    requisition: Requisition,
    frequencyVector: StripedByteFrequencyVector,
    populationSpec: PopulationSpec,
    requisitionsMetadata: Map<String, RequisitionMetadata>,
  ) {

    val requisitionProcessingTimer = TimeSource.Monotonic.markNow()
    // Update the Requisition status on the ImpressionMetadataStorage
    val requisitionMetadata =
      requisitionsMetadata[requisition.name]
        ?: throw IllegalArgumentException(
          "Requisition metadata not found for requisition: ${requisition.name}"
        )

    val measurementSpec: MeasurementSpec = requisition.measurementSpec.message.unpack()
    val signedRequisitionSpec: SignedMessage =
      try {
        withContext(Dispatchers.IO) {
          val result =
            decryptRequisitionSpec(requisition.encryptedRequisitionSpec, privateEncryptionKey)
          result
        }
      } catch (e: GeneralSecurityException) {
        throw Exception("RequisitionSpec decryption failed", e)
      }
    val requisitionSpec: RequisitionSpec = signedRequisitionSpec.unpack() // TODO: Issue #2914
    val reportId =
      requireNotNull(requisitionMetadata.report) {
        "Report ID is missing from requisition metadata: ${requisitionMetadata.name}"
      }

    Tracing.traceSuspending(
      spanName = SPAN_REQUISITION_FULFILLMENT,
      attributes = Attributes.empty(),
    ) {
      val span = Span.current()
      try {
        val updateMetadataResult =
          metrics.networkTasksDuration.measureSuspending {
            signalRequisitionStartProcessing(requisitionMetadata)
          }
        span.addEvent(
          EVENT_REQUISITION_START_PROCESSING_SIGNALED,
          Attributes.builder()
            .put(ATTR_REQUISITION_METADATA_NAME_KEY, updateMetadataResult.name)
            .put(ATTR_REQUISITION_NAME_KEY, requisition.name)
            .put(ATTR_GROUP_ID_KEY, groupedRequisitions.groupId)
            .put(ATTR_REPORT_ID_KEY, reportId)
            .build(),
        )

        val fulfiller =
          fulfillerSelector.selectFulfiller(
            requisition,
            measurementSpec,
            requisitionSpec,
            frequencyVector,
            populationSpec,
          )
        val fulfillerType =
          fulfiller::class.simpleName
            ?: fulfiller::class.java.simpleName
            ?: fulfiller::class.java.name

        metrics.sendDuration.measureSuspending {
          withContext(Dispatchers.IO) { fulfiller.fulfillRequisition() }
        }
        span.addEvent(
          EVENT_REQUISITION_FULFILLMENT_SENT,
          Attributes.builder()
            .put(ATTR_FULFILLER_TYPE_KEY, fulfillerType)
            .put(ATTR_REQUISITION_NAME_KEY, requisition.name)
            .put(ATTR_GROUP_ID_KEY, groupedRequisitions.groupId)
            .put(ATTR_REPORT_ID_KEY, reportId)
            .build(),
        )

        val fulfilledMetadata =
          metrics.networkTasksDuration.measureSuspending {
            signalRequisitionFulfilled(updateMetadataResult)
          }
        span.addEvent(
          EVENT_REQUISITION_METADATA_FULFILLED,
          Attributes.builder()
            .put(ATTR_REQUISITION_METADATA_NAME_KEY, fulfilledMetadata.name)
            .put(ATTR_REQUISITION_NAME_KEY, requisition.name)
            .put(ATTR_GROUP_ID_KEY, groupedRequisitions.groupId)
            .put(ATTR_REPORT_ID_KEY, reportId)
            .build(),
        )

        recordRequisitionCompletion(
          span = span,
          requisitionProcessingTimer = requisitionProcessingTimer,
          requisitionMetadata = requisitionMetadata,
        )
      } catch (t: Throwable) {
        recordRequisitionFailure(
          span = span,
          throwable = t,
          requisitionMetadata = requisitionMetadata,
          requisitionName = requisition.name,
        )
        throw t
      }
    }
  }

  // List requisitions metadata for the goup id being processed.
  private suspend fun listRequisitionMetadata(): List<RequisitionMetadata> {
    val requisitionsMetadata: Flow<RequisitionMetadata> =
      requisitionMetadataStub
        .listResources { pageToken: String ->
          val request = listRequisitionMetadataRequest {
            parent = dataProvider
            filter =
              ListRequisitionMetadataRequestKt.filter { groupId = groupedRequisitions.groupId }
            if (responsePageSize != null) {
              pageSize = responsePageSize
            }
            this.pageToken = pageToken
          }
          val response: ListRequisitionMetadataResponse =
            try {
              requisitionMetadataStub.listRequisitionMetadata(request)
            } catch (e: StatusException) {
              throw Exception(
                "Error listing requisition metadata for group id: ${groupedRequisitions.groupId}",
                e,
              )
            }
          ResourceList(response.requisitionMetadataList, response.nextPageToken)
        }
        .flattenConcat()
    return requisitionsMetadata.toList()
  }

  private suspend fun signalRequisitionStartProcessing(
    requisitionMetadata: RequisitionMetadata
  ): RequisitionMetadata {
    val startProcessingRequisitionMetadataRequest = startProcessingRequisitionMetadataRequest {
      name = requisitionMetadata.name
      etag = requisitionMetadata.etag
    }
    return requisitionMetadataStub.startProcessingRequisitionMetadata(
      startProcessingRequisitionMetadataRequest
    )
  }

  private suspend fun signalRequisitionFulfilled(
    requisitionMetadata: RequisitionMetadata
  ): RequisitionMetadata {
    val fulfillRequisitionMetadataRequest = fulfillRequisitionMetadataRequest {
      name = requisitionMetadata.name
      etag = requisitionMetadata.etag
    }
    return requisitionMetadataStub.fulfillRequisitionMetadata(fulfillRequisitionMetadataRequest)
  }

  private suspend fun signalRequisitionRefused(
    requisitionMetadata: RequisitionMetadata,
    refusalMessage: String,
  ): RequisitionMetadata {
    val refuseRequisitionMetadataRequest = refuseRequisitionMetadataRequest {
      name = requisitionMetadata.name
      etag = requisitionMetadata.etag
      this.refusalMessage = refusalMessage
    }
    return requisitionMetadataStub.refuseRequisitionMetadata(refuseRequisitionMetadataRequest)
  }

  private suspend fun signalRequisitionWithdrawn(
    requisitionMetadata: RequisitionMetadata
  ): RequisitionMetadata {
    val markWithdrawnRequisitionMetadataRequest = markWithdrawnRequisitionMetadataRequest {
      name = requisitionMetadata.name
      etag = requisitionMetadata.etag
    }
    return requisitionMetadataStub.markWithdrawnRequisitionMetadata(
      markWithdrawnRequisitionMetadataRequest
    )
  }

  private suspend fun <T> DoubleHistogram.measureSuspending(block: suspend () -> T): T {
    val timer = TimeSource.Monotonic.markNow()
    return try {
      block()
    } finally {
      record(timer.elapsedNow().inWholeNanoseconds / NANOS_TO_SECONDS)
    }
  }

  private fun recordReportCompletion(
    span: Span,
    requisitionCount: Int,
    modelLine: String,
    processingTimer: TimeSource.Monotonic.ValueTimeMark,
    earliestCreateTime: Instant,
    groupId: String,
    reportId: String,
  ) {
    span.addEvent(
      EVENT_REQUISITIONS_FULFILLMENT_FINISHED,
      Attributes.builder()
        .put(ATTR_REQUISITION_COUNT_KEY, requisitionCount.toLong())
        .put(ATTR_MODEL_LINE_KEY, modelLine)
        .put(ATTR_GROUP_ID_KEY, groupId)
        .put(ATTR_REPORT_ID_KEY, reportId)
        .build(),
    )

    val processingDurationSeconds =
      processingTimer.elapsedNow().inWholeNanoseconds / NANOS_TO_SECONDS
    metrics.reportProcessingDuration.record(processingDurationSeconds)
    val reportCompletionTime = Instant.now()
    val reportLatencySeconds =
      Duration.between(earliestCreateTime, reportCompletionTime).toNanos().toDouble() /
        NANOS_TO_SECONDS
    metrics.reportFulfillmentLatency.record(reportLatencySeconds)
    span.addEvent(
      EVENT_REPORT_PROCESSING_FINISHED,
      Attributes.builder()
        .put(ATTR_STATUS_KEY, STATUS_SUCCESS)
        .put(ATTR_REPORT_LATENCY_SECONDS_KEY, reportLatencySeconds)
        .put(ATTR_GROUP_ID_KEY, groupId)
        .put(ATTR_REPORT_ID_KEY, reportId)
        .build(),
    )
  }

  private fun recordRequisitionCompletion(
    span: Span,
    requisitionProcessingTimer: TimeSource.Monotonic.ValueTimeMark,
    requisitionMetadata: RequisitionMetadata,
  ) {
    val requisitionProcessingDurationSeconds =
      requisitionProcessingTimer.elapsedNow().inWholeNanoseconds / NANOS_TO_SECONDS
    metrics.requisitionProcessingDuration.record(requisitionProcessingDurationSeconds)
    val requisitionLatency =
      Duration.between(requisitionMetadata.cmmsCreateTime.toInstant(), Instant.now())
    metrics.requisitionFulfillmentLatency.record(
      requisitionLatency.toNanos().toDouble() / NANOS_TO_SECONDS
    )
    metrics.requisitionsProcessed.add(1, ResultsFulfillerMetrics.statusSuccess)
    span.addEvent(
      EVENT_REQUISITION_PROCESSING_FINISHED,
      Attributes.builder()
        .put(ATTR_STATUS_KEY, STATUS_SUCCESS)
        .put(ATTR_GROUP_ID_KEY, requisitionMetadata.groupId)
        .put(ATTR_REPORT_ID_KEY, requisitionMetadata.report!!)
        .put(ATTR_REQUISITION_NAME_KEY, requisitionMetadata.cmmsRequisition)
        .build(),
    )
  }

  private fun recordRequisitionFailure(
    span: Span,
    throwable: Throwable,
    requisitionMetadata: RequisitionMetadata?,
    requisitionName: String,
  ) {
    span.addEvent(
      EVENT_REQUISITION_PROCESSING_FAILED,
      Attributes.builder()
        .put(ATTR_STATUS_KEY, STATUS_FAILURE)
        .put(
          ATTR_ERROR_TYPE_KEY,
          throwable::class.simpleName
            ?: throwable::class.java.simpleName
            ?: throwable::class.java.name,
        )
        .put(ATTR_GROUP_ID_KEY, requisitionMetadata?.groupId ?: groupedRequisitions.groupId)
        .put(ATTR_REPORT_ID_KEY, requisitionMetadata?.report ?: UNKNOWN_REPORT_ID)
        .apply {
          val metadataRequisition = requisitionMetadata?.cmmsRequisition
          if (!metadataRequisition.isNullOrEmpty()) {
            put(ATTR_REQUISITION_NAME_KEY, metadataRequisition)
          } else if (requisitionName.isNotEmpty()) {
            put(ATTR_REQUISITION_NAME_KEY, requisitionName)
          }
        }
        .build(),
    )
    metrics.requisitionsProcessed.add(1, ResultsFulfillerMetrics.statusFailure)
  }

  companion object {
    private val logger: Logger = Logger.getLogger(this::class.java.name)

    // Memory-based parallelism limit to prevent OOM with large frequency vectors
    // With 360M sized frequency vector sampled at 10%, we get 36M x 16 bytes for
    // the intermediate integer list in FrequencyVectorBuilder, ~0.5GB per requisition.
    // For 15 parallel fulfillments we end up at ~8GB in total.
    private const val MAX_FULFILLMENT_PARALLELISM: Int = 15

    /** Utilize all cpu cores but keep one free for GC and system work, capped by memory limit. */
    private val DEFAULT_FULFILLMENT_PARALLELISM: Int =
      ((Runtime.getRuntime().availableProcessors()).coerceAtLeast(2) - 1).coerceAtMost(
        MAX_FULFILLMENT_PARALLELISM
      )

    /** Conversion factor from nanoseconds to seconds. */
    private const val NANOS_TO_SECONDS = 1_000_000_000.0

    /** Conversion factor from nanoseconds to milliseconds. */
    private const val NANOS_TO_MILLIS = 1_000_000

    /** Mask for converting signed byte to unsigned int. */
    private const val BYTE_TO_UNSIGNED_MASK = 0xFF

    private const val SPAN_REPORT_FULFILLMENT = "report_fulfillment"
    private const val SPAN_REQUISITION_FULFILLMENT = "requisition_fulfillment"

    private val ATTR_GROUP_ID_KEY = AttributeKey.stringKey("edpa.results_fulfiller.group_id")
    private val ATTR_REPORT_ID_KEY = AttributeKey.stringKey("edpa.results_fulfiller.report_id")
    private val ATTR_REQUISITION_NAME_KEY =
      AttributeKey.stringKey("edpa.results_fulfiller.cmms_requisition")
    private val ATTR_MODEL_LINE_KEY = AttributeKey.stringKey("edpa.results_fulfiller.model_line")
    private val ATTR_REQUISITION_COUNT_KEY =
      AttributeKey.longKey("edpa.results_fulfiller.requisition_count")
    private val ATTR_PROCESSING_DURATION_SECONDS_KEY =
      AttributeKey.doubleKey("edpa.results_fulfiller.processing_duration_seconds")
    private val ATTR_REPORT_LATENCY_SECONDS_KEY =
      AttributeKey.doubleKey("edpa.results_fulfiller.report_latency_seconds")
    private val ATTR_STATUS_KEY = AttributeKey.stringKey("edpa.results_fulfiller.status")
    private val ATTR_REQUISITION_METADATA_NAME_KEY =
      AttributeKey.stringKey("edpa.results_fulfiller.requisition_metadata_name")
    private val ATTR_FULFILLER_TYPE_KEY =
      AttributeKey.stringKey("edpa.results_fulfiller.fulfiller_type")
    private val ATTR_ERROR_TYPE_KEY = AttributeKey.stringKey("edpa.results_fulfiller.error_type")

    private const val EVENT_FREQUENCY_VECTOR_FINISHED = "frequency_vector_computation_finished"
    private const val EVENT_REQUISITIONS_FULFILLMENT_FINISHED = "requisitions_fulfillment_finished"
    private const val EVENT_REPORT_PROCESSING_FINISHED = "report_processing_finished"
    private const val EVENT_REQUISITION_START_PROCESSING_SIGNALED =
      "requisition_start_processing_signaled"
    private const val EVENT_REQUISITION_FULFILLMENT_SENT = "requisition_fulfillment_sent"
    private const val EVENT_REQUISITION_METADATA_FULFILLED = "requisition_metadata_fulfilled"
    private const val EVENT_REQUISITION_PROCESSING_FINISHED = "requisition_processing_finished"
    private const val EVENT_REQUISITION_PROCESSING_FAILED = "requisition_processing_failed"

    private const val STATUS_SUCCESS = "success"
    private const val STATUS_FAILURE = "failure"
    private const val UNKNOWN_REPORT_ID = "unknown"
  }
}
