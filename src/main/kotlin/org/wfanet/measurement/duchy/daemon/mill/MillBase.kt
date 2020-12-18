// Copyright 2020 The Measurement System Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.wfanet.measurement.duchy.daemon.mill

import com.google.protobuf.ByteString
import io.grpc.Status
import io.grpc.StatusException
import java.lang.management.ManagementFactory
import java.lang.management.ThreadMXBean
import java.time.Clock
import java.time.Duration
import java.util.logging.Level
import java.util.logging.Logger
import kotlinx.coroutines.CoroutineName
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.dropWhile
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.onCompletion
import kotlinx.coroutines.flow.onEach
import kotlinx.coroutines.flow.onStart
import kotlinx.coroutines.withContext
import org.wfanet.measurement.common.asBufferedFlow
import org.wfanet.measurement.common.flatten
import org.wfanet.measurement.common.logAndSuppressExceptionSuspend
import org.wfanet.measurement.common.protoTimestamp
import org.wfanet.measurement.common.throttler.MinimumIntervalThrottler
import org.wfanet.measurement.duchy.db.computation.BlobRef
import org.wfanet.measurement.duchy.db.computation.ComputationDataClients
import org.wfanet.measurement.duchy.db.computation.singleOutputBlobMetadata
import org.wfanet.measurement.duchy.mpcAlgorithm
import org.wfanet.measurement.duchy.name
import org.wfanet.measurement.duchy.number
import org.wfanet.measurement.internal.duchy.ClaimWorkRequest
import org.wfanet.measurement.internal.duchy.ClaimWorkResponse
import org.wfanet.measurement.internal.duchy.ComputationDetails.CompletedReason
import org.wfanet.measurement.internal.duchy.ComputationStage
import org.wfanet.measurement.internal.duchy.ComputationStatsGrpcKt.ComputationStatsCoroutineStub
import org.wfanet.measurement.internal.duchy.ComputationToken
import org.wfanet.measurement.internal.duchy.ComputationTypeEnum.ComputationType
import org.wfanet.measurement.internal.duchy.CreateComputationStatRequest
import org.wfanet.measurement.internal.duchy.EnqueueComputationRequest
import org.wfanet.measurement.internal.duchy.FinishComputationRequest
import org.wfanet.measurement.internal.duchy.GetComputationTokenRequest
import org.wfanet.measurement.internal.duchy.GetMetricValueRequest
import org.wfanet.measurement.internal.duchy.MetricValue
import org.wfanet.measurement.internal.duchy.MetricValue.ResourceKey
import org.wfanet.measurement.internal.duchy.MetricValuesGrpcKt
import org.wfanet.measurement.internal.duchy.StreamMetricValueRequest
import org.wfanet.measurement.protocol.RequisitionKey
import org.wfanet.measurement.system.v1alpha.AdvanceComputationRequest
import org.wfanet.measurement.system.v1alpha.ComputationControlGrpcKt.ComputationControlCoroutineStub
import org.wfanet.measurement.system.v1alpha.CreateGlobalComputationStatusUpdateRequest
import org.wfanet.measurement.system.v1alpha.GlobalComputationStatusUpdate
import org.wfanet.measurement.system.v1alpha.GlobalComputationStatusUpdate.ErrorDetails.ErrorType.PERMANENT
import org.wfanet.measurement.system.v1alpha.GlobalComputationStatusUpdate.ErrorDetails.ErrorType.TRANSIENT
import org.wfanet.measurement.system.v1alpha.GlobalComputationsGrpcKt.GlobalComputationsCoroutineStub
import org.wfanet.measurement.system.v1alpha.MetricRequisitionKey

/**
 * A [MillBase] wrapping common functionalities of mills.
 *
 * @param millId The identifier of this mill, used to claim a work.
 * @param dataClients clients that have access to local computation storage, i.e., spanner
 *    table and blob store.
 * @param globalComputationsClient client of the kingdom's GlobalComputationsService.
 * @param metricValuesClient client of the own duchy's MetricValuesService.
 * @param computationStatsClient client of the duchy's ComputationStatsService.
 * @param throttler A throttler used to rate limit the frequency of the mill polling from the
 *    computation table.
 * @param computationType The [ComputationType] this mill is working on.
 * @param requestChunkSizeBytes The size of data chunk when sending result to other duchies.
 * @param clock A clock
*/
abstract class MillBase(
  protected val millId: String,
  protected val dataClients: ComputationDataClients,
  protected val globalComputationsClient: GlobalComputationsCoroutineStub,
  private val metricValuesClient: MetricValuesGrpcKt.MetricValuesCoroutineStub,
  private val computationStatsClient: ComputationStatsCoroutineStub,
  private val throttler: MinimumIntervalThrottler,
  private val computationType: ComputationType,
  private val requestChunkSizeBytes: Int = 1024 * 32, // 32 KiB
  private val clock: Clock = Clock.systemUTC()
) {
  abstract val endingStage: ComputationStage

  /**
   * The main function of the mill.
   * Continually poll and work on available computations from the queue.
   * The polling interval is controlled by the [MinimumIntervalThrottler].
   */
  suspend fun continuallyProcessComputationQueue() {
    logger.info("Starting...")
    withContext(CoroutineName("Mill $millId")) {
      throttler.loopOnReady {
        pollAndProcessNextComputation()
      }
    }
  }

  /**
   * Poll and work on the next available computations.
   */
  suspend fun pollAndProcessNextComputation() {
    logger.info("@Mill $millId: Polling available computations...")
    val claimWorkRequest = ClaimWorkRequest.newBuilder()
      .setComputationType(computationType)
      .setOwner(millId)
      .build()
    val claimWorkResponse: ClaimWorkResponse =
      dataClients.computationsClient.claimWork(claimWorkRequest)
    if (claimWorkResponse.hasToken()) {
      val wallDurationLogger = wallDurationLogger()
      val cpuDurationLogger = cpuDurationLogger()
      val token = claimWorkResponse.token
      processComputation(token)
      wallDurationLogger.logStageDurationMetric(token, STAGE_WALL_CLOCK_DURATION)
      cpuDurationLogger.logStageDurationMetric(token, STAGE_CPU_DURATION)
    } else {
      logger.info("@Mill $millId: No computation available, waiting for the next poll...")
    }
  }

  /**
   * Process the computation according to its protocol and status.
   */
  private suspend fun processComputation(token: ComputationToken) {
    // Log the current mill memory usage before processing.
    logStageMetric(token, CURRENT_RUNTIME_MEMORY_MAXIMUM, Runtime.getRuntime().maxMemory())
    logStageMetric(token, CURRENT_RUNTIME_MEMORY_TOTAL, Runtime.getRuntime().totalMemory())
    logStageMetric(token, CURRENT_RUNTIME_MEMORY_FREE, Runtime.getRuntime().freeMemory())
    val stage = token.computationStage
    val globalId = token.globalComputationId
    logger.info("@Mill $millId: Processing computation $globalId, stage $stage")

    try {
      processComputationImpl(token)
    } catch (e: Exception) {
      // The token version may have already changed. We need the latest token in order to complete
      // or enqueue the computation.
      val latestToken = getLatestComputationToken(globalId)
      when (e) {
        is IllegalStateException,
        is IllegalArgumentException,
        is PermanentComputationError -> {
          logger.log(Level.SEVERE, "$globalId@$millId: PERMANENT error:", e)
          sendStatusUpdateToKingdom(
            newErrorUpdateRequest(latestToken, e.toString(), PERMANENT)
          )
          // Mark the computation FAILED for all permanent errors
          completeComputation(latestToken, CompletedReason.FAILED)
        }
        else -> {
          // Treat all other errors as transient.
          logger.log(Level.SEVERE, "$globalId@$millId: TRANSIENT error", e)
          sendStatusUpdateToKingdom(
            newErrorUpdateRequest(latestToken, e.toString(), TRANSIENT)
          )
          // Enqueue the computation again for future retry
          enqueueComputation(latestToken)
        }
      }
    }
  }

  /**
   * Actual implementation of processComputation().
   */
  protected abstract suspend fun processComputationImpl(token: ComputationToken)

  /**
   * Returns whether a value exists for the requisition by calling the
   * MetricValues service.
   */
  suspend fun metricValueExists(key: RequisitionKey): Boolean {
    return try {
      metricValuesClient.getMetricValue(
        GetMetricValueRequest.newBuilder()
          .setResourceKey(key.toResourceKey())
          .build()
      )
      true
    } catch (e: StatusException) {
      when (e.status.code) {
        Status.Code.NOT_FOUND -> false
        else -> throw e
      }
    }
  }

  /**
   * Fetches all requisitions from the MetricValuesService, discards the headers and streams all
   * data payloads as a [Flow]
   */
  protected fun streamMetricValueContents(
    availableRequisitions: Iterable<RequisitionKey>
  ): Flow<Flow<ByteString>> = flow {
    for (requisitionKey in availableRequisitions) {
      val responses = metricValuesClient.streamMetricValue(
        StreamMetricValueRequest.newBuilder()
          .setResourceKey(requisitionKey.toResourceKey())
          .build()
      )
      emit(responses.dropWhile { it.hasHeader() }.map { it.chunk.data })
    }
  }

  /**
   * Sends status update to the Kingdom's GlobalComputationsService.
   */
  private suspend fun sendStatusUpdateToKingdom(
    request: CreateGlobalComputationStatusUpdateRequest
  ) {
    try {
      globalComputationsClient.createGlobalComputationStatusUpdate(request)
    } catch (ignored: Exception) {
      logger.warning("Failed to update status change to the kingdom. $ignored")
    }
  }

  /**
   * Writes stage metric to the [Logger] and also sends to the ComputationStatsService.
   */
  private suspend fun logStageMetric(
    token: ComputationToken,
    metricName: String,
    metricValue: Long
  ) {
    logger.info(
      "@Mill $millId, ${token.globalComputationId}/${token.computationStage.name}/$metricName:" +
        " $metricValue"
    )
    sendComputationStats(token, metricName, metricValue)
  }

  /**
   * Writes stage duration metric to the [Logger] and also sends to the ComputationStatsService.
   */
  protected suspend fun logStageDurationMetric(
    token: ComputationToken,
    metricName: String,
    metricValue: Long
  ) {
    logger.info(
      "@Mill $millId, ${token.globalComputationId}/${token.computationStage.name}/$metricName:" +
        " ${metricValue.toHumanFriendlyDuration()}"
    )
    sendComputationStats(token, metricName, metricValue)
  }

  /**
   * Sends state metric to the ComputationStatsService.
   */
  private suspend fun sendComputationStats(
    token: ComputationToken,
    metricName: String,
    metricValue: Long
  ) {
    logAndSuppressExceptionSuspend {
      computationStatsClient.createComputationStat(
        CreateComputationStatRequest.newBuilder()
          .setLocalComputationId(token.localComputationId)
          .setAttempt(token.attempt)
          .setComputationStage(token.computationStage)
          .setMetricName(metricName)
          .setMetricValue(metricValue)
          .build()
      )
    }
  }

  /**
   * Builds a [CreateGlobalComputationStatusUpdateRequest] to update the new error.
   */
  private fun newErrorUpdateRequest(
    token: ComputationToken,
    message: String,
    type: GlobalComputationStatusUpdate.ErrorDetails.ErrorType
  ): CreateGlobalComputationStatusUpdateRequest {
    val timestamp = clock.protoTimestamp()
    return CreateGlobalComputationStatusUpdateRequest.newBuilder().apply {
      parentBuilder.globalComputationId = token.globalComputationId
      statusUpdateBuilder.apply {
        selfReportedIdentifier = millId
        stageDetailsBuilder.apply {
          algorithm = token.computationStage.mpcAlgorithm
          stageNumber = token.computationStage.number.toLong()
          stageName = token.computationStage.name
          start = timestamp
          attemptNumber = token.attempt.toLong()
        }
        updateMessage = "Computation ${token.globalComputationId} at stage " +
          "${token.computationStage.name}, attempt ${token.attempt} failed."
        errorDetailsBuilder.apply {
          errorTime = timestamp
          errorType = type
          errorMessage = "${token.globalComputationId}@$millId: $message"
        }
      }
    }.build()
  }

  /** Adds a logging hook to the flow to log the total number of bytes sent out in the rpc. */
  @OptIn(ExperimentalCoroutinesApi::class) // For `onCompletion`.
  protected fun addLoggingHook(
    token: ComputationToken,
    bytes: Flow<ByteString>
  ): Flow<ByteString> {
    var numOfBytes = 0L
    return bytes.onEach { numOfBytes += it.size() }
      .onCompletion {
        logStageMetric(
          token,
          BYTES_OF_DATA_IN_RPC,
          numOfBytes
        )
      }
  }

  /** Sends an AdvanceComputationRequest to the target duchy. */
  @OptIn(ExperimentalCoroutinesApi::class) // For `onStart`.
  protected suspend fun sendAdvanceComputationRequest(
    header: AdvanceComputationRequest.Header,
    content: Flow<ByteString>,
    stub: ComputationControlCoroutineStub
  ) {
    val requestFlow = content.asBufferedFlow(requestChunkSizeBytes).map {
      AdvanceComputationRequest.newBuilder().apply {
        bodyChunkBuilder.partialData = it
      }.build()
    }.onStart { emit(AdvanceComputationRequest.newBuilder().setHeader(header).build()) }
    stub.advanceComputation(requestFlow)
  }

  /**
   * Fetches the cached result if available, otherwise compute the new result by executing [block].
   */
  protected suspend fun existingOutputOr(
    token: ComputationToken,
    block: suspend () -> ByteString
  ): CachedResult {
    if (token.singleOutputBlobMetadata().path.isNotEmpty()) {
      // Reuse cached result if it exists
      return CachedResult(
        checkNotNull(dataClients.readSingleOutputBlob(token)),
        token
      )
    }
    val newResult: ByteString =
      try {
        val wallDurationLogger = wallDurationLogger()
        val result = block()
        wallDurationLogger.logStageDurationMetric(token, JNI_WALL_CLOCK_DURATION)
        result
      } catch (error: Throwable) {
        // All errors from block() are permanent and would cause the computation to FAIL
        throw PermanentComputationError(error)
      }
    return CachedResult(
      flowOf(newResult),
      dataClients.writeSingleOutputBlob(token, newResult)
    )
  }

  /**
   * Reads all input blobs and combines all the bytes together.
   */
  protected suspend fun readAndCombineAllInputBlobs(token: ComputationToken, count: Int):
    ByteString {
      val blobMap: Map<BlobRef, ByteString> = dataClients.readInputBlobs(token)
      if (blobMap.size != count) {
        throw PermanentComputationError(
          Exception("Unexpected number of input blobs. expected $count, actual ${blobMap.size}.")
        )
      }
      return blobMap.values.flatten()
    }

  /**
   * Completes a computation and records the [CompletedReason]
   */
  protected suspend fun completeComputation(
    token: ComputationToken,
    reason: CompletedReason
  ): ComputationToken {
    val response = dataClients.computationsClient.finishComputation(
      FinishComputationRequest.newBuilder().also {
        it.token = token
        it.endingComputationStage = endingStage
        it.reason = reason
      }.build()
    )
    return response.token
  }

  /**
   * Gets the latest [ComputationToken] for computation with [globalId].
   */
  private suspend fun getLatestComputationToken(globalId: String): ComputationToken {
    return dataClients.computationsClient.getComputationToken(
      GetComputationTokenRequest.newBuilder().apply {
        globalComputationId = globalId
      }.build()
    ).token
  }

  /**
   * Enqueue a computation with a delay.
   */
  private suspend fun enqueueComputation(token: ComputationToken) {
    dataClients.computationsClient.enqueueComputation(
      EnqueueComputationRequest.newBuilder()
        .setToken(token)
        .setDelaySecond(minOf(60, token.attempt * 5))
        .build()
    )
  }

  private fun getCpuTimeMillis(): Long {
    val cpuTime = Duration.ofNanos(threadBean.allThreadIds.map(threadBean::getThreadCpuTime).sum())
    return cpuTime.toMillis()
  }

  private inner class DurationLogger(private val getTimeMillis: () -> Long) {
    private val start = getTimeMillis()
    suspend fun logStageDurationMetric(token: ComputationToken, metricName: String) {
      val time = getTimeMillis() - start
      logStageDurationMetric(token, metricName, time)
    }
  }
  private fun cpuDurationLogger(): DurationLogger = DurationLogger(this::getCpuTimeMillis)
  private fun wallDurationLogger(): DurationLogger = DurationLogger(System::currentTimeMillis)

  companion object {
    private val logger: Logger = Logger.getLogger(this::class.java.name)
    private val threadBean: ThreadMXBean = ManagementFactory.getThreadMXBean()
  }
}

const val CRYPTO_LIB_CPU_DURATION = "crypto_lib_cpu_duration"
const val JNI_WALL_CLOCK_DURATION = "jni_wall_clock_duration"
const val STAGE_CPU_DURATION = "stage_cpu_duration"
const val STAGE_WALL_CLOCK_DURATION = "stage_wall_clock_duration"
const val BYTES_OF_DATA_IN_RPC = "bytes_of_data_in_rpc"
const val CURRENT_RUNTIME_MEMORY_MAXIMUM = "current_runtime_memory_maximum"
const val CURRENT_RUNTIME_MEMORY_TOTAL = "current_runtime_memory_total"
const val CURRENT_RUNTIME_MEMORY_FREE = "current_runtime_memory_free"

data class CachedResult(val bytes: Flow<ByteString>, val token: ComputationToken)

data class LiquidLegionsConfig(val decayRate: Double, val size: Long, val maxFrequency: Int)

class PermanentComputationError(cause: Throwable) : Exception(cause)

fun RequisitionKey.toResourceKey(): MetricValue.ResourceKey {
  return ResourceKey.newBuilder().apply {
    dataProviderResourceId = dataProviderId
    campaignResourceId = campaignId
    metricRequisitionResourceId = metricRequisitionId
  }.build()
}

fun RequisitionKey.toMetricRequisitionKey(): MetricRequisitionKey {
  return MetricRequisitionKey.newBuilder()
    .setCampaignId(campaignId)
    .setDataProviderId(dataProviderId)
    .setMetricRequisitionId(metricRequisitionId)
    .build()
}

/**
 * Converts a milliseconds to a human friendly string.
 */
fun Long.toHumanFriendlyDuration(): String {
  val seconds = this / 1000
  val ms = this % 1000
  val hh = seconds / 3600
  val mm = (seconds % 3600) / 60
  val ss = seconds % 60
  val hoursString = if (hh == 0L) "" else "$hh hours "
  val minutesString = if (mm == 0L) "" else "$mm minutes "
  val secondsString = if (ss == 0L) "" else "$ss seconds "
  return "$hoursString$minutesString$secondsString$ms milliseconds"
}
