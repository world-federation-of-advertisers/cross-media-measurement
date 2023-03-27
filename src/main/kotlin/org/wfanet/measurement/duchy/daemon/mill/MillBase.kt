// Copyright 2020 The Cross-Media Measurement Authors
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
import io.opentelemetry.api.OpenTelemetry
import io.opentelemetry.api.metrics.LongHistogram
import io.opentelemetry.api.metrics.Meter
import java.lang.management.ManagementFactory
import java.lang.management.ThreadMXBean
import java.security.cert.X509Certificate
import java.time.Clock
import java.time.Duration
import java.util.logging.Level
import java.util.logging.Logger
import kotlin.math.pow
import kotlin.random.Random
import kotlin.time.ExperimentalTime
import kotlin.time.TimeSource
import kotlinx.coroutines.CoroutineName
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.onCompletion
import kotlinx.coroutines.flow.onEach
import kotlinx.coroutines.flow.onStart
import kotlinx.coroutines.withContext
import org.wfanet.measurement.common.asBufferedFlow
import org.wfanet.measurement.common.crypto.SigningKeyHandle
import org.wfanet.measurement.common.flatten
import org.wfanet.measurement.common.logAndSuppressExceptionSuspend
import org.wfanet.measurement.common.protoTimestamp
import org.wfanet.measurement.common.throttler.MinimumIntervalThrottler
import org.wfanet.measurement.common.toProtoDuration
import org.wfanet.measurement.duchy.db.computation.BlobRef
import org.wfanet.measurement.duchy.db.computation.ComputationDataClients
import org.wfanet.measurement.duchy.db.computation.singleOutputBlobMetadata
import org.wfanet.measurement.duchy.name
import org.wfanet.measurement.duchy.number
import org.wfanet.measurement.internal.duchy.ComputationDetails.CompletedReason
import org.wfanet.measurement.internal.duchy.ComputationStage
import org.wfanet.measurement.internal.duchy.ComputationStatsGrpcKt.ComputationStatsCoroutineStub
import org.wfanet.measurement.internal.duchy.ComputationToken
import org.wfanet.measurement.internal.duchy.ComputationTypeEnum.ComputationType
import org.wfanet.measurement.internal.duchy.CreateComputationStatRequest
import org.wfanet.measurement.internal.duchy.EnqueueComputationRequest
import org.wfanet.measurement.internal.duchy.FinishComputationRequest
import org.wfanet.measurement.internal.duchy.GetComputationTokenRequest
import org.wfanet.measurement.internal.duchy.claimWorkRequest
import org.wfanet.measurement.system.v1alpha.AdvanceComputationRequest
import org.wfanet.measurement.system.v1alpha.ComputationControlGrpcKt.ComputationControlCoroutineStub
import org.wfanet.measurement.system.v1alpha.ComputationKey
import org.wfanet.measurement.system.v1alpha.ComputationLogEntriesGrpcKt.ComputationLogEntriesCoroutineStub
import org.wfanet.measurement.system.v1alpha.ComputationLogEntry.ErrorDetails.Type
import org.wfanet.measurement.system.v1alpha.ComputationParticipantKey
import org.wfanet.measurement.system.v1alpha.ComputationParticipantsGrpcKt.ComputationParticipantsCoroutineStub
import org.wfanet.measurement.system.v1alpha.ComputationsGrpcKt.ComputationsCoroutineStub as SystemComputationsCoroutineStub
import org.wfanet.measurement.system.v1alpha.CreateComputationLogEntryRequest
import org.wfanet.measurement.system.v1alpha.FailComputationParticipantRequest
import org.wfanet.measurement.system.v1alpha.setComputationResultRequest

/**
 * A [MillBase] wrapping common functionalities of mills.
 *
 * @param millId The identifier of this mill, used to claim a work.
 * @param duchyId The identifier of this duchy who owns this mill.
 * @param signingKey handle to a signing private key for consent signaling.
 * @param consentSignalCert The [Certificate] used for consent signaling.
 * @param dataClients clients that have access to local computation storage, i.e., spanner table and
 *   blob store.
 * @param systemComputationParticipantsClient client of the kingdom's system
 *   ComputationParticipantsService.
 * @param systemComputationsClient client of the kingdom's system computationsService.
 * @param systemComputationLogEntriesClient client of the kingdom's system
 *   computationLogEntriesService.
 * @param computationStatsClient client of the duchy's internal ComputationStatsService.
 * @param throttler A throttler used to rate limit the frequency of the mill polling from the
 *   computation table.
 * @param computationType The [ComputationType] this mill is working on.
 * @param requestChunkSizeBytes The size of data chunk when sending result to other duchies.
 * @param maximumAttempts The maximum number of attempts on a computation at the same stage.
 * @param clock A clock
 */
abstract class MillBase(
  protected val millId: String,
  protected val duchyId: String,
  protected val signingKey: SigningKeyHandle,
  protected val consentSignalCert: Certificate,
  protected val dataClients: ComputationDataClients,
  protected val systemComputationParticipantsClient: ComputationParticipantsCoroutineStub,
  private val systemComputationsClient: SystemComputationsCoroutineStub,
  private val systemComputationLogEntriesClient: ComputationLogEntriesCoroutineStub,
  private val computationStatsClient: ComputationStatsCoroutineStub,
  private val throttler: MinimumIntervalThrottler,
  private val computationType: ComputationType,
  private val workLockDuration: Duration,
  private val requestChunkSizeBytes: Int,
  private val maximumAttempts: Int,
  private val clock: Clock,
  openTelemetry: OpenTelemetry,
) {
  abstract val endingStage: ComputationStage

  private val meter: Meter = openTelemetry.getMeter(MillBase::class.java.name)

  init {
    meter.gaugeBuilder("active_non_daemon_thread_count").ofLongs().buildWithCallback {
      it.record((threadBean.threadCount - threadBean.daemonThreadCount).toLong())
    }
  }

  private val jniWallClockDurationHistogram: LongHistogram =
    meter.histogramBuilder("jni_wall_clock_duration_millis").ofLongs().build()

  private val stageWallClockDurationHistogram: LongHistogram =
    meter.histogramBuilder("stage_wall_clock_duration_millis").ofLongs().build()

  private val stageCpuTimeDurationHistogram: LongHistogram =
    meter.histogramBuilder("stage_cpu_time_duration_millis").ofLongs().build()

  /**
   * The main function of the mill. Continually poll and work on available computations from the
   * queue. The polling interval is controlled by the [MinimumIntervalThrottler].
   */
  suspend fun continuallyProcessComputationQueue() {
    logger.info("Mill starting...")
    withContext(CoroutineName("Mill $millId")) {
      throttler.loopOnReady {
        // All errors thrown inside the loop should be suppressed such that the mill doesn't crash.
        logAndSuppressExceptionSuspend { pollAndProcessNextComputation() }
      }
    }
  }

  private var computationsServerReady = false
  /** Poll and work on the next available computations. */
  suspend fun pollAndProcessNextComputation() {
    logger.fine("@Mill $millId: Polling available computations...")

    val claimWorkRequest = claimWorkRequest {
      computationType = this@MillBase.computationType
      owner = millId
      lockDuration = workLockDuration.toProtoDuration()
    }
    val claimWorkResponse =
      try {
        dataClients.computationsClient.claimWork(claimWorkRequest)
      } catch (ex: StatusException) {
        if (!computationsServerReady && ex.status.code == Status.Code.UNAVAILABLE) {
          logger.info("ComputationServer not ready")
          return
        }
        throw Exception("Error claiming work", ex)
      }
    computationsServerReady = true

    if (claimWorkResponse.hasToken()) {
      val token = claimWorkResponse.token
      if (token.attempt > maximumAttempts) {
        failComputation(
          token,
          "Failing computation due to too many failed ComputationStageAttempts."
        )
      }

      val wallDurationLogger = wallDurationLogger()
      val cpuDurationLogger = cpuDurationLogger()

      processComputation(token)
      wallDurationLogger.logStageDurationMetric(
        token,
        STAGE_WALL_CLOCK_DURATION,
        stageWallClockDurationHistogram
      )
      cpuDurationLogger.logStageDurationMetric(
        token,
        STAGE_CPU_DURATION,
        stageCpuTimeDurationHistogram
      )
    } else {
      logger.fine("@Mill $millId: No computation available, waiting for the next poll...")
    }
  }

  /** Process the computation according to its protocol and status. */
  private suspend fun processComputation(token: ComputationToken) {
    // Log the current mill memory usage before processing.
    logStageMetric(token, CURRENT_RUNTIME_MEMORY_MAXIMUM, Runtime.getRuntime().maxMemory())
    logStageMetric(token, CURRENT_RUNTIME_MEMORY_TOTAL, Runtime.getRuntime().totalMemory())
    logStageMetric(token, CURRENT_RUNTIME_MEMORY_FREE, Runtime.getRuntime().freeMemory())
    val stage = token.computationStage
    val globalId = token.globalComputationId
    logger.info("$globalId@$millId: Processing computation, stage $stage")

    try {
      processComputationImpl(token)
    } catch (e: Exception) {
      // The token version may have already changed. We need the latest token in order to complete
      // or enqueue the computation.
      val latestToken = getLatestComputationToken(globalId)
      handleExceptions(latestToken, e)
    }
    logger.info("$globalId@$millId: Processed computation ")
  }

  private suspend fun handleExceptions(token: ComputationToken, e: Exception) {
    val globalId = token.globalComputationId
    when (e) {
      is IllegalStateException,
      is IllegalArgumentException,
      is PermanentComputationError -> {
        failComputation(token, "PERMANENT error: ${e.localizedMessage}")
      }
      else -> {
        // Treat all other errors as transient.
        logger.log(Level.WARNING, "$globalId@$millId: TRANSIENT error", e)
        sendStatusUpdateToKingdom(newErrorUpdateRequest(token, e.localizedMessage, Type.TRANSIENT))
        if (token.attempt > maximumAttempts) {
          failComputation(token, "Failing computation due to too many failed attempts.")
        } else {
          // Enqueue the computation again for future retry
          enqueueComputation(token)
        }
      }
    }
  }

  /**
   * Sends request to the kingdom's system ComputationParticipantsService to fail the computation..
   */
  private suspend fun failComputationAtKingdom(token: ComputationToken, errorMessage: String) {
    val request =
      FailComputationParticipantRequest.newBuilder()
        .apply {
          name = ComputationParticipantKey(token.globalComputationId, duchyId).toName()
          failureBuilder.also {
            it.participantChildReferenceId = millId
            it.errorMessage = errorMessage
            it.errorTime = clock.protoTimestamp()
            it.stageAttemptBuilder.apply {
              stage = token.computationStage.number
              stageName = token.computationStage.name
              attemptNumber = token.attempt.toLong()
            }
          }
        }
        .build()
    systemComputationParticipantsClient.failComputationParticipant(request)
  }

  private suspend fun failComputation(token: ComputationToken, errorMessage: String) {
    logger.log(Level.SEVERE, "${token.globalComputationId}@$millId: $errorMessage")
    failComputationAtKingdom(token, errorMessage)
    completeComputation(token, CompletedReason.FAILED)
  }

  /** Actual implementation of processComputation(). */
  protected abstract suspend fun processComputationImpl(token: ComputationToken)

  /** Sends status update to the Kingdom's ComputationLogEntriesService. */
  private suspend fun sendStatusUpdateToKingdom(request: CreateComputationLogEntryRequest) {
    try {
      systemComputationLogEntriesClient.createComputationLogEntry(request)
    } catch (ignored: Exception) {
      logger.warning("Failed to update status change to the kingdom. $ignored")
    }
  }

  /** Sends measurement result to the kingdom's system computationsService. */
  protected suspend fun sendResultToKingdom(
    globalId: String,
    certificate: Certificate,
    resultPublicKey: ByteString,
    encryptedResult: ByteString
  ) {
    val request = setComputationResultRequest {
      name = ComputationKey(globalId).toName()
      aggregatorCertificate = certificate.name
      this.resultPublicKey = resultPublicKey
      this.encryptedResult = encryptedResult
    }
    systemComputationsClient.setComputationResult(request)
  }

  /** Writes stage metric to the [Logger] and also sends to the ComputationStatsService. */
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
   * Writes stage duration metric to the [Logger], records the metric in a histogram, and also sends
   * to the ComputationStatsService.
   */
  protected suspend fun logStageDurationMetric(
    token: ComputationToken,
    metricName: String,
    metricValue: Long,
    histogram: LongHistogram,
  ) {
    histogram.record(metricValue)
    logger.info(
      "@Mill $millId, ${token.globalComputationId}/${token.computationStage.name}/$metricName:" +
        " ${metricValue.toHumanFriendlyDuration()}"
    )
    sendComputationStats(token, metricName, metricValue)
  }

  /** Sends state metric to the ComputationStatsService. */
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

  /** Builds a [CreateComputationLogEntryRequest] to update the new error. */
  private fun newErrorUpdateRequest(
    token: ComputationToken,
    message: String,
    type: Type
  ): CreateComputationLogEntryRequest {
    val timestamp = clock.protoTimestamp()
    return CreateComputationLogEntryRequest.newBuilder()
      .apply {
        parent = ComputationParticipantKey(token.globalComputationId, duchyId).toName()
        computationLogEntryBuilder.apply {
          participantChildReferenceId = millId
          logMessage =
            "Computation ${token.globalComputationId} at stage " +
              "${token.computationStage.name}, attempt ${token.attempt} failed, $message"
          stageAttemptBuilder.apply {
            stage = token.computationStage.number
            stageName = token.computationStage.name
            stageStartTime = timestamp
            attemptNumber = token.attempt.toLong()
          }
          errorDetailsBuilder.also {
            it.errorTime = timestamp
            it.type = type
          }
        }
      }
      .build()
  }

  /** Adds a logging hook to the flow to log the total number of bytes sent out in the rpc. */
  protected fun addLoggingHook(token: ComputationToken, bytes: Flow<ByteString>): Flow<ByteString> {
    var numOfBytes = 0L
    return bytes
      .onEach { numOfBytes += it.size() }
      .onCompletion { logStageMetric(token, BYTES_OF_DATA_IN_RPC, numOfBytes) }
  }

  /** Sends an AdvanceComputationRequest to the target duchy. */
  protected suspend fun sendAdvanceComputationRequest(
    header: AdvanceComputationRequest.Header,
    content: Flow<ByteString>,
    stub: ComputationControlCoroutineStub
  ) {
    val requestFlow =
      content
        .asBufferedFlow(requestChunkSizeBytes)
        .map {
          AdvanceComputationRequest.newBuilder().apply { bodyChunkBuilder.partialData = it }.build()
        }
        .onStart { emit(AdvanceComputationRequest.newBuilder().setHeader(header).build()) }
    stub.advanceComputation(requestFlow)
  }

  /**
   * Fetches the cached result if available, otherwise compute the new result by executing [block].
   */
  protected suspend fun existingOutputOr(
    token: ComputationToken,
    block: suspend () -> ByteString
  ): ComputationResult {
    if (token.singleOutputBlobMetadata().path.isNotEmpty()) {
      // Reuse cached result if it exists
      return ComputationResult(checkNotNull(dataClients.readSingleOutputBlob(token)), token)
    }
    val newResult: ByteString =
      try {
        val wallDurationLogger = wallDurationLogger()
        val result = block()
        wallDurationLogger.logStageDurationMetric(
          token,
          JNI_WALL_CLOCK_DURATION,
          jniWallClockDurationHistogram
        )
        result
      } catch (error: Throwable) {
        // All errors from block() are permanent and would cause the computation to FAIL
        throw PermanentComputationError(error)
      }
    return ComputationResult(flowOf(newResult), dataClients.writeSingleOutputBlob(token, newResult))
  }

  /** Reads all input blobs and combines all the bytes together. */
  protected suspend fun readAndCombineAllInputBlobs(
    token: ComputationToken,
    count: Int
  ): ByteString {
    val blobMap: Map<BlobRef, ByteString> = dataClients.readInputBlobs(token)
    if (blobMap.size != count) {
      throw PermanentComputationError(
        Exception("Unexpected number of input blobs. expected $count, actual ${blobMap.size}.")
      )
    }
    return blobMap.values.flatten()
  }

  /** Completes a computation and records the [CompletedReason] */
  protected suspend fun completeComputation(
    token: ComputationToken,
    reason: CompletedReason
  ): ComputationToken {
    val response =
      dataClients.computationsClient.finishComputation(
        FinishComputationRequest.newBuilder()
          .also {
            it.token = token
            it.endingComputationStage = endingStage
            it.reason = reason
          }
          .build()
      )
    return response.token
  }

  /** Gets the latest [ComputationToken] for computation with [globalId]. */
  private suspend fun getLatestComputationToken(globalId: String): ComputationToken {
    return dataClients.computationsClient
      .getComputationToken(
        GetComputationTokenRequest.newBuilder().apply { globalComputationId = globalId }.build()
      )
      .token
  }

  /** Enqueue a computation with a delay. */
  private suspend fun enqueueComputation(token: ComputationToken) {
    // Exponential backoff
    val baseDelay = minOf(600.0, (2.0.pow(token.attempt))).toInt()
    // A random delay in the range of [baseDelay, 2*baseDelay]
    val delaySecond = baseDelay + Random.nextInt(baseDelay + 1)
    dataClients.computationsClient.enqueueComputation(
      EnqueueComputationRequest.newBuilder().setToken(token).setDelaySecond(delaySecond).build()
    )
  }

  private fun getCpuTimeMillis(): Long {
    val cpuTime = Duration.ofNanos(threadBean.allThreadIds.sumOf(threadBean::getThreadCpuTime))
    return cpuTime.toMillis()
  }

  private inner class CpuDurationLogger(private val getTimeMillis: () -> Long) {
    private val start = getTimeMillis()
    suspend fun logStageDurationMetric(
      token: ComputationToken,
      metricName: String,
      histogram: LongHistogram
    ) {
      val time = getTimeMillis() - start
      logStageDurationMetric(token, metricName, time, histogram)
    }
  }
  private fun cpuDurationLogger(): CpuDurationLogger = CpuDurationLogger(this::getCpuTimeMillis)

  @OptIn(ExperimentalTime::class)
  private inner class WallDurationLogger() {
    private val timeMark = TimeSource.Monotonic.markNow()
    suspend fun logStageDurationMetric(
      token: ComputationToken,
      metricName: String,
      histogram: LongHistogram
    ) {
      val time = timeMark.elapsedNow().inWholeMilliseconds
      logStageDurationMetric(token, metricName, time, histogram)
    }
  }
  private fun wallDurationLogger(): WallDurationLogger = WallDurationLogger()

  companion object {
    private val logger: Logger = Logger.getLogger(this::class.java.name)
    private val threadBean: ThreadMXBean = ManagementFactory.getThreadMXBean()
  }
}

const val CRYPTO_LIB_CPU_DURATION = "crypto_lib_cpu_duration_ms"
const val JNI_WALL_CLOCK_DURATION = "jni_wall_clock_duration_ms"
const val STAGE_CPU_DURATION = "stage_cpu_duration_ms"
const val STAGE_WALL_CLOCK_DURATION = "stage_wall_clock_duration_ms"
const val BYTES_OF_DATA_IN_RPC = "bytes_of_data_in_rpc"
const val CURRENT_RUNTIME_MEMORY_MAXIMUM = "current_runtime_memory_maximum"
const val CURRENT_RUNTIME_MEMORY_TOTAL = "current_runtime_memory_total"
const val CURRENT_RUNTIME_MEMORY_FREE = "current_runtime_memory_free"

data class ComputationResult(val bytes: Flow<ByteString>, val token: ComputationToken)

data class Certificate(
  // The public API name of this certificate.
  val name: String,
  // The value of the certificate.
  val value: X509Certificate
)

class PermanentComputationError(cause: Throwable) : Exception(cause)

/** Converts a milliseconds to a human friendly string. */
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
