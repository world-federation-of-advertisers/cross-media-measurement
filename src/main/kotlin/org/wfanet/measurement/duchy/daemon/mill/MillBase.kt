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
import io.opentelemetry.api.common.AttributeKey
import io.opentelemetry.api.common.Attributes
import io.opentelemetry.api.metrics.DoubleHistogram
import io.opentelemetry.api.metrics.Meter
import java.security.cert.X509Certificate
import java.time.Clock
import java.time.Duration
import java.util.logging.Level
import java.util.logging.Logger
import kotlin.math.pow
import kotlin.random.Random
import kotlin.time.ExperimentalTime
import kotlin.time.TimeSource
import kotlin.time.toJavaDuration
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.onCompletion
import kotlinx.coroutines.flow.onEach
import kotlinx.coroutines.flow.onStart
import kotlinx.coroutines.time.delay
import kotlinx.coroutines.yield
import org.wfanet.measurement.api.Version
import org.wfanet.measurement.common.ExponentialBackoff
import org.wfanet.measurement.common.asBufferedFlow
import org.wfanet.measurement.common.crypto.SigningKeyHandle
import org.wfanet.measurement.common.flatten
import org.wfanet.measurement.common.logAndSuppressExceptionSuspend
import org.wfanet.measurement.common.protoTimestamp
import org.wfanet.measurement.common.toProtoDuration
import org.wfanet.measurement.consent.client.duchy.encryptResult
import org.wfanet.measurement.consent.client.duchy.signResult
import org.wfanet.measurement.duchy.daemon.utils.ComputationResult
import org.wfanet.measurement.duchy.daemon.utils.toV2AlphaEncryptionPublicKey
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
import org.wfanet.measurement.internal.duchy.FinishComputationResponse
import org.wfanet.measurement.internal.duchy.GetComputationTokenResponse
import org.wfanet.measurement.internal.duchy.UpdateComputationDetailsRequest
import org.wfanet.measurement.internal.duchy.UpdateComputationDetailsResponse
import org.wfanet.measurement.internal.duchy.claimWorkRequest
import org.wfanet.measurement.internal.duchy.enqueueComputationRequest
import org.wfanet.measurement.internal.duchy.finishComputationRequest
import org.wfanet.measurement.internal.duchy.getComputationTokenRequest
import org.wfanet.measurement.system.v1alpha.AdvanceComputationRequest
import org.wfanet.measurement.system.v1alpha.ComputationControlGrpcKt.ComputationControlCoroutineStub
import org.wfanet.measurement.system.v1alpha.ComputationKey
import org.wfanet.measurement.system.v1alpha.ComputationLogEntriesGrpcKt.ComputationLogEntriesCoroutineStub
import org.wfanet.measurement.system.v1alpha.ComputationLogEntry
import org.wfanet.measurement.system.v1alpha.ComputationParticipant
import org.wfanet.measurement.system.v1alpha.ComputationParticipantKey
import org.wfanet.measurement.system.v1alpha.ComputationParticipantKt
import org.wfanet.measurement.system.v1alpha.ComputationParticipantsGrpcKt.ComputationParticipantsCoroutineStub
import org.wfanet.measurement.system.v1alpha.ComputationsGrpcKt.ComputationsCoroutineStub as SystemComputationsCoroutineStub
import org.wfanet.measurement.system.v1alpha.CreateComputationLogEntryRequest
import org.wfanet.measurement.system.v1alpha.failComputationParticipantRequest
import org.wfanet.measurement.system.v1alpha.getComputationParticipantRequest
import org.wfanet.measurement.system.v1alpha.setComputationResultRequest
import org.wfanet.measurement.system.v1alpha.setParticipantRequisitionParamsRequest
import org.wfanet.measurement.system.v1alpha.stageAttempt

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
 * @param computationType The [ComputationType] this mill is working on.
 * @param requestChunkSizeBytes The size of data chunk when sending result to other duchies.
 * @param maximumAttempts The maximum number of attempts on a computation at the same stage.
 * @param clock A clock
 * @param rpcRetryBackoff backoff for RPC retries
 * @param rpcMaxAttempts maximum number of attempts for an RPC
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
  private val computationType: ComputationType,
  private val workLockDuration: Duration,
  private val requestChunkSizeBytes: Int,
  private val maximumAttempts: Int,
  private val clock: Clock,
  openTelemetry: OpenTelemetry,
  private val rpcRetryBackoff: ExponentialBackoff = ExponentialBackoff(),
  private val rpcMaxAttempts: Int = 3,
) {
  abstract val endingStage: ComputationStage

  private val meter: Meter = openTelemetry.getMeter(this::class.qualifiedName!!)

  protected val cryptoWallClockDurationHistogram: DoubleHistogram =
    meter
      .histogramBuilder("halo_cmm.computation.stage.crypto.time")
      .setDescription("Wall time spent in crypto operations for a computation stage")
      .setUnit("s")
      .build()

  protected val cryptoCpuDurationHistogram: DoubleHistogram =
    meter
      .histogramBuilder("halo_cmm.computation.stage.crypto.cpu.time")
      .setDescription("CPU time spent in crypto operations for a computation stage")
      .setUnit("s")
      .build()

  private val stageWallClockDurationHistogram: DoubleHistogram =
    meter
      .histogramBuilder("halo_cmm.computation.stage.time")
      .setDescription("Wall time for a computation stage")
      .setUnit("s")
      .build()

  protected val stageDataTransmissionDurationHistogram: DoubleHistogram =
    meter
      .histogramBuilder("halo_cmm.computation.stage.transmission.time")
      .setDescription("Wall time for data transmission")
      .setUnit("s")
      .build()

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
      } catch (e: StatusException) {
        if (!computationsServerReady && e.status.code == Status.Code.UNAVAILABLE) {
          logger.info("ComputationServer not ready")
          return
        }
        throw Exception("Error claiming work", e)
      }
    computationsServerReady = true

    if (claimWorkResponse.hasToken()) {
      val token = claimWorkResponse.token
      if (token.attempt > maximumAttempts) {
        failComputation(
          token,
          "Failing computation due to too many failed ComputationStageAttempts.",
        )
        return
      }

      val wallDurationLogger = wallDurationLogger()

      processComputation(token)
      wallDurationLogger.logStageDurationMetric(
        token,
        STAGE_WALL_CLOCK_DURATION,
        stageWallClockDurationHistogram,
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
      try {
        // The token version may have already changed. We need the latest token in order to complete
        // or enqueue the computation.
        val latestToken = getLatestComputationToken(globalId)
        handleExceptions(latestToken, e)
      } catch (e: Exception) {
        handleExceptions(token, e)
      }
    }
    logger.info("$globalId@$millId: Processed computation ")
  }

  private suspend fun handleExceptions(token: ComputationToken, e: Exception) {
    val globalId = token.globalComputationId
    when (e) {
      is ComputationDataClients.TransientErrorException -> {
        if (token.attempt > maximumAttempts) {
          failComputation(
            token,
            message = "Failing computation due to too many failed attempts.",
            cause = e,
          )
        } else {
          logger.log(Level.WARNING, e) { "$globalId@$millId: TRANSIENT error" }
          sendStatusUpdateToKingdom(newErrorUpdateRequest(token, e.localizedMessage))
          // Enqueue the computation again for future retry
          enqueueComputation(token)
        }
      }
      is StatusException -> throw IllegalStateException("Programming bug: uncaught gRPC error", e)
      else -> {
        // Treat any other exception type as a permanent computation error.
        failComputation(token, cause = e)
      }
    }
  }

  /**
   * Updates the [ComputationParticipant] at the Kingdom.
   *
   * @param token current [ComputationToken]
   * @param callKingdom function which calls the Kingdom to update the [ComputationParticipant],
   *   bubbling up [StatusException]. This function may be called multiple times.
   * @return the return value from [callKingdom]
   */
  protected suspend fun updateComputationParticipant(
    token: ComputationToken,
    callKingdom: suspend (participant: ComputationParticipant) -> Unit,
  ) {
    val participantKey = ComputationParticipantKey(token.globalComputationId, duchyId)

    var attempt = 1
    suspend fun prepareRetry(message: String, cause: StatusException) {
      if (attempt >= rpcMaxAttempts) {
        throw ComputationDataClients.PermanentErrorException(message, cause)
      }

      when (cause.status.code) {
        Status.Code.UNAVAILABLE,
        Status.Code.DEADLINE_EXCEEDED,
        Status.Code.ABORTED -> {
          delay(rpcRetryBackoff.durationForAttempt(attempt))
          attempt++
        }
        else -> throw ComputationDataClients.PermanentErrorException(message, cause)
      }
    }
    while (true) {
      yield()
      val participant: ComputationParticipant =
        try {
          systemComputationParticipantsClient.getComputationParticipant(
            getComputationParticipantRequest { name = participantKey.toName() }
          )
        } catch (e: StatusException) {
          prepareRetry("Error getting ComputationParticipant from Kingdom", e)
          continue
        }

      try {
        return callKingdom(participant)
      } catch (e: StatusException) {
        prepareRetry("Error updating ComputationParticipant", e)
        continue
      }
    }
  }

  protected suspend fun sendRequisitionParamsToKingdom(
    token: ComputationToken,
    requisitionParams: ComputationParticipant.RequisitionParams,
  ) {
    updateComputationParticipant(token) { participant ->
      if (participant.hasRequisitionParams()) {
        logger.warning {
          val globalComputationId = token.globalComputationId
          "Skipping SetParticipantRequisitionParams for $globalComputationId: params already set"
        }
        return@updateComputationParticipant
      }

      systemComputationParticipantsClient.setParticipantRequisitionParams(
        setParticipantRequisitionParamsRequest {
          name = participant.name
          etag = participant.etag
          this.requisitionParams = requisitionParams
        }
      )
    }
  }

  /**
   * Sends request to the kingdom's system ComputationParticipantsService to fail the computation.
   */
  private suspend fun failComputationAtKingdom(token: ComputationToken, errorMessage: String) {
    val timestamp = clock.protoTimestamp()
    val failure =
      ComputationParticipantKt.failure {
        participantChildReferenceId = millId
        this.errorMessage = errorMessage
        errorTime = timestamp
        stageAttempt = stageAttempt {
          stage = token.computationStage.number
          stageName = token.computationStage.name
          stageStartTime = timestamp
          attemptNumber = token.attempt.toLong()
        }
      }

    updateComputationParticipant(token) { participant ->
      if (participant.state == ComputationParticipant.State.FAILED) {
        return@updateComputationParticipant
      }

      val request = failComputationParticipantRequest {
        name = participant.name
        etag = participant.etag
        this.failure = failure
      }
      try {
        systemComputationParticipantsClient.failComputationParticipant(request)
      } catch (e: StatusException) {
        when (e.status.code) {
          Status.Code.FAILED_PRECONDITION -> {
            // Assume that the Computation is already failed, so just log an continue.
            // TODO(@SanjayVas): Use error details once they are plumbed instead.
            logger.log(Level.WARNING, e) { "Error failing computation at Kingdom" }
          }
          else -> throw e // Let retry logic handle this.
        }
      }
    }
  }

  private suspend fun failComputation(token: ComputationToken, cause: Exception) {
    failComputation(token, message = cause.message.orEmpty(), cause = cause)
  }

  private suspend fun failComputation(
    token: ComputationToken,
    message: String? = null,
    cause: Throwable? = null,
  ) {
    val errorMessage: String = message ?: cause?.message.orEmpty()
    val logMessageSupplier = { "${token.globalComputationId}@$millId: $errorMessage" }
    if (cause == null) {
      logger.log(Level.SEVERE, logMessageSupplier)
    } else {
      logger.log(Level.SEVERE, cause, logMessageSupplier)
    }
    failComputationAtKingdom(token, errorMessage)
    completeComputation(token, CompletedReason.FAILED)
  }

  /** Actual implementation of processComputation(). */
  protected abstract suspend fun processComputationImpl(token: ComputationToken)

  /** Sends status update to the Kingdom's ComputationLogEntriesService. */
  private suspend fun sendStatusUpdateToKingdom(request: CreateComputationLogEntryRequest) {
    try {
      systemComputationLogEntriesClient.createComputationLogEntry(request)
    } catch (e: StatusException) {
      logger.log(Level.WARNING, e) { "Failed to update status change to the kingdom." }
    }
  }

  /** Send encrypted result of Computation to the Kingdom. */
  protected suspend fun sendResultToKingdom(
    token: ComputationToken,
    computationResult: ComputationResult,
  ) {
    val kingdomComputation = token.computationDetails.kingdomComputation
    val serializedPublicApiEncryptionPublicKey: ByteString
    val publicApiVersion = Version.fromString(kingdomComputation.publicApiVersion)
    val encryptedResultCiphertext: ByteString =
      when (publicApiVersion) {
        Version.V2_ALPHA -> {
          val signedResult = signResult(computationResult.toV2AlphaMeasurementResult(), signingKey)
          val publicApiEncryptionPublicKey =
            kingdomComputation.measurementPublicKey.toV2AlphaEncryptionPublicKey()
          serializedPublicApiEncryptionPublicKey = publicApiEncryptionPublicKey.toByteString()
          encryptResult(signedResult, publicApiEncryptionPublicKey).ciphertext
        }
      }
    sendResultToKingdom(
      globalId = token.globalComputationId,
      certificate = consentSignalCert,
      resultPublicKey = serializedPublicApiEncryptionPublicKey,
      encryptedResult = encryptedResultCiphertext,
      publicApiVersion = publicApiVersion,
    )
  }

  /** Sends measurement result to the kingdom's system computationsService. */
  private suspend fun sendResultToKingdom(
    globalId: String,
    certificate: Certificate,
    resultPublicKey: ByteString,
    encryptedResult: ByteString,
    publicApiVersion: Version,
  ) {
    val request = setComputationResultRequest {
      name = ComputationKey(globalId).toName()
      aggregatorCertificate = certificate.name
      this.resultPublicKey = resultPublicKey
      this.encryptedResult = encryptedResult
      this.publicApiVersion = publicApiVersion.string
    }
    try {
      systemComputationsClient.setComputationResult(request)
    } catch (e: StatusException) {
      val message = "Error sending result to Kingdom"
      throw when (e.status.code) {
        Status.Code.UNAVAILABLE,
        Status.Code.DEADLINE_EXCEEDED,
        Status.Code.ABORTED -> ComputationDataClients.TransientErrorException(message, e)
        else -> ComputationDataClients.PermanentErrorException(message, e)
      }
    }
  }

  /** Writes stage metric to the [Logger] and also sends to the ComputationStatsService. */
  private suspend fun logStageMetric(
    token: ComputationToken,
    metricName: String,
    metricValue: Long,
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
    metricValue: Duration,
    histogram: DoubleHistogram,
  ) {
    histogram.record(
      metricValue.toMillis() / 1000.0,
      Attributes.of(
        COMPUTATION_TYPE_ATTRIBUTE_KEY,
        computationType.name,
        COMPUTATION_STAGE_ATTRIBUTE_KEY,
        token.computationStage.name,
      ),
    )
    logger.info(
      "@Mill $millId, ${token.globalComputationId}/${token.computationStage.name}/$metricName:" +
        " ${metricValue.toHumanFriendlyDuration()}"
    )
    sendComputationStats(token, metricName, metricValue.toMillis())
  }

  /** Sends state metric to the ComputationStatsService. */
  private suspend fun sendComputationStats(
    token: ComputationToken,
    metricName: String,
    metricValue: Long,
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
            it.type = ComputationLogEntry.ErrorDetails.Type.TRANSIENT
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
    stub: ComputationControlCoroutineStub,
  ) {
    val requestFlow =
      content
        .asBufferedFlow(requestChunkSizeBytes)
        .map {
          AdvanceComputationRequest.newBuilder().apply { bodyChunkBuilder.partialData = it }.build()
        }
        .onStart { emit(AdvanceComputationRequest.newBuilder().setHeader(header).build()) }
    try {
      stub.advanceComputation(requestFlow)
    } catch (e: StatusException) {
      val message = "Error advancing computation at other Duchy"
      throw when (e.status.code) {
        Status.Code.UNAVAILABLE,
        Status.Code.ABORTED -> ComputationDataClients.TransientErrorException(message, e)
        else -> ComputationDataClients.PermanentErrorException(message, e)
      }
    }
  }

  /**
   * Fetches the cached result if available, otherwise compute the new result by executing [block].
   *
   * @param block function which computes a new result using cryptographic operations
   */
  protected suspend fun existingOutputOr(
    token: ComputationToken,
    block: suspend () -> ByteString,
  ): EncryptedComputationResult {
    if (token.singleOutputBlobMetadata().path.isNotEmpty()) {
      // Reuse cached result if it exists
      return EncryptedComputationResult(
        checkNotNull(dataClients.readSingleOutputBlob(token)),
        token,
      )
    }
    val wallDurationLogger = wallDurationLogger()
    val result = block()
    wallDurationLogger.logStageDurationMetric(
      token,
      CRYPTO_WALL_CLOCK_DURATION,
      cryptoWallClockDurationHistogram,
    )
    return EncryptedComputationResult(
      flowOf(result),
      dataClients.writeSingleOutputBlob(token, result),
    )
  }

  /**
   * Always execute the block [block]. However, if the token contains a cached result (from previous
   * run), return the cached result instead of the result of the current run.
   */
  protected suspend fun existingOutputAnd(
    token: ComputationToken,
    block: suspend () -> ByteString,
  ): EncryptedComputationResult {
    val wallDurationLogger = wallDurationLogger()
    val result = block()
    wallDurationLogger.logStageDurationMetric(
      token,
      CRYPTO_WALL_CLOCK_DURATION,
      cryptoWallClockDurationHistogram,
    )
    // Reuse cached result if it exists even though the block has been rerun.
    if (token.singleOutputBlobMetadata().path.isNotEmpty()) {
      return EncryptedComputationResult(
        checkNotNull(dataClients.readSingleOutputBlob(token)),
        token,
      )
    }
    return EncryptedComputationResult(
      flowOf(result),
      dataClients.writeSingleOutputBlob(token, result),
    )
  }

  /** Reads all input blobs and combines all the bytes together. */
  protected suspend fun readAndCombineAllInputBlobs(
    token: ComputationToken,
    count: Int,
  ): ByteString {
    val blobMap: Map<BlobRef, ByteString> = dataClients.readInputBlobs(token)
    if (blobMap.size != count) {
      throw ComputationDataClients.PermanentErrorException(
        "Unexpected number of input blobs. expected $count, actual ${blobMap.size}."
      )
    }
    return blobMap.values.flatten()
  }

  /** Completes a computation and records the [CompletedReason] */
  protected suspend fun completeComputation(
    token: ComputationToken,
    reason: CompletedReason,
  ): ComputationToken {
    val response: FinishComputationResponse =
      try {
        dataClients.computationsClient.finishComputation(
          finishComputationRequest {
            this.token = token
            endingComputationStage = endingStage
            this.reason = reason
          }
        )
      } catch (e: StatusException) {
        val message = "Error finishing computation"
        throw when (e.status.code) {
          Status.Code.UNAVAILABLE,
          Status.Code.ABORTED -> ComputationDataClients.TransientErrorException(message, e)
          else -> ComputationDataClients.PermanentErrorException(message, e)
        }
      }
    return response.token
  }

  /** Gets the latest [ComputationToken] for computation with [globalId]. */
  private suspend fun getLatestComputationToken(globalId: String): ComputationToken {
    val response: GetComputationTokenResponse =
      try {
        dataClients.computationsClient.getComputationToken(
          getComputationTokenRequest { globalComputationId = globalId }
        )
      } catch (e: StatusException) {
        val message = "Error retrieving computation token"
        throw when (e.status.code) {
          Status.Code.UNAVAILABLE,
          Status.Code.ABORTED -> ComputationDataClients.TransientErrorException(message, e)
          else -> ComputationDataClients.PermanentErrorException(message, e)
        }
      }

    return response.token
  }

  /** Enqueue a computation with a delay. */
  private suspend fun enqueueComputation(token: ComputationToken) {
    require(token.computationStage != endingStage) {
      "Computation with ending stage cannot be enqueued."
    }
    // Exponential backoff
    val baseDelay = minOf(600.0, (2.0.pow(token.attempt))).toInt()
    // A random delay in the range of [baseDelay, 2*baseDelay]
    val delaySecond = baseDelay + Random.nextInt(baseDelay + 1)

    try {
      dataClients.computationsClient.enqueueComputation(
        enqueueComputationRequest {
          this.token = token
          this.delaySecond = delaySecond
        }
      )
    } catch (e: StatusException) {
      val message = "Error enqueuing computation"
      throw when (e.status.code) {
        Status.Code.UNAVAILABLE,
        Status.Code.ABORTED -> ComputationDataClients.TransientErrorException(message, e)
        else -> ComputationDataClients.PermanentErrorException(message, e)
      }
    }
  }

  protected suspend fun updateComputationDetails(
    request: UpdateComputationDetailsRequest
  ): ComputationToken {
    val response: UpdateComputationDetailsResponse =
      try {
        dataClients.computationsClient.updateComputationDetails(request)
      } catch (e: StatusException) {
        val message = "Error updating computation details"
        throw when (e.status.code) {
          Status.Code.UNAVAILABLE,
          Status.Code.ABORTED -> ComputationDataClients.TransientErrorException(message, e)
          else -> ComputationDataClients.PermanentErrorException(message, e)
        }
      }

    return response.token
  }

  @OptIn(ExperimentalTime::class)
  private inner class WallDurationLogger {
    private val timeMark = TimeSource.Monotonic.markNow()

    suspend fun logStageDurationMetric(
      token: ComputationToken,
      metricName: String,
      histogram: DoubleHistogram,
    ) {
      val time: Duration = timeMark.elapsedNow().toJavaDuration()
      logStageDurationMetric(token, metricName, time, histogram)
    }
  }

  private fun wallDurationLogger(): WallDurationLogger = WallDurationLogger()

  /** Executes the block and records the wall clock duration. */
  protected suspend fun <T> logWallClockDuration(
    token: ComputationToken,
    metricName: String,
    histogram: DoubleHistogram,
    block: suspend () -> T,
  ): T {
    val durationLogger = wallDurationLogger()

    val result = block()
    durationLogger.logStageDurationMetric(token, metricName, histogram)
    return result
  }

  companion object {
    private val COMPUTATION_TYPE_ATTRIBUTE_KEY = AttributeKey.stringKey("halo_cmm.computation.type")
    private val COMPUTATION_STAGE_ATTRIBUTE_KEY =
      AttributeKey.stringKey("halo_cmm.computation.stage")

    private val logger: Logger = Logger.getLogger(this::class.java.name)
  }
}

const val CRYPTO_CPU_DURATION = "crypto_cpu_duration_ms"
const val CRYPTO_WALL_CLOCK_DURATION = "crypto_wall_clock_duration_ms"
const val STAGE_WALL_CLOCK_DURATION = "stage_wall_clock_duration_ms"
const val DATA_TRANSMISSION_RPC_WALL_CLOCK_DURATION = "data_transmission_rpc_wall_clock_duration_ms"
const val BYTES_OF_DATA_IN_RPC = "bytes_of_data_in_rpc"
const val CURRENT_RUNTIME_MEMORY_MAXIMUM = "current_runtime_memory_maximum"
const val CURRENT_RUNTIME_MEMORY_TOTAL = "current_runtime_memory_total"
const val CURRENT_RUNTIME_MEMORY_FREE = "current_runtime_memory_free"

data class EncryptedComputationResult(val bytes: Flow<ByteString>, val token: ComputationToken)

data class Certificate(
  // The public API name of this certificate.
  val name: String,
  // The value of the certificate.
  val value: X509Certificate,
)

/** Converts a milliseconds to a human friendly string. */
fun Duration.toHumanFriendlyDuration(): String {
  val hours = toHours()
  val minutes = toMinutesPart()
  val seconds = toSecondsPart()
  val millis = toMillisPart()
  val hoursString = if (hours == 0L) "" else "$hours hours "
  val minutesString = if (minutes == 0) "" else "$minutes minutes "
  val secondsString = if (seconds == 0) "" else "$seconds seconds "
  val millisString = if (millis == 0) "" else "$millis milliseconds"
  return "$hoursString$minutesString$secondsString$millisString"
}
