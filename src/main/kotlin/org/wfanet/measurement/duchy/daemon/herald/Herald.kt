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

package org.wfanet.measurement.duchy.daemon.herald

import io.grpc.Status
import io.grpc.StatusException
import java.time.Clock
import java.time.Duration
import java.util.logging.Level
import java.util.logging.Logger
import kotlin.coroutines.coroutineContext
import kotlin.math.pow
import kotlin.random.Random
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.catch
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.Semaphore
import org.wfanet.measurement.common.grpc.grpcStatusCode
import org.wfanet.measurement.common.protoTimestamp
import org.wfanet.measurement.duchy.daemon.utils.MeasurementType
import org.wfanet.measurement.duchy.daemon.utils.key
import org.wfanet.measurement.duchy.daemon.utils.toMeasurementType
import org.wfanet.measurement.duchy.service.internal.computations.toGetTokenRequest
import org.wfanet.measurement.internal.duchy.ComputationDetails
import org.wfanet.measurement.internal.duchy.ComputationsGrpcKt.ComputationsCoroutineStub
import org.wfanet.measurement.internal.duchy.config.ProtocolsSetupConfig
import org.wfanet.measurement.internal.duchy.deleteComputationRequest
import org.wfanet.measurement.internal.duchy.finishComputationRequest
import org.wfanet.measurement.system.v1alpha.Computation
import org.wfanet.measurement.system.v1alpha.Computation.State
import org.wfanet.measurement.system.v1alpha.ComputationParticipantKey
import org.wfanet.measurement.system.v1alpha.ComputationParticipantKt
import org.wfanet.measurement.system.v1alpha.ComputationParticipantsGrpcKt.ComputationParticipantsCoroutineStub as SystemComputationParticipantsCoroutineStub
import org.wfanet.measurement.system.v1alpha.ComputationsGrpcKt.ComputationsCoroutineStub as SystemComputationsCoroutineStub
import org.wfanet.measurement.system.v1alpha.failComputationParticipantRequest
import org.wfanet.measurement.system.v1alpha.streamActiveComputationsRequest

// Number of attempts that herald would retry processSystemComputationChange() when catch transient
// exceptions.
private const val MAX_ATTEMPTS = 3

private val TERMINAL_STATES = listOf(State.SUCCEEDED, State.FAILED, State.CANCELLED)

/**
 * The Herald looks to the kingdom for status of computations.
 *
 * It is responsible for inserting new computations into the database, and for moving computations
 * out of the WAIT_TO_START stage once the kingdom has gotten confirmation from all duchies that
 * they are able to start the computation.
 *
 * @param internalComputationsClient manages interactions with duchy internal computations service.
 * @param systemComputationsClient stub for communicating with the Kingdom's system Computations
 *   Service.
 * @param protocolsSetupConfig duchy's local protocolsSetupConfig
 * @param blobStorageBucket blob storage path prefix.
 * @param maxAttempts maximum number of attempts to start a computation.
 */
class Herald(
  private val heraldId: String,
  private val duchyId: String,
  private val internalComputationsClient: ComputationsCoroutineStub,
  private val systemComputationsClient: SystemComputationsCoroutineStub,
  private val systemComputationParticipantClient: SystemComputationParticipantsCoroutineStub,
  private val continuationTokenManager: ContinuationTokenManager,
  private val protocolsSetupConfig: ProtocolsSetupConfig,
  private val clock: Clock,
  private val blobStorageBucket: String = "computation-blob-storage",
  private val maxAttempts: Int = 5,
  private val maxStreamingAttempts: Int = 5,
  maxConcurrency: Int = 5,
  private val retryBackoff: ExponentialBackoff = ExponentialBackoff(),
  private val deletableComputationStates: Set<State> = emptySet(),
) {
  private val semaphore = Semaphore(maxConcurrency)

  init {
    for (state in deletableComputationStates) {
      require(state in TERMINAL_STATES) {
        "Unexpected deletable computation state $state while initializing Herald."
      }
    }
  }

  /**
   * Syncs the status of computations stored at the kingdom with those stored locally continually in
   * a forever loop.
   */
  suspend fun continuallySyncStatuses() {
    logger.info("Server starting...")

    // TODO(world-federation-of-advertisers/cross-media-measurement#695): Use gRPC service config
    // rather than custom retry logic w/backoff.
    var attemptNumber = 1
    while (coroutineContext.isActive) {
      try {
        syncStatuses()
      } catch (e: StreamingException) {
        when (val statusCode = e.cause.status.code) {
          Status.Code.UNAVAILABLE,
          Status.Code.DEADLINE_EXCEEDED -> {
            if (attemptNumber == maxStreamingAttempts) {
              throw e
            }
            logger.warning { "Sync attempt $attemptNumber failed with $statusCode. Retrying..." }
            retryBackoff.delay(attemptNumber)
            attemptNumber++
            continue
          }
          else -> throw e
        }
      }

      // Reset attempt number due to success.
      attemptNumber = 1
    }
  }

  /**
   * Syncs the status of computations stored at the kingdom, via the system computation service,
   * with those stored locally.
   */
  suspend fun syncStatuses() {
    // Continuation token signifying the last computation in an active state at the kingdom that
    // was processed by this job. It has updateTimeSince and lastSeenExternalComputationId to
    // specify where to continue. When empty, all active computations at the kingdom will be
    // streamed in the response.
    val continuationToken = continuationTokenManager.getLatestContinuationToken()
    logger.info("Reading stream of active computations since \"$continuationToken\".")

    val streamRequest = streamActiveComputationsRequest {
      this.continuationToken = continuationToken
    }

    coroutineScope {
      systemComputationsClient
        .streamActiveComputations(streamRequest)
        .catch { cause ->
          if (cause !is StatusException) throw cause
          throw StreamingException("Error streaming active computations", cause)
        }
        .collect { response ->
          continuationTokenManager.addPendingToken(response.continuationToken)

          semaphore.acquire()
          launch {
            try {
              processSystemComputation(response.computation, MAX_ATTEMPTS)
              continuationTokenManager.markTokenProcessed(response.continuationToken)
            } finally {
              semaphore.release()
            }
          }
        }
    }
  }

  private suspend fun processSystemComputation(computation: Computation, maxAttempts: Int = 10) {
    var attemptNumber = 0
    while (coroutineContext.isActive) {
      attemptNumber++
      try {
        processSystemComputation(computation)
        return
      } catch (e: CancellationException) {
        // Ensure that coroutine cancellation bubbles up immediately.
        val globalId: String = computation.key.computationId
        logger.log(Level.INFO, "[id=$globalId] Coroutine cancelled.")
        throw e
      } catch (e: Exception) {
        // TODO(@renjiezh): Catch StatusException at the RPC site instead, wrapping in a more
        // specific exception if it needs to be handled at a higher level.
        if (!mayBeTransientGrpcError(e) || attemptNumber >= maxAttempts) {
          val globalId: String = computation.key.computationId
          logger.log(Level.SEVERE, "[id=$globalId] Non-transient error:", e)
          failComputationAtKingdom(
            computation,
            "Herald failed after $attemptNumber attempts. ${e.message}"
          )
          failComputationAtDuchy(computation)
          return
        }
      }
    }
  }

  private suspend fun processSystemComputation(computation: Computation) {
    require(computation.name.isNotEmpty()) { "Resource name not specified" }
    val globalId: String = computation.key.computationId
    logger.fine("[id=$globalId]: Processing updated GlobalComputation")
    val state = computation.state
    when (state) {
      // Creates a new computation if it is not already present in the database.
      State.PENDING_REQUISITION_PARAMS -> createComputation(computation)
      // Updates a computation for duchy confirmation.
      State.PENDING_PARTICIPANT_CONFIRMATION -> confirmParticipant(computation)
      // Starts a computation locally.
      State.PENDING_COMPUTATION -> startComputing(computation)
      // Confirm failure at Kingdom
      State.FAILED,
      State.CANCELLED -> {
        failComputationAtDuchy(computation)
      }
      State.SUCCEEDED -> {}
      else -> logger.warning("Unexpected global computation state '$state'")
    }
    if (state in deletableComputationStates) {
      deleteComputationAtDuchy(computation)
    }
  }

  /** Creates a new computation. */
  private suspend fun createComputation(systemComputation: Computation) {
    require(systemComputation.name.isNotEmpty()) { "Resource name not specified" }
    val globalId: String = systemComputation.key.computationId
    logger.info("[id=$globalId] Creating Computation...")
    try {
      when (systemComputation.toMeasurementType()) {
        MeasurementType.REACH_AND_FREQUENCY -> {
          LiquidLegionsV2Starter.createComputation(
            internalComputationsClient,
            systemComputation,
            protocolsSetupConfig.liquidLegionsV2,
            blobStorageBucket
          )
        }
      }
      logger.info("[id=$globalId]: Created Computation")
    } catch (e: StatusException) {
      // TODO(@renjiezh): Catch StatusException at the RPC site instead, wrapping in a more
      // specific exception if it needs to be handled at a higher level.
      if (e.status.code == Status.Code.ALREADY_EXISTS) {
        logger.info("[id=$globalId]: Computation already exists")
      } else {
        throw e // rethrow all other exceptions.
      }
    }
  }

  class AttemptsExhaustedException(cause: Throwable, buildMessage: () -> String) :
    Exception(buildMessage(), cause)

  /**
   * Runs [block] for [systemComputation], retrying up to a total of [maxAttempts] attempts.
   *
   * Retrying is necessary since there is a race condition between the mill updating local
   * computation state and the herald retrieving a systemComputation update from the kingdom.
   *
   * TODO(world-federation-of-advertisers/cross-media-measurement#585): Be more specific about retry
   *   conditions rather than unconditionally retrying.
   */
  private suspend fun <R> runWithRetries(
    systemComputation: Computation,
    block: suspend (systemComputation: Computation) -> R
  ): R {
    val globalId = systemComputation.key.computationId
    val finalResult =
      (1..maxAttempts).fold(Result.failure<R>(IllegalStateException())) { previous, attemptNumber ->
        if (previous.isSuccess) {
          return@fold previous
        }
        val result = runCatching { block(systemComputation) }
        if (result.isFailure) {
          logger.info("[id=$globalId] Waiting to retry attempt #${ attemptNumber + 1 }...")
          retryBackoff.delay(attemptNumber)
        }
        result
      }
    return finalResult.getOrElse {
      throw AttemptsExhaustedException(it) {
        "[id=$globalId] Attempts reach maximum after $maxAttempts attempts"
      }
    }
  }

  /** Attempts to update a new computation from duchy confirmation. */
  private suspend fun confirmParticipant(systemComputation: Computation) {
    val globalId = systemComputation.key.computationId
    runWithRetries(systemComputation) {
      logger.info("[id=$globalId]: Confirming Participant...")
      val token = internalComputationsClient.getComputationToken(globalId.toGetTokenRequest()).token
      when (token.computationDetails.protocolCase) {
        ComputationDetails.ProtocolCase.LIQUID_LEGIONS_V2 ->
          LiquidLegionsV2Starter.updateRequisitionsAndKeySets(
            token,
            internalComputationsClient,
            systemComputation,
            protocolsSetupConfig.liquidLegionsV2.externalAggregatorDuchyId
          )
        else -> error("Unknown or unsupported protocol.")
      }
      logger.info("[id=$globalId]: Confirmed Computation")
    }
  }

  /** Attempts to start a computation that is in WAIT_TO_START. */
  private suspend fun startComputing(systemComputation: Computation) {
    val globalId = systemComputation.key.computationId
    runWithRetries(systemComputation) {
      logger.info("[id=$globalId]: Starting Computation...")
      val token = internalComputationsClient.getComputationToken(globalId.toGetTokenRequest()).token
      when (token.computationDetails.protocolCase) {
        ComputationDetails.ProtocolCase.LIQUID_LEGIONS_V2 ->
          LiquidLegionsV2Starter.startComputation(token, internalComputationsClient)
        else -> error("Unknown or unsupported protocol.")
      }
      logger.info("[id=$globalId]: Started Computation")
    }
  }

  private suspend fun deleteComputationAtDuchy(computation: Computation) {
    val globalId = computation.key.computationId
    try {
      val token = internalComputationsClient.getComputationToken(globalId.toGetTokenRequest()).token
      internalComputationsClient.deleteComputation(
        deleteComputationRequest { localComputationId = token.localComputationId }
      )
    } catch (e: StatusException) {
      logger.warning("[id=$globalId]: Failed to delete computation. $e")
    }
  }

  /** Call failComputationParticipant at Kingdom to fail the Computation */
  private suspend fun failComputationAtKingdom(computation: Computation, errorMessage: String) {
    val globalId = computation.key.computationId
    val request = failComputationParticipantRequest {
      name = ComputationParticipantKey(globalId, duchyId).toName()
      failure =
        ComputationParticipantKt.failure {
          participantChildReferenceId = heraldId
          this.errorMessage = errorMessage
          errorTime = clock.protoTimestamp()
        }
    }
    try {
      systemComputationParticipantClient.failComputationParticipant(request)
    } catch (e: StatusException) {
      logger.log(Level.WARNING, e) { "[id=$globalId]: Error when failComputationAtKingdom" }
    }
  }

  /** Call finishComputation to fail local Computation. */
  private suspend fun failComputationAtDuchy(computation: Computation) {
    val globalId = computation.key.computationId
    val token =
      try {
        internalComputationsClient.getComputationToken(globalId.toGetTokenRequest()).token
      } catch (e: StatusException) {
        null
      } ?: return

    if (
      token.computationDetails.hasLiquidLegionsV2() &&
        token.computationStage == LiquidLegionsV2Starter.TERMINAL_STAGE
    ) {
      return
    }

    val finishRequest = finishComputationRequest {
      this.token = token
      endingComputationStage =
        when (token.computationDetails.protocolCase) {
          ComputationDetails.ProtocolCase.LIQUID_LEGIONS_V2 -> LiquidLegionsV2Starter.TERMINAL_STAGE
          else -> error { "Unknown or unsupported protocol." }
        }
      reason = ComputationDetails.CompletedReason.FAILED
    }
    try {
      internalComputationsClient.finishComputation(finishRequest)
    } catch (e: StatusException) {
      logger.log(Level.WARNING, e) { "[id=$globalId]: Error when failComputationAtDuchy" }
    }
  }

  private class StreamingException(message: String, override val cause: StatusException) :
    Exception(message, cause)

  companion object {
    private val logger: Logger = Logger.getLogger(this::class.java.name)
  }
}

/**
 * Returns `true` if the error may be transient, i.e. retrying the request may succeed.
 *
 * TODO(world-federation-of-advertisers/cross-media-measurement#695): Use service config to apply
 *   per-method retry logic. Whether a status code indicates that a method is safe to retry depends
 *   on the method. e.g. [DEADLINE_EXCEEDED][Status.Code.DEADLINE_EXCEEDED] is not necessarily safe
 *   to retry if the method is non-idempotent.
 */
fun mayBeTransientGrpcError(error: Exception): Boolean {
  val statusCode = error.grpcStatusCode() ?: return false
  return when (statusCode) {
    Status.Code.ABORTED,
    Status.Code.DEADLINE_EXCEEDED,
    Status.Code.RESOURCE_EXHAUSTED,
    Status.Code.UNKNOWN,
    Status.Code.UNAVAILABLE -> true
    else -> false
  }
}

data class ExponentialBackoff(
  val initialDelay: Duration = Duration.ofSeconds(1),
  val multiplier: Double = 2.0,
  val randomnessFactor: Double = 0.3,
  val random: Random = Random.Default,
) {
  suspend fun delay(attemptNumber: Int) {
    delay(
      random
        .randomizedDuration(
          exponentialDuration(
            initialDelay = initialDelay,
            multiplier = multiplier,
            attempts = attemptNumber,
          ),
          randomnessFactor = randomnessFactor,
        )
        .toMillis()
    )
  }

  companion object {
    private fun exponentialDuration(
      initialDelay: Duration,
      multiplier: Double,
      attempts: Int
    ): Duration {
      if (attempts == 1) {
        return initialDelay
      }
      return Duration.ofMillis((initialDelay.toMillis() * (multiplier.pow(attempts - 1))).toLong())
    }

    private fun Random.randomizedDuration(
      delay: Duration,
      randomnessFactor: Double,
    ): Duration {
      if (randomnessFactor == 0.0) {
        return delay
      }
      val maxOffset = randomnessFactor * delay.toMillis()
      return delay.plusMillis(nextDouble(-maxOffset, maxOffset).toLong())
    }
  }
}
