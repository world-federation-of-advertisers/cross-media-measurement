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
import java.time.Clock
import java.time.Duration
import java.util.logging.Level
import java.util.logging.Logger
import kotlin.math.pow
import kotlin.random.Random
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.onEach
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.Semaphore
import org.wfanet.measurement.common.grpc.grpcStatusCode
import org.wfanet.measurement.common.logAndSuppressExceptionSuspend
import org.wfanet.measurement.common.protoTimestamp
import org.wfanet.measurement.common.throttler.Throttler
import org.wfanet.measurement.duchy.daemon.utils.MeasurementType
import org.wfanet.measurement.duchy.daemon.utils.key
import org.wfanet.measurement.duchy.daemon.utils.toMeasurementType
import org.wfanet.measurement.duchy.service.internal.computations.toGetTokenRequest
import org.wfanet.measurement.internal.duchy.ComputationDetails
import org.wfanet.measurement.internal.duchy.ComputationsGrpcKt.ComputationsCoroutineStub
import org.wfanet.measurement.internal.duchy.config.ProtocolsSetupConfig
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

/**
 * The Herald looks to the kingdom for status of computations.
 *
 * It is responsible for inserting new computations into the database, and for moving computations
 * out of the WAIT_TO_START stage once the kingdom has gotten confirmation from all duchies that
 * they are able to start the computation.
 *
 * @param internalComputationsClient manages interactions with duchy internal computations service.
 * @param systemComputationsClient stub for communicating with the Kingdom's system Computations
 * Service.
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
  private val protocolsSetupConfig: ProtocolsSetupConfig,
  private val clock: Clock,
  private val blobStorageBucket: String = "computation-blob-storage",
  private val maxAttempts: Int = 5,
  private val maxConcurrency: Int = 5,
  private val retryBackoff: ExponentialBackoff = ExponentialBackoff(),
) {
  private val semaphore = Semaphore(maxConcurrency)

  /**
   * Syncs the status of computations stored at the kingdom with those stored locally continually in
   * a forever loop. The [pollingThrottler] is used to limit how often the kingdom and local
   * computation storage service are polled.
   *
   * @param pollingThrottler throttles how often to get active computations from the Global
   * Computation Service
   */
  suspend fun continuallySyncStatuses(pollingThrottler: Throttler) {
    logger.info("Server starting...")
    // Token signifying the last computation in an active state at the kingdom that was processed by
    // this job. When empty, all active computations at the kingdom will be streamed in the
    // response. The first execution of the loop will then compare all active computations at
    // the kingdom with all active computations locally.
    var lastProcessedContinuationToken = ""

    pollingThrottler.loopOnReady {
      logAndSuppressExceptionSuspend {
        lastProcessedContinuationToken = syncStatuses(lastProcessedContinuationToken)
      }
    }
  }

  /**
   * Syncs the status of computations stored at the kingdom, via the system computation service,
   * with those stored locally.
   *
   * @param continuationToken the continuation token of the last computation in the stream which was
   * processed by the herald.
   * @return the continuation token of the last computation processed in that stream of active
   * computations from the system computation service.
   */
  suspend fun syncStatuses(continuationToken: String): String = coroutineScope {
    logger.info("Reading stream of active computations since \"$continuationToken\".")

    val streamRequest = streamActiveComputationsRequest {
      this.continuationToken = continuationToken
    }
    var lastProcessedContinuationToken = continuationToken

    systemComputationsClient
      .streamActiveComputations(streamRequest)
      .onEach { response ->
        semaphore.acquire()
        launch { processSystemComputationAndSuppressException(response.computation, MAX_ATTEMPTS) }
        lastProcessedContinuationToken = response.continuationToken
      }
      .collect()

    return@coroutineScope lastProcessedContinuationToken
  }

  private suspend fun processSystemComputationAndSuppressException(
    computation: Computation,
    maxAttempts: Int = 10
  ) {
    var attemptNumber = 0
    while (true) {
      attemptNumber++
      try {
        processSystemComputation(computation)
        semaphore.release()
        return
      } catch (ex: Throwable) {
        if (!mayBeTransientGrpcError(ex) || attemptNumber >= maxAttempts) {
          val globalId: String = computation.key.computationId
          logger.log(Level.SEVERE, "[id=$globalId] Non-transient error:", ex)
          failComputationAtKingdom(
            computation,
            "Herald failed after $attemptNumber attempts. ${ex.message}"
          )
          failComputationAtDuchy(computation)
          semaphore.release()
          return
        }
      }
    }
  }

  private suspend fun processSystemComputation(computation: Computation) {
    require(computation.name.isNotEmpty()) { "Resource name not specified" }
    val globalId: String = computation.key.computationId
    logger.fine("[id=$globalId]: Processing updated GlobalComputation")
    when (val state = computation.state) {
      // Creates a new computation if it is not already present in the database.
      State.PENDING_REQUISITION_PARAMS -> createComputation(computation)
      // Updates a computation for duchy confirmation.
      State.PENDING_PARTICIPANT_CONFIRMATION -> confirmParticipant(computation)
      // Starts a computation locally.
      State.PENDING_COMPUTATION -> startComputing(computation)
      else -> logger.warning("Unexpected global computation state '$state'")
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
    } catch (e: Exception) {
      if (e.grpcStatusCode() == Status.Code.ALREADY_EXISTS) {
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
   * conditions rather than unconditionally retrying.
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
        else -> error { "Unknown or unsupported protocol." }
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
        else -> error { "Unknown or unsupported protocol." }
      }
      logger.info("[id=$globalId]: Started Computation")
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
    } catch (ex: Throwable) {
      logger.warning("[id=$globalId]: Error when failComputationAtKingdom.\n$ex")
    }
  }

  /** Call finishComputation to fail local Computation. */
  private suspend fun failComputationAtDuchy(computation: Computation) {
    val globalId = computation.key.computationId
    val token =
      try {
        internalComputationsClient.getComputationToken(globalId.toGetTokenRequest()).token
      } catch (ex: Throwable) {
        null
      } ?: return

    val finishRequest = finishComputationRequest {
      this.token = token
      endingComputationStage =
        when (token.computationDetails.protocolCase) {
          ComputationDetails.ProtocolCase.LIQUID_LEGIONS_V2 -> LiquidLegionsV2Starter.failStage
          else -> error { "Unknown or unsupported protocol." }
        }
      reason = ComputationDetails.CompletedReason.FAILED
    }
    try {
      internalComputationsClient.finishComputation(finishRequest)
    } catch (ex: Throwable) {
      logger.warning("[id=$globalId]: Error when failComputationAtDuchy.\n$ex")
    }
  }

  companion object {
    private val logger: Logger = Logger.getLogger(this::class.java.name)
  }
}

/** Returns true if the error may be transient, i.e. retrying the request may succeed. */
fun mayBeTransientGrpcError(error: Throwable): Boolean {
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
