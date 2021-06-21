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
import java.util.logging.Level
import java.util.logging.Logger
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.catch
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.launch
import org.wfanet.measurement.api.v2alpha.ProtocolConfig
import org.wfanet.measurement.common.grpc.grpcStatusCode
import org.wfanet.measurement.common.throttler.Throttler
import org.wfanet.measurement.common.withRetriesOnEach
import org.wfanet.measurement.duchy.daemon.utils.MeasurementType
import org.wfanet.measurement.duchy.daemon.utils.key
import org.wfanet.measurement.duchy.daemon.utils.toMeasurementType
import org.wfanet.measurement.duchy.db.computation.ComputationProtocolStageDetails
import org.wfanet.measurement.duchy.service.internal.computation.toGetTokenRequest
import org.wfanet.measurement.internal.duchy.ComputationDetails
import org.wfanet.measurement.internal.duchy.ComputationsGrpcKt.ComputationsCoroutineStub
import org.wfanet.measurement.internal.duchy.config.ProtocolsSetupConfig
import org.wfanet.measurement.system.v1alpha.Computation
import org.wfanet.measurement.system.v1alpha.Computation.State
import org.wfanet.measurement.system.v1alpha.ComputationsGrpcKt.ComputationsCoroutineStub as SystemComputationsCoroutineStub
import org.wfanet.measurement.system.v1alpha.StreamActiveComputationsRequest
import org.wfanet.measurement.system.v1alpha.StreamActiveComputationsResponse

/**
 * The Herald looks to the kingdom for status of computations.
 *
 * It is responsible for inserting new computations into the database, and for moving computations
 * out of the WAIT_TO_START stage once the kingdom has gotten confirmation from all duchies that
 * they are able to start the computation.
 *
 * @param computationStorageClient manages interactions with computations storage service.
 * @param systemComputationsClient stub for communicating with the Kingdom's system Computations
 * Service.
 * @param protocolsSetupConfig duchy's local protocolsSetupConfig
 * @param configMaps a map of public Api ProtocolConfigIds to the configs.
 * @param blobStorageBucket blob storage path prefix.
 * @param maxAttempts maximum number of attempts to start a computation.
 */
class Herald(
  otherDuchiesInComputation: List<String>,
  private val computationStorageClient: ComputationsCoroutineStub,
  private val systemComputationsClient: SystemComputationsCoroutineStub,
  private val protocolsSetupConfig: ProtocolsSetupConfig,
  private val configMaps: Map<String, ProtocolConfig>,
  private val blobStorageBucket: String = "computation-blob-storage",
  private val maxAttempts: Int = 10
) {
  private val computationProtocolStageDetails =
    ComputationProtocolStageDetails(otherDuchiesInComputation)

  // If one of the GlobalScope coroutines launched by `start` fails, it populates this.
  // TODO(world-federation-of-advertisers/cross-media-measurement#87): Fail the computation and
  //  carry on instead of crashing the herald.
  private lateinit var attemptException: Throwable

  /**
   * Syncs the status of computations stored at the kingdom with those stored locally continually in
   * a forever loop. The [pollingThrottler] is used to limit how often the kingdom and local
   * computation storage service are polled.
   *
   * @param pollingThrottler throttles how often to get active computations from the Global
   * Computation Service
   */
  suspend fun continuallySyncStatuses(pollingThrottler: Throttler) {
    logger.info("Starting...")
    // Token signifying the last computation in an active state at the kingdom that was processed by
    // this job. When empty, all active computations at the kingdom will be streamed in the
    // response. The first execution of the loop will then compare all active computations at
    // the kingdom with all active computations locally.
    var lastProcessedContinuationToken = ""

    pollingThrottler.loopOnReady {
      lastProcessedContinuationToken = syncStatuses(lastProcessedContinuationToken)
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
  suspend fun syncStatuses(continuationToken: String): String {
    if (this::attemptException.isInitialized) {
      throw attemptException
    }

    var lastProcessedContinuationToken = continuationToken
    logger.info("Reading stream of active computations since $continuationToken.")
    systemComputationsClient
      .streamActiveComputations(
        StreamActiveComputationsRequest.newBuilder().setContinuationToken(continuationToken).build()
      )
      .withRetriesOnEach(maxAttempts = 3, retryPredicate = ::mayBeTransientGrpcError) { response ->
        processSystemComputationChange(response)
        lastProcessedContinuationToken = response.continuationToken
      }
      // Cancel the flow on the first error, but don't actually throw the error. This will keep
      // the continuation token at the last successfully processed item. A later execution of
      // syncStatuses() may be successful if the state at the kingdom and/or this duchy was updated.
      .catch { e -> logger.log(Level.SEVERE, "Exception:", e) }
      .collect()

    return lastProcessedContinuationToken
  }

  private suspend fun processSystemComputationChange(response: StreamActiveComputationsResponse) {
    require(response.computation.name.isNotEmpty()) { "Resource name not specified" }
    val globalId: String = response.computation.key.computationId
    logger.info("[id=$globalId]: Processing updated GlobalComputation")
    when (val state = response.computation.state) {
      // Creates a new computation if it is not already present in the database.
      State.PENDING_REQUISITION_PARAMS -> create(response.computation)
      // Updates a computation for duchy confirmation.
      State.PENDING_PARTICIPANT_CONFIRMATION -> update(response.computation)
      // Starts a computation locally.
      State.PENDING_COMPUTATION -> start(response.computation)
      else -> logger.warning("Unexpected global computation state '$state'")
    }
  }

  /** Creates a new computation. */
  private suspend fun create(systemComputation: Computation) {
    require(systemComputation.name.isNotEmpty()) { "Resource name not specified" }
    val globalId: String = systemComputation.key.computationId
    logger.info("[id=$globalId] Creating Computation")
    try {
      when (systemComputation.toMeasurementType()) {
        MeasurementType.REACH_AND_FREQUENCY -> {
          LiquidLegionsV2Starter.createComputation(
            computationStorageClient,
            systemComputation,
            protocolsSetupConfig.liquidLegionsV2,
            configMaps,
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

  /**
   * Attempts the block once and if that fails, launches a coroutine to continue retrying in the
   * background. Retrying is necessary since there is a race condition between the mill updating
   * local computation state and the herald retrieving a systemComputation update from the kingdom.
   */
  private suspend fun runWithRetry(
    systemComputation: Computation,
    block: suspend (systemComputation: Computation) -> Unit
  ) {
    val globalId = systemComputation.key.computationId
    val attempt: suspend () -> Boolean = { runCatching { block(systemComputation) }.isSuccess }
    if (!attempt()) {
      // TODO: use another scope other than the GlobalScope.
      GlobalScope.launch(Dispatchers.IO) {
        for (i in 2..maxAttempts) {
          logger.info("[id=$globalId] Attempt #$i")
          delay(timeMillis = minOf((1L shl i) * 1000L, 60_000L))
          if (attempt()) {
            return@launch
          }
        }
        val message = "[id=$globalId] Giving up after $maxAttempts attempts"
        logger.severe(message)
        attemptException = IllegalStateException(message)
      }
    }
  }

  /** Attempts to update a new computation from duchy confirmation. */
  private suspend fun update(systemComputation: Computation) {
    val globalId = systemComputation.key.computationId
    runWithRetry(systemComputation) {
      logger.info("[id=$globalId]: Updating Computation")
      val token = computationStorageClient.getComputationToken(globalId.toGetTokenRequest()).token
      when (token.computationDetails.protocolCase) {
        ComputationDetails.ProtocolCase.LIQUID_LEGIONS_V2 ->
          LiquidLegionsV2Starter.updateRequisitionsAndKeySets(
            token,
            computationStorageClient,
            computationProtocolStageDetails,
            systemComputation,
            protocolsSetupConfig.liquidLegionsV2.externalAggregatorDuchyId,
            logger
          )
        else -> error { "Unknown or unsupported protocol." }
      }
    }
  }

  /** Attempts to start a computation that is in WAIT_TO_START. */
  private suspend fun start(systemComputation: Computation) {
    val globalId = systemComputation.key.computationId
    runWithRetry(systemComputation) {
      logger.info("[id=$globalId]: Starting Computation")
      val token = computationStorageClient.getComputationToken(globalId.toGetTokenRequest()).token
      when (token.computationDetails.protocolCase) {
        ComputationDetails.ProtocolCase.LIQUID_LEGIONS_V2 ->
          LiquidLegionsV2Starter.startComputation(
            token,
            computationStorageClient,
            computationProtocolStageDetails,
            logger
          )
        else -> error { "Unknown or unsupported protocol." }
      }
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
