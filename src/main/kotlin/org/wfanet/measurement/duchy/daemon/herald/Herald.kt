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
import org.wfanet.measurement.common.DuchyOrder
import org.wfanet.measurement.common.grpc.grpcStatusCode
import org.wfanet.measurement.common.throttler.Throttler
import org.wfanet.measurement.common.withRetriesOnEach
import org.wfanet.measurement.duchy.db.computation.ComputationProtocolStageDetails
import org.wfanet.measurement.duchy.service.internal.computation.toGetTokenRequest
import org.wfanet.measurement.internal.duchy.ComputationDetails
import org.wfanet.measurement.internal.duchy.ComputationsGrpcKt.ComputationsCoroutineStub
import org.wfanet.measurement.protocol.RequisitionKey
import org.wfanet.measurement.system.v1alpha.GlobalComputation
import org.wfanet.measurement.system.v1alpha.GlobalComputation.State
import org.wfanet.measurement.system.v1alpha.GlobalComputationsGrpcKt.GlobalComputationsCoroutineStub
import org.wfanet.measurement.system.v1alpha.StreamActiveGlobalComputationsRequest
import org.wfanet.measurement.system.v1alpha.StreamActiveGlobalComputationsResponse

/**
 * The Herald looks to the kingdom for status of computations.
 *
 * It is responsible for inserting new computations into the database, and for moving computations
 * out of the WAIT_TO_START stage once the kingdom has gotten confirmation from all duchies that
 * they are able to start the computation.
 *
 * @param computationStorageClient manages interactions with computations storage service.
 * @param globalComputationsClient stub for communicating with the Global Computations Service
 */
class Herald(
  otherDuchiesInComputation: List<String>,
  private val computationStorageClient: ComputationsCoroutineStub,
  private val globalComputationsClient: GlobalComputationsCoroutineStub,
  private val duchyName: String,
  private val duchyOrder: DuchyOrder,
  private val blobStorageBucket: String = "computation-blob-storage",
  private val maxStartAttempts: Int = 10
) {
  private val computationProtocolStageDetails =
    ComputationProtocolStageDetails(otherDuchiesInComputation)

  // If one of the GlobalScope coroutines launched by `start` fails, it populates this.
  private lateinit var startException: Throwable

  /**
   * Syncs the status of computations stored at the kingdom with those stored locally continually
   * in a forever loop. The [pollingThrottler] is used to limit how often the kingdom and
   * local computation storage service are polled.
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
   * Syncs the status of computations stored at the kingdom, via the global computation service,
   * with those stored locally.
   *
   * @param continuationToken the continuation token of the last computation in the stream which was
   * processed by the herald.
   * @return the continuation token of the last computation processed in that stream of active
   * computations from the global computation service.
   */
  suspend fun syncStatuses(continuationToken: String): String {
    if (this::startException.isInitialized) { throw startException }

    var lastProcessedContinuationToken = continuationToken
    logger.info("Reading stream of active computations since $continuationToken.")
    globalComputationsClient.streamActiveGlobalComputations(
      StreamActiveGlobalComputationsRequest.newBuilder()
        .setContinuationToken(continuationToken)
        .build()
    )
      .withRetriesOnEach(maxAttempts = 3, retryPredicate = ::mayBeTransientGrpcError) { response ->
        processGlobalComputationChange(response)
        lastProcessedContinuationToken = response.continuationToken
      }
      // Cancel the flow on the first error, but don't actually throw the error. This will keep
      // the continuation token at the last successfully processed item. A later execution of
      // syncStatuses() may be successful if the state at the kingdom and/or this duchy was updated.
      .catch { e -> logger.log(Level.SEVERE, "Exception:", e) }
      .collect()
    return lastProcessedContinuationToken
  }

  private suspend fun processGlobalComputationChange(
    response: StreamActiveGlobalComputationsResponse
  ) {
    // TODO: create computation according to the protocol specified by the kingdom.
    val globalId: String = checkNotNull(response.globalComputation.key?.globalComputationId)
    logger.info("[id=$globalId]: Processing updated GlobalComputation")
    when (val state = response.globalComputation.state) {
      // Create a new computation if it is not already present in the database.
      State.CONFIRMING -> create(response.globalComputation)
      // Start the computation if it is in WAIT_TO_START.
      // TODO: Resume a computation that was once SUSPENDED.
      State.RUNNING -> start(globalId)
      State.SUSPENDED ->
        logger.warning("Pause/Resume of computations based on kingdom state not yet supported.")
      else ->
        logger.warning("Unexpected global computation state '$state'")
    }
  }

  /** Creates a new computation. */
  // TODO: create Computation of type mapping the protocol specified by the kingdom.
  private suspend fun create(globalComputation: GlobalComputation) {
    val globalId: String = checkNotNull(globalComputation.key?.globalComputationId)
    logger.info("[id=$globalId] Creating Computation")
    val computationAtThisDuchy = duchyOrder.positionFor(globalId, duchyName)
    try {
      // TODO: get the protocol from the kingdom's response and create a corresponding computation.
      LiquidLegionsV2Starter.createComputation(
        computationStorageClient, globalComputation, computationAtThisDuchy, blobStorageBucket
      )
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
   * Starts a computation that is in WAIT_TO_START.
   *
   * This immediately attempts once and if that failes, launches a coroutine to continue retrying
   * in the background.
   */
  private suspend fun start(globalId: String) {
    val attempt: suspend () -> Boolean = { runCatching { startAttempt(globalId) }.isSuccess }
    if (!attempt()) {
      GlobalScope.launch(Dispatchers.IO) {
        for (i in 2..maxStartAttempts) {
          logger.info("[id=$globalId] Attempt #$i to start")
          delay(timeMillis = minOf((1L shl i) * 1000L, 60_000L))
          if (attempt()) {
            return@launch
          }
        }
        val message = "[id=$globalId] Giving up after $maxStartAttempts attempts to start"
        logger.severe(message)
        startException = IllegalStateException(message)
      }
    }
  }

  /** Attempts to start a computation that is in WAIT_TO_START. */
  private suspend fun startAttempt(globalId: String) {
    logger.info("[id=$globalId]: Starting Computation")
    val token =
      computationStorageClient
        .getComputationToken(globalId.toGetTokenRequest())
        .token
    when (token.computationDetails.detailsCase) {
      ComputationDetails.DetailsCase.LIQUID_LEGIONS_V1 ->
        LiquidLegionsV1Starter.startComputation(
          token, computationStorageClient, computationProtocolStageDetails, logger
        )
      ComputationDetails.DetailsCase.LIQUID_LEGIONS_V2 ->
        LiquidLegionsV2Starter.startComputation(
          token, computationStorageClient, computationProtocolStageDetails, logger
        )
      else -> error { "Unknown or unsupported protocol." }
    }
  }

  companion object {
    private val logger: Logger = Logger.getLogger(this::class.java.name)
  }
}

fun GlobalComputation.toRequisitionKeys(): List<RequisitionKey> =
  metricRequisitionsList.map {
    RequisitionKey.newBuilder().apply {
      dataProviderId = it.dataProviderId
      campaignId = it.campaignId
      metricRequisitionId = it.metricRequisitionId
    }.build()
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
