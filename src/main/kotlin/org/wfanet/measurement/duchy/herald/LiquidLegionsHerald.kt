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

package org.wfanet.measurement.duchy.herald

import io.grpc.Status
import io.grpc.StatusRuntimeException
import kotlinx.coroutines.flow.catch
import kotlinx.coroutines.flow.collect
import org.wfanet.measurement.api.v1alpha.GlobalComputation.State
import org.wfanet.measurement.api.v1alpha.GlobalComputationsGrpcKt.GlobalComputationsCoroutineStub
import org.wfanet.measurement.api.v1alpha.StreamActiveGlobalComputationsRequest
import org.wfanet.measurement.api.v1alpha.StreamActiveGlobalComputationsResponse
import org.wfanet.measurement.common.Throttler
import org.wfanet.measurement.common.withRetriesOnEach
import org.wfanet.measurement.db.duchy.computation.LiquidLegionsSketchAggregationComputationStorageClients
import org.wfanet.measurement.db.duchy.computation.LiquidLegionsSketchAggregationProtocol
import org.wfanet.measurement.internal.LiquidLegionsSketchAggregationStage
import org.wfanet.measurement.internal.LiquidLegionsSketchAggregationStage.WAIT_TO_START
import org.wfanet.measurement.internal.duchy.ComputationBlobDependency.INPUT
import org.wfanet.measurement.internal.duchy.ComputationDetails
import org.wfanet.measurement.internal.duchy.ComputationTypeEnum.ComputationType
import org.wfanet.measurement.internal.duchy.CreateComputationRequest
import org.wfanet.measurement.internal.duchy.GetComputationIdsRequest
import org.wfanet.measurement.internal.duchy.ToConfirmRequisitionsStageDetails.RequisitionKey
import org.wfanet.measurement.service.internal.duchy.computation.storage.toGetTokenRequest
import org.wfanet.measurement.service.internal.duchy.computation.storage.toProtocolStage
import java.util.logging.Level
import java.util.logging.Logger

/**
 * The Herald looks to the kingdom for status of computations.
 *
 * It is responsible for inserting new computations into the database, and for moving computations
 * out of the WAIT_TO_START stage once the kingdom has gotten confirmation from all duchies that
 * they are able to start the computation.
 *
 * @param storageClients manages interactions with computations storage service.
 * @param globalComputationsClient stub for communicating with the Global Computations Service
 */
class LiquidLegionsHerald(
  private val storageClients: LiquidLegionsSketchAggregationComputationStorageClients,
  private val globalComputationsClient: GlobalComputationsCoroutineStub
) {

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
    while (true) {
      pollingThrottler.onReady {
        lastProcessedContinuationToken = syncStatuses(lastProcessedContinuationToken)
      }
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
    // By design this is the only job which is creating new computations and removing them from the
    // WAIT_TO_START stage. There should only be one copy of it running in the duchy. Both of these
    // assumptions allow us to query for the current state of computations once before processing
    // a stream of active computations from the Kingdom. If those assumption were to break the
    // storage service/database layer would reject any invalid state changes to the data.
    logger.info("Getting all local computations that are waiting to start.")
    val waitingToStart =
      storageClients.computationStorageClient
        .getComputationIds(getComputationIdsWaitingToStartRequest)
        .globalIdsList.toSet()
    logger.info("Getting all active local computations.")
    val allActive =
      storageClients.computationStorageClient.getComputationIds(getActiveComputationIdsRequest)
        .globalIdsList.toSet()

    var lastProcessedContinuationToken = continuationToken
    logger.info("Reading stream of active computations since $continuationToken.")
    globalComputationsClient.streamActiveGlobalComputations(
      StreamActiveGlobalComputationsRequest.newBuilder()
        .setContinuationToken(continuationToken)
        .build()
    )
      .withRetriesOnEach(maxAttempts = 3, retryPredicate = ::mayBeTransientGrpcError) { response ->
        val globalId = checkNotNull(response.globalComputation.key?.globalComputationId?.toLong())
        when (val state = response.globalComputation.state) {
          // Create a new computation if it is not already present in the database.
          State.CONFIRMING ->
            if (globalId !in allActive) create(globalId, response.toRequisitionKeys())
          // Start the computation if it is in WAIT_TO_START.
          // TODO: Resume a computation that was once SUSPENDED.
          State.RUNNING -> if (globalId in waitingToStart) start(globalId)
          State.SUSPENDED ->
            logger.warning(
              "Pause/Resume of computations based on kingdom state not yet supported."
            )
          else ->
            logger.warning("Unexpected global computation state '$state'")
        }
        lastProcessedContinuationToken = response.continuationToken
      }
      // Cancel the flow on the first error, but don't actually throw the error. This will keep
      // the continuation token at the last successfully processed item. A later execution of
      // syncStatuses() may be successful if the state at the kingdom and/or this duchy was updated.
      .catch { e -> logger.log(Level.SEVERE, "Exception:", e) }
      .collect()
    return lastProcessedContinuationToken
  }

  /** Creates a new computation. */
  private suspend fun create(
    globalId: Long,
    requisitionsAtThisDuchy: List<RequisitionKey>
  ) =
    storageClients.computationStorageClient.createComputation(
      CreateComputationRequest.newBuilder().apply {
        computationType = COMPUTATION_TYPE
        globalComputationId = globalId
        stageDetailsBuilder.apply {
          toConfirmRequisitionsStageDetailsBuilder.addAllKeys(requisitionsAtThisDuchy)
        }
      }.build()
    )

  /** Starts a computation that is in WAIT_TO_START. */
  private suspend fun start(globalId: Long) {
    val token = storageClients.computationStorageClient
      .getComputationToken(globalId.toGetTokenRequest(COMPUTATION_TYPE)).token
    check(token.role == ComputationDetails.RoleInComputation.SECONDARY) {
      "[id=$globalId]: Computations in the WAIT_TO_START stage should have SECONDARY role. " +
        "token=$token"
    }
    check(token.computationStage.liquidLegionsSketchAggregation == WAIT_TO_START) {
      "[id=$globalId]: expected stage to be WAIT_TO_START, was ${token.computationStage}"
    }
    storageClients.transitionComputationToStage(
      computationToken = token,
      // The inputs of WAIT_TO_START are copies of the sketches stored locally. These are the very
      // sketches required for the TO_ADD_NOISE step of the computation.
      inputsToNextStage = token.blobsList.filter { it.dependencyType == INPUT }.map { it.path },
      stage = LiquidLegionsSketchAggregationStage.TO_ADD_NOISE
    )
  }

  companion object {
    private val getComputationIdsWaitingToStartRequest =
      GetComputationIdsRequest.newBuilder()
        .addStages(WAIT_TO_START.toProtocolStage())
        .build()

    private val activeComputationStages =
      LiquidLegionsSketchAggregationStage.values().toSet()
        .minus(LiquidLegionsSketchAggregationStage.UNRECOGNIZED)
        .minus(LiquidLegionsSketchAggregationStage.SKETCH_AGGREGATION_STAGE_UNKNOWN)
        .minus(LiquidLegionsSketchAggregationProtocol.EnumStages.validTerminalStages)
        .map { it.toProtocolStage() }

    private val getActiveComputationIdsRequest =
      GetComputationIdsRequest.newBuilder().addAllStages(activeComputationStages).build()

    val COMPUTATION_TYPE = ComputationType.LIQUID_LEGIONS_SKETCH_AGGREGATION_V1
    val logger: Logger = Logger.getLogger(this::class.java.name)
  }
}

private fun StreamActiveGlobalComputationsResponse.toRequisitionKeys(): List<RequisitionKey> =
  globalComputation
    .metricRequisitionsList
    .map {
      RequisitionKey.newBuilder().apply {
        dataProviderId = it.dataProviderId
        campaignId = it.campaignId
        metricRequisitionId = it.metricRequisitionId
      }.build()
    }

/** Returns true if the error may be transient, i.e. retrying the request may succeed. */
fun mayBeTransientGrpcError(error: Throwable): Boolean {
  if (error is StatusRuntimeException) {
    return when (error.status.code) {
      Status.Code.ABORTED,
      Status.Code.DEADLINE_EXCEEDED,
      Status.Code.RESOURCE_EXHAUSTED,
      Status.Code.UNKNOWN,
      Status.Code.UNAVAILABLE -> true
      else -> false
    }
  }
  return false
}
