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

package org.wfanet.measurement.service.internal.duchy.computation.control

import com.google.protobuf.ByteString
import io.grpc.Status
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.mapNotNull
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.flow.withIndex
import org.wfanet.measurement.common.identity.DuchyIdentity
import org.wfanet.measurement.common.identity.duchyIdentityFromContext
import org.wfanet.measurement.common.toByteArray
import org.wfanet.measurement.db.duchy.computation.LiquidLegionsSketchAggregationComputationStorageClients
import org.wfanet.measurement.db.duchy.computation.singleOutputBlobMetadata
import org.wfanet.measurement.db.duchy.computation.toNoisedSketchBlobMetadataFor
import org.wfanet.measurement.internal.LiquidLegionsSketchAggregationStage
import org.wfanet.measurement.internal.duchy.ComputationControlServiceGrpcKt.ComputationControlServiceCoroutineImplBase
import org.wfanet.measurement.internal.duchy.ComputationDetails.RoleInComputation
import org.wfanet.measurement.internal.duchy.ComputationToken
import org.wfanet.measurement.internal.duchy.HandleConcatenatedSketchRequest
import org.wfanet.measurement.internal.duchy.HandleConcatenatedSketchResponse
import org.wfanet.measurement.internal.duchy.HandleEncryptedFlagsAndCountsRequest
import org.wfanet.measurement.internal.duchy.HandleEncryptedFlagsAndCountsResponse
import org.wfanet.measurement.internal.duchy.HandleNoisedSketchRequest
import org.wfanet.measurement.internal.duchy.HandleNoisedSketchResponse
import org.wfanet.measurement.service.internal.duchy.computation.storage.toGetTokenRequest
import org.wfanet.measurement.service.v1alpha.common.failGrpc
import org.wfanet.measurement.service.v1alpha.common.grpcRequire
import org.wfanet.measurement.service.v1alpha.common.grpcTryAndRethrow
import java.util.logging.Logger

class LiquidLegionsComputationControlServiceImpl(
  private val clients: LiquidLegionsSketchAggregationComputationStorageClients,
  private val duchyIdentityProvider: () -> DuchyIdentity = ::duchyIdentityFromContext
) : ComputationControlServiceCoroutineImplBase() {

  override suspend fun handleConcatenatedSketch(
    requests: Flow<HandleConcatenatedSketchRequest>
  ): HandleConcatenatedSketchResponse {
    val (token, sketch) = requests
      .map { it.computationId to it.partialSketch }
      .reduceToTokenAndAppendedBytesPairOrNullIf { token ->
        logger.info("[id=${token.globalComputationId}]: Received blind position request.")
        when (token.computationStage.liquidLegionsSketchAggregation) {
          // Check to see if it was already written.
          LiquidLegionsSketchAggregationStage.WAIT_CONCATENATED ->
            token.singleOutputBlobMetadata().path.isNotEmpty()
          // Ack message early if in a stage that is downstream of WAIT_CONCATENATED
          LiquidLegionsSketchAggregationStage.TO_BLIND_POSITIONS,
          LiquidLegionsSketchAggregationStage.TO_BLIND_POSITIONS_AND_JOIN_REGISTERS,
          LiquidLegionsSketchAggregationStage.WAIT_FLAG_COUNTS -> true
          else -> failGrpc { "Did not expect to get concatenated sketch for $token" }
        }
      } ?: return HandleConcatenatedSketchResponse.getDefaultInstance()

    val id = token.globalComputationId

    // The next stage to be worked depends upon the duchy's role in the computation.
    val nextStage = when (token.role) {
      RoleInComputation.PRIMARY ->
        LiquidLegionsSketchAggregationStage.TO_BLIND_POSITIONS_AND_JOIN_REGISTERS
      RoleInComputation.SECONDARY ->
        LiquidLegionsSketchAggregationStage.TO_BLIND_POSITIONS
      else -> failGrpc { "Unknown role in computation ${token.role}" }
    }
    logger.info("[id=$id]: Saving concatenated sketch.")
    val tokenAfterWrite = clients.writeSingleOutputBlob(token, sketch)

    logger.info("[id=$id]: transitioning to $nextStage")
    clients.transitionComputationToStage(
      computationToken = tokenAfterWrite,
      inputsToNextStage = listOf(tokenAfterWrite.singleOutputBlobMetadata().path),
      stage = nextStage
    )

    logger.info("[id=$id]: Saved sketch and transitioned stage to $nextStage")
    return HandleConcatenatedSketchResponse.getDefaultInstance() // Ack the request
  }

  override suspend fun handleEncryptedFlagsAndCounts(
    requests: Flow<HandleEncryptedFlagsAndCountsRequest>
  ): HandleEncryptedFlagsAndCountsResponse {
    val (token, bytes) = requests
      .map { it.computationId to it.partialData }
      .reduceToTokenAndAppendedBytesPairOrNullIf { token ->
        logger.info("[id=${token.globalComputationId}]: Received decrypt flags and counts request.")
        when (token.computationStage.liquidLegionsSketchAggregation) {
          // Check to see if it was already written.
          LiquidLegionsSketchAggregationStage.WAIT_FLAG_COUNTS ->
            token.singleOutputBlobMetadata().path.isNotEmpty()
          // Ack message early if in a stage that is downstream of WAIT_FLAG_COUNTS
          LiquidLegionsSketchAggregationStage.TO_DECRYPT_FLAG_COUNTS,
          LiquidLegionsSketchAggregationStage.TO_DECRYPT_FLAG_COUNTS_AND_COMPUTE_METRICS,
          LiquidLegionsSketchAggregationStage.COMPLETED -> true
          else -> failGrpc { "Did not expect to get flag counts for $token" }
        }
      } ?: return HandleEncryptedFlagsAndCountsResponse.getDefaultInstance()

    val id = token.globalComputationId

    logger.info("[id=$id]: Saving encrypted flags and counts.")
    val tokenAfterWrite = clients.writeSingleOutputBlob(token, bytes)

    // The next stage to be worked depends upon the duchy's role in the computation.
    val nextStage = when (token.role) {
      RoleInComputation.PRIMARY ->
        LiquidLegionsSketchAggregationStage.TO_DECRYPT_FLAG_COUNTS_AND_COMPUTE_METRICS
      RoleInComputation.SECONDARY ->
        LiquidLegionsSketchAggregationStage.TO_DECRYPT_FLAG_COUNTS
      else -> failGrpc { "Unknown role in computation ${token.role}" }
    }
    logger.info("[id=$id]: transitioning to $nextStage")
    clients.transitionComputationToStage(
      computationToken = tokenAfterWrite,
      inputsToNextStage = listOf(tokenAfterWrite.singleOutputBlobMetadata().path),
      stage = nextStage
    )

    logger.info("[id=$id]: Saved sketch and transitioned stage to $nextStage")
    return HandleEncryptedFlagsAndCountsResponse.getDefaultInstance() // Ack the request
  }

  override suspend fun handleNoisedSketch(
    requests: Flow<HandleNoisedSketchRequest>
  ): HandleNoisedSketchResponse {
    val sender = duchyIdentityProvider().id
    val (token, bytes) = requests
      .map { it.computationId to it.partialSketch }
      .reduceToTokenAndAppendedBytesPairOrNullIf { token ->
        logger.info(
          "[id=${token.globalComputationId}]: Received noised sketch request from $sender."
        )
        grpcRequire(token.role == RoleInComputation.PRIMARY, Status.FAILED_PRECONDITION) {
          "Duchy is not the primary server but received a sketch from $sender for $token"
        }
        when (token.computationStage.liquidLegionsSketchAggregation) {
          // Check to see if it was already written.
          LiquidLegionsSketchAggregationStage.WAIT_SKETCHES ->
            token.toNoisedSketchBlobMetadataFor(sender).path.isNotEmpty()
          // Ack message early if in a stage that is downstream of WAIT_SKETCHES
          LiquidLegionsSketchAggregationStage.TO_APPEND_SKETCHES_AND_ADD_NOISE,
          LiquidLegionsSketchAggregationStage.WAIT_CONCATENATED -> true
          else -> failGrpc { "Did not expect to get noised sketch for $token." }
        }
      } ?: return HandleNoisedSketchResponse.getDefaultInstance()

    val id = token.globalComputationId
    logger.info("[id=$id]: Saving noised sketch from $sender.")
    val tokenAfterWrite = clients.writeReceivedNoisedSketch(token, bytes, sender)
    enqueueAppendSketchesOperationIfReceivedAllSketches(tokenAfterWrite)
    return HandleNoisedSketchResponse.getDefaultInstance() // Ack the request
  }

  private suspend fun enqueueAppendSketchesOperationIfReceivedAllSketches(
    token: ComputationToken
  ) {
    val id = token.globalComputationId
    val sketchesNotYetReceived = token.blobsList.count { it.path.isEmpty() }

    if (sketchesNotYetReceived == 0) {
      val nextStage = LiquidLegionsSketchAggregationStage.TO_APPEND_SKETCHES_AND_ADD_NOISE
      logger.info("[id=$id]: transitioning to $nextStage")
      clients.transitionComputationToStage(
        computationToken = token,
        inputsToNextStage = token.blobsList.map { it.path }.toList(),
        stage = nextStage
      )
      logger.info("[id=$id]: Saved sketch and transitioned stage to $nextStage.")
    } else {
      logger.info("[id=$id]: Saved sketch but still waiting on $sketchesNotYetReceived more.")
    }
  }

  /**
   * Reduces flow of pairs of ids and [ByteString]s into a single pair [ComputationToken] for that
   * id and a concatenated byte array, or a null value if the predicate is true for token.
   *
   * A token is retrieved when processing the first item in the flow and is passed to the
   * [wipeFlowPredicate] to determine if the bytes in the flow should be collected or not.
   *
   * @param wipeFlowPredicate when evaluates to true the bytes of the flow are not collected and
   * this function returns null
   */
  private suspend fun Flow<Pair<Long, ByteString>>.reduceToTokenAndAppendedBytesPairOrNullIf(
    wipeFlowPredicate: suspend (ComputationToken) -> Boolean
  ): Pair<ComputationToken, ByteArray>? {
    var wipeFlow = false
    var tokenToReturn: ComputationToken? = null
    val bytes =
      withIndex().mapNotNull { (index, value) ->
        if (index == 0) {
          val id = value.first
          val token = grpcTryAndRethrow(
            failureStatus = Status.NOT_FOUND,
            errorMessage = { "Unknown computation $id." }
          ) {
            clients.computationStorageClient
              .getComputationToken(id.toGetTokenRequest())
              .token
          }
          wipeFlow = wipeFlowPredicate(token)
          tokenToReturn = token
        } else {
          grpcRequire(tokenToReturn?.globalComputationId == value.first, Status.INVALID_ARGUMENT) {
            "Stream has multiple ids ${tokenToReturn?.globalComputationId} and ${value.first}"
          }
        }
        // Filter out all elements in the flow if it matched the wipeFlowPredicate
        // This will make the toCollection call basically a noop. Without this we would
        // be concatenating all bytes anyways.
        if (wipeFlow) null else value.second
      }.toList(mutableListOf())
    // The token is retrieved when processing the first item in the flow. If it is still null after
    // collecting the flow that means there wasn't a first item in the flow.
    grpcRequire(tokenToReturn != null, Status.INVALID_ARGUMENT) { "Empty request stream" }
    return if (wipeFlow) null else tokenToReturn!! to bytes.toByteArray()
  }

  companion object {
    val logger: Logger = Logger.getLogger(this::class.java.name)
  }
}
