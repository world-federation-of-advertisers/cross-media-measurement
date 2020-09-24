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
import io.grpc.StatusException
import java.util.logging.Logger
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.map
import org.wfanet.measurement.common.ConsumedFlowItem
import org.wfanet.measurement.common.consumeFirst
import org.wfanet.measurement.common.identity.DuchyIdentity
import org.wfanet.measurement.common.identity.duchyIdentityFromContext
import org.wfanet.measurement.db.duchy.computation.LiquidLegionsSketchAggregationComputationStorageClients
import org.wfanet.measurement.db.duchy.computation.singleOutputBlobMetadata
import org.wfanet.measurement.db.duchy.computation.toNoisedSketchBlobMetadataFor
import org.wfanet.measurement.internal.LiquidLegionsSketchAggregationStage
import org.wfanet.measurement.internal.duchy.ComputationControlServiceGrpcKt.ComputationControlServiceCoroutineImplBase
import org.wfanet.measurement.internal.duchy.ComputationDetails.RoleInComputation
import org.wfanet.measurement.internal.duchy.ComputationToken
import org.wfanet.measurement.internal.duchy.ComputationTypeEnum.ComputationType
import org.wfanet.measurement.internal.duchy.GetComputationTokenRequest
import org.wfanet.measurement.internal.duchy.GetComputationTokenResponse
import org.wfanet.measurement.internal.duchy.HandleConcatenatedSketchRequest
import org.wfanet.measurement.internal.duchy.HandleConcatenatedSketchResponse
import org.wfanet.measurement.internal.duchy.HandleEncryptedFlagsAndCountsRequest
import org.wfanet.measurement.internal.duchy.HandleEncryptedFlagsAndCountsResponse
import org.wfanet.measurement.internal.duchy.HandleNoisedSketchRequest
import org.wfanet.measurement.internal.duchy.HandleNoisedSketchResponse
import org.wfanet.measurement.service.v1alpha.common.failGrpc
import org.wfanet.measurement.service.v1alpha.common.grpcRequire
import org.wfanet.measurement.service.v1alpha.common.grpcRequireNotNull

private val COMPUTATION_TYPE = ComputationType.LIQUID_LEGIONS_SKETCH_AGGREGATION_V1

class LiquidLegionsComputationControlServiceImpl(
  private val clients: LiquidLegionsSketchAggregationComputationStorageClients,
  private val duchyIdentityProvider: () -> DuchyIdentity = ::duchyIdentityFromContext
) : ComputationControlServiceCoroutineImplBase() {

  override suspend fun handleConcatenatedSketch(
    requests: Flow<HandleConcatenatedSketchRequest>
  ): HandleConcatenatedSketchResponse {
    val tokenAfterWrite = consumeRequestsAndWriteBlob(
      requests,
      { header.computationId },
      { bodyChunk.partialSketch }
    ) { token, content ->

      val globalComputationId = token.globalComputationId
      logger.info("[id=$globalComputationId]: Received blind position sketch.")
      val shouldWriteBlob = when (token.computationStage.liquidLegionsSketchAggregation) {
        // Check to see if it was already written.
        LiquidLegionsSketchAggregationStage.WAIT_CONCATENATED ->
          token.singleOutputBlobMetadata().path.isEmpty()
        // Ack message early if in a stage that is downstream of WAIT_CONCATENATED
        LiquidLegionsSketchAggregationStage.TO_BLIND_POSITIONS,
        LiquidLegionsSketchAggregationStage.TO_BLIND_POSITIONS_AND_JOIN_REGISTERS,
        LiquidLegionsSketchAggregationStage.WAIT_FLAG_COUNTS -> false
        else -> failGrpc { "Did not expect to get concatenated sketch for $token" }
      }

      if (shouldWriteBlob) {
        logger.info("[id=$globalComputationId]: Saving concatenated sketch.")
        clients.writeSingleOutputBlob(token, content)
      } else {
        null
      }
    } ?: return HandleConcatenatedSketchResponse.getDefaultInstance()

    val id = tokenAfterWrite.globalComputationId

    // The next stage to be worked depends upon the duchy's role in the computation.
    @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA") // Proto enum accessors never return null.
    val nextStage = when (val role = tokenAfterWrite.role) {
      RoleInComputation.PRIMARY ->
        LiquidLegionsSketchAggregationStage.TO_BLIND_POSITIONS_AND_JOIN_REGISTERS
      RoleInComputation.SECONDARY ->
        LiquidLegionsSketchAggregationStage.TO_BLIND_POSITIONS
      RoleInComputation.UNKNOWN, RoleInComputation.UNRECOGNIZED ->
        failGrpc { "Unknown role in computation $role" }
    }

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
    val tokenAfterWrite = consumeRequestsAndWriteBlob(
      requests,
      { header.computationId },
      { bodyChunk.partialData }
    ) { token, content ->

      val id = token.globalComputationId
      logger.info("[id=${token.globalComputationId}]: Received decrypt flags and counts request.")
      val shouldWriteBlob = when (token.computationStage.liquidLegionsSketchAggregation) {
        // Check to see if it was already written.
        LiquidLegionsSketchAggregationStage.WAIT_FLAG_COUNTS ->
          token.singleOutputBlobMetadata().path.isEmpty()
        // Ack message early if in a stage that is downstream of WAIT_FLAG_COUNTS
        LiquidLegionsSketchAggregationStage.TO_DECRYPT_FLAG_COUNTS,
        LiquidLegionsSketchAggregationStage.TO_DECRYPT_FLAG_COUNTS_AND_COMPUTE_METRICS,
        LiquidLegionsSketchAggregationStage.COMPLETED -> false
        else -> failGrpc { "Did not expect to get flag counts for $token" }
      }

      if (shouldWriteBlob) {
        logger.info("[id=$id]: Saving encrypted flags and counts.")
        clients.writeSingleOutputBlob(token, content)
      } else {
        null
      }
    } ?: return HandleEncryptedFlagsAndCountsResponse.getDefaultInstance()

    val id = tokenAfterWrite.globalComputationId

    // The next stage to be worked depends upon the duchy's role in the computation.
    val nextStage = when (val role = tokenAfterWrite.role) {
      RoleInComputation.PRIMARY ->
        LiquidLegionsSketchAggregationStage.TO_DECRYPT_FLAG_COUNTS_AND_COMPUTE_METRICS
      RoleInComputation.SECONDARY ->
        LiquidLegionsSketchAggregationStage.TO_DECRYPT_FLAG_COUNTS
      else -> failGrpc { "Unknown role in computation $role" }
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
    val tokenAfterWrite = consumeRequestsAndWriteBlob(
      requests,
      { header.computationId },
      { bodyChunk.partialSketch }
    ) { token, content ->

      val id = token.globalComputationId
      val sender = duchyIdentityProvider().id
      logger.info("[id=$id]: Received noised sketch request from $sender.")
      val shouldWriteBlob = when (token.computationStage.liquidLegionsSketchAggregation) {
        // Check to see if it was already written.
        LiquidLegionsSketchAggregationStage.WAIT_SKETCHES ->
          token.toNoisedSketchBlobMetadataFor(sender).path.isEmpty()
        // Ack message early if in a stage that is downstream of WAIT_SKETCHES
        LiquidLegionsSketchAggregationStage.TO_APPEND_SKETCHES_AND_ADD_NOISE,
        LiquidLegionsSketchAggregationStage.WAIT_CONCATENATED -> false
        else -> failGrpc { "Did not expect to get noised sketch for $token." }
      }

      if (shouldWriteBlob) {
        logger.info("[id=$id]: Saving noised sketch from $sender.")
        clients.writeReceivedNoisedSketch(token, content, sender)
      } else {
        null
      }
    } ?: return HandleNoisedSketchResponse.getDefaultInstance()

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
   * Consumes the given [requests] and writes the body chunk content using
   * [writeBlob].
   *
   * @param getComputationId function which returns the global computation ID
   *     for a header request message receiver
   * @param getChunkContent function which returns the [ByteString] content for
   *     a body chunk request message receiver
   * @param writeBlob function which writes a computation blob with the given
   *     contents for the given [ComputationToken], returning the resulting
   *     token or `null` if write was skipped due to stage
   * @return the resulting [ComputationToken] from [writeBlob]
   */
  private suspend fun <T> consumeRequestsAndWriteBlob(
    requests: Flow<T>,
    getComputationId: T.() -> String,
    getChunkContent: T.() -> ByteString,
    writeBlob: suspend (ComputationToken, Flow<ByteString>) -> ComputationToken?
  ): ComputationToken? {
    return grpcRequireNotNull(requests.consumeFirst()) { "Empty request stream" }
      .use { consumed: ConsumedFlowItem<T> ->
        val id = consumed.item.getComputationId()
        grpcRequire(id.isNotEmpty()) { "Missing computation ID" }
        grpcRequire(consumed.hasRemaining) { "Request stream has no body" }

        val token = getComputationToken(id)
        val bodyRequests = consumed.remaining
        writeBlob(token, bodyRequests.map { it.getChunkContent() })
      }
  }

  /** Retrieves a [ComputationToken] from the `ComputationStorage` service. */
  private suspend fun getComputationToken(globalComputationId: String): ComputationToken {
    val request = GetComputationTokenRequest.newBuilder().apply {
      setGlobalComputationId(globalComputationId)
      computationType = COMPUTATION_TYPE
    }.build()

    val response: GetComputationTokenResponse = try {
      clients.computationStorageClient.getComputationToken(request)
    } catch (e: StatusException) {
      val status = e.status.withCause(e).apply {
        if (code != Status.Code.NOT_FOUND) {
          withDescription("Unable to retrieve token for computation $globalComputationId.")
        }
      }
      throw status.asRuntimeException()
    }

    return response.token
  }

  companion object {
    private val logger: Logger = Logger.getLogger(this::class.java.name)
  }
}
