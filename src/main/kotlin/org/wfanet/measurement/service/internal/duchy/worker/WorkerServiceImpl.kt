package org.wfanet.measurement.service.internal.duchy.worker

import java.util.logging.Logger
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.reduce
import org.wfanet.measurement.common.DuchyRole
import org.wfanet.measurement.db.duchy.BlobDependencyType
import org.wfanet.measurement.db.duchy.ComputationToken
import org.wfanet.measurement.db.duchy.SketchAggregationComputationManager
import org.wfanet.measurement.internal.SketchAggregationStage
import org.wfanet.measurement.internal.duchy.BlindPositionsRequest
import org.wfanet.measurement.internal.duchy.BlindPositionsResponse
import org.wfanet.measurement.internal.duchy.DecryptFlagAndCountRequest
import org.wfanet.measurement.internal.duchy.DecryptFlagAndCountResponse
import org.wfanet.measurement.internal.duchy.TransmitNoisedSketchRequest
import org.wfanet.measurement.internal.duchy.TransmitNoisedSketchResponse
import org.wfanet.measurement.internal.duchy.WorkerServiceGrpcKt

@ExperimentalCoroutinesApi
class WorkerServiceImpl(
  private val computationManager: SketchAggregationComputationManager
) :
  WorkerServiceGrpcKt.WorkerServiceCoroutineImplBase() {

  override suspend fun blindPositions(
    requests: Flow<BlindPositionsRequest>
  ): BlindPositionsResponse {
    val (id, sketch) =
      requests.map { it.computationId to it.partialSketch.toByteArray() }.appendAllByteArrays()
    logger.info("[id=$id]: Received blind position request.")
    val token = requireNotNull(computationManager.getToken(id)) {
      "Received BlindPositionsRequest for unknown computation $id"
    }

    logger.info("[id=$id]: Saving concatenated sketch.")
    val (tokenAfterWrite, path) = computationManager.writeReceivedConcatenatedSketch(token, sketch)

    // The next stage to be worked depends upon the duchy's role in the computation.
    val nextStage = when (token.role) {
      DuchyRole.PRIMARY -> SketchAggregationStage.TO_BLIND_POSITIONS_AND_JOIN_REGISTERS
      DuchyRole.SECONDARY -> SketchAggregationStage.TO_BLIND_POSITIONS
    }
    logger.info("[id=$id]: transitioning to $nextStage")
    computationManager.transitionComputationToStage(
      token = tokenAfterWrite,
      inputsToNextStage = listOf(path),
      stage = nextStage
    )

    logger.info("[id=$id]: Saved sketch and transitioned stage to $nextStage")
    return BlindPositionsResponse.getDefaultInstance() // Ack the request
  }

  override suspend fun decryptFlagAndCount(
    requests: Flow<DecryptFlagAndCountRequest>
  ): DecryptFlagAndCountResponse {
    val (id, bytes) =
      requests.map { it.computationId to it.partialSketch.toByteArray() }.appendAllByteArrays()
    logger.info("[id=$id]: Received decrypt flags and counts request.")
    val token = requireNotNull(computationManager.getToken(id)) {
      "Received DecryptFlagAndCountRequest for unknown computation $id"
    }

    logger.info("[id=$id]: Saving encrypted flags and counts.")
    val (tokenAfterWrite, path) = computationManager.writeReceivedFlagsAndCounts(token, bytes)

    // The next stage to be worked depends upon the duchy's role in the computation.
    val nextStage = when (token.role) {
      DuchyRole.PRIMARY -> SketchAggregationStage.TO_DECRYPT_FLAG_COUNTS_AND_COMPUTE_METRICS
      DuchyRole.SECONDARY -> SketchAggregationStage.TO_DECRYPT_FLAG_COUNTS
    }
    logger.info("[id=$id]: transitioning to $nextStage")
    computationManager.transitionComputationToStage(
      token = tokenAfterWrite,
      inputsToNextStage = listOf(path),
      stage = nextStage
    )

    logger.info("[id=$id]: Saved sketch and transitioned stage to $nextStage")
    return DecryptFlagAndCountResponse.getDefaultInstance() // Ack the request
  }

  override suspend fun transmitNoisedSketch(
    requests: Flow<TransmitNoisedSketchRequest>
  ): TransmitNoisedSketchResponse {
    val (id, sender, bytes) =
      requests
        .map {
          Triple(
            it.computationId,
            checkNotNull(it.sender),
            checkNotNull(it.partialSketch.toByteArray())
          )
        }
        .reduce { x, y ->
          require(x.first == y.first) {
            "Stream has multiple computations $x and $y"
          }
          require(x.second == y.second) {
            "Stream has multiple senders $x and $y"
          }
          Triple(x.first, x.second, (x.third + y.third))
        }
    logger.info("[id=$id]: Received noised sketch request from $sender.")
    val token = requireNotNull(computationManager.getToken(id)) {
      "Received TransmitNoisedSketchRequest for unknown computation $id"
    }
    require(token.role == DuchyRole.PRIMARY) {
      "Duchy is not the primary server but received a sketch from $sender for $token"
    }

    logger.info("[id=$id]: Saving noised sketch from $sender.")
    val tokenAfterWrite = computationManager.writeReceivedNoisedSketch(token, bytes, sender)
    enqueueAppendSketchesOperationIfReceivedAllSketches(tokenAfterWrite)

    return TransmitNoisedSketchResponse.getDefaultInstance() // Ack the request
  }

  private suspend fun enqueueAppendSketchesOperationIfReceivedAllSketches(
    token: ComputationToken<SketchAggregationStage>
  ) {
    val id = token.globalId
    val sketches =
      computationManager.readBlobReferences(token, BlobDependencyType.ANY).values

    val sketchesNotYetReceived = sketches.count { it == null }
    if (sketchesNotYetReceived == 0) {
      val nextStage = SketchAggregationStage.TO_APPEND_SKETCHES
      logger.info("[id=$id]: transitioning to $nextStage")
      computationManager.transitionComputationToStage(
        token = token,
        inputsToNextStage = sketches.filterNotNull().toList(),
        stage = nextStage
      )
      logger.info("[id=$id]: Saved sketch and transitioned stage to $nextStage.")
    } else {
      logger.info("[id=$id]: Saved sketch but still waiting on $sketchesNotYetReceived more.")
    }
  }

  companion object {
    val logger: Logger = Logger.getLogger(this::class.java.name)
  }
}

/**
 * Reduces flow of pairs of ids and byte arrays into a single flow of id and concatenated byte
 * array. This function requires that all ids in the flow are equal.
 */
@ExperimentalCoroutinesApi
private suspend fun Flow<Pair<Long, ByteArray>>.appendAllByteArrays(): Pair<Long, ByteArray> =
  this.reduce { x, y ->
    require(x.first == y.first) { "Stream has multiple computations $x and $y" }
    x.first to (x.second + y.second)
  }
