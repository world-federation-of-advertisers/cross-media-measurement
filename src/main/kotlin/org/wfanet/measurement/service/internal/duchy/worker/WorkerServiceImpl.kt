package org.wfanet.measurement.service.internal.duchy.worker

import com.google.protobuf.ByteString
import java.util.logging.Logger
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.reduce
import org.wfanet.measurement.common.DuchyRole
import org.wfanet.measurement.db.duchy.SketchAggregationComputationManager
import org.wfanet.measurement.internal.SketchAggregationStage
import org.wfanet.measurement.internal.duchy.BlindPositionsRequest
import org.wfanet.measurement.internal.duchy.BlindPositionsResponse
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

    // The next stage to be worked depends upon the duchy'es role in the computation.
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

  override suspend fun transmitNoisedSketch(
    requests: Flow<TransmitNoisedSketchRequest>
  ): TransmitNoisedSketchResponse {
    // TODO: Currently this function is implemented in a way to show a peasant polled for work,
    // it needs to be updated to store sketches received while in the WAIT_SKETCHES stage.

    logger.info("Handling request...")
    var sketch = ByteString.EMPTY
    requests.collect {
      logger.info("Partial sketch: ${it.partialSketch}")
      sketch = sketch.concat(it.partialSketch)
    }
    logger.info("Complete sketch: $sketch")
    logger.info("Returning response")
    return TransmitNoisedSketchResponse.getDefaultInstance()
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
