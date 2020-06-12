package org.wfanet.measurement.service.internal.duchy.worker

import com.google.protobuf.ByteString
import io.grpc.Status
import io.grpc.StatusRuntimeException
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.collect
import org.wfanet.measurement.internal.duchy.TraceRequest
import org.wfanet.measurement.internal.duchy.TraceResponse
import org.wfanet.measurement.internal.duchy.TransmitNoisedSketchRequest
import org.wfanet.measurement.internal.duchy.TransmitNoisedSketchResponse
import org.wfanet.measurement.internal.duchy.WorkerServiceGrpcKt
import java.util.logging.Logger

class WorkerServiceImpl(
  // TODO Eliminate trace method and these arguments.
  private val workerServiceStub: WorkerServiceGrpcKt.WorkerServiceCoroutineStub,
  private val nameForLogging: String
) :
  WorkerServiceGrpcKt.WorkerServiceCoroutineImplBase() {
  // This limits the maximum number of hops that a trace is allowed to make. This is a sanity check
  // to prevent a caller from requesting traces that just bounce around the system endlessly.
  private val maxTraceCount = 5

  override suspend fun trace(request: TraceRequest): TraceResponse {
    if (request.count > maxTraceCount) {
      throw StatusRuntimeException(
        Status.INVALID_ARGUMENT
          .withDescription("Counts greater than $maxTraceCount are not permitted")
      )
    }
    val response =
      if (request.count > 0) {
        workerServiceStub.trace(TraceRequest.newBuilder().setCount(request.count - 1).build())
      } else {
        null
      }
    return TraceResponse.newBuilder()
      .addHop(
        TraceResponse.Hop.newBuilder()
          .setName(nameForLogging)
          .setCountdown(request.count)
          .build()
      )
      .addAllHop(response?.hopList ?: listOf())
      .build()
  }

  override suspend fun transmitNoisedSketch(
    requests: Flow<TransmitNoisedSketchRequest>
  ): TransmitNoisedSketchResponse {
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
