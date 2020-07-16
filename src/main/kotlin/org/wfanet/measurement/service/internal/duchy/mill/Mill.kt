package org.wfanet.measurement.service.internal.duchy.mill

import com.google.protobuf.ByteString
import io.grpc.StatusException
import java.util.logging.Logger
import kotlin.system.measureTimeMillis
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.flow
import org.wfanet.measurement.internal.duchy.HandleNoisedSketchRequest
import org.wfanet.measurement.internal.duchy.HandleNoisedSketchResponse
import org.wfanet.measurement.internal.duchy.ComputationControlServiceGrpcKt

class Mill(
  private val workerStubs: Map<String, ComputationControlServiceGrpcKt.ComputationControlServiceCoroutineStub>,
  private val minimumPollingDelayMillis: Long
) {
  // TODO Make this use the ComputationManager to claim work. This is just test code right now.
  suspend fun pollForWork(): Flow<HandleNoisedSketchResponse> = flow {
    logger.info("Starting Mill...")

    while (true) {
      val elapsed = measureTimeMillis {
        logger.info("Mill polling for work...")
        try {
          val response = workerStubs.values
            .first()
            .handleNoisedSketch(
              "1,2,3".split(',')
                .map {
                  HandleNoisedSketchRequest.newBuilder()
                    .setPartialSketch(ByteString.copyFromUtf8(it))
                    .build()
                }.asFlow()
            )
          emit(response)
          logger.info("Received: $response")
        } catch (e: StatusException) {
          logger.severe("Error: $e")
        }
      }
      if (elapsed < minimumPollingDelayMillis) {
        val delayMillis = minimumPollingDelayMillis - elapsed
        logger.info("Mill sleeping for $delayMillis millis")
        delay(delayMillis)
      }
    }
  }

  companion object {
    val logger: Logger = Logger.getLogger(this::class.java.name)
  }
}
