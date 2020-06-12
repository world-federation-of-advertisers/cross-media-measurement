package org.wfanet.measurement.service.internal.duchy.peasant

import com.google.protobuf.ByteString
import io.grpc.StatusException
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.flow
import org.wfanet.measurement.internal.duchy.TransmitNoisedSketchRequest
import org.wfanet.measurement.internal.duchy.TransmitNoisedSketchResponse
import org.wfanet.measurement.internal.duchy.WorkerServiceGrpcKt
import java.util.logging.Logger
import kotlin.system.measureTimeMillis

class Peasant(
  private val workerStubs: Map<String, WorkerServiceGrpcKt.WorkerServiceCoroutineStub>,
  private val minimumPollingDelayMillis: Long
) {
  // TODO Make this use the ComputationManager to claim work. This is just test code right now.
  suspend fun pollForWork(): Flow<TransmitNoisedSketchResponse> = flow {
    logger.info("Starting peasant...")

    while (true) {
      val elapsed = measureTimeMillis {
        logger.info("Peasant polling for work...")
        try {
          val response = workerStubs.values
            .first()
            .transmitNoisedSketch(
              "1,2,3".split(',')
                .map {
                  TransmitNoisedSketchRequest.newBuilder()
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
        logger.info("Peasant sleeping for $delayMillis millis")
        delay(delayMillis)
      }
    }
  }

  companion object {
    val logger: Logger = Logger.getLogger(this::class.java.name)
  }
}
