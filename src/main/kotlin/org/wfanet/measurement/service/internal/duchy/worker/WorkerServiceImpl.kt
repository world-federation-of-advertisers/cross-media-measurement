package org.wfanet.measurement.service.internal.duchy.worker

import io.grpc.Status
import io.grpc.StatusRuntimeException
import org.wfanet.measurement.client.internal.duchy.worker.WorkerClient
import org.wfanet.measurement.internal.duchy.TraceRequest
import org.wfanet.measurement.internal.duchy.TraceResponse
import org.wfanet.measurement.internal.duchy.WorkerServiceGrpcKt

class WorkerServiceImpl(
  private val workerClient: WorkerClient,
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
        workerClient.use { it.trace(request.count - 1) }
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
}
