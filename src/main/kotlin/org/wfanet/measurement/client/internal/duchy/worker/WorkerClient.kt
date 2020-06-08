package org.wfanet.measurement.client.internal.duchy.worker

import io.grpc.ManagedChannel
import kotlinx.coroutines.coroutineScope
import org.wfanet.measurement.internal.duchy.TraceRequest
import org.wfanet.measurement.internal.duchy.TraceResponse
import org.wfanet.measurement.internal.duchy.WorkerServiceGrpcKt
import java.io.Closeable
import java.util.concurrent.TimeUnit

class WorkerClient(
  private val channel: ManagedChannel,
  private val stub: WorkerServiceGrpcKt.WorkerServiceCoroutineStub
) :
  Closeable {

  override fun close() {
    channel.shutdown().awaitTermination(5, TimeUnit.SECONDS)
  }

  suspend fun trace(count: Int): TraceResponse = coroutineScope {
    stub.trace(TraceRequest.newBuilder().setCount(count).build())
  }
}
