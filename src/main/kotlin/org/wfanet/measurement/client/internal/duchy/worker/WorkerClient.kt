package org.wfanet.measurement.client.internal.duchy.worker

import io.grpc.ManagedChannel
import java.io.Closeable
import java.util.concurrent.TimeUnit
import org.wfanet.measurement.internal.duchy.ComputationControlServiceGrpcKt

class WorkerClient(
  private val channel: ManagedChannel,
  private val stub: ComputationControlServiceGrpcKt.ComputationControlServiceCoroutineStub
) :
  Closeable {

  override fun close() {
    channel.shutdown().awaitTermination(5, TimeUnit.SECONDS)
  }
}
