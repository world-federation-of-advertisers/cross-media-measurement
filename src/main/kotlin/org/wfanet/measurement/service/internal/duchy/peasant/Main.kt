package org.wfanet.measurement.service.internal.duchy.peasant

import io.grpc.ManagedChannelBuilder
import java.time.Duration
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.runBlocking
import org.wfanet.measurement.common.Flags
import org.wfanet.measurement.common.addChannelShutdownHooks
import org.wfanet.measurement.common.durationFlag
import org.wfanet.measurement.common.stringFlag
import org.wfanet.measurement.internal.duchy.WorkerServiceGrpcKt

fun main(args: Array<String>) {
  val channelShutdownTimeout =
    durationFlag("channel-shutdown-timeout", Duration.ofSeconds(5))
  val minimumPollingInterval =
    durationFlag("minimum-polling-interval", Duration.ofSeconds(1))
  // TODO Figure out how to configure a collection of duchies. Config file?
  // The URI of the next WorkerService in the ring.
  val nextWorker = stringFlag("next-worker", "localhost:8080")
  Flags.parse(args.asIterable())

  val channel =
    ManagedChannelBuilder.forTarget(nextWorker.value)
      .usePlaintext()
      .build()

  addChannelShutdownHooks(Runtime.getRuntime(), channelShutdownTimeout.value, channel)

  val stub = WorkerServiceGrpcKt.WorkerServiceCoroutineStub(channel)

  val peasant = Peasant(mapOf(Pair("Alsace", stub)), minimumPollingInterval.value.toMillis())
  runBlocking {
    peasant.pollForWork().collect {
      // Deliberately empty
    }
  }
}
