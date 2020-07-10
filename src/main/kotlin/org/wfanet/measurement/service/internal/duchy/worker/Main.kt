package org.wfanet.measurement.service.internal.duchy.worker

import io.grpc.ManagedChannelBuilder
import java.util.concurrent.TimeUnit
import org.wfanet.measurement.common.CommonServer
import org.wfanet.measurement.common.Flags
import org.wfanet.measurement.common.intFlag
import org.wfanet.measurement.common.stringFlag
import org.wfanet.measurement.internal.duchy.WorkerServiceGrpcKt

fun main(args: Array<String>) {
  // Port for this gRPC server to listen on,
  val port = intFlag("port", 8080)
  // Name to include in log messages to help with debugging.
  val nameForLogging = stringFlag("name-for-logging", "WorkerServer")
  // The hostname of the next WorkerService in the ring.
  val nextWorkerHost = stringFlag("next-worker-host", "localhost")
  // The port of the next WorkerService in the ring.
  val newWorkerPort = intFlag("next-worker-port", 8080)
  Flags.parse(args.asIterable())

  val channel =
    ManagedChannelBuilder.forAddress(nextWorkerHost.value, newWorkerPort.value)
      .usePlaintext()
      .build()
  val stub = WorkerServiceGrpcKt.WorkerServiceCoroutineStub(channel)

  // Give the channel some time to finish what it's doing.
  Runtime.getRuntime().addShutdownHook(
    Thread {
      // Use stderr here since the logger may have been reset by its JVM shutdown hook.
      System.err.println("*** Shutting down...")
      channel.shutdown().awaitTermination(5, TimeUnit.SECONDS)
      System.err.println("*** Shut down")
    }
  )

  CommonServer(nameForLogging.value, port.value, WorkerServiceImpl(stub, nameForLogging.value))
    .start()
    .blockUntilShutdown()
}
