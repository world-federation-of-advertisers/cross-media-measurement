package org.wfanet.measurement.service.internal.duchy.worker

import io.grpc.ManagedChannelBuilder
import org.wfanet.measurement.client.internal.duchy.worker.WorkerClient
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
  val client = WorkerClient(channel, stub)

  CommonServer(nameForLogging.value, port.value, WorkerServiceImpl(client, nameForLogging.value))
    .start()
    .blockUntilShutdown()
}
