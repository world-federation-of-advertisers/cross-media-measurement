package org.wfanet.measurement.common

import io.grpc.BindableService
import io.grpc.Server
import io.grpc.ServerBuilder
import java.io.IOException
import java.util.logging.Level
import java.util.logging.Logger

class CommonServer(private val serversEnum: CommonServerType,
                   vararg services: BindableService) {
  private var server: Server

  init {
    val builder = ServerBuilder.forPort(serversEnum.port)
    services.forEach {
      builder.addService(it)
    }
    server = builder.build()
  }

  @Throws(IOException::class)
  fun start(): CommonServer {
    server.start()
    logger.log(Level.INFO,
               "${serversEnum.nameForLogging} started, listening on ${serversEnum.port}")
    Runtime.getRuntime().addShutdownHook(object : Thread() {
      override fun run() {
        // Use stderr here since the logger may have been reset by its JVM shutdown hook.
        System.err.println("*** ${serversEnum.nameForLogging} shutting down...")
        this@CommonServer.stop()
        System.err.println("*** ${serversEnum.nameForLogging} shut down")
      }
    })
    return this
  }

  private fun stop() {
    server.shutdown()
  }

  @Throws(InterruptedException::class)
  fun blockUntilShutdown() {
    server.awaitTermination()
  }

  companion object {
    private val logger = Logger.getLogger(this::class.java.name)
  }
}

enum class CommonServerType(val nameForLogging: String, val port: Int) {
  COMPUTATION_CONTROL("ComputationControlServer", 31124),
  PUBLISHER_DATA("PublisherDataServer", 31125),
  REQUISITION("RequisitionServer", 31126)
}
