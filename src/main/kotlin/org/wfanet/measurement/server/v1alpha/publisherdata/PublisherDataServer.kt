package org.wfanet.measurement.server.v1alpha.publisherdata

import io.grpc.Server
import io.grpc.ServerBuilder
import java.io.IOException
import java.util.logging.Level
import java.util.logging.Logger
import org.wfanet.measurement.service.v1alpha.publisherdata.PublisherDataImpl

class PublisherDataServer {
  private val port: Int = 31125
  private var server: Server

  init {
    server = ServerBuilder.forPort(port)
      .addService(PublisherDataImpl())
      .build()
  }

  @Throws(IOException::class)
  fun start() {
    server.start()
    logger.log(Level.INFO, "PublisherDataServer started, listening on {0}", port)
    Runtime.getRuntime().addShutdownHook(object : Thread() {
      override fun run() {
        System.err.println("PublisherDataServer shutting down...")
        this@PublisherDataServer.stop()
        System.err.println("PublisherDataServer shut down")
      }
    })
  }

  private fun stop() {
    server.shutdown()
  }

  @Throws(InterruptedException::class)
  fun blockUntilShutdown() {
    server.awaitTermination()
  }

  companion object {
    private val logger = Logger.getLogger(PublisherDataServer::class.java.name)
  }
}
