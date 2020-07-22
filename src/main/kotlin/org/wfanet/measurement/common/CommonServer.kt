// Copyright 2020 The Measurement System Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.wfanet.measurement.common

import io.grpc.BindableService
import io.grpc.Server
import io.grpc.ServerBuilder
import java.io.IOException
import java.util.logging.Level
import java.util.logging.Logger

class CommonServer(
  private val nameForLogging: String,
  private val port: Int,
  vararg services: BindableService
) {
  private var server: Server

  init {
    val builder = ServerBuilder.forPort(port)
    services.forEach {
      builder.addService(it)
    }
    server = builder.build()
  }

  @Throws(IOException::class)
  fun start(): CommonServer {
    server.start()
    logger.log(
      Level.INFO,
      "$nameForLogging started, listening on $port"
    )
    Runtime.getRuntime().addShutdownHook(object : Thread() {
      override fun run() {
        // Use stderr here since the logger may have been reset by its JVM shutdown hook.
        System.err.println("*** $nameForLogging shutting down...")
        this@CommonServer.stop()
        System.err.println("*** $nameForLogging shut down")
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
