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

package org.wfanet.measurement.common.grpc

import io.grpc.BindableService
import io.grpc.Server
import io.grpc.ServerBuilder
import io.grpc.ServerServiceDefinition
import io.grpc.health.v1.HealthCheckResponse.ServingStatus
import io.grpc.services.HealthStatusManager
import java.io.IOException
import java.util.logging.Level
import java.util.logging.Logger
import kotlin.properties.Delegates
import picocli.CommandLine

class CommonServer private constructor(
  private val nameForLogging: String,
  private val port: Int,
  services: Iterable<ServerServiceDefinition>
) {
  private val healthStatusManager = HealthStatusManager()

  private val server: Server by lazy {
    ServerBuilder.forPort(port).apply {
      addService(healthStatusManager.healthService)
      services.forEach { addService(it) }
    }.build()
  }

  @Throws(IOException::class)
  fun start(): CommonServer {
    server.start()
    server.services.forEach {
      healthStatusManager.setStatus(it.serviceDescriptor.name, ServingStatus.SERVING)
    }

    logger.log(
      Level.INFO,
      "$nameForLogging started, listening on $port"
    )
    Runtime.getRuntime().addShutdownHook(
      object : Thread() {
        override fun run() {
          // Use stderr here since the logger may have been reset by its JVM shutdown hook.
          System.err.println("*** $nameForLogging shutting down...")
          this@CommonServer.stop()
          System.err.println("*** $nameForLogging shut down")
        }
      }
    )
    return this
  }

  private fun stop() {
    server.shutdown()
  }

  @Throws(InterruptedException::class)
  fun blockUntilShutdown() {
    server.awaitTermination()
  }

  class Flags {
    @set:CommandLine.Option(
      names = ["--port", "-p"],
      description = ["TCP port for gRPC server."],
      defaultValue = "8080"
    )
    var port by Delegates.notNull<Int>()
      private set

    @set:CommandLine.Option(
      names = ["--debug-verbose-grpc-server-logging"],
      description = ["Debug mode: log ALL gRPC requests and responses"],
      defaultValue = "false"
    )
    var debugVerboseGrpcLogging by Delegates.notNull<Boolean>()
      private set
  }

  companion object {
    private val logger = Logger.getLogger(this::class.java.name)

    /** Constructs a [CommonServer] from command-line flags. */
    @JvmName("fromFlagsServiceDefinition")
    fun fromFlags(
      flags: Flags,
      nameForLogging: String,
      services: Iterable<ServerServiceDefinition>
    ): CommonServer {
      return CommonServer(
        nameForLogging,
        flags.port,
        services.run {
          if (flags.debugVerboseGrpcLogging) map { it.withVerboseLogging() } else this
        }
      )
    }

    /** Constructs a [CommonServer] from command-line flags. */
    fun fromFlags(
      flags: Flags,
      nameForLogging: String,
      vararg services: ServerServiceDefinition
    ): CommonServer = fromFlags(flags, nameForLogging, services.asIterable())

    /** Constructs a [CommonServer] from command-line flags. */
    fun fromFlags(
      flags: Flags,
      nameForLogging: String,
      services: Iterable<BindableService>
    ): CommonServer = fromFlags(flags, nameForLogging, services.map { it.bindService() })

    /** Constructs a [CommonServer] from command-line flags. */
    fun fromFlags(
      flags: Flags,
      nameForLogging: String,
      vararg services: BindableService
    ): CommonServer = fromFlags(flags, nameForLogging, services.map { it.bindService() })
  }
}
