/*
 * Copyright 2023 The Cross-Media Measurement Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.wfanet.measurement.reporting.deploy.v2.common

import io.grpc.Server
import io.grpc.ServerServiceDefinition
import io.grpc.inprocess.InProcessServerBuilder
import java.util.concurrent.TimeUnit
import org.wfanet.measurement.common.grpc.CommonServer
import org.wfanet.measurement.common.grpc.ErrorLoggingServerInterceptor
import org.wfanet.measurement.common.grpc.LoggingServerInterceptor

object InProcessServersMethods {
  fun startInProcessServerWithService(
    serverName: String,
    commonServerFlags: CommonServer.Flags,
    service: ServerServiceDefinition,
  ): Server {
    val server: Server =
      InProcessServerBuilder.forName(serverName)
        .apply {
          directExecutor()
          addService(service)
          if (commonServerFlags.debugVerboseGrpcLogging) {
            intercept(LoggingServerInterceptor)
          } else {
            intercept(ErrorLoggingServerInterceptor)
          }
        }
        .build()

    Runtime.getRuntime()
      .addShutdownHook(
        object : Thread() {
          override fun run() {
            server.shutdown()
            try {
              // Wait for in-flight RPCs to complete.
              server.awaitTermination(
                commonServerFlags.shutdownGracePeriodSeconds.toLong(),
                TimeUnit.SECONDS,
              )
            } catch (e: InterruptedException) {
              currentThread().interrupt()
            }
            server.shutdownNow()
          }
        }
      )

    return server.start()
  }
}
