/*
 * Copyright 2022 The Cross-Media Measurement Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.wfanet.measurement.common.k8s.testing

import io.kubernetes.client.PortForward
import io.kubernetes.client.openapi.ApiClient
import io.kubernetes.client.openapi.Configuration
import io.kubernetes.client.openapi.models.V1Pod
import java.io.IOException
import java.net.ServerSocket
import java.util.logging.Logger
import kotlinx.coroutines.CoroutineName
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.cancel
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.ensureActive
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import org.jetbrains.annotations.Blocking

/**
 * Forwarder for local ports to ports on a pod.
 *
 * Note that this only supports a single connection per port, after which the remote streams are
 * closed.
 */
class PortForwarder(
  private val pod: V1Pod,
  private val port: Int,
  private val apiClient: ApiClient = Configuration.getDefaultApiClient()
) : AutoCloseable {
  private val scope: CoroutineScope =
    CoroutineScope(Dispatchers.IO + CoroutineName("$podName forwarding"))

  private val podName: String
    get() = pod.metadata?.name.orEmpty()

  private lateinit var job: Job
  private val started
    get() = this::job.isInitialized

  private lateinit var serverSocket: ServerSocket

  @Synchronized
  fun start(): Int {
    check(!started) { "Already started" }
    serverSocket = ServerSocket(0)

    @Suppress("BlockingMethodInNonBlockingContext") // Blocking dispatcher.
    job =
      scope.launch {
        try {
          coroutineScope {
            logger.info { "Listening for connections for $podName:$port" }
            val socket = serverSocket.accept()
            logger.info { "Receiving connection for $podName:$port" }
            ensureActive()
            val forwarding: PortForward.PortForwardResult =
              PortForward(apiClient).forward(pod, listOf(port))
            ensureActive()
            launch {
              try {
                forwarding.getInputStream(port).use { input ->
                  socket.getOutputStream().use { output -> input.copyTo(output) }
                }
              } catch (e: IOException) {
                if (coroutineContext.isActive) throw e
              }
            }
            launch {
              try {
                socket.getInputStream().use { input ->
                  forwarding.getOutboundStream(port).use { output -> input.copyTo(output) }
                }
              } catch (e: IOException) {
                if (coroutineContext.isActive) throw e
              }
            }
          }
        } finally {
          logger.info { "Disconnected from $podName:$port" }
          serverSocket.close()
        }
      }

    return serverSocket.localPort
  }

  fun stop() {
    scope.cancel("Stopping port forwarding")
    if (this::serverSocket.isInitialized) {
      serverSocket.close()
    }
  }

  @Blocking
  override fun close() {
    synchronized(this) { if (!started) return }

    stop()
    runBlocking { job.join() }
  }

  companion object {
    private val logger: Logger = Logger.getLogger(this::class.java.name)
  }
}
