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
import java.net.InetAddress
import java.net.InetSocketAddress
import java.nio.ByteBuffer
import java.nio.channels.AsynchronousServerSocketChannel
import java.nio.channels.AsynchronousSocketChannel
import java.nio.channels.Channels
import java.nio.channels.ReadableByteChannel
import java.nio.channels.WritableByteChannel
import java.util.logging.Logger
import kotlin.coroutines.CoroutineContext
import kotlinx.coroutines.CoroutineName
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.cancel
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.ensureActive
import kotlinx.coroutines.isActive
import kotlinx.coroutines.job
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.runInterruptible
import kotlinx.coroutines.supervisorScope
import kotlinx.coroutines.suspendCancellableCoroutine
import org.jetbrains.annotations.Blocking
import org.jetbrains.annotations.BlockingExecutor
import org.wfanet.measurement.common.ContinuationCompletionHandler
import org.wfanet.measurement.common.SuspendingByteChannel

/** Forwarder from a local port to [port] on [pod]. */
class PortForwarder(
  private val pod: V1Pod,
  private val port: Int,
  private val apiClient: ApiClient = Configuration.getDefaultApiClient(),
  private val localAddress: InetAddress = InetAddress.getLoopbackAddress(),
  coroutineContext: @BlockingExecutor CoroutineContext = Dispatchers.IO
) : AutoCloseable {
  private val scope: CoroutineScope =
    CoroutineScope(coroutineContext + CoroutineName("$podName forwarding"))

  private val podName: String
    get() = pod.metadata?.name.orEmpty()

  private lateinit var serverSocketChannel: AsynchronousServerSocketChannel
  private val started
    get() = this::serverSocketChannel.isInitialized

  /**
   * Opens a local socket and starts listening for connections to forward.
   *
   * @return the local socket address
   */
  @Synchronized
  @Blocking
  fun start(): InetSocketAddress {
    check(!started) { "Already started" }
    serverSocketChannel =
      AsynchronousServerSocketChannel.open().bind(InetSocketAddress(localAddress, 0))
    val boundAddress = serverSocketChannel.localAddress as InetSocketAddress

    @Suppress("BlockingMethodInNonBlockingContext") // Blocking dispatcher.
    scope.launch {
      while (coroutineContext.isActive) {
        supervisorScope {
          launch {
            logger.info { "Listening for connection on $boundAddress for $podName:$port" }
            val socketChannel: SuspendingByteChannel = listenForConnection()

            logger.info { "Handling connection for $podName:$port" }
            try {
              handleConnection(socketChannel)
            } catch (e: IOException) {
              if (coroutineContext.isActive) throw e
            } finally {
              logger.info { "Disconnected from $podName:$port" }
              socketChannel.close()
            }
          }
        }
      }
    }

    return boundAddress
  }

  private suspend fun listenForConnection(): SuspendingByteChannel {
    val socketChannel =
      suspendCancellableCoroutine<AsynchronousSocketChannel> { continuation ->
        serverSocketChannel.accept(continuation, socketCompletionHandler)
      }
    return SuspendingByteChannel(socketChannel)
  }

  @Throws(IOException::class)
  @Blocking
  private suspend fun handleConnection(socketChannel: SuspendingByteChannel) {
    coroutineScope {
      val forwarding: PortForward.PortForwardResult =
        PortForward(apiClient).forward(pod, listOf(port))
      ensureActive()
      launch {
        Channels.newChannel(forwarding.getInputStream(port)).use { input ->
          input.copyTo(socketChannel)
        }
      }
      launch {
        Channels.newChannel(forwarding.getOutboundStream(port)).use { output ->
          socketChannel.copyTo(output)
        }
      }
    }
  }

  @Blocking
  fun stop() {
    scope.cancel("Stopping port forwarding")
    serverSocketChannel.close()
  }

  @Blocking
  override fun close() {
    synchronized(this) { if (!started) return }

    stop()
    runBlocking { scope.coroutineContext.job.join() }
  }

  companion object {
    private val logger: Logger = Logger.getLogger(this::class.java.name)

    private val socketCompletionHandler = ContinuationCompletionHandler<AsynchronousSocketChannel>()
  }
}

@Blocking
private suspend fun ReadableByteChannel.copyTo(destination: SuspendingByteChannel) {
  val buffer = ByteBuffer.allocate(DEFAULT_BUFFER_SIZE)
  while (runInterruptible { read(buffer) } >= 0) {
    buffer.flip()
    while (buffer.hasRemaining()) {
      destination.write(buffer)
    }
    buffer.clear()
  }
}

@Blocking
private suspend fun SuspendingByteChannel.copyTo(destination: WritableByteChannel) {
  val buffer = ByteBuffer.allocate(DEFAULT_BUFFER_SIZE)
  while (read(buffer) >= 0) {
    buffer.flip()
    while (buffer.hasRemaining()) {
      runInterruptible { destination.write(buffer) }
    }
    buffer.clear()
  }
}
