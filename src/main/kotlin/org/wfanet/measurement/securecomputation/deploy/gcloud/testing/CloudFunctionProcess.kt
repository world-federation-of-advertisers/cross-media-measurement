/*
 * Copyright 2025 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.securecomputation.deploy.gcloud.testing

import java.net.ServerSocket
import java.nio.file.Files
import java.nio.file.Path
import java.util.concurrent.TimeUnit
import java.util.logging.Level
import java.util.logging.Logger
import kotlin.coroutines.CoroutineContext
import kotlin.properties.Delegates
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds
import kotlin.time.TimeSource
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.coroutines.withContext
import kotlinx.coroutines.yield
import org.jetbrains.annotations.BlockingExecutor
import org.wfanet.measurement.common.getRuntimePath

/** Wrapper for the DataWatcher java_binary process. */
class CloudFunctionProcess(
  private val coroutineContext: @BlockingExecutor CoroutineContext = Dispatchers.IO,
  private val functionBinaryPath: Path,
  private val gcfTarget: String,
  private val logger: Logger,
) : AutoCloseable {
  private val startMutex = Mutex()
  @Volatile private lateinit var process: Process
  private var localPort by Delegates.notNull<Int>()
  /** Indicates whether the process has started. */
  val started: Boolean
    get() = this::process.isInitialized
  /** Returns the port the process is listening on. */
  val port: Int
    get() {
      check(started) { "DataWatcher process not started" }
      return localPort
    }

  /**
   * Starts the DataWatcher process if it has not already been started.
   *
   * This suspends until the process is ready.
   *
   * @param env Environment variables to pass to the process
   * @return The HTTP port the process is listening on
   */
  suspend fun start(env: Map<String, String>): Int {
    if (started) {
      return port
    }
    return startMutex.withLock {
      /** Double-checked locking. */
      if (started) {
        return@withLock port
      }
      return withContext(coroutineContext) {

        // Open a socket on `port`. This should reduce the likelihood that the port
        // is in use. Additionally, this will allocate a port if `port` is 0.
        localPort = ServerSocket(0).use { it.localPort }
        val runtimePath = getRuntimePath(functionBinaryPath)
        check(runtimePath != null && Files.exists(runtimePath)) {
          "$functionBinaryPath not found in runfiles"
        }

        val processBuilder =
          ProcessBuilder(
              "java",
              "-jar",
              runtimePath.toString(),
              /** Add HTTP port configuration */
              /** Add HTTP port configuration */
              "--port",
              localPort.toString(),
              "--target",
              gcfTarget,
            )
            .redirectErrorStream(true)
            .redirectOutput(ProcessBuilder.Redirect.PIPE)

        // Set environment variables
        processBuilder.environment().putAll(env)
        // Start the process
        process = processBuilder.start()
        val reader = process.inputStream.bufferedReader()
        val readyPattern = "Serving function..."
        var isReady = false

        // Start a thread to read output
        Thread {
            try {
              while (true) {
                val line = reader.readLine() ?: break
                logger.info("Process output: $line")
                /** Check if the ready message is in the output */
                /** Check if the ready message is in the output */
                if (line.contains(readyPattern)) {
                  isReady = true
                }
              }
            } catch (e: Exception) {
              if (process.isAlive) {
                logger.log(Level.WARNING, "Error reading process output: ${e.message}")
              }
            }
          }
          .start()

        // Wait for the ready message or timeout
        val timeout: Duration = 10.seconds
        val startTime = TimeSource.Monotonic.markNow()
        while (!isReady) {
          yield()
          check(process.isAlive) { "Google Cloud Function stopped unexpectedly" }
          if (startTime.elapsedNow() >= timeout) {
            throw IllegalStateException("Timeout waiting for Google Cloud Function to start")
          }
        }
        localPort
      }
    }
  }

  /** Closes the process if it has been started. */
  override fun close() {
    if (started) {
      process.destroy()
      try {
        if (!process.waitFor(5, TimeUnit.SECONDS)) {
          process.destroyForcibly()
        }
      } catch (e: InterruptedException) {
        process.destroyForcibly()
        Thread.currentThread().interrupt()
      }
    }
  }
}
