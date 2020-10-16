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

package org.wfanet.measurement.gcloud.spanner.testing

import java.net.ServerSocket
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import java.time.Duration
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import kotlinx.coroutines.withTimeout
import org.wfanet.measurement.common.getRuntimePath

private const val EMULATOR_HOSTNAME = "localhost"
private const val INVALID_HOST_MESSAGE =
  "emulator host must be of the form $EMULATOR_HOSTNAME:<port>"

/**
 * Wrapper for Cloud Spanner Emulator binary.
 *
 * @param port TCP port that the emulator should listen on, or 0 to allocate a
 *     port automatically
 */
class SpannerEmulator(private val port: Int = 0) : AutoCloseable {
  private lateinit var emulator: Process
  private lateinit var emulatorHost: String

  /**
   * Starts the emulator process.
   */
  fun start() {
    check(!this::emulator.isInitialized)

    // Open a socket on `port`. This should reduce the likelihood that the port
    // is in use. Additionally, this will allocate a port if `port` is 0.
    val localPort = ServerSocket(port).use {
      it.localPort
    }

    emulatorHost = "$EMULATOR_HOSTNAME:$localPort"
    emulator =
      ProcessBuilder(emulatorPath.toString(), "--host_port=$emulatorHost")
        .redirectError(ProcessBuilder.Redirect.INHERIT)
        .start()
  }

  /**
   * Suspends until the emulator is ready.
   *
   * @param timeout Timeout for how long to wait before throwing a
   *     [kotlinx.coroutines.TimeoutCancellationException].
   * @return the emulator host, which can be passed to
   *    [com.google.cloud.spanner.SpannerOptions.Builder.setEmulatorHost].
   */
  suspend fun waitUntilReady(timeout: Duration = Duration.ofSeconds(10)): String {
    withTimeout(timeout.toMillis()) {
      emulatorReady()
    }
    return emulatorHost
  }

  /**
   * Returns when emulator is ready.
   */
  private suspend fun emulatorReady() {
    /** Suffix of line of emulator output that will tell us that it's ready. */
    val readyLineSuffix = "Server address: $emulatorHost"

    emulator.inputStream.use { input ->
      input.bufferedReader().use { reader ->
        withContext(Dispatchers.IO) {
          do {
            check(emulator.isAlive) { "Emulator stopped unexpectedly" }
            val line = reader.readLine()
          } while (!line.endsWith(readyLineSuffix))
        }
      }
    }
  }

  override fun close() {
    if (this::emulator.isInitialized) {
      emulator.destroy()
    }
  }

  companion object {
    private val emulatorPath: Path
    init {
      val runfilesRelativePath = Paths.get("cloud_spanner_emulator", "emulator")
      val runtimePath = getRuntimePath(runfilesRelativePath)
      check(runtimePath != null && Files.exists(runtimePath)) {
        "$runfilesRelativePath not found in runfiles"
      }
      check(Files.isExecutable(runtimePath)) { "$runtimePath is not executable" }

      emulatorPath = runtimePath
    }

    fun withHost(emulatorHost: String): SpannerEmulator {
      val lazyMessage: () -> String = { INVALID_HOST_MESSAGE }

      val parts = emulatorHost.split(':', limit = 2)
      require(parts.size == 2 && parts[0] == EMULATOR_HOSTNAME, lazyMessage)
      val port = requireNotNull(parts[1].toIntOrNull(), lazyMessage)

      return SpannerEmulator(port)
    }
  }
}
