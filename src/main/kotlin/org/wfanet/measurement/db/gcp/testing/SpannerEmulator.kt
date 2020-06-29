package org.wfanet.measurement.db.gcp.testing

import com.google.devtools.build.runfiles.Runfiles
import java.io.File
import java.lang.AutoCloseable
import java.lang.Process
import java.net.ServerSocket
import java.nio.file.Paths
import java.time.Duration
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withContext
import kotlinx.coroutines.withTimeout

/** Relative runfiles path to Cloud Spanner Emulator binary. */
private const val EMULATOR_RUNFILES_PATH = "wfa_measurement_system/src/main/kotlin/org/wfanet/" +
  "measurement/db/gcp/testing/cloud_spanner_emulator"

private const val EMULATOR_HOSTNAME = "localhost"

/**
 * Wrapper for Cloud Spanner Emulator binary.
 */
class SpannerEmulator : AutoCloseable {
  private val emulatorFile: File
  private lateinit var emulator: Process
  private lateinit var emulatorHost: String

  init {
    val runfiles = Runfiles.create()
    emulatorFile = Paths.get(runfiles.rlocation(EMULATOR_RUNFILES_PATH)).toFile()
    check(emulatorFile.exists()) { "$EMULATOR_RUNFILES_PATH not found in runfiles" }
    check(emulatorFile.canExecute()) { "$emulatorFile is not executable" }
  }

  /**
   * Starts the emulator process.
   */
  fun start() {
    check(!this::emulator.isInitialized)

    // There's a potential race condition between finding an unused port and the emulator binding
    // it, so we don't want to find the port until we're about to start the emulator process.
    val port = findUnusedPort()
    emulatorHost = "$EMULATOR_HOSTNAME:$port"
    emulator =
      ProcessBuilder(emulatorFile.canonicalPath, "--host_port=$emulatorHost")
        .redirectError(ProcessBuilder.Redirect.INHERIT)
        .start()
  }

  /**
   * Blocks until the emulator is ready.
   *
   * @param timeout Timeout for how long to wait before throwing a
   *     [kotlinx.coroutines.TimeoutCancellationException].
   * @return the emulator host, which can be passed to
   *    [com.google.cloud.spanner.SpannerOptions.Builder.setEmulatorHost].
   */
  fun blockUntilReady(timeout: Duration = Duration.ofSeconds(10)): String {
    runBlocking {
      withTimeout(timeout.toMillis()) {
        emulatorReady()
      }
    }
    return emulatorHost
  }

  /**
   * Finds an unused port by attempting to bind it.
   */
  private fun findUnusedPort(): Int {
    ServerSocket(0).use { socket ->
      return socket.localPort
    }
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
}
