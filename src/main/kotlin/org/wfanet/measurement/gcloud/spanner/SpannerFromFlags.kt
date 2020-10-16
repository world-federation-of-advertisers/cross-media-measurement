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

package org.wfanet.measurement.gcloud.spanner

import com.google.cloud.spanner.DatabaseClient
import com.google.cloud.spanner.DatabaseId
import com.google.cloud.spanner.Instance
import com.google.cloud.spanner.Spanner
import java.time.Duration
import java.util.logging.Logger
import kotlinx.coroutines.TimeoutCancellationException
import picocli.CommandLine

class SpannerFromFlags(
  flags: Flags,
  emulatorHost: String? = flags.spannerEmulatorHost
) : AutoCloseable {
  private val instanceName = flags.instanceName
  private val projectName = flags.projectName
  private val readyTimeout = flags.readyTimeout

  private val spanner: Spanner = buildSpanner(
    flags.projectName,
    emulatorHost
  )

  val databaseId: DatabaseId =
    DatabaseId.of(flags.projectName, flags.instanceName, flags.databaseName)

  private val internalDatabaseClient: DatabaseClient by lazy {
    spanner.getDatabaseClient(databaseId)
  }

  val databaseClient: AsyncDatabaseClient
    get() = internalDatabaseClient.asAsync()

  fun createInstance(
    configId: String,
    nodeCount: Int,
    displayName: String = instanceName
  ): Instance {
    return spanner.createInstance(
      projectName = projectName,
      instanceName = instanceName,
      displayName = displayName,
      instanceConfigId = configId,
      instanceNodeCount = nodeCount
    )
  }

  /**
   * Suspends until [databaseClient] is ready, throwing a
   * [kotlinx.coroutines.TimeoutCancellationException] if [readyTimeout] is
   * reached.
   */
  suspend fun waitUntilReady() {
    databaseClient.waitUntilReady(readyTimeout)
  }

  override fun close() {
    spanner.close()
  }

  class Flags {
    @CommandLine.Option(
      names = ["--spanner-project"],
      description = ["Name of the Spanner project."],
      required = true
    )
    lateinit var projectName: String
      private set

    @CommandLine.Option(
      names = ["--spanner-instance"],
      description = ["Name of the Spanner instance."],
      required = true
    )
    lateinit var instanceName: String
      private set

    @CommandLine.Option(
      names = ["--spanner-database"],
      description = ["Name of the Spanner database."],
      required = true
    )
    lateinit var databaseName: String
      private set

    @CommandLine.Option(
      names = ["--spanner-ready-timeout"],
      description = ["How long to wait for Spanner to be ready."],
      defaultValue = "10s"
    )
    lateinit var readyTimeout: Duration
      private set

    @CommandLine.Option(
      names = ["--spanner-emulator-host"],
      description = ["Host name and port of the spanner emulator."],
      required = false
    )
    var spannerEmulatorHost: String? = null
      private set

    /**
     * Executes [block] with a [SpannerFromFlags] resource once it's ready,
     * ensuring that the resource is closed.
     */
    suspend fun <R> usingSpanner(block: suspend (spanner: SpannerFromFlags) -> R): R {
      SpannerFromFlags(this).use { spanner ->
        try {
          spanner.waitUntilReady()
        } catch (e: TimeoutCancellationException) {
          // Closing Spanner can take a long time (e.g. 1 minute) and delay the
          // exception being surfaced, so we log here to give immediate feedback.
          logger.severe { "Timed out waiting for Spanner to be ready" }
          throw e
        }
        return block(spanner)
      }
    }
  }

  companion object {
    private val logger = Logger.getLogger(this::class.java.name)
  }
}
