// Copyright 2020 The Cross-Media Measurement Authors
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

/**
 * Wraps a connection to a Spanner database for convenient access to an [AsyncDatabaseClient],
 * the [DatabaseId], waiting for the connection to be ready, etc.
 */
class SpannerDatabaseConnector(
  private val instanceName: String,
  private val projectName: String,
  private val readyTimeout: Duration,
  databaseName: String,
  emulatorHost: String?
) : AutoCloseable {

  private val spanner: Spanner = buildSpanner(projectName, emulatorHost)

  val databaseId: DatabaseId =
    DatabaseId.of(projectName, instanceName, databaseName)

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

  /**
   * Executes [block] with this [SpannerDatabaseConnector] once it's ready, ensuring that this is
   * closed when done.
   */
  suspend fun <R> usingSpanner(block: suspend (spanner: SpannerDatabaseConnector) -> R): R {
    use { spanner ->
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

  companion object {
    private val logger = Logger.getLogger(this::class.java.name)
  }
}
