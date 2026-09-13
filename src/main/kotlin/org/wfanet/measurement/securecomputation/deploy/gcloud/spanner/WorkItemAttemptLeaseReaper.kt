/*
 * Copyright 2026 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.securecomputation.deploy.gcloud.spanner

import java.time.Clock
import java.time.Duration
import java.util.logging.Level
import java.util.logging.Logger
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.isActive
import org.wfanet.measurement.gcloud.spanner.AsyncDatabaseClient
import org.wfanet.measurement.securecomputation.deploy.gcloud.spanner.db.readExpiredWorkItemAttempts
import org.wfanet.measurement.securecomputation.deploy.gcloud.spanner.db.recoverExpiredWorkItemAttempt

/** Recovers WorkItems whose active worker has stopped renewing its attempt lease. */
class WorkItemAttemptLeaseReaper(
  private val databaseClient: AsyncDatabaseClient,
  private val clock: Clock = Clock.systemUTC(),
  private val pollInterval: Duration = DEFAULT_POLL_INTERVAL,
) {
  init {
    require(pollInterval > Duration.ZERO) { "pollInterval must be positive" }
  }

  /** Recovers up to [limit] expired attempts and returns the number recovered. */
  suspend fun recoverExpiredAttempts(limit: Int = DEFAULT_BATCH_SIZE): Int {
    require(limit > 0) { "limit must be positive" }
    val now = clock.instant()
    val expiredAttempts =
      databaseClient.singleUse().use { transaction ->
        transaction.readExpiredWorkItemAttempts(now, limit).toList()
      }
    var recoveredCount = 0
    for (attempt in expiredAttempts) {
      try {
        val recovered =
          databaseClient.readWriteTransaction().run { transaction ->
            transaction.recoverExpiredWorkItemAttempt(attempt, now)
          }
        if (recovered) {
          recoveredCount++
          logger.warning(
            "Recovered expired WorkItemAttempt ${attempt.workItemId}/${attempt.workItemAttemptId}"
          )
        }
      } catch (e: CancellationException) {
        throw e
      } catch (e: Exception) {
        logger.log(Level.WARNING, "Unable to recover an expired WorkItemAttempt", e)
      }
    }
    return recoveredCount
  }

  /** Continuously recovers expired attempts until cancelled. */
  suspend fun run() {
    while (currentCoroutineContext().isActive) {
      try {
        recoverExpiredAttempts()
      } catch (e: CancellationException) {
        throw e
      } catch (e: Exception) {
        logger.log(Level.WARNING, "Unable to process expired WorkItemAttempts", e)
      }
      delay(pollInterval.toMillis())
    }
  }

  companion object {
    val DEFAULT_POLL_INTERVAL: Duration = Duration.ofSeconds(30)
    private const val DEFAULT_BATCH_SIZE = 100
    private val logger: Logger = Logger.getLogger(WorkItemAttemptLeaseReaper::class.java.name)
  }
}
