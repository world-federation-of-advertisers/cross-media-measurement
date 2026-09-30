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

package org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner

import com.google.common.base.Optional
import java.time.Clock
import java.time.Duration
import java.util.UUID
import java.util.logging.Level
import java.util.logging.Logger
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import org.wfanet.measurement.common.telemetry.XmmTraceAttributes
import org.wfanet.measurement.edpaggregator.dataavailability.DataAvailabilitySyncTaskPublisher
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.db.DataAvailabilitySyncTaskPublication
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.db.claimDataAvailabilitySyncTaskPublication
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.db.completeDataAvailabilitySyncTaskPublication
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.db.reconcileDataAvailabilitySyncTaskPublications
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.db.retryDataAvailabilitySyncTaskPublication
import org.wfanet.measurement.edpaggregator.telemetry.VidLabelingTraceAttributes
import org.wfanet.measurement.edpaggregator.telemetry.VidLabelingTraceLogging
import org.wfanet.measurement.gcloud.spanner.AsyncDatabaseClient

/** Publishes pending availability tasks from the Spanner transactional outbox. */
class DataAvailabilitySyncTaskPublicationRunner(
  private val databaseClient: AsyncDatabaseClient,
  private val publisher: DataAvailabilitySyncTaskPublisher,
  private val clock: Clock = Clock.systemUTC(),
  private val pollInterval: Duration = DEFAULT_POLL_INTERVAL,
  private val leaseDuration: Duration = DEFAULT_LEASE_DURATION,
  private val initialRetryDelay: Duration = DEFAULT_INITIAL_RETRY_DELAY,
  private val maxRetryDelay: Duration = DEFAULT_MAX_RETRY_DELAY,
  private val staleTaskDuration: Duration = DEFAULT_STALE_TASK_DURATION,
) {
  init {
    require(pollInterval > Duration.ZERO)
    require(leaseDuration > Duration.ZERO)
    require(initialRetryDelay > Duration.ZERO)
    require(maxRetryDelay >= initialRetryDelay)
    require(staleTaskDuration > Duration.ZERO)
  }

  suspend fun publishPendingTasks(limit: Int = DEFAULT_BATCH_SIZE): Int {
    require(limit > 0)
    reconcile(limit)
    var publishedCount = 0
    repeat(limit) {
      val publication = claim() ?: return publishedCount
      if (publish(publication)) publishedCount++
    }
    return publishedCount
  }

  suspend fun run() {
    while (currentCoroutineContext().isActive) {
      try {
        publishPendingTasks()
      } catch (e: CancellationException) {
        throw e
      } catch (e: Exception) {
        logger.log(Level.WARNING, "Unable to publish availability tasks", e)
      }
      delay(pollInterval.toMillis())
    }
  }

  private suspend fun reconcile(limit: Int) {
    try {
      databaseClient.readWriteTransaction().run { transaction ->
        val now = clock.instant()
        transaction.reconcileDataAvailabilitySyncTaskPublications(
          limit,
          now,
          now.minus(staleTaskDuration),
        )
      }
    } catch (e: CancellationException) {
      throw e
    } catch (e: Exception) {
      logger.log(Level.WARNING, "Unable to reconcile availability task publications", e)
    }
  }

  private suspend fun claim(): DataAvailabilitySyncTaskPublication? {
    val now = clock.instant()
    val publication: Optional<DataAvailabilitySyncTaskPublication> =
      databaseClient.readWriteTransaction().run { transaction ->
        Optional.fromNullable(
          transaction.claimDataAvailabilitySyncTaskPublication(
            UUID.randomUUID().toString(),
            now,
            now.plus(leaseDuration),
          )
        )
      }
    return publication.orNull()
  }

  private suspend fun publish(publication: DataAvailabilitySyncTaskPublication): Boolean {
    log(publication.taskName, "started")
    try {
      publisher.publish(publication.taskName)
    } catch (e: CancellationException) {
      throw e
    } catch (e: Exception) {
      log(publication.taskName, "retryable_failure", e)
      databaseClient.readWriteTransaction().run { transaction ->
        transaction.retryDataAvailabilitySyncTaskPublication(
          publication,
          clock.instant().plus(retryDelay(publication.attemptCount)),
        )
      }
      return false
    }
    databaseClient.readWriteTransaction().run { transaction ->
      transaction.completeDataAvailabilitySyncTaskPublication(publication)
    }
    log(publication.taskName, "succeeded")
    return true
  }

  private fun retryDelay(attemptCount: Long): Duration {
    val exponent = (attemptCount - 1L).coerceIn(0L, MAX_RETRY_EXPONENT.toLong()).toInt()
    return minOf(initialRetryDelay.multipliedBy(1L shl exponent), maxRetryDelay)
  }

  private fun log(taskName: String, outcome: String, error: Throwable? = null) {
    VidLabelingTraceLogging.log(
      logger,
      if (error == null) Level.INFO else Level.WARNING,
      "edpa.data_availability_sync_task.publication",
      VidLabelingTraceAttributes.DATA_AVAILABILITY_SYNC_TASK_NAME_STRING to taskName,
      XmmTraceAttributes.LIFECYCLE_STAGE_STRING to "availability_task_publication",
      XmmTraceAttributes.OUTCOME_STRING to outcome,
      XmmTraceAttributes.ERROR_TYPE_STRING to error?.let(XmmTraceAttributes::errorType),
      XmmTraceAttributes.ERROR_CODE_STRING to error?.let(XmmTraceAttributes::errorCode),
    )
  }

  companion object {
    val DEFAULT_POLL_INTERVAL: Duration = Duration.ofSeconds(1)
    val DEFAULT_LEASE_DURATION: Duration = Duration.ofMinutes(1)
    private val DEFAULT_INITIAL_RETRY_DELAY: Duration = Duration.ofSeconds(1)
    private val DEFAULT_MAX_RETRY_DELAY: Duration = Duration.ofMinutes(1)
    private val DEFAULT_STALE_TASK_DURATION: Duration = Duration.ofHours(1)
    private const val DEFAULT_BATCH_SIZE = 100
    private const val MAX_RETRY_EXPONENT = 16
    private val logger =
      Logger.getLogger(DataAvailabilitySyncTaskPublicationRunner::class.java.name)
  }
}
