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

import com.google.common.base.Optional
import java.time.Clock
import java.time.Duration
import java.util.UUID
import java.util.logging.Level
import java.util.logging.Logger
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.awaitCancellation
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import org.wfanet.measurement.common.telemetry.ReportTraceAttributes
import org.wfanet.measurement.common.telemetry.ReportTraceLogging
import org.wfanet.measurement.gcloud.spanner.AsyncDatabaseClient
import org.wfanet.measurement.securecomputation.deploy.gcloud.spanner.db.WorkItemPublicationClaimResult
import org.wfanet.measurement.securecomputation.deploy.gcloud.spanner.db.WorkItemPublicationResult
import org.wfanet.measurement.securecomputation.deploy.gcloud.spanner.db.claimWorkItemPublication
import org.wfanet.measurement.securecomputation.deploy.gcloud.spanner.db.completeWorkItemPublication
import org.wfanet.measurement.securecomputation.deploy.gcloud.spanner.db.reconcileWorkItemPublications
import org.wfanet.measurement.securecomputation.deploy.gcloud.spanner.db.retryWorkItemPublication
import org.wfanet.measurement.securecomputation.service.WorkItemKey
import org.wfanet.measurement.securecomputation.service.internal.QueueMapping
import org.wfanet.measurement.securecomputation.service.internal.WorkItemPublisher

/** Publishes WorkItems recorded in the Spanner transactional outbox. */
class WorkItemPublicationRunner(
  private val databaseClient: AsyncDatabaseClient,
  private val queueMapping: QueueMapping,
  private val workItemPublisher: WorkItemPublisher,
  private val clock: Clock = Clock.systemUTC(),
  private val pollInterval: Duration = DEFAULT_POLL_INTERVAL,
  private val leaseDuration: Duration = DEFAULT_LEASE_DURATION,
  private val initialRetryDelay: Duration = DEFAULT_INITIAL_RETRY_DELAY,
  private val maxRetryDelay: Duration = DEFAULT_MAX_RETRY_DELAY,
  private val publicationEnabled: Boolean = true,
) {
  init {
    require(pollInterval > Duration.ZERO) { "pollInterval must be positive" }
    require(leaseDuration > Duration.ZERO) { "leaseDuration must be positive" }
    require(initialRetryDelay > Duration.ZERO) { "initialRetryDelay must be positive" }
    require(maxRetryDelay >= initialRetryDelay) {
      "maxRetryDelay must not be less than initialRetryDelay"
    }
  }

  /** Attempts to publish the pending outbox record for [workItemId]. */
  suspend fun publishWorkItem(workItemId: Long): Boolean {
    if (!publicationEnabled) {
      return false
    }
    return try {
      when (val claim = claimWorkItemPublication(workItemId)) {
        is WorkItemPublicationClaimResult.Claimed -> publishClaimedWorkItem(claim.publication)
        is WorkItemPublicationClaimResult.Skipped -> {
          logSkippedPublication(claim)
          false
        }
        null -> false
      }
    } catch (e: CancellationException) {
      throw e
    } catch (e: Exception) {
      logger.log(Level.WARNING, "Unable to publish WorkItem $workItemId", e)
      false
    }
  }

  /** Publishes up to [limit] pending outbox records. */
  suspend fun publishPendingWorkItems(limit: Int = DEFAULT_BATCH_SIZE): Int {
    require(limit > 0) { "limit must be positive" }
    if (!publicationEnabled) {
      return 0
    }
    reconcileLegacyWorkItems(limit)
    var publishedCount = 0
    repeat(limit) {
      val claim =
        try {
          claimWorkItemPublication()
        } catch (e: CancellationException) {
          throw e
        } catch (e: Exception) {
          logger.log(Level.WARNING, "Unable to claim a pending WorkItem publication", e)
          return publishedCount
        } ?: return publishedCount

      if (claim is WorkItemPublicationClaimResult.Skipped) {
        logSkippedPublication(claim)
        return@repeat
      }
      check(claim is WorkItemPublicationClaimResult.Claimed)

      val published =
        try {
          publishClaimedWorkItem(claim.publication)
        } catch (e: CancellationException) {
          throw e
        } catch (e: Exception) {
          logger.log(Level.WARNING, "Unable to update WorkItem publication bookkeeping", e)
          false
        }
      if (published) {
        publishedCount++
      }
    }
    return publishedCount
  }

  /** Continuously publishes pending outbox records until the coroutine is cancelled. */
  suspend fun run() {
    if (!publicationEnabled) {
      logger.info("WorkItem publication and reconciliation are disabled")
      awaitCancellation()
    }
    while (currentCoroutineContext().isActive) {
      try {
        publishPendingWorkItems()
      } catch (e: CancellationException) {
        throw e
      } catch (e: Exception) {
        logger.log(Level.WARNING, "Unable to process the current WorkItem publication batch", e)
      }
      delay(pollInterval.toMillis())
    }
  }

  private suspend fun reconcileLegacyWorkItems(limit: Int) {
    try {
      databaseClient.readWriteTransaction().run { transaction ->
        transaction.reconcileWorkItemPublications(limit)
      }
    } catch (e: CancellationException) {
      throw e
    } catch (e: Exception) {
      logger.log(Level.WARNING, "Unable to reconcile legacy WorkItem publications", e)
      return
    }
  }

  private suspend fun claimWorkItemPublication(
    workItemId: Long? = null
  ): WorkItemPublicationClaimResult? {
    val now = clock.instant()
    val leaseToken = UUID.randomUUID().toString()
    val publication: Optional<WorkItemPublicationClaimResult> =
      databaseClient.readWriteTransaction().run { transaction ->
        Optional.fromNullable(
          transaction.claimWorkItemPublication(
            queueMapping = queueMapping,
            leaseToken = leaseToken,
            now = now,
            leaseExpirationTime = now.plus(leaseDuration),
            workItemId = workItemId,
          )
        )
      }
    return publication.orNull()
  }

  private fun logSkippedPublication(claim: WorkItemPublicationClaimResult.Skipped) {
    when (claim.reason) {
      WorkItemPublicationClaimResult.Skipped.Reason.QUEUE_NOT_FOUND -> {
        logReportTraceLifecycle(
          claim.workItemResourceId,
          outcome = "failed",
          errorType = "QueueNotFound",
        )
        logger.warning(
          "Deferring WorkItem ${claim.workItemResourceId}: queue ID ${claim.queueId} is not in " +
            "the configured queue mapping"
        )
      }
      WorkItemPublicationClaimResult.Skipped.Reason.LEGACY_DEAD_LETTER_TERMINALIZED -> {
        logReportTraceLifecycle(
          claim.workItemResourceId,
          outcome = "failed",
          errorType = "LegacyDeadLetterTerminalized",
        )
        logger.warning(
          "Terminalized WorkItem ${claim.workItemResourceId} from a legacy application-managed " +
            "dead-letter publication"
        )
      }
      WorkItemPublicationClaimResult.Skipped.Reason.WORK_ITEM_STATE_MISMATCH -> Unit
    }
  }

  private suspend fun publishClaimedWorkItem(publication: WorkItemPublicationResult): Boolean {
    logReportTraceLifecycle(
      publication.workItem.workItemResourceId,
      outcome = "started",
      errorType = null,
    )
    try {
      workItemPublisher.publishMessage(publication.queueResourceId, publication.workItem)
    } catch (e: CancellationException) {
      throw e
    } catch (e: Exception) {
      logReportTraceLifecycle(
        publication.workItem.workItemResourceId,
        outcome = "retryable_failure",
        error = e,
      )
      scheduleRetry(publication)
      return false
    }

    logReportTraceLifecycle(
      publication.workItem.workItemResourceId,
      outcome = "succeeded",
      errorType = null,
    )
    databaseClient.readWriteTransaction().run { transaction ->
      transaction.completeWorkItemPublication(publication.workItemId, publication.leaseToken)
    }
    return true
  }

  private fun logReportTraceLifecycle(
    workItemResourceId: String,
    outcome: String,
    errorType: String?,
  ) {
    ReportTraceLogging.log(
      logger,
      "secure_computation.work_item.publication",
      ReportTraceAttributes.WORK_ITEM_NAME_STRING to WorkItemKey(workItemResourceId).toName(),
      ReportTraceAttributes.LIFECYCLE_STAGE_STRING to "work_item_publication",
      ReportTraceAttributes.OUTCOME_STRING to outcome,
      ReportTraceAttributes.ERROR_TYPE_STRING to errorType,
    )
  }

  private fun logReportTraceLifecycle(
    workItemResourceId: String,
    outcome: String,
    error: Throwable,
  ) {
    ReportTraceLogging.log(
      logger,
      Level.WARNING,
      error,
      "secure_computation.work_item.publication",
      ReportTraceAttributes.WORK_ITEM_NAME_STRING to WorkItemKey(workItemResourceId).toName(),
      ReportTraceAttributes.LIFECYCLE_STAGE_STRING to "work_item_publication",
      ReportTraceAttributes.OUTCOME_STRING to outcome,
      ReportTraceAttributes.ERROR_TYPE_STRING to ReportTraceAttributes.errorType(error),
      ReportTraceAttributes.ERROR_CODE_STRING to ReportTraceAttributes.errorCode(error),
    )
  }

  private suspend fun scheduleRetry(publication: WorkItemPublicationResult) {
    val retryDelay = retryDelay(publication.attemptCount)
    databaseClient.readWriteTransaction().run { transaction ->
      transaction.retryWorkItemPublication(
        publication.workItemId,
        publication.leaseToken,
        clock.instant().plus(retryDelay),
      )
    }
  }

  private fun retryDelay(attemptCount: Long): Duration {
    val exponent = (attemptCount - 1L).coerceIn(0L, MAX_RETRY_EXPONENT.toLong()).toInt()
    val delay = initialRetryDelay.multipliedBy(1L shl exponent)
    return minOf(delay, maxRetryDelay)
  }

  companion object {
    val DEFAULT_POLL_INTERVAL: Duration = Duration.ofSeconds(1)
    val DEFAULT_LEASE_DURATION: Duration = Duration.ofMinutes(1)
    private val DEFAULT_INITIAL_RETRY_DELAY: Duration = Duration.ofSeconds(1)
    private val DEFAULT_MAX_RETRY_DELAY: Duration = Duration.ofMinutes(1)
    private const val DEFAULT_BATCH_SIZE = 100
    private const val MAX_RETRY_EXPONENT = 16
    private val logger: Logger = Logger.getLogger(WorkItemPublicationRunner::class.java.name)
  }
}
