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

package org.wfanet.measurement.edpaggregator.eventgroupactivities

import io.grpc.StatusException
import io.grpc.StatusRuntimeException
import io.opentelemetry.api.GlobalOpenTelemetry
import io.opentelemetry.api.common.AttributeKey
import io.opentelemetry.api.common.Attributes
import io.opentelemetry.api.trace.Tracer
import java.time.Instant
import java.time.LocalDate
import java.time.ZoneOffset
import java.util.logging.Level
import java.util.logging.Logger
import kotlin.time.TimeSource
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.toList
import org.wfanet.measurement.api.v2alpha.EventGroupActivitiesGrpcKt.EventGroupActivitiesCoroutineStub
import org.wfanet.measurement.api.v2alpha.EventGroupActivity
import org.wfanet.measurement.api.v2alpha.EventGroupActivityKey
import org.wfanet.measurement.api.v2alpha.EventGroupKey
import org.wfanet.measurement.api.v2alpha.batchDeleteEventGroupActivitiesRequest
import org.wfanet.measurement.api.v2alpha.batchUpdateEventGroupActivitiesRequest
import org.wfanet.measurement.api.v2alpha.eventGroupActivity
import org.wfanet.measurement.api.v2alpha.listEventGroupActivitiesRequest
import org.wfanet.measurement.api.v2alpha.updateEventGroupActivityRequest
import org.wfanet.measurement.common.ExponentialBackoff
import org.wfanet.measurement.common.Instrumentation
import org.wfanet.measurement.common.api.grpc.ResourceList
import org.wfanet.measurement.common.api.grpc.flattenConcat
import org.wfanet.measurement.common.api.grpc.listResources
import org.wfanet.measurement.common.throttler.Throttler
import org.wfanet.measurement.common.toLocalDate
import org.wfanet.measurement.common.toProtoDate
import org.wfanet.measurement.edpaggregator.telemetry.withSpan

/**
 * A single input record describing an activity for an EventGroup.
 *
 * @property parent the EventGroup resource name the activity belongs to. Format:
 *   `dataProviders/{data_provider}/eventGroups/{event_group}`.
 * @property eventGroupActivityDate the instant of the activity; converted to a [LocalDate] at UTC.
 */
data class SpotRecord(val parent: String, val eventGroupActivityDate: Instant)

/** Aggregate result of an [EventGroupActivitySync.sync] run. */
data class SyncResult(
  val totalInputRecords: Int,
  val eventGroupsProcessed: Int,
  val activitiesCreated: Int,
  val activitiesDeleted: Int,
  val activitiesUnchanged: Int,
  val errors: List<SyncError>,
)

/** A recoverable error encountered while syncing a single EventGroup. */
data class SyncError(val eventGroup: String, val message: String, val cause: Throwable? = null)

/**
 * Synchronizes `EventGroupActivity` resources for the EventGroups of a single DataProvider against
 * the CMMS Public API.
 *
 * For each EventGroup referenced by the input records this:
 * 1. Lists the existing activities in the Kingdom.
 * 2. Computes the set difference between the input dates and the existing dates.
 * 3. Creates activities present in the input but missing in the Kingdom.
 * 4. Deletes activities present in the Kingdom but missing from the input.
 *
 * Activities that already match are left unchanged. Failures for a single EventGroup are logged and
 * recorded as a [SyncError] but do not abort the overall sync.
 *
 * @property eventGroupActivitiesClient stub for the EventGroupActivities service.
 * @property throttler regulates the rate of gRPC calls.
 * @property dataProviderName resource name of the DataProvider, e.g. `dataProviders/CqJcvwaa5tI`.
 *   Every input record's [SpotRecord.parent] must be an EventGroup under this DataProvider.
 * @property listPageSize page size used when listing existing activities.
 * @property batchSize maximum number of activities per batch update/delete request.
 * @property maxDeleteFraction data-loss guard. If, for a single EventGroup, the fraction of
 *   existing activities that would be deleted exceeds this value, the deletions for that EventGroup
 *   are skipped (creates are still applied) and a [SyncError] is recorded. A value of `1.0`
 *   disables the guard. This protects against truncated or partial input files causing mass
 *   deletions.
 * @property maxAttempts maximum number of attempts (including the first) for each gRPC call when it
 *   fails with a transient status (UNAVAILABLE or DEADLINE_EXCEEDED).
 * @property retryBackoff exponential backoff used between transient retries.
 * @property tracer OpenTelemetry tracer.
 */
class EventGroupActivitySync(
  private val eventGroupActivitiesClient: EventGroupActivitiesCoroutineStub,
  private val throttler: Throttler,
  private val dataProviderName: String,
  private val listPageSize: Int,
  private val batchSize: Int = 1000,
  private val maxDeleteFraction: Double = 1.0,
  private val maxAttempts: Int = 3,
  private val retryBackoff: ExponentialBackoff = ExponentialBackoff(),
  private val tracer: Tracer = GlobalOpenTelemetry.getTracer("wfa.edpa"),
) {
  private val metrics = EventGroupActivitySyncMetrics(Instrumentation.meter)

  private fun metricAttributes(): Attributes =
    Attributes.of(AttributeKey.stringKey("data_provider_name"), dataProviderName)

  /**
   * Synchronizes the EventGroup activities described by [inputActivities].
   *
   * @param inputActivities the desired activities, grouped internally by EventGroup parent.
   * @param dryRun when true, lists and diffs but makes no BatchUpdate/BatchDelete calls. The
   *   returned [SyncResult] still reports the would-be created/deleted/unchanged counts.
   * @return a [SyncResult] with totals and any per-EventGroup [SyncError]s.
   * @throws IllegalArgumentException if any record's parent is not an EventGroup under
   *   [dataProviderName].
   */
  suspend fun sync(inputActivities: List<SpotRecord>, dryRun: Boolean = false): SyncResult {
    return withSpan(
      tracer,
      "EventGroupActivitySync",
      Attributes.of(AttributeKey.stringKey("data_provider_name"), dataProviderName),
      errorMessage = "EventGroupActivitySync failed",
    ) { _ ->
      val syncStartTime = TimeSource.Monotonic.markNow()

      // 1. VALIDATE: every record must reference an EventGroup under this DataProvider.
      val parentPrefix = "$dataProviderName/eventGroups/"
      for (record in inputActivities) {
        require(record.parent.startsWith(parentPrefix)) {
          "Record parent '${record.parent}' is not an EventGroup under DataProvider " +
            "'$dataProviderName'"
        }
      }

      // 2. GROUP by EventGroup parent into a map of parent -> desired activity dates.
      val datesByEventGroup: Map<String, Set<LocalDate>> =
        inputActivities
          .groupBy { it.parent }
          .mapValues { (_, records) ->
            records.map { it.eventGroupActivityDate.atZone(ZoneOffset.UTC).toLocalDate() }.toSet()
          }

      var activitiesCreated = 0
      var activitiesDeleted = 0
      var activitiesUnchanged = 0
      var eventGroupsProcessed = 0
      val errors = mutableListOf<SyncError>()

      // 3. Process each EventGroup, sorted for determinism.
      for (eventGroupName in datesByEventGroup.keys.sorted()) {
        val fileDates: Set<LocalDate> = datesByEventGroup.getValue(eventGroupName)
        try {
          val eventGroupKey =
            requireNotNull(EventGroupKey.fromName(eventGroupName)) {
              "Malformed EventGroup resource name: $eventGroupName"
            }

          val existingDates: Set<LocalDate> = listExistingDates(eventGroupName)

          val datesToCreate: Set<LocalDate> = fileDates - existingDates
          var datesToDelete: Set<LocalDate> = existingDates - fileDates
          val unchanged: Int = fileDates.intersect(existingDates).size

          // FIX 1: data-loss guard. If too large a fraction of existing activities would be
          // deleted, skip the deletes for this EventGroup but still apply the (safe, additive)
          // creates.
          val deleteFraction: Double =
            if (existingDates.isEmpty()) 0.0 else datesToDelete.size.toDouble() / existingDates.size
          if (existingDates.isNotEmpty() && deleteFraction > maxDeleteFraction) {
            val percent = (deleteFraction * 100).toInt()
            val message =
              "Deletion of ${datesToDelete.size}/${existingDates.size} ($percent%) exceeds max " +
                "delete fraction $maxDeleteFraction; skipping deletes"
            logger.warning { "EventGroup $eventGroupName: $message" }
            errors.add(SyncError(eventGroupName, message, null))
            metrics.deletesSkippedGuard.add(1, metricAttributes())
            datesToDelete = emptySet()
          }

          logger.info(
            "EventGroup $eventGroupName: ${datesToCreate.size} to create, " +
              "${datesToDelete.size} to delete, $unchanged unchanged (dryRun=$dryRun)"
          )

          if (!dryRun) {
            for (batch in datesToCreate.sorted().chunked(batchSize)) {
              upsertBatch(eventGroupKey, eventGroupName, batch)
            }
            for (batch in datesToDelete.sorted().chunked(batchSize)) {
              deleteBatch(eventGroupKey, eventGroupName, batch)
            }
          }

          activitiesCreated += datesToCreate.size
          activitiesDeleted += datesToDelete.size
          activitiesUnchanged += unchanged
          eventGroupsProcessed++
          metrics.activitiesCreated.add(datesToCreate.size.toLong(), metricAttributes())
          metrics.activitiesDeleted.add(datesToDelete.size.toLong(), metricAttributes())
          metrics.activitiesUnchanged.add(unchanged.toLong(), metricAttributes())
          // A guard-skipped EventGroup is still processed (listed, diffed, creates applied), so
          // it counts toward eventGroupsProcessed consistently with the SyncResult field.
          metrics.eventGroupsProcessed.add(1, metricAttributes())
        } catch (e: CancellationException) {
          throw e
        } catch (e: Exception) {
          // FIX 6: split the failure metric by error_type.
          val errorType: String = errorTypeOf(e)
          logger.log(Level.SEVERE, e) {
            "Failed to sync EventGroup $eventGroupName: error_type=$errorType"
          }
          errors.add(SyncError(eventGroupName, e.message ?: "Unknown error", e))
          metrics.syncErrors.add(
            1,
            Attributes.builder()
              .putAll(metricAttributes())
              .put(AttributeKey.stringKey("error_type"), errorType)
              .build(),
          )
        }
      }

      val syncLatency = syncStartTime.elapsedNow().inWholeMilliseconds / 1000.0
      metrics.syncLatency.record(syncLatency, metricAttributes())

      SyncResult(
        totalInputRecords = inputActivities.size,
        eventGroupsProcessed = eventGroupsProcessed,
        activitiesCreated = activitiesCreated,
        activitiesDeleted = activitiesDeleted,
        activitiesUnchanged = activitiesUnchanged,
        errors = errors,
      )
    }
  }

  /** Lists all existing activity dates for [eventGroupName], paginating through every page. */
  @OptIn(ExperimentalCoroutinesApi::class) // For `flattenConcat`.
  private suspend fun listExistingDates(eventGroupName: String): Set<LocalDate> {
    val activities: List<EventGroupActivity> =
      eventGroupActivitiesClient
        .listResources { pageToken: String ->
          val response = retryTransient {
            throttler.onReady {
              eventGroupActivitiesClient.listEventGroupActivities(
                listEventGroupActivitiesRequest {
                  parent = eventGroupName
                  pageSize = listPageSize
                  this.pageToken = pageToken
                }
              )
            }
          }
          ResourceList(response.eventGroupActivitiesList, response.nextPageToken)
        }
        .flattenConcat()
        .toList()
    return activities.map { it.date.toLocalDate() }.toSet()
  }

  /** Batch-upserts the activities for [dates] under [eventGroupName]. */
  private suspend fun upsertBatch(
    eventGroupKey: EventGroupKey,
    eventGroupName: String,
    dates: List<LocalDate>,
  ) {
    retryTransient {
      throttler.onReady {
        eventGroupActivitiesClient.batchUpdateEventGroupActivities(
          batchUpdateEventGroupActivitiesRequest {
            parent = eventGroupName
            for (date in dates) {
              requests += updateEventGroupActivityRequest {
                eventGroupActivity = eventGroupActivity {
                  name =
                    EventGroupActivityKey(
                        eventGroupKey.dataProviderId,
                        eventGroupKey.eventGroupId,
                        date.toString(),
                      )
                      .toName()
                  this.date = date.toProtoDate()
                }
                allowMissing = true
              }
            }
          }
        )
      }
    }
  }

  /** Batch-deletes the activities for [dates] under [eventGroupName]. */
  private suspend fun deleteBatch(
    eventGroupKey: EventGroupKey,
    eventGroupName: String,
    dates: List<LocalDate>,
  ) {
    retryTransient {
      throttler.onReady {
        eventGroupActivitiesClient.batchDeleteEventGroupActivities(
          batchDeleteEventGroupActivitiesRequest {
            parent = eventGroupName
            for (date in dates) {
              names +=
                EventGroupActivityKey(
                    eventGroupKey.dataProviderId,
                    eventGroupKey.eventGroupId,
                    date.toString(),
                  )
                  .toName()
            }
          }
        )
      }
    }
  }

  /**
   * Runs [block], retrying up to [maxAttempts] total attempts when it fails with a transient gRPC
   * status (UNAVAILABLE or DEADLINE_EXCEEDED), backing off via [retryBackoff] between attempts.
   *
   * Non-transient failures are rethrown immediately without retrying.
   */
  private suspend fun <T> retryTransient(block: suspend () -> T): T {
    var attempt = 1
    while (true) {
      try {
        return block()
      } catch (e: Exception) {
        if (e is CancellationException) throw e
        val statusCode = transientStatusCodeOf(e)
        if (statusCode == null || attempt >= maxAttempts) {
          throw e
        }
        logger.warning { "Transient gRPC failure ($statusCode); retrying (attempt $attempt)" }
        delay(retryBackoff.durationForAttempt(attempt).toMillis())
        attempt++
      }
    }
  }

  companion object {
    private val logger: Logger = Logger.getLogger(this::class.java.name)

    /**
     * Returns the gRPC status code of [e] (or its cause) if it is a transient (retryable) status,
     * otherwise `null`.
     */
    private fun transientStatusCodeOf(e: Throwable): io.grpc.Status.Code? {
      val status =
        when (e) {
          is StatusException -> e.status
          is StatusRuntimeException -> e.status
          else ->
            when (val cause = e.cause) {
              is StatusException -> cause.status
              is StatusRuntimeException -> cause.status
              else -> return null
            }
        }
      return when (status.code) {
        io.grpc.Status.Code.UNAVAILABLE,
        io.grpc.Status.Code.DEADLINE_EXCEEDED -> status.code
        else -> null
      }
    }

    /** Derives an `error_type` label for a failure: gRPC status code name, or exception class. */
    private fun errorTypeOf(e: Throwable): String {
      val status =
        when (e) {
          is StatusException -> e.status
          is StatusRuntimeException -> e.status
          else ->
            when (val cause = e.cause) {
              is StatusException -> cause.status
              is StatusRuntimeException -> cause.status
              else -> null
            }
        }
      return status?.code?.name ?: e.javaClass.simpleName
    }
  }
}
