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
import kotlinx.coroutines.flow.Flow
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
import org.wfanet.measurement.common.api.ResourceKey
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
  val eventGroupsSucceeded: Int,
  val eventGroupsGuardSkipped: Int,
  val eventGroupsFailed: Int,
  val activitiesCreated: Int,
  val activitiesDeleted: Int,
  val activitiesUnchanged: Int,
  val guardSkipped: List<SyncError>,
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
 * Activities that already match are left unchanged.
 *
 * Per-EventGroup isolation applies to **runtime** failures: an RPC error during the list or batch
 * for a single EventGroup is logged and recorded as a [SyncError] but does not abort the overall
 * sync. Input-shape validation failures (a record whose parent is not a concrete EventGroup under
 * this DataProvider) are different: they abort the entire sync by throwing.
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

  private fun eventGroupsAttributes(outcome: String): Attributes =
    Attributes.builder()
      .putAll(metricAttributes())
      .put(AttributeKey.stringKey("outcome"), outcome)
      .build()

  /**
   * Synchronizes the EventGroup activities described by [inputActivities].
   *
   * Per-EventGroup isolation applies to **runtime** failures (RPC errors during list/batch);
   * input-shape validation failures (a record whose parent is not a concrete EventGroup under this
   * DataProvider) abort the entire sync.
   *
   * @param inputActivities the desired activities, grouped internally by EventGroup parent.
   * @param dryRun when true, lists and diffs but makes no BatchUpdate/BatchDelete calls. The
   *   returned [SyncResult] still reports the would-be created/deleted/unchanged counts.
   * @return a [SyncResult] with totals and any per-EventGroup [SyncError]s.
   * @throws IllegalArgumentException if any record's parent is not a concrete EventGroup under
   *   [dataProviderName].
   */
  suspend fun sync(inputActivities: Flow<SpotRecord>, dryRun: Boolean = false): SyncResult {
    return withSpan(
      tracer,
      "EventGroupActivitySync",
      Attributes.of(AttributeKey.stringKey("data_provider_name"), dataProviderName),
      errorMessage = "EventGroupActivitySync failed",
    ) { _ ->
      val syncStartTime = TimeSource.Monotonic.markNow()

      // 1. VALIDATE and GROUP incrementally: every record must reference a concrete EventGroup
      // under this DataProvider. Build the map of parent -> desired activity dates as records
      // stream in.
      val parentPrefix = "$dataProviderName/eventGroups/"
      val datesByEventGroup = mutableMapOf<String, MutableSet<LocalDate>>()
      var totalInputRecords = 0
      inputActivities.collect { record ->
        require(record.parent.startsWith(parentPrefix)) {
          "Record parent '${record.parent}' is not an EventGroup under DataProvider " +
            "'$dataProviderName'"
        }
        val eventGroupId = record.parent.removePrefix(parentPrefix)
        require(eventGroupId != ResourceKey.WILDCARD_ID && !eventGroupId.contains('/')) {
          "Record parent '${record.parent}' must reference a concrete EventGroup, not a wildcard"
        }
        datesByEventGroup
          .getOrPut(record.parent) { mutableSetOf() }
          .add(record.eventGroupActivityDate.atZone(ZoneOffset.UTC).toLocalDate())
        totalInputRecords++
      }

      var activitiesCreated = 0
      var activitiesDeleted = 0
      var activitiesUnchanged = 0
      var eventGroupsSucceeded = 0
      var eventGroupsGuardSkipped = 0
      var eventGroupsFailed = 0
      val errors = mutableListOf<SyncError>()
      val guardSkips = mutableListOf<SyncError>()

      // 2. Process each EventGroup, sorted for determinism.
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

          val deleteFraction: Double =
            if (existingDates.isEmpty()) 0.0 else datesToDelete.size.toDouble() / existingDates.size
          var guardSkipped = false
          if (existingDates.isNotEmpty() && deleteFraction > maxDeleteFraction) {
            val percent = (deleteFraction * 100).toInt()
            val message =
              "Deletion of ${datesToDelete.size}/${existingDates.size} ($percent%) exceeds max " +
                "delete fraction $maxDeleteFraction; skipping deletes"
            logger.warning { "EventGroup $eventGroupName: $message" }
            guardSkips.add(SyncError(eventGroupName, message, null))
            metrics.deletesSkippedGuard.add(1, metricAttributes())
            datesToDelete = emptySet()
            guardSkipped = true
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
          metrics.activitiesCreated.add(datesToCreate.size.toLong(), metricAttributes())
          metrics.activitiesDeleted.add(datesToDelete.size.toLong(), metricAttributes())
          metrics.activitiesUnchanged.add(unchanged.toLong(), metricAttributes())
          if (guardSkipped) {
            eventGroupsGuardSkipped++
            metrics.eventGroupsProcessed.add(1, eventGroupsAttributes("guard_skipped"))
          } else {
            eventGroupsSucceeded++
            metrics.eventGroupsProcessed.add(1, eventGroupsAttributes("success"))
          }
        } catch (e: CancellationException) {
          throw e
        } catch (e: Exception) {
          val errorType: String = errorTypeOf(e)
          logger.log(Level.SEVERE, e) {
            "Failed to sync EventGroup $eventGroupName: error_type=$errorType"
          }
          errors.add(SyncError(eventGroupName, e.message ?: "Unknown error", e))
          eventGroupsFailed++
          metrics.eventGroupsProcessed.add(1, eventGroupsAttributes("failed"))
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
        totalInputRecords = totalInputRecords,
        eventGroupsSucceeded = eventGroupsSucceeded,
        eventGroupsGuardSkipped = eventGroupsGuardSkipped,
        eventGroupsFailed = eventGroupsFailed,
        activitiesCreated = activitiesCreated,
        activitiesDeleted = activitiesDeleted,
        activitiesUnchanged = activitiesUnchanged,
        guardSkipped = guardSkips,
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

    /** Returns the gRPC [io.grpc.Status] of [this] (or its cause), or `null` if there is none. */
    private fun Throwable.grpcStatus(): io.grpc.Status? =
      when (this) {
        is StatusException -> status
        is StatusRuntimeException -> status
        else ->
          when (val c = cause) {
            is StatusException -> c.status
            is StatusRuntimeException -> c.status
            else -> null
          }
      }

    /**
     * Returns the gRPC status code of [e] (or its cause) if it is a transient (retryable) status,
     * otherwise `null`.
     */
    private fun transientStatusCodeOf(e: Throwable): io.grpc.Status.Code? =
      e.grpcStatus()?.code?.takeIf {
        it == io.grpc.Status.Code.UNAVAILABLE || it == io.grpc.Status.Code.DEADLINE_EXCEEDED
      }

    /** Derives an `error_type` label for a failure: gRPC status code name, or exception class. */
    private fun errorTypeOf(e: Throwable): String =
      e.grpcStatus()?.code?.name ?: e.javaClass.simpleName
  }
}
