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

package org.wfanet.measurement.edpaggregator.eventgroups

import io.grpc.StatusException
import io.opentelemetry.api.GlobalOpenTelemetry
import io.opentelemetry.api.common.AttributeKey
import io.opentelemetry.api.common.Attributes
import io.opentelemetry.api.trace.Tracer
import java.util.logging.Level
import java.util.logging.Logger
import kotlin.time.TimeMark
import kotlin.time.TimeSource
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.FlowCollector
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.toList
import org.wfanet.measurement.api.v2alpha.ClientAccountsGrpcKt.ClientAccountsCoroutineStub
import org.wfanet.measurement.api.v2alpha.CreateEventGroupRequest
import org.wfanet.measurement.api.v2alpha.EventGroup as CmmsEventGroup
import org.wfanet.measurement.api.v2alpha.EventGroupKt as CmmsEventGroupKt
import org.wfanet.measurement.api.v2alpha.EventGroupMetadata as CmmsEventGroupMetadataMessage
import org.wfanet.measurement.api.v2alpha.EventGroupMetadataKt as CmmsEventGroupMetadataKt
import org.wfanet.measurement.api.v2alpha.EventGroupMetadataKt.AdMetadataKt as CmmsAdMetadataKt
import org.wfanet.measurement.api.v2alpha.EventGroupsGrpcKt.EventGroupsCoroutineStub
import org.wfanet.measurement.api.v2alpha.ListClientAccountsRequestKt
import org.wfanet.measurement.api.v2alpha.ListClientAccountsResponse
import org.wfanet.measurement.api.v2alpha.ListEventGroupsRequestKt.filter as listEventGroupsFilter
import org.wfanet.measurement.api.v2alpha.MeasurementConsumerClientAccountKey
import org.wfanet.measurement.api.v2alpha.MeasurementConsumerKey
import org.wfanet.measurement.api.v2alpha.MediaType as CmmsMediaType
import org.wfanet.measurement.api.v2alpha.UpdateEventGroupRequest
import org.wfanet.measurement.api.v2alpha.batchCreateEventGroupsRequest
import org.wfanet.measurement.api.v2alpha.batchUpdateEventGroupsRequest
import org.wfanet.measurement.api.v2alpha.copy
import org.wfanet.measurement.api.v2alpha.createEventGroupRequest
import org.wfanet.measurement.api.v2alpha.deleteEventGroupRequest
import org.wfanet.measurement.api.v2alpha.eventGroup as cmmsEventGroup
import org.wfanet.measurement.api.v2alpha.eventGroupMetadata as cmmsEventGroupMetadata
import org.wfanet.measurement.api.v2alpha.listClientAccountsRequest
import org.wfanet.measurement.api.v2alpha.listEventGroupsRequest
import org.wfanet.measurement.api.v2alpha.updateEventGroupRequest
import org.wfanet.measurement.common.Instrumentation
import org.wfanet.measurement.common.api.grpc.ResourceList
import org.wfanet.measurement.common.api.grpc.flattenConcat
import org.wfanet.measurement.common.api.grpc.listResources
import org.wfanet.measurement.common.throttler.Throttler
import org.wfanet.measurement.edpaggregator.eventgroups.v1alpha.EventGroup
import org.wfanet.measurement.edpaggregator.eventgroups.v1alpha.EventGroup.MediaType
import org.wfanet.measurement.edpaggregator.eventgroups.v1alpha.MappedEventGroup
import org.wfanet.measurement.edpaggregator.eventgroups.v1alpha.copy
import org.wfanet.measurement.edpaggregator.eventgroups.v1alpha.mappedEventGroup
import org.wfanet.measurement.edpaggregator.telemetry.withSpan

class UnresolvedMeasurementConsumerException(message: String) : Exception(message)

/**
 * Key used to uniquely identify an event group.
 *
 * This combines:
 * - [eventGroupReferenceId]: identifier of the EventGroup resource
 * - [measurementConsumer]: the owner/consumer of the measurement
 *
 * This is intended to be used as a map key when grouping or associating event groups by both
 * attributes, ensuring uniqueness across consumers.
 */
data class EventGroupKey(val eventGroupReferenceId: String, val measurementConsumer: String)

/**
 * Composite key for matching event groups by their entity key.
 *
 * @property entityType the entity type (e.g. "campaign", "creative-id")
 * @property entityId the entity ID within the data provider's system
 * @property measurementConsumer resource name of the MeasurementConsumer
 */
data class EntityKeyId(
  val entityType: String,
  val entityId: String,
  val measurementConsumer: String,
)

/** A buffered batch write request tagged with identifying info for response zipping and metrics. */
private data class PendingWrite<T>(
  val eventGroupReferenceId: String,
  val entityType: String,
  val entityId: String,
  val startTime: TimeMark,
  val request: T,
)

/** The write a queued EventGroup needs: a batched create, a batched update, or nothing. */
private sealed interface WriteAction {
  data class Create(val request: CreateEventGroupRequest) : WriteAction

  data class Update(val request: UpdateEventGroupRequest) : WriteAction

  object None : WriteAction
}

/*
 * Syncs event groups with the CMMS Public API.
 * 1. **Creates** any EventGroups that exist in the input flow but not in CMMS.
 * 2. **Updates** existing EventGroups in CMMS if their data has changed.
 * 3. **Deletes** EventGroups whose state is [EventGroup.State.DELETED] from CMMS.
 * 4. **Returns** a flow of [MappedEventGroup], one element for each successfully
 *    created or updated EventGroup (deleted EventGroups are not included).
 *
 * EventGroups that exist in CMMS but are absent from the input flow are left as-is.
 */
class EventGroupSync(
  private val edpName: String,
  private val eventGroupsStub: EventGroupsCoroutineStub,
  private val clientAccountsStub: ClientAccountsCoroutineStub,
  private val eventGroups: Flow<EventGroup>,
  private val throttler: Throttler,
  private val listEventGroupPageSize: Int,
  private val entityKeyTypes: List<String>,
  private val tracer: Tracer = GlobalOpenTelemetry.getTracer("wfa.edpa"),
) {
  private val metrics = EventGroupSyncMetrics(Instrumentation.meter)

  /**
   * Cache for ClientAccount lookups to avoid repeated API calls within a sync batch.
   *
   * Key: client_account_reference_id Value: list of resolved MeasurementConsumerKeys. A single
   * client account can map to multiple Measurement Consumers. Empty list if resolution failed.
   */
  private val clientAccountCache = mutableMapOf<String, List<MeasurementConsumerKey>>()

  /** Creates metric attributes with data provider name. */
  private fun metricAttributes() =
    Attributes.of(AttributeKey.stringKey("data_provider_name"), edpName)

  /**
   * Synchronizes EventGroups with the CMMS Public API by creating, updating, or deleting as needed.
   *
   * For EventGroups with `client_account_reference_id`, resolves all associated
   * MeasurementConsumers and creates separate EventGroup resources for each.
   *
   * EventGroups with [EventGroup.State.DELETED] are deleted from CMMS if they exist. EventGroups
   * absent from the input flow are left unchanged.
   *
   * @return Flow of [MappedEventGroup] for each successfully synced EventGroup. Failed syncs are
   *   skipped and logged. Deleted EventGroups are not included.
   */
  suspend fun sync(): Flow<MappedEventGroup> = flow {
    withSpan(
      tracer,
      "EventGroupSync",
      Attributes.of(
        AttributeKey.stringKey("data_provider_name"),
        edpName,
        AttributeKey.stringKey("source"),
        "kingdom",
      ),
      errorMessage = "EventGroupSync failed",
    ) { _ ->
      val fetchedEventGroups = fetchEventGroups().toList()

      val cmmsEventGroups: Map<EventGroupKey, CmmsEventGroup> =
        fetchedEventGroups
          .filter { it.eventGroupReferenceId.isNotBlank() }
          .associateBy { eventGroup ->
            EventGroupKey(eventGroup.eventGroupReferenceId, eventGroup.measurementConsumer)
          }

      val cmmsEventGroupsByEntityKey: Map<EntityKeyId, CmmsEventGroup> =
        fetchedEventGroups
          .filter { it.hasEntityKey() && it.entityKey.entityId.isNotBlank() }
          .associateBy { eventGroup ->
            EntityKeyId(
              eventGroup.entityKey.entityType,
              eventGroup.entityKey.entityId,
              eventGroup.measurementConsumer,
            )
          }

      // Buffers for batched create/update writes, each capped at [MAX_BATCH_SIZE]. Every entry
      // carries its eventGroupReferenceId so the batch response can be zipped back into the output.
      val pendingCreates = mutableListOf<PendingWrite<CreateEventGroupRequest>>()
      val pendingUpdates = mutableListOf<PendingWrite<UpdateEventGroupRequest>>()

      // Collect the EDP EventGroups as a stream rather than materializing the full decoded list.
      eventGroups.collect { eventGroup ->
        if (eventGroup.state == EventGroup.State.DELETED) {
          // Deletes act as a barrier: flush queued creates/updates first so a buffered write is not
          // reordered after a delete of the same EventGroup (which would NOT_FOUND or leave CMMS
          // inconsistent with the input order).
          flushCreates(pendingCreates)
          flushUpdates(pendingUpdates)
          try {
            validateDeletedEventGroup(eventGroup)
          } catch (e: Exception) {
            logger.log(Level.SEVERE, e) {
              "Skipping deleted Event Group ${eventGroup.eventGroupReferenceId}" +
                (if (eventGroup.hasEntityKey())
                  " (entityType=${eventGroup.entityKey.entityType}," +
                    " entityId=${eventGroup.entityKey.entityId})"
                else "") +
                ": Validation failed"
            }
            metrics.invalidEventGroupFailure.add(1, metricAttributes())
            return@collect
          }

          if (eventGroup.hasEntityKey() && eventGroup.entityKey.entityId.isNotBlank()) {
            val entityKeyId =
              EntityKeyId(
                eventGroup.entityKey.entityType,
                eventGroup.entityKey.entityId,
                eventGroup.measurementConsumer,
              )
            val cmmsEventGroup = cmmsEventGroupsByEntityKey[entityKeyId]
            if (cmmsEventGroup != null) {
              deleteCmmsEventGroup(cmmsEventGroup)
              metrics.syncDeleted.add(1, metricAttributes())
            }
          }
          return@collect
        }

        try {
          validateEventGroup(eventGroup)
        } catch (e: Exception) {
          logger.log(Level.SEVERE, e) {
            "Skipping Event Group ${eventGroup.eventGroupReferenceId}" +
              (if (eventGroup.hasEntityKey())
                " (entityType=${eventGroup.entityKey.entityType}," +
                  " entityId=${eventGroup.entityKey.entityId})"
              else "") +
              ": Validation failed"
          }
          metrics.invalidEventGroupFailure.add(1, metricAttributes())
          return@collect
        }

        val measurementConsumerKeys: Set<MeasurementConsumerKey> =
          try {
            resolveMeasurementConsumers(eventGroup)
          } catch (e: UnresolvedMeasurementConsumerException) {
            logger.log(Level.SEVERE, e) {
              "Skipping Event Group ${eventGroup.eventGroupReferenceId}" +
                (if (eventGroup.hasEntityKey())
                  " (entityType=${eventGroup.entityKey.entityType}," +
                    " entityId=${eventGroup.entityKey.entityId})"
                else "") +
                ": No measurement consumer resolved"
            }
            metrics.syncFailure.add(1, metricAttributes())
            return@collect
          }

        for (measurementConsumerKey in measurementConsumerKeys) {
          try {
            val eventGroupWithConsumer =
              eventGroup.copy { measurementConsumer = measurementConsumerKey.toName() }
            queueEventGroupItem(
              eventGroupWithConsumer,
              cmmsEventGroups,
              cmmsEventGroupsByEntityKey,
              pendingCreates,
              pendingUpdates,
            )
          } catch (e: Exception) {
            if (e is CancellationException) throw e
            logger.log(Level.SEVERE, e) {
              "Skipping Event Group ${eventGroup.eventGroupReferenceId}" +
                (if (eventGroup.hasEntityKey())
                  " (entityType=${eventGroup.entityKey.entityType}," +
                    " entityId=${eventGroup.entityKey.entityId})"
                else "") +
                ": Failed to process for measurement consumer"
            }
            metrics.syncFailure.add(1, metricAttributes())
          }
        }
      }

      // Flush any remaining buffered writes after the input stream is exhausted.
      flushCreates(pendingCreates)
      flushUpdates(pendingUpdates)
    }
  }

  /**
   * Queues a single resolved EventGroup for a batched create or update.
   *
   * The create-vs-update decision is made against the precache via [findExistingEventGroup]. An
   * EventGroup that needs no change is emitted immediately with no RPC. Otherwise the request is
   * appended to the matching buffer, which is flushed as one batched RPC once it reaches
   * [MAX_BATCH_SIZE].
   */
  private suspend fun FlowCollector<MappedEventGroup>.queueEventGroupItem(
    eventGroup: EventGroup,
    existingByKey: Map<EventGroupKey, CmmsEventGroup>,
    existingByEntityKey: Map<EntityKeyId, CmmsEventGroup>,
    pendingCreates: MutableList<PendingWrite<CreateEventGroupRequest>>,
    pendingUpdates: MutableList<PendingWrite<UpdateEventGroupRequest>>,
  ) {
    val referenceId = eventGroup.eventGroupReferenceId
    val entityType = eventGroup.entityKey.entityType
    val entityId = eventGroup.entityKey.entityId
    // Per-item start, captured before queueing so latency includes time spent buffered before
    // flush.
    val itemStartTime = TimeSource.Monotonic.markNow()

    // Decide create / update / no-op for this event group inside the per-item span. This makes no
    // RPCs, so the span reflects only this item; buffering and flushing happen after the span, so a
    // flushed batch is never attributed to the current item's span/trace.
    val action: WriteAction =
      withSpan(
        tracer,
        "EventGroupSync.Item",
        Attributes.of(
          AttributeKey.stringKey("event_group_reference_id"),
          referenceId,
          AttributeKey.stringKey("data_provider_name"),
          edpName,
        ),
        errorMessage = "Event Group sync failed",
      ) { _ ->
        metrics.syncAttempts.add(1, metricAttributes())
        val existingEventGroup: CmmsEventGroup? =
          findExistingEventGroup(eventGroup, existingByKey, existingByEntityKey)

        if (existingEventGroup == null) {
          WriteAction.Create(buildCreateRequest(eventGroup))
        } else {
          val updatedEventGroup: CmmsEventGroup = updateEventGroup(existingEventGroup, eventGroup)
          if (updatedEventGroup == existingEventGroup) {
            // Already up to date; emit the mapping without issuing a write RPC.
            metrics.syncSuccess.add(1, metricAttributes())
            metrics.syncLatency.record(
              itemStartTime.elapsedNow().inWholeMilliseconds / 1000.0,
              metricAttributes(),
            )
            emit(
              mappedEventGroup {
                eventGroupReferenceId = existingEventGroup.eventGroupReferenceId
                eventGroupResource = existingEventGroup.name
              }
            )
            WriteAction.None
          } else {
            WriteAction.Update(updateEventGroupRequest { this.eventGroup = updatedEventGroup })
          }
        }
      }

    when (action) {
      is WriteAction.Create -> {
        // A duplicate request_id within one BatchCreateEventGroups call is rejected by the kingdom,
        // so flush first if it is already buffered. Duplicate input rows then land in separate
        // batches, where the kingdom dedupes by request_id (idempotent) instead of failing the
        // batch.
        if (pendingCreates.any { it.request.requestId == action.request.requestId }) {
          flushCreates(pendingCreates)
        }
        pendingCreates.add(
          PendingWrite(referenceId, entityType, entityId, itemStartTime, action.request)
        )
        if (pendingCreates.size >= MAX_BATCH_SIZE) {
          flushCreates(pendingCreates)
        }
      }
      is WriteAction.Update -> {
        // Likewise avoid two updates to the same EventGroup resource within one batch.
        if (pendingUpdates.any { it.request.eventGroup.name == action.request.eventGroup.name }) {
          flushUpdates(pendingUpdates)
        }
        pendingUpdates.add(
          PendingWrite(referenceId, entityType, entityId, itemStartTime, action.request)
        )
        if (pendingUpdates.size >= MAX_BATCH_SIZE) {
          flushUpdates(pendingUpdates)
        }
      }
      WriteAction.None -> {}
    }
  }

  /**
   * Flushes buffered creates as one [batchCreateEventGroups] RPC and emits the resulting mappings.
   */
  private suspend fun FlowCollector<MappedEventGroup>.flushCreates(
    pendingCreates: MutableList<PendingWrite<CreateEventGroupRequest>>
  ) {
    if (pendingCreates.isEmpty()) {
      return
    }
    val response =
      try {
        throttler.onReady {
          withSpan(
            tracer,
            "EventGroupSync.BatchCreate",
            Attributes.of(
              AttributeKey.longKey("batch_size"),
              pendingCreates.size.toLong(),
              AttributeKey.stringKey("data_provider_name"),
              edpName,
            ),
            errorMessage = "BatchCreateEventGroups failed",
          ) { _ ->
            eventGroupsStub.batchCreateEventGroups(
              batchCreateEventGroupsRequest {
                parent = edpName
                requests += pendingCreates.map { it.request }
              }
            )
          }
        }
      } catch (e: Exception) {
        if (e is CancellationException) throw e
        // The whole batch RPC failed (e.g. one invalid sub-request). Fall back to per-item creates
        // so a single bad EventGroup doesn't block the rest, matching the pre-batching resilience.
        flushCreatesIndividually(pendingCreates, e)
        pendingCreates.clear()
        return
      }
    // Pair responses to requests by (referenceId, measurementConsumer) rather than list position,
    // since the batch response order is not guaranteed. The per-batch dedup guard keeps this key
    // unique within the batch.
    val pendingByKey =
      pendingCreates.associateBy {
        EventGroupKey(
          it.request.eventGroup.eventGroupReferenceId,
          it.request.eventGroup.measurementConsumer,
        )
      }
    response.eventGroupsList.forEach { syncedEventGroup ->
      val item =
        pendingByKey.getValue(
          EventGroupKey(
            syncedEventGroup.eventGroupReferenceId,
            syncedEventGroup.measurementConsumer,
          )
        )
      metrics.syncSuccess.add(1, metricAttributes())
      metrics.syncLatency.record(
        item.startTime.elapsedNow().inWholeMilliseconds / 1000.0,
        metricAttributes(),
      )
      emit(
        mappedEventGroup {
          eventGroupReferenceId = item.eventGroupReferenceId
          eventGroupResource = syncedEventGroup.name
        }
      )
    }
    pendingCreates.clear()
  }

  /**
   * Issues each buffered create as an individual RPC after a batch failure, isolating the bad
   * item(s) so valid EventGroups still sync.
   */
  private suspend fun FlowCollector<MappedEventGroup>.flushCreatesIndividually(
    pending: List<PendingWrite<CreateEventGroupRequest>>,
    batchError: Exception,
  ) {
    logger.log(Level.WARNING, batchError) {
      "Batch create of ${pending.size} Event Groups failed; retrying individually"
    }
    for (item in pending) {
      try {
        val syncedEventGroup = throttler.onReady { eventGroupsStub.createEventGroup(item.request) }
        metrics.syncSuccess.add(1, metricAttributes())
        metrics.syncLatency.record(
          item.startTime.elapsedNow().inWholeMilliseconds / 1000.0,
          metricAttributes(),
        )
        emit(
          mappedEventGroup {
            eventGroupReferenceId = item.eventGroupReferenceId
            eventGroupResource = syncedEventGroup.name
          }
        )
      } catch (e: Exception) {
        if (e is CancellationException) throw e
        recordItemFailure(e, item)
      }
    }
  }

  /**
   * Flushes buffered updates as one [batchUpdateEventGroups] RPC and emits the resulting mappings.
   */
  private suspend fun FlowCollector<MappedEventGroup>.flushUpdates(
    pendingUpdates: MutableList<PendingWrite<UpdateEventGroupRequest>>
  ) {
    if (pendingUpdates.isEmpty()) {
      return
    }
    val response =
      try {
        throttler.onReady {
          withSpan(
            tracer,
            "EventGroupSync.BatchUpdate",
            Attributes.of(
              AttributeKey.longKey("batch_size"),
              pendingUpdates.size.toLong(),
              AttributeKey.stringKey("data_provider_name"),
              edpName,
            ),
            errorMessage = "BatchUpdateEventGroups failed",
          ) { _ ->
            eventGroupsStub.batchUpdateEventGroups(
              batchUpdateEventGroupsRequest {
                parent = edpName
                requests += pendingUpdates.map { it.request }
              }
            )
          }
        }
      } catch (e: Exception) {
        if (e is CancellationException) throw e
        // The whole batch RPC failed (e.g. one invalid sub-request). Fall back to per-item updates
        // so a single bad EventGroup doesn't block the rest, matching the pre-batching resilience.
        flushUpdatesIndividually(pendingUpdates, e)
        pendingUpdates.clear()
        return
      }
    // Pair responses to requests by EventGroup resource name (unique within the batch via the dedup
    // guard) rather than list position, since the batch response order is not guaranteed.
    val pendingByName = pendingUpdates.associateBy { it.request.eventGroup.name }
    response.eventGroupsList.forEach { syncedEventGroup ->
      val item = pendingByName.getValue(syncedEventGroup.name)
      metrics.syncSuccess.add(1, metricAttributes())
      metrics.syncLatency.record(
        item.startTime.elapsedNow().inWholeMilliseconds / 1000.0,
        metricAttributes(),
      )
      emit(
        mappedEventGroup {
          eventGroupReferenceId = item.eventGroupReferenceId
          eventGroupResource = syncedEventGroup.name
        }
      )
    }
    pendingUpdates.clear()
  }

  /**
   * Issues each buffered update as an individual RPC after a batch failure, isolating the bad
   * item(s) so valid EventGroups still sync.
   */
  private suspend fun FlowCollector<MappedEventGroup>.flushUpdatesIndividually(
    pending: List<PendingWrite<UpdateEventGroupRequest>>,
    batchError: Exception,
  ) {
    logger.log(Level.WARNING, batchError) {
      "Batch update of ${pending.size} Event Groups failed; retrying individually"
    }
    for (item in pending) {
      try {
        val syncedEventGroup = throttler.onReady { eventGroupsStub.updateEventGroup(item.request) }
        metrics.syncSuccess.add(1, metricAttributes())
        metrics.syncLatency.record(
          item.startTime.elapsedNow().inWholeMilliseconds / 1000.0,
          metricAttributes(),
        )
        emit(
          mappedEventGroup {
            eventGroupReferenceId = item.eventGroupReferenceId
            eventGroupResource = syncedEventGroup.name
          }
        )
      } catch (e: Exception) {
        if (e is CancellationException) throw e
        recordItemFailure(e, item)
      }
    }
  }

  /**
   * Records a single failed sync as one [EventGroupSyncMetrics.syncFailure] with item attributes.
   */
  private fun recordItemFailure(e: Exception, item: PendingWrite<*>) {
    val errorType =
      if (e is StatusException || e.cause is StatusException) {
        (e as? StatusException ?: e.cause as StatusException).status.code.name
      } else {
        e.javaClass.simpleName
      }
    metrics.syncFailure.add(
      1,
      Attributes.builder()
        .putAll(metricAttributes())
        .put(AttributeKey.stringKey("error_type"), errorType)
        .put(AttributeKey.stringKey("event_group_reference_id"), item.eventGroupReferenceId)
        .put(AttributeKey.stringKey("entity_type"), item.entityType)
        .put(AttributeKey.stringKey("entity_id"), item.entityId)
        .build(),
    )
    logger.log(Level.SEVERE, e) {
      "Unable to sync Event Group ${item.eventGroupReferenceId}: error_type=$errorType"
    }
  }

  /** Builds a [CreateEventGroupRequest] for [eventGroup] without issuing it. */
  private fun buildCreateRequest(eventGroup: EventGroup): CreateEventGroupRequest {
    return createEventGroupRequest {
      parent = edpName
      requestId = "${eventGroup.eventGroupReferenceId}-${eventGroup.measurementConsumer}"
      this.eventGroup = cmmsEventGroup {
        measurementConsumer = eventGroup.measurementConsumer
        eventGroupReferenceId = eventGroup.eventGroupReferenceId
        this.eventGroupMetadata = eventGroup.toCmmsEventGroupMetadata()
        mediaTypes += eventGroup.mediaTypesList.map { it.toCmmsMediaType() }
        dataAvailabilityInterval = eventGroup.dataAvailabilityInterval
        if (eventGroup.hasEntityKey()) {
          this.entityKey = eventGroup.entityKey.toCmmsEntityKey()
        }
      }
    }
  }

  /**
   * Resolves the MeasurementConsumerKeys for an EventGroup.
   *
   * Resolution logic:
   * 1. Collects direct measurementConsumer if set (EDP's mapping)
   * 2. Also looks up clientAccountReferenceId if set (operator's mapping)
   * 3. Returns the union of both, supporting dual mapping capability
   *
   * Uses an in-memory cache to avoid repeated API calls for the same client_account_reference_id
   * within a sync batch.
   *
   * @param eventGroup The event group to resolve the measurement consumers for
   * @return Set of MeasurementConsumerKeys from both direct and lookup sources, or empty set when
   *   unable to resolve
   * @throws StatusException if ClientAccount lookup API call fails
   * @throws IllegalArgumentException if measurementConsumer field has invalid resource name
   * @throws IllegalStateException if a ClientAccount has invalid resource name
   */
  private suspend fun resolveMeasurementConsumers(
    eventGroup: EventGroup
  ): Set<MeasurementConsumerKey> {
    val measurementConsumerKeys = mutableSetOf<MeasurementConsumerKey>()

    // Collect direct measurement consumer (EDP's mapping)
    if (eventGroup.measurementConsumer.isNotEmpty()) {
      val key = MeasurementConsumerKey.fromName(eventGroup.measurementConsumer)
      if (key != null && key.measurementConsumerId.isNotBlank()) {
        measurementConsumerKeys.add(key)
      } else {
        logger.log(Level.WARNING) {
          "Ignoring malformed measurement_consumer: ${eventGroup.measurementConsumer}" +
            " for Event Group ${eventGroup.eventGroupReferenceId}" +
            (if (eventGroup.hasEntityKey())
              " (entityType=${eventGroup.entityKey.entityType}," +
                " entityId=${eventGroup.entityKey.entityId})"
            else "")
        }
      }
    }

    // Also collect from client_account_reference_id lookup (operator's mapping)
    if (eventGroup.clientAccountReferenceId.isNotEmpty()) {
      val refId = eventGroup.clientAccountReferenceId

      val lookedUpKeys =
        clientAccountCache.getOrPut(refId) {
          // Not in cache - lookup via API
          val response: ListClientAccountsResponse =
            throttler.onReady {
              clientAccountsStub.listClientAccounts(
                listClientAccountsRequest {
                  parent = edpName
                  filter = ListClientAccountsRequestKt.filter { clientAccountReferenceId = refId }
                }
              )
            }

          val resolvedKeys: List<MeasurementConsumerKey> =
            // Extract MeasurementConsumerKeys from all ClientAccounts
            // Format: measurementConsumers/{mc}/clientAccounts/{ca}
            response.clientAccountsList.map { clientAccount ->
              val key =
                checkNotNull(MeasurementConsumerClientAccountKey.fromName(clientAccount.name)) {
                  "Invalid ClientAccount resource name: ${clientAccount.name}"
                }
              MeasurementConsumerKey(key.measurementConsumerId)
            }

          if (resolvedKeys.isEmpty()) {
            metrics.unmappedClientAccounts.add(1, metricAttributes())
            logger.info("No MeasurementConsumers found for reference ID: $refId")
          } else {
            logger.info(
              "ClientAccount reference ID $refId maps to ${resolvedKeys.size} " +
                "Measurement Consumers: ${resolvedKeys.map { it.toName() }}"
            )
          }

          resolvedKeys
        }

      measurementConsumerKeys.addAll(lookedUpKeys)
    }

    if (measurementConsumerKeys.isEmpty()) {
      metrics.unmappedEventGroups.add(1, metricAttributes())
      throw UnresolvedMeasurementConsumerException(
        "EventGroup ${eventGroup.eventGroupReferenceId} has neither measurementConsumer " +
          "nor clientAccountReferenceId, or both failed to resolve"
      )
    }

    return measurementConsumerKeys
  }

  /*
   * Deletes an EventGroup from CMMS.
   */
  private suspend fun deleteCmmsEventGroup(eventGroup: CmmsEventGroup) {
    val request = deleteEventGroupRequest { name = eventGroup.name }
    throttler.onReady { eventGroupsStub.deleteEventGroup(request) }
  }

  /*
   * Returns a copy of a [CmmsEventGroup] with information from an [EventGroup].
   * Used to determine if a CmmsEventGroup needs updating.
   */
  private fun updateEventGroup(
    existingEventGroup: CmmsEventGroup,
    eventGroup: EventGroup,
  ): CmmsEventGroup {
    return existingEventGroup.copy {
      measurementConsumer = eventGroup.measurementConsumer
      eventGroupReferenceId = eventGroup.eventGroupReferenceId
      this.eventGroupMetadata = eventGroup.toCmmsEventGroupMetadata()
      mediaTypes.clear()
      mediaTypes += eventGroup.mediaTypesList.map { it.toCmmsMediaType() }
      dataAvailabilityInterval = eventGroup.dataAvailabilityInterval
      if (eventGroup.hasEntityKey()) {
        this.entityKey = eventGroup.entityKey.toCmmsEntityKey()
      } else {
        clearEntityKey()
      }
    }
  }

  /**
   * Finds an existing CMMS event group matching the given event group, preferring entity key match
   * over reference ID match.
   */
  private fun findExistingEventGroup(
    eventGroup: EventGroup,
    syncedEventGroups: Map<EventGroupKey, CmmsEventGroup>,
    syncedEventGroupsByEntityKey: Map<EntityKeyId, CmmsEventGroup>,
  ): CmmsEventGroup? {
    if (eventGroup.hasEntityKey() && eventGroup.entityKey.entityId.isNotBlank()) {
      val entityKeyId =
        EntityKeyId(
          eventGroup.entityKey.entityType,
          eventGroup.entityKey.entityId,
          eventGroup.measurementConsumer,
        )
      syncedEventGroupsByEntityKey[entityKeyId]?.let {
        return it
      }
    }

    if (eventGroup.eventGroupReferenceId.isNotBlank()) {
      val eventGroupKey =
        EventGroupKey(eventGroup.eventGroupReferenceId, eventGroup.measurementConsumer)
      syncedEventGroups[eventGroupKey]?.let {
        return it
      }
    }

    return null
  }

  @OptIn(ExperimentalCoroutinesApi::class) // For `flattenConcat`.
  private fun fetchEventGroups(): Flow<CmmsEventGroup> {
    return eventGroupsStub
      .listResources { pageToken: String ->
        val response =
          try {
            throttler.onReady {
              eventGroupsStub.listEventGroups(
                listEventGroupsRequest {
                  parent = edpName
                  this.pageToken = pageToken
                  pageSize = listEventGroupPageSize
                  if (entityKeyTypes.isNotEmpty()) {
                    filter = listEventGroupsFilter { entityTypeIn += entityKeyTypes }
                  }
                }
              )
            }
          } catch (e: StatusException) {
            throw Exception("Error listing EventGroups", e)
          }
        ResourceList(response.eventGroupsList, response.nextPageToken)
      }
      .flattenConcat()
  }

  private fun MediaType.toCmmsMediaType(): CmmsMediaType {
    return when (this) {
      MediaType.MEDIA_TYPE_UNSPECIFIED -> error("Media type must be set")
      MediaType.VIDEO -> CmmsMediaType.VIDEO
      MediaType.DISPLAY -> CmmsMediaType.DISPLAY
      MediaType.OTHER -> CmmsMediaType.OTHER
      MediaType.NATIVE -> CmmsMediaType.NATIVE
      MediaType.UNRECOGNIZED -> error("Not a real media type")
    }
  }

  private fun EventGroup.toCmmsEventGroupMetadata(): CmmsEventGroupMetadataMessage {
    val source = this.eventGroupMetadata
    return cmmsEventGroupMetadata {
      this.adMetadata =
        CmmsEventGroupMetadataKt.adMetadata {
          this.campaignMetadata =
            CmmsAdMetadataKt.campaignMetadata {
              brandName = source.adMetadata.campaignMetadata.brand
              campaignName = source.adMetadata.campaignMetadata.campaign
            }
        }
      if (source.hasEntityMetadata()) {
        this.entityMetadata = source.entityMetadata
      }
    }
  }

  private fun EventGroup.EntityKey.toCmmsEntityKey(): CmmsEventGroup.EntityKey {
    val source = this
    return CmmsEventGroupKt.entityKey {
      entityType = source.entityType
      entityId = source.entityId
    }
  }

  companion object {
    /** Maximum number of sub-requests per BatchCreate/BatchUpdate EventGroups RPC. */
    private const val MAX_BATCH_SIZE = 50

    private val logger: Logger = Logger.getLogger(this::class.java.name)

    /*
     * Validates that event groups fields are populated
     * Throws exceptions for any invalid fields.
     */
    fun validateEventGroup(eventGroup: EventGroup) {
      check(eventGroup.mediaTypesList.size > 0) { "At least one media type must be set" }
      check(eventGroup.hasDataAvailabilityInterval()) { "Data availability must be set" }
      check(eventGroup.hasEventGroupMetadata()) { "Event Group Metadata must be set" }
      check(
        eventGroup.eventGroupReferenceId.isNotBlank() ||
          (eventGroup.hasEntityKey() && eventGroup.entityKey.entityId.isNotBlank())
      ) {
        "Either Event Group Reference Id or Entity Key with entity ID must be set"
      }
      if (eventGroup.measurementConsumer.isNotBlank()) {
        val mcKey = MeasurementConsumerKey.fromName(eventGroup.measurementConsumer)
        check(mcKey != null && mcKey.measurementConsumerId.isNotBlank()) {
          "Malformed measurement_consumer: ${eventGroup.measurementConsumer}"
        }
      } else {
        check(eventGroup.clientAccountReferenceId.isNotEmpty()) {
          "Either Measurement Consumer or Client Account Reference ID must be set"
        }
      }
    }

    /** Validates minimum fields required to identify and delete an EventGroup. */
    fun validateDeletedEventGroup(eventGroup: EventGroup) {
      check(eventGroup.hasEntityKey() && eventGroup.entityKey.entityId.isNotBlank()) {
        "Entity Key with entity ID must be set for deleted Event Groups"
      }
      check(eventGroup.measurementConsumer.isNotBlank()) {
        "Measurement Consumer must be set for deleted Event Groups"
      }
    }
  }
}
