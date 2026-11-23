// Copyright 2026 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.kingdom.deploy.gcloud.spanner.writers

import com.google.cloud.spanner.Value as SpannerValue
import com.google.protobuf.Struct
import com.google.protobuf.Value
import com.google.protobuf.struct
import com.google.protobuf.util.Timestamps
import com.google.type.Interval
import com.google.type.endTimeOrNull
import com.google.type.interval
import com.google.type.startTimeOrNull
import kotlinx.coroutines.flow.toList
import org.wfanet.measurement.api.Version
import org.wfanet.measurement.common.identity.InternalId
import org.wfanet.measurement.gcloud.common.toGcloudTimestamp
import org.wfanet.measurement.gcloud.spanner.appendClause
import org.wfanet.measurement.gcloud.spanner.bind
import org.wfanet.measurement.gcloud.spanner.bufferUpdateMutation
import org.wfanet.measurement.gcloud.spanner.set
import org.wfanet.measurement.gcloud.spanner.statement
import org.wfanet.measurement.gcloud.spanner.to
import org.wfanet.measurement.gcloud.spanner.toInt64
import org.wfanet.measurement.internal.kingdom.CreateEventGroupRequest
import org.wfanet.measurement.internal.kingdom.EventGroup
import org.wfanet.measurement.internal.kingdom.EventGroupHierarchyDetails
import org.wfanet.measurement.internal.kingdom.EventGroupHierarchyDetails.Level
import org.wfanet.measurement.internal.kingdom.EventGroupHierarchyState
import org.wfanet.measurement.internal.kingdom.EventGroupKt.entityKey
import org.wfanet.measurement.internal.kingdom.MediaType
import org.wfanet.measurement.internal.kingdom.copy
import org.wfanet.measurement.internal.kingdom.createEventGroupRequest
import org.wfanet.measurement.internal.kingdom.eventGroup
import org.wfanet.measurement.internal.kingdom.eventGroupDetails
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.EventGroupHierarchyConflictException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.EventGroupHierarchyPath
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.InvalidFieldValueException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.metadatastaging.MetadataPublicationSequences
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.metadatastaging.MetadataWriteGuard
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.EventGroupReader
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.EventGroupReader.HierarchyKey

/** Maintains hierarchy paths and ancestor rollups in a single transaction. */
internal class EventGroupHierarchyWriter(
  private val transaction: SpannerWriter.TransactionScope,
  private val dataProviderId: InternalId,
) {
  data class Creation(
    val request: CreateEventGroupRequest,
    val measurementConsumerId: InternalId,
    val batchIndex: Int? = null,
  ) {
    fun eventGroupFieldPath(fieldName: String): String {
      val batchPrefix = batchIndex?.let { "requests.$it." }.orEmpty()
      return "${batchPrefix}event_group.$fieldName"
    }
  }

  private data class PathEntry(
    val key: HierarchyKey,
    val path: String,
    val depth: Int,
    val metadata: Struct,
    val comparisonKeys: Set<String>,
  )

  private class Ancestor(val entry: PathEntry, val externalMeasurementConsumerId: Long) {
    val mediaTypes = mutableSetOf<MediaType>()
    var availability: Interval = Interval.getDefaultInstance()
  }

  suspend fun create(creations: List<Creation>): List<EventGroup> {
    val activeHierarchies =
      if (creations.any { it.request.eventGroup.entityKey.canBelongToHierarchy() }) {
        readActiveHierarchyLevelsByType()
      } else {
        emptyMap()
      }
    val paths = creations.map { creation -> derivePath(creation, activeHierarchies) }
    for ((consumer, entries) in creations.withIndex().groupBy { it.value.measurementConsumerId }) {
      MetadataWriteGuard.checkAvailable(
        transaction.transactionContext,
        dataProviderId.value,
        consumer.value,
        entries.mapNotNull { paths[it.index].lastOrNull()?.path }.toSet(),
      )
    }
    val requestedLeaves = paths.mapNotNull { it.lastOrNull()?.key }.toSet()
    val ancestors = collectAncestors(creations, paths, requestedLeaves)
    val existing =
      EventGroupReader()
        .readHierarchyMembers(
          transaction.transactionContext,
          dataProviderId,
          requestedLeaves + ancestors.keys,
        )

    for (key in requestedLeaves) {
      if (existing[key]?.isLeaf == false) {
        throw EventGroupHierarchyConflictException(
          "A derived ancestor already occupies the requested EventGroup's entity key."
        )
      }
    }
    for ((key, ancestor) in ancestors) {
      val stored = existing[key] ?: continue
      if (stored.isLeaf || stored.eventGroup.state != EventGroup.State.ACTIVE) {
        throw EventGroupHierarchyConflictException(
          "An ordinary or inactive EventGroup already occupies a requested ancestor's entity key."
        )
      }
      if (
        stored.entityPath != ancestor.entry.path ||
          ancestor.entry.comparisonKeys.any {
            stored.eventGroup.entityMetadata.fieldsMap[it] != ancestor.entry.metadata.fieldsMap[it]
          }
      ) {
        throw EventGroupHierarchyConflictException(
          "A requested ancestor's path or metadata conflicts with the stored ancestor."
        )
      }
    }

    for (ancestor in ancestors.values.sortedBy { it.entry.depth }) {
      val stored = existing[ancestor.entry.key]
      if (stored == null) {
        insertAncestor(ancestor)
      } else {
        updateAncestorRollup(ancestor, stored)
      }
    }
    return creations.mapIndexed { index, creation ->
      transaction.createEventGroup(
        dataProviderId,
        creation.measurementConsumerId,
        creation.request,
        entityPath = paths[index].lastOrNull()?.path,
      )
    }
  }

  data class Update(val request: EventGroup, val stored: EventGroupReader.Result)

  private data class ScopedPath(val consumer: InternalId, val path: String)

  suspend fun update(updates: List<Update>) {
    if (updates.isEmpty()) return
    for (update in updates) update.stored.validateUpdate(update.request)
    val activeHierarchies = readActiveHierarchyLevelsByType()
    val creations =
      updates.map { update ->
        Creation(
          createEventGroupRequest { eventGroup = update.request },
          update.stored.internalMeasurementConsumerId,
        )
      }
    val paths = creations.map { derivePath(it, activeHierarchies) }
    for ((index, update) in updates.withIndex()) {
      val stored = update.stored
      val oldMetadata = stored.eventGroup.entityMetadata.fieldsMap
      val metadata = update.request.entityMetadata.fieldsMap
      for (level in activeHierarchies[stored.eventGroup.entityKey.entityType].orEmpty()) {
        if (level.entityType == stored.eventGroup.entityKey.entityType) continue
        if (
          oldMetadata[level.idEntityMetadataKey] == metadata[level.idEntityMetadataKey] &&
            level.entityMetadataKeysList.any { oldMetadata[it] != metadata[it] }
        ) {
          throw EventGroupHierarchyConflictException(
            "Metadata owned by another hierarchy level cannot be changed."
          )
        }
      }
      MetadataWriteGuard.checkAvailable(
        transaction.transactionContext,
        dataProviderId.value,
        stored.internalMeasurementConsumerId.value,
        setOfNotNull(stored.entityPath, paths[index].lastOrNull()?.path),
      )
    }
    val ancestors =
      collectAncestors(creations, paths, paths.mapNotNull { it.lastOrNull()?.key }.toSet())
    val existing =
      EventGroupReader()
        .readHierarchyMembers(transaction.transactionContext, dataProviderId, ancestors.keys)
    for ((key, ancestor) in ancestors) {
      val stored = existing[key] ?: continue
      if (
        stored.isLeaf ||
          stored.entityPath != ancestor.entry.path ||
          ancestor.entry.comparisonKeys.any {
            stored.eventGroup.entityMetadata.fieldsMap[it] != ancestor.entry.metadata.fieldsMap[it]
          }
      ) {
        throw EventGroupHierarchyConflictException(
          "Destination ancestor metadata conflicts with the requested path."
        )
      }
    }
    val versions =
      updates
        .map { it.stored.internalMeasurementConsumerId }
        .toSet()
        .associateWith {
          MetadataPublicationSequences.next(
            transaction.transactionContext,
            dataProviderId.value,
            it.value,
          )
        }
    val replacements = mutableMapOf<ScopedPath, EventGroup?>()
    for (update in updates) {
      val oldPath = update.stored.entityPath
      if (oldPath != null) {
        replacements[ScopedPath(update.stored.internalMeasurementConsumerId, oldPath)] = null
      }
    }
    for ((index, update) in updates.withIndex()) {
      val consumer = update.stored.internalMeasurementConsumerId
      val newPath = paths[index].lastOrNull()?.path
      if (newPath != null) replacements[ScopedPath(consumer, newPath)] = update.request
      transaction.updateEventGroup(
        update.request,
        update.stored,
        newPath,
        versions.getValue(consumer),
      )
    }
    reconcileAncestors(replacements, ancestors)
  }

  suspend fun delete(stored: EventGroupReader.Result) {
    if (!stored.isLeaf)
      throw EventGroupHierarchyConflictException("A derived ancestor cannot be deleted.")
    MetadataWriteGuard.checkAvailable(
      transaction.transactionContext,
      dataProviderId.value,
      stored.internalMeasurementConsumerId.value,
      setOfNotNull(stored.entityPath),
    )
    val path = stored.entityPath
    if (path != null) {
      reconcileAncestors(
        mapOf(ScopedPath(stored.internalMeasurementConsumerId, path) to null),
        emptyMap(),
      )
    }
  }

  private suspend fun reconcileAncestors(
    replacements: Map<ScopedPath, EventGroup?>,
    newAncestors: Map<HierarchyKey, Ancestor>,
  ) {
    val pending = replacements.toMutableMap()
    val ancestorPaths =
      replacements.keys
        .flatMap { scoped ->
          val segments = scoped.path.trimEnd('/').split('/')
          (2 until segments.size step 2).map {
            ScopedPath(scoped.consumer, segments.take(it).joinToString("/", postfix = "/"))
          }
        }
        .toSet()
    val keysByPath =
      ancestorPaths.associateWith { scoped ->
        val segments = scoped.path.trimEnd('/').split('/')
        HierarchyKey(
          scoped.consumer,
          entityKey {
            entityType = segments[segments.size - 2]
            entityId = segments.last()
          },
        )
      }
    val existing =
      EventGroupReader()
        .readHierarchyMembers(
          transaction.transactionContext,
          dataProviderId,
          keysByPath.values.toSet(),
        )
    for (scoped in ancestorPaths.sortedByDescending { it.path.count { c -> c == '/' } }) {
      val key = keysByPath.getValue(scoped)
      val stored = existing[key]
      val created = newAncestors[key]
      check(stored != null || created != null) { "Ancestor missing from the stored hierarchy" }
      val children =
        EventGroupReader()
          .fillStatementBuilder {
            appendClause(
              """
              WHERE EventGroups.DataProviderId = @provider
                AND EventGroups.MeasurementConsumerId = @consumer
                AND EventGroups.State != @deleted
                AND STARTS_WITH(EventGroups.EntityPath, @path)
                AND ARRAY_LENGTH(SPLIT(EventGroups.EntityPath, '/')) = @segments
              """
                .trimIndent()
            )
            bind("provider").to(dataProviderId.value)
            bind("consumer").to(scoped.consumer.value)
            bind("deleted").to(EventGroup.State.DELETED.number.toLong())
            bind("path").to(scoped.path)
            bind("segments").to(scoped.path.count { it == '/' }.toLong() + 3L)
          }
          .execute(transaction.transactionContext)
          .toList()
          .associate { ScopedPath(scoped.consumer, checkNotNull(it.entityPath)) to it.eventGroup }
          .toMutableMap()
      for ((path, replacement) in pending) {
        if (path.consumer != scoped.consumer || parentPath(path.path) != scoped.path) continue
        children.remove(path)
        if (replacement != null) children[path] = replacement
      }
      if (stored == null) {
        checkNotNull(created)
        created.mediaTypes.clear()
        created.mediaTypes.addAll(children.values.flatMap { it.mediaTypesList })
        created.availability =
          children.values.fold(Interval.getDefaultInstance()) { span, child ->
            span.span(child.dataAvailabilityInterval)
          }
        insertAncestor(created)
        pending[scoped] = eventGroup {
          mediaTypes += created.mediaTypes
          dataAvailabilityInterval = created.availability
        }
        continue
      }
      val updated =
        stored.eventGroup.copy {
          if (children.isNotEmpty()) {
            mediaTypes.clear()
            mediaTypes += children.values.flatMap { it.mediaTypesList }.toSet()
            dataAvailabilityInterval =
              children.values.fold(Interval.getDefaultInstance()) { span, child ->
                span.span(child.dataAvailabilityInterval)
              }
          }
        }
      transaction.transactionContext.bufferUpdateMutation(Table.EVENT_GROUPS) {
        set("DataProviderId").to(dataProviderId)
        set("EventGroupId").to(stored.internalEventGroupId)
        set("IsLeaf").to(children.isEmpty())
        set("UpdateTime").to(SpannerValue.COMMIT_TIMESTAMP)
        set("DataAvailabilityStartTime")
          .to(updated.dataAvailabilityInterval.startTimeOrNull?.toGcloudTimestamp())
        set("DataAvailabilityEndTime")
          .to(updated.dataAvailabilityInterval.endTimeOrNull?.toGcloudTimestamp())
      }
      transaction.transactionContext.syncMediaTypes(
        dataProviderId,
        stored.internalEventGroupId,
        updated.mediaTypesList.toSet(),
      )
      pending[scoped] = updated
    }
  }

  private fun parentPath(path: String): String =
    path.trimEnd('/').split('/').dropLast(2).joinToString("/", postfix = "/")

  private suspend fun readActiveHierarchyLevelsByType(): Map<String, List<Level>> {
    val query =
      statement(
        """
      SELECT EventGroupHierarchyDetails
      FROM EventGroupHierarchies
      WHERE DataProviderId = @dataProviderId AND State = @state
      """
          .trimIndent()
      ) {
        bind("dataProviderId" to dataProviderId)
        bind("state").toInt64(EventGroupHierarchyState.ACTIVE)
      }
    return buildMap {
      transaction.transactionContext.executeQuery(query).collect { row ->
        val levels =
          row
            .getProtoMessage(
              "EventGroupHierarchyDetails",
              EventGroupHierarchyDetails.getDefaultInstance(),
            )
            .levelsList
        for (level in levels) {
          check(URL_SAFE_ID.matches(level.entityType)) {
            "A hierarchy entity type must be URL-safe."
          }
          check(put(level.entityType, levels) == null) {
            "An entity type occurs more than once in the active hierarchy configuration."
          }
        }
      }
    }
  }

  private fun derivePath(
    creation: Creation,
    activeHierarchies: Map<String, List<Level>>,
  ): List<PathEntry> {
    val eventGroup = creation.request.eventGroup
    val requestKey = eventGroup.entityKey
    if (!requestKey.canBelongToHierarchy()) return emptyList()
    val levels = activeHierarchies[requestKey.entityType] ?: return emptyList()
    if (requestKey.entityId.isEmpty()) {
      throw InvalidFieldValueException(creation.eventGroupFieldPath("entity_key.entity_id")) {
        "$it is required for EventGroups in an active hierarchy."
      }
    }
    val metadata = eventGroup.entityMetadata.fieldsMap
    val metadataFieldPath = creation.eventGroupFieldPath("entity_metadata")
    val levelIndex = levels.indexOfFirst { it.entityType == requestKey.entityType }
    val path = StringBuilder()
    val idsByMetadataKey = mutableMapOf<String, Value>()
    val inheritedMetadata = mutableMapOf<String, Value>()
    return levels.take(levelIndex + 1).mapIndexed { index, level ->
      val metadataKey = level.idEntityMetadataKey
      val id = metadata[metadataKey]
      if (
        id == null ||
          id.kindCase != Value.KindCase.STRING_VALUE ||
          !URL_SAFE_ID.matches(id.stringValue)
      ) {
        throw InvalidFieldValueException(metadataFieldPath) {
          "$it must contain a URL-safe string for the hierarchy ID key '$metadataKey'."
        }
      }
      if (index == levelIndex && id.stringValue != requestKey.entityId) {
        throw InvalidFieldValueException(metadataFieldPath) {
          "$it must provide the request's entity ID at hierarchy ID key '$metadataKey'."
        }
      }
      val memberKey = entityKey {
        entityType = level.entityType
        entityId = id.stringValue
      }
      path.append(memberKey.entityType).append('/').append(memberKey.entityId).append('/')
      val entityPath = path.toString()
      if (!EventGroupHierarchyPath.fitsIndexKey(memberKey.entityType, entityPath)) {
        throw InvalidFieldValueException(metadataFieldPath) {
          "$it specifies a hierarchy path that exceeds the Spanner index key size limit."
        }
      }
      idsByMetadataKey[metadataKey] = id
      inheritedMetadata[metadataKey] = id
      for (key in level.entityMetadataKeysList) {
        val value = metadata[key]
        if (value != null) inheritedMetadata[key] = value
      }
      PathEntry(
        HierarchyKey(creation.measurementConsumerId, memberKey),
        entityPath,
        index,
        struct { fields.putAll(inheritedMetadata) },
        idsByMetadataKey.keys + level.entityMetadataKeysList,
      )
    }
  }

  private fun EventGroup.EntityKey.canBelongToHierarchy(): Boolean {
    return entityType.isNotEmpty() &&
      (entityType != Table.DEFAULT_ENTITY_TYPE || entityId.isNotEmpty())
  }

  private fun collectAncestors(
    creations: List<Creation>,
    paths: List<List<PathEntry>>,
    requestedLeaves: Set<HierarchyKey>,
  ): Map<HierarchyKey, Ancestor> {
    val ancestors = mutableMapOf<HierarchyKey, Ancestor>()
    for ((index, creation) in creations.withIndex()) {
      val eventGroup = creation.request.eventGroup
      for (entry in paths[index].dropLast(1)) {
        if (entry.key in requestedLeaves) {
          throw EventGroupHierarchyConflictException(
            "A requested EventGroup cannot also be created as a derived ancestor."
          )
        }
        val ancestor =
          ancestors.getOrPut(entry.key) {
            Ancestor(entry, eventGroup.externalMeasurementConsumerId)
          }
        if (ancestor.entry.path != entry.path || ancestor.entry.metadata != entry.metadata) {
          throw InvalidFieldValueException(creation.eventGroupFieldPath("entity_metadata")) {
            "$it conflicts with another request's path or metadata for a shared ancestor."
          }
        }
        ancestor.mediaTypes.addAll(eventGroup.mediaTypesList)
        ancestor.availability = ancestor.availability.span(eventGroup.dataAvailabilityInterval)
      }
    }
    return ancestors
  }

  private fun insertAncestor(ancestor: Ancestor) {
    val request = createEventGroupRequest {
      eventGroup = eventGroup {
        externalMeasurementConsumerId = ancestor.externalMeasurementConsumerId
        entityKey = ancestor.entry.key.entityKey
        entityMetadata = ancestor.entry.metadata
        details = eventGroupDetails { apiVersion = Version.V2_ALPHA.string }
        mediaTypes += ancestor.mediaTypes
        if (ancestor.availability.hasStartTime()) {
          dataAvailabilityInterval = ancestor.availability
        }
      }
    }
    transaction.createEventGroup(
      dataProviderId,
      ancestor.entry.key.measurementConsumerId,
      request,
      entityPath = ancestor.entry.path,
      isLeaf = false,
    )
  }

  private fun updateAncestorRollup(ancestor: Ancestor, stored: EventGroupReader.Result) {
    val newMediaTypes = ancestor.mediaTypes - stored.eventGroup.mediaTypesList.toSet()
    val oldAvailability = stored.eventGroup.dataAvailabilityInterval
    val availability = oldAvailability.span(ancestor.availability)
    if (newMediaTypes.isEmpty() && availability == oldAvailability) return

    transaction.transactionContext.bufferUpdateMutation(Table.EVENT_GROUPS) {
      set("DataProviderId").to(dataProviderId)
      set("EventGroupId").to(stored.internalEventGroupId)
      set("UpdateTime").to(SpannerValue.COMMIT_TIMESTAMP)
      set("DataAvailabilityStartTime").to(availability.startTimeOrNull?.toGcloudTimestamp())
      set("DataAvailabilityEndTime").to(availability.endTimeOrNull?.toGcloudTimestamp())
    }
    transaction.insertEventGroupMediaTypes(
      dataProviderId,
      stored.internalEventGroupId,
      newMediaTypes,
    )
  }

  private fun Interval.span(other: Interval): Interval {
    if (!other.hasStartTime()) return this
    if (!hasStartTime()) return other
    return interval {
      startTime = minOf(this@span.startTime, other.startTime, Timestamps.comparator())
      if (this@span.hasEndTime() && other.hasEndTime()) {
        endTime = maxOf(this@span.endTime, other.endTime, Timestamps.comparator())
      }
    }
  }

  companion object {
    private val URL_SAFE_ID = Regex("[A-Za-z0-9._~-]+")
  }
}
