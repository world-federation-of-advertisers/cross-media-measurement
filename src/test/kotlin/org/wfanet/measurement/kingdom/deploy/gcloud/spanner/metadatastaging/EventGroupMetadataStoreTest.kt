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

package org.wfanet.measurement.kingdom.deploy.gcloud.spanner.metadatastaging

import com.google.cloud.ByteArray
import com.google.cloud.Timestamp
import com.google.cloud.spanner.ErrorCode
import com.google.cloud.spanner.Key
import com.google.cloud.spanner.Mutation
import com.google.cloud.spanner.SpannerException
import com.google.cloud.spanner.Statement
import com.google.cloud.spanner.Value as SpannerValue
import com.google.common.truth.Truth.assertThat
import com.google.common.truth.extensions.proto.ProtoTruth.assertThat
import com.google.protobuf.NullValue
import com.google.protobuf.Struct
import com.google.protobuf.listValue
import com.google.protobuf.struct
import com.google.protobuf.value
import io.grpc.Status
import io.grpc.StatusRuntimeException
import kotlin.test.assertFailsWith
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import org.junit.ClassRule
import org.junit.Rule
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.gcloud.spanner.testing.SpannerEmulatorDatabaseRule
import org.wfanet.measurement.gcloud.spanner.testing.SpannerEmulatorRule
import org.wfanet.measurement.internal.kingdom.EventGroup
import org.wfanet.measurement.internal.kingdom.MeasurementConsumerDetails
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.metadatastaging.EventGroupMetadataStore.Progress
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.metadatastaging.EventGroupMetadataStore.Request
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.metadatastaging.EventGroupMetadataStore.State
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.testing.Schemata

@RunWith(JUnit4::class)
class EventGroupMetadataStoreTest {
  @get:Rule
  val database = SpannerEmulatorDatabaseRule(spannerEmulator, Schemata.KINGDOM_CHANGELOG_PATH)

  @Test
  fun `stageNextBatch commits bounded batches and resumes without changing live metadata`() =
    runBlocking<Unit> {
      seedEventGroups(*(1L..123L).map { eventGroup(it) }.toTypedArray())
      val original = readMetadata("EventGroups", "EntityMetadata")
      val firstWorker = EventGroupMetadataStore(database.databaseClient)
      val work = firstWorker.start(REQUEST)
      assertThat(firstWorker.stageNextBatch(work, 40)).isEqualTo(Progress(40, false))
      assertThat(readMetadata("EventGroupMetadataVersions", "Metadata")).hasSize(40)
      assertThat(readMetadata("EventGroups", "EntityMetadata")).isEqualTo(original)

      val restartedWorker = EventGroupMetadataStore(database.databaseClient)
      for ((count, complete) in listOf(80L to false, 120L to false, 123L to true)) {
        assertThat(restartedWorker.stageNextBatch(work, 40)).isEqualTo(Progress(count, complete))
        assertThat(readMetadata("EventGroupMetadataVersions", "Metadata")).hasSize(count.toInt())
        assertThat(readMetadata("EventGroups", "EntityMetadata")).isEqualTo(original)
      }
      assertThat(restartedWorker.stageNextBatch(work, 40)).isEqualTo(Progress(123, true))
      assertThat(readMetadata("EventGroupMetadataVersions", "Metadata").values)
        .containsExactlyElementsIn((1L..123L).map { expectedMetadata(it) })
    }

  @Test
  fun `stageNextBatch includes the ancestor but excludes other scopes siblings and deleted rows`() =
    runBlocking<Unit> {
      seedEventGroups(
        eventGroup(1, path = REQUEST.entityPath, isLeaf = false),
        eventGroup(2),
        eventGroup(3, dataProviderId = 2),
        eventGroup(4, measurementConsumerId = 2),
        eventGroup(5, path = "account/X2/campaign/5/"),
        eventGroup(6, state = EventGroup.State.DELETED),
        eventGroup(7, path = null),
      )
      val worker = EventGroupMetadataStore(database.databaseClient)
      val work = worker.start(REQUEST)
      assertThat(worker.stageNextBatch(work, 40)).isEqualTo(Progress(2, true))
      assertThat(readMetadata("EventGroupMetadataVersions", "Metadata"))
        .containsExactly(1L, expectedMetadata(1), 2L, expectedMetadata(2))
      worker.publish(work)
      database.databaseClient.readOnlyTransaction().use { context ->
        val reader = PublishedMetadataReader(context, 1, 1)
        assertThat(reader.list().keys).containsExactly(1L, 2L, 5L, 7L)
        assertThat(reader.search("new"))
          .containsExactly(1L, expectedMetadata(1), 2L, expectedMetadata(2))
        assertThat(reader.search("old").keys).containsExactly(5L, 7L)
        assertThat(reader.get(3)).isNull()
        assertThat(reader.get(6)).isNull()
        assertThat(PublishedMetadataReader(context, 2, 1).search("old").keys).containsExactly(3L)
        assertThat(PublishedMetadataReader(context, 1, 2).search("old").keys).containsExactly(4L)
      }
    }

  @Test
  fun `stageNextBatch completes when the last batch fills the limit exactly`() =
    runBlocking<Unit> {
      seedEventGroups(eventGroup(1), eventGroup(2))
      val worker = EventGroupMetadataStore(database.databaseClient)
      val work = worker.start(REQUEST)
      assertThat(worker.stageNextBatch(work, 2)).isEqualTo(Progress(2, false))
      assertThat(worker.stageNextBatch(work, 2)).isEqualTo(Progress(2, true))
      assertThat(readMetadata("EventGroupMetadataVersions", "Metadata")).hasSize(2)
    }

  @Test
  fun `stageNextBatch rolls back the batch and checkpoint when a staged insert fails`() =
    runBlocking<Unit> {
      seedEventGroups(eventGroup(1), eventGroup(2))
      val worker = EventGroupMetadataStore(database.databaseClient)
      val work = worker.start(REQUEST)
      database.databaseClient.write(
        Mutation.newInsertBuilder("EventGroupMetadataVersions")
          .apply {
            set("OperationId").to(REQUEST.operationId)
            set("EventGroupId").to(2L)
            set("Metadata").to(SpannerValue.protoMessage(Struct.getDefaultInstance()))
          }
          .build()
      )

      val exception = assertFailsWith<SpannerException> { worker.stageNextBatch(work, 2) }
      assertThat(exception.errorCode).isEqualTo(ErrorCode.ALREADY_EXISTS)
      assertThat(readMetadata("EventGroupMetadataVersions", "Metadata").keys).containsExactly(2L)

      database.databaseClient.write(
        Mutation.delete("EventGroupMetadataVersions", Key.of(REQUEST.operationId, 2L))
      )
      val restartedWorker = EventGroupMetadataStore(database.databaseClient)
      assertThat(restartedWorker.stageNextBatch(work, 2)).isEqualTo(Progress(2, false))
      assertThat(readMetadata("EventGroupMetadataVersions", "Metadata"))
        .containsExactly(1L, expectedMetadata(1), 2L, expectedMetadata(2))
    }

  @Test
  fun `stageNextBatch completes an empty subtree`() =
    runBlocking<Unit> {
      val worker = EventGroupMetadataStore(database.databaseClient)
      val work = worker.start(REQUEST)
      assertThat(worker.stageNextBatch(work, 40)).isEqualTo(Progress(0, true))
      assertThat(readMetadata("EventGroupMetadataVersions", "Metadata")).isEmpty()
    }

  @Test
  fun `start rejects replacement keys not owned by the ancestor`() =
    runBlocking<Unit> {
      val worker = EventGroupMetadataStore(database.databaseClient)
      assertFailsWith<IllegalArgumentException> {
        worker.start(
          REQUEST.copy(
            metadata = struct { fields["campaign_name"] = value { stringValue = "invalid" } }
          )
        )
      }
    }

  @Test
  fun `publish switches Get List and search only after all batches are staged`() =
    runBlocking<Unit> {
      seedEventGroups(*(1L..123L).map { eventGroup(it) }.toTypedArray())
      val old = readMetadata("EventGroups", "EntityMetadata")
      val worker = EventGroupMetadataStore(database.databaseClient)
      val work = worker.start(REQUEST)
      assertViews(old, "old", "new")
      for (count in listOf(40L, 80L, 120L, 123L)) {
        assertThat(worker.stageNextBatch(work, 40).stagedCount).isEqualTo(count)
        assertViews(old, "old", "new")
      }

      assertThat(worker.publish(work)).isEqualTo(1L)
      assertViews((1L..123L).associateWith { expectedMetadata(it) }, "new", "old")
      assertThat(readMetadata("EventGroups", "EntityMetadata")).isEqualTo(old)
      assertThat(worker.publish(work)).isEqualTo(1L)
    }

  @Test
  fun `publish rejects incomplete staging without exposing any staged rows`() =
    runBlocking<Unit> {
      seedEventGroups(eventGroup(1), eventGroup(2))
      val old = readMetadata("EventGroups", "EntityMetadata")
      val worker = EventGroupMetadataStore(database.databaseClient)
      val work = worker.start(REQUEST)
      worker.stageNextBatch(work, 1)
      assertFailsWith<IllegalStateException> { worker.publish(work) }
      assertViews(old, "old", "new")
    }

  @Test
  fun `publish leaves existing read snapshots on the old version`() =
    runBlocking<Unit> {
      seedEventGroups(eventGroup(1), eventGroup(2))
      val old = readMetadata("EventGroups", "EntityMetadata")
      val worker = EventGroupMetadataStore(database.databaseClient)
      val work = worker.start(REQUEST)
      worker.stageNextBatch(work, 40)
      database.databaseClient.readOnlyTransaction().use { context ->
        val reader = PublishedMetadataReader(context, 1, 1)
        assertThat(reader.list()).isEqualTo(old)
        worker.publish(work)
        assertThat(reader.list()).isEqualTo(old)
        assertThat(reader.get(1)).isEqualTo(old.getValue(1))
        assertThat(reader.search("old")).isEqualTo(old)
        assertThat(reader.search("new")).isEmpty()
      }
      assertViews((1L..2L).associateWith { expectedMetadata(it) }, "new", "old")
    }

  @Test
  fun `publish preserves prior updates and hides superseded search documents`() =
    runBlocking<Unit> {
      seedEventGroups(eventGroup(1), eventGroup(2))
      val worker = EventGroupMetadataStore(database.databaseClient)
      val work = worker.start(REQUEST)
      worker.stageNextBatch(work, 40)
      worker.publish(work)
      val child =
        REQUEST.copy(
          operationId = "child-update",
          entityPath = "account/X/campaign/1/",
          ownedKeys = setOf("campaign_name"),
          metadata = struct { fields["campaign_name"] = value { stringValue = "refreshed" } },
        )
      val childWork = worker.start(child)
      worker.stageNextBatch(childWork, 40)
      worker.publish(childWork)
      val latest =
        REQUEST.copy(
          operationId = "latest-update",
          metadata = struct { fields["account_name"] = value { stringValue = "final" } },
        )
      val latestWork = worker.start(latest)
      worker.stageNextBatch(latestWork, 40)
      worker.publish(latestWork)
      val expected =
        (1L..2L).associateWith { id ->
          struct {
            fields.putAll(expectedMetadata(id).fieldsMap)
            fields.putAll(latest.metadata.fieldsMap)
            if (id == 1L) fields.putAll(child.metadata.fieldsMap)
          }
        }
      assertViews(expected, "final", "new")
      database.databaseClient.readOnlyTransaction().use { context ->
        val reader = PublishedMetadataReader(context, 1, 1)
        assertThat(reader.search("old")).isEmpty()
        assertThat(reader.search("final refreshed")).containsExactly(1L, expected.getValue(1))
        assertThat(reader.search("campaign")).containsExactly(2L, expected.getValue(2))
      }
    }

  @Test
  fun `start rejects overlapping operations but permits disjoint publications`() =
    runBlocking<Unit> {
      seedEventGroups(eventGroup(1), eventGroup(2, path = "account/X2/campaign/2/"))
      val worker = EventGroupMetadataStore(database.databaseClient)
      val first = worker.start(REQUEST)
      for (path in listOf(REQUEST.entityPath, "account/", "account/X/campaign/1/")) {
        assertAborted { worker.start(REQUEST.copy(operationId = "overlap", entityPath = path)) }
      }
      val second = worker.start(REQUEST.copy(operationId = "sibling", entityPath = "account/X2/"))
      worker.stageNextBatch(first, 40)
      worker.stageNextBatch(second, 40)
      assertThat(worker.publish(first)).isEqualTo(1L)
      assertThat(worker.publish(second)).isEqualTo(2L)
      assertViews((1L..2L).associateWith { expectedMetadata(it) }, "new", "old")
    }

  @Test
  fun `search publishes nested values without indexing metadata keys`() =
    runBlocking<Unit> {
      seedEventGroups(eventGroup(1))
      val worker = EventGroupMetadataStore(database.databaseClient)
      val request =
        REQUEST.copy(
          ownedKeys = setOf("secretkey"),
          metadata =
            struct {
              fields["secretkey"] = value {
                structValue = struct {
                  fields["nestedkey"] = value {
                    listValue = listValue {
                      values += value { stringValue = "emerald" }
                      values += value { numberValue = 42.0 }
                      values += value { boolValue = true }
                      values += value { nullValue = NullValue.NULL_VALUE }
                    }
                  }
                }
              }
            },
        )
      val work = worker.start(request)
      worker.stageNextBatch(work, 40)
      database.databaseClient.singleUse().use { context ->
        assertThat(PublishedMetadataReader(context, 1, 1).search("emerald")).isEmpty()
      }
      worker.publish(work)
      database.databaseClient.readOnlyTransaction().use { context ->
        val reader = PublishedMetadataReader(context, 1, 1)
        for (query in listOf("emerald", "42", "true", "emerald campaign")) {
          assertThat(reader.search(query).keys).containsExactly(1L)
        }
        for (query in listOf("secretkey", "nestedkey", "null")) {
          assertThat(reader.search(query)).isEmpty()
        }
      }
    }

  @Test
  fun `write rejects create update delete and both directions of a move in a locked subtree`() =
    runBlocking<Unit> {
      seedEventGroups(eventGroup(1), eventGroup(2, path = "account/X2/campaign/2/"))
      val original = readMetadata("EventGroups", "EntityMetadata")
      val worker = EventGroupMetadataStore(database.databaseClient)
      worker.start(REQUEST)
      val guard = MetadataWriteGuard(database.databaseClient)
      assertAborted {
        guard.write(1, 1, emptySet(), setOf("account/X/campaign/3/")) { it.buffer(eventGroup(3)) }
      }
      assertAborted {
        guard.write(1, 1, setOf(1L)) { transaction ->
          transaction.buffer(
            Mutation.newUpdateBuilder("EventGroups")
              .apply {
                set("DataProviderId").to(1L)
                set("EventGroupId").to(1L)
                set("EntityMetadata").to(SpannerValue.protoMessage(Struct.getDefaultInstance()))
              }
              .build()
          )
        }
      }
      assertAborted {
        guard.write(1, 1, setOf(1L)) { it.buffer(Mutation.delete("EventGroups", Key.of(1L, 1L))) }
      }
      for ((id, destination) in
        mapOf(1L to "account/X2/campaign/1/", 2L to "account/X/campaign/2/")) {
        assertAborted {
          guard.write(1, 1, setOf(id), setOf(destination)) { transaction ->
            transaction.buffer(
              Mutation.newUpdateBuilder("EventGroups")
                .apply {
                  set("DataProviderId").to(1L)
                  set("EventGroupId").to(id)
                  set("EntityPath").to(destination)
                }
                .build()
            )
          }
        }
      }
      assertViews(original, "old", "new")
      guard.write(1, 1, emptySet(), setOf("account/X2/campaign/3/")) {
        it.buffer(eventGroup(3, path = "account/X2/campaign/3/"))
      }
      assertThat(readMetadata("EventGroups", "EntityMetadata").keys).containsExactly(1L, 2L, 3L)
    }

  @Test
  fun `write guard permits independent providers and consumers`() =
    runBlocking<Unit> {
      seedEventGroups(
        eventGroup(1),
        eventGroup(2, dataProviderId = 2),
        eventGroup(3, measurementConsumerId = 2),
      )
      EventGroupMetadataStore(database.databaseClient).start(REQUEST)
      val guard = MetadataWriteGuard(database.databaseClient)
      guard.write(2, 1, setOf(2L)) { it.buffer(Mutation.delete("EventGroups", Key.of(2L, 2L))) }
      guard.write(1, 2, setOf(3L)) { it.buffer(Mutation.delete("EventGroups", Key.of(1L, 3L))) }
      assertThat(readMetadata("EventGroups", "EntityMetadata").keys).containsExactly(1L)
    }

  @Test
  fun `hierarchy changes and metadata operations exclude each other across consumers`() =
    runBlocking<Unit> {
      val worker = EventGroupMetadataStore(database.databaseClient)
      val guard = MetadataWriteGuard(database.databaseClient)
      worker.start(REQUEST.copy(measurementConsumerId = 2))
      assertAborted { guard.beginHierarchyChange(1, "activation") }
      assertThat(worker.cancel(REQUEST.operationId)).isEqualTo(State.CANCELLED)
      guard.beginHierarchyChange(1, "activation")
      assertAborted { worker.start(REQUEST.copy(operationId = "blocked")) }
      assertAborted { guard.beginHierarchyChange(1, "configuration") }
      assertAborted {
        guard.write(1, 1, emptySet(), setOf("account/X2/campaign/1/")) {
          error("Guard allowed a write")
        }
      }
      val independent =
        worker.start(REQUEST.copy(operationId = "other-provider", dataProviderId = 2))
      worker.cancel(independent.operationId)
      guard.finishHierarchyChange("activation")
      guard.finishHierarchyChange("activation")
      worker.start(REQUEST.copy(operationId = "after-activation"))
    }

  @Test
  fun `cancel preserves old reads releases the lock and rejects stale work`() =
    runBlocking<Unit> {
      seedEventGroups(eventGroup(1), eventGroup(2))
      val original = readMetadata("EventGroups", "EntityMetadata")
      val worker = EventGroupMetadataStore(database.databaseClient)
      val work = worker.start(REQUEST)
      worker.stageNextBatch(work, 1)
      val restarted = EventGroupMetadataStore(database.databaseClient)
      assertThat(restarted.cancel(work.operationId)).isEqualTo(State.CANCELLED)
      assertThat(restarted.cancel(work.operationId)).isEqualTo(State.CANCELLED)
      assertThat(restarted.getOperation(work.operationId).publicationSequence).isNull()
      assertTerminal { worker.stageNextBatch(work, 40) }
      assertTerminal { worker.publish(work) }
      assertTerminal { restarted.resume(work.operationId) }
      assertViews(original, "old", "new")
      val next = restarted.start(REQUEST.copy(operationId = "after-cancel"))
      assertTerminal { worker.publish(work) }
      assertAborted {
        MetadataWriteGuard(database.databaseClient).beginHierarchyChange(1, "still-locked")
      }
      restarted.stageNextBatch(next, 40)
      restarted.publish(next)
      assertViews((1L..2L).associateWith { expectedMetadata(it) }, "new", "old")
    }

  @Test
  fun `fail preserves visible data and releases the subtree for retry`() =
    runBlocking<Unit> {
      seedEventGroups(eventGroup(1))
      val original = readMetadata("EventGroups", "EntityMetadata")
      val worker = EventGroupMetadataStore(database.databaseClient)
      val work = worker.start(REQUEST)
      worker.stageNextBatch(work, 40)
      assertThat(worker.fail(work)).isEqualTo(State.FAILED)
      assertThat(worker.cancel(work.operationId)).isEqualTo(State.FAILED)
      assertTerminal { worker.publish(work) }
      assertViews(original, "old", "new")
      worker.start(REQUEST.copy(operationId = "retry"))
    }

  @Test
  fun `resume fences the previous worker and recovers before and after publication`() =
    runBlocking<Unit> {
      seedEventGroups(eventGroup(1), eventGroup(2))
      val worker = EventGroupMetadataStore(database.databaseClient)
      val oldWork = worker.start(REQUEST)
      worker.stageNextBatch(oldWork, 1)
      val restarted = EventGroupMetadataStore(database.databaseClient)
      val resumed = restarted.resume(oldWork.operationId)
      assertAborted { worker.stageNextBatch(oldWork, 40) }
      assertAborted { worker.fail(oldWork) }
      assertAborted { worker.publish(oldWork) }
      assertThat(restarted.stageNextBatch(resumed, 40)).isEqualTo(Progress(2, true))
      val beforePublication = EventGroupMetadataStore(database.databaseClient)
      val publisher = beforePublication.resume(oldWork.operationId)
      assertAborted { restarted.publish(resumed) }
      assertThat(beforePublication.publish(publisher)).isEqualTo(1L)
      val afterPublication = EventGroupMetadataStore(database.databaseClient)
      assertThat(afterPublication.publish(publisher)).isEqualTo(1L)
      assertThat(afterPublication.getOperation(publisher.operationId).state)
        .isEqualTo(State.SUCCEEDED)
      assertThat(afterPublication.cancel(publisher.operationId)).isEqualTo(State.SUCCEEDED)
      assertViews((1L..2L).associateWith { expectedMetadata(it) }, "new", "old")
      MetadataWriteGuard(database.databaseClient).beginHierarchyChange(1, "after-success")
    }

  @Test
  fun `concurrent cancel and publish produce one durable outcome`() =
    runBlocking<Unit> {
      seedEventGroups(eventGroup(1))
      val original = readMetadata("EventGroups", "EntityMetadata")
      val worker = EventGroupMetadataStore(database.databaseClient)
      val work = worker.start(REQUEST)
      worker.stageNextBatch(work, 40)
      val start = CompletableDeferred<Unit>()
      val publication = async {
        start.await()
        runCatching { worker.publish(work) }
      }
      val cancellation = async {
        start.await()
        worker.cancel(work.operationId)
      }
      start.complete(Unit)
      val result = publication.await()
      val state = cancellation.await()
      assertThat(worker.getOperation(work.operationId).state).isEqualTo(state)
      assertThat(state).isAnyOf(State.SUCCEEDED, State.CANCELLED)
      assertThat(result.isSuccess).isEqualTo(state == State.SUCCEEDED)
      result.getOrNull()?.let { assertThat(it).isEqualTo(1L) }
      result.exceptionOrNull()?.let {
        assertThat((it as StatusRuntimeException).status.code)
          .isEqualTo(Status.Code.FAILED_PRECONDITION)
      }
      val expected =
        mapOf(State.SUCCEEDED to mapOf(1L to expectedMetadata(1)), State.CANCELLED to original)
      val present = mapOf(State.SUCCEEDED to "new", State.CANCELLED to "old")
      val absent = mapOf(State.SUCCEEDED to "old", State.CANCELLED to "new")
      assertViews(expected.getValue(state), present.getValue(state), absent.getValue(state))
      MetadataWriteGuard(database.databaseClient).beginHierarchyChange(1, "after-race")
    }

  @Test
  fun `concurrent starts cannot acquire overlapping locks`() =
    runBlocking<Unit> {
      val worker = EventGroupMetadataStore(database.databaseClient)
      val start = CompletableDeferred<Unit>()
      val attempts =
        listOf("one", "two").map { id ->
          async {
            start.await()
            runCatching { worker.start(REQUEST.copy(operationId = id)) }
          }
        }
      start.complete(Unit)
      val results = attempts.awaitAll()
      assertThat(results.count { it.isSuccess }).isEqualTo(1)
      val rejected = results.single { it.isFailure }.exceptionOrNull() as StatusRuntimeException
      assertThat(rejected.status.code).isEqualTo(Status.Code.ABORTED)
      assertThat(worker.cancel(results.single { it.isSuccess }.getOrThrow().operationId))
        .isEqualTo(State.CANCELLED)
      worker.start(REQUEST.copy(operationId = "after-race"))
    }

  @Test
  fun `cancellation during a staging batch cannot make staged values visible`() =
    runBlocking<Unit> {
      seedEventGroups(eventGroup(1), eventGroup(2))
      val original = readMetadata("EventGroups", "EntityMetadata")
      val worker = EventGroupMetadataStore(database.databaseClient)
      val work = worker.start(REQUEST)
      val start = CompletableDeferred<Unit>()
      val batch = async {
        start.await()
        runCatching { worker.stageNextBatch(work, 1) }
      }
      val cancellation = async {
        start.await()
        worker.cancel(work.operationId)
      }
      start.complete(Unit)
      val result = batch.await()
      assertThat(cancellation.await()).isEqualTo(State.CANCELLED)
      result.getOrNull()?.let { assertThat(it).isEqualTo(Progress(1, false)) }
      result.exceptionOrNull()?.let {
        assertThat((it as StatusRuntimeException).status.code)
          .isEqualTo(Status.Code.FAILED_PRECONDITION)
      }
      val operation = worker.getOperation(work.operationId)
      assertThat(operation.state).isEqualTo(State.CANCELLED)
      assertThat(operation.progress.stagedCount).isEqualTo(result.getOrNull()?.stagedCount ?: 0L)
      assertTerminal { worker.publish(work) }
      assertViews(original, "old", "new")
      worker.start(REQUEST.copy(operationId = "after-cancel-race"))
    }

  @Test
  fun `write racing lock acquisition either commits before staging or is rejected`() =
    runBlocking<Unit> {
      seedEventGroups(eventGroup(1))
      val worker = EventGroupMetadataStore(database.databaseClient)
      val writer = OrdinaryMetadataWriter(database.databaseClient)
      val changed = struct {
        fields.putAll(readMetadata("EventGroups", "EntityMetadata").getValue(1).fieldsMap)
        fields["campaign_name"] = value { stringValue = "concurrent campaign" }
      }
      val start = CompletableDeferred<Unit>()
      val acquisition = async {
        start.await()
        worker.start(REQUEST)
      }
      val write = async {
        start.await()
        runCatching { writer.update(1, 1, 1, changed, PROTECTED_KEYS) }
      }
      start.complete(Unit)
      val work = acquisition.await()
      val writeResult = write.await()
      writeResult.exceptionOrNull()?.let {
        assertThat((it as StatusRuntimeException).status.code).isEqualTo(Status.Code.ABORTED)
      }
      worker.stageNextBatch(work, 40)
      worker.publish(work)
      val expected = struct {
        fields.putAll(expectedMetadata(1).fieldsMap)
        if (writeResult.isSuccess)
          fields["campaign_name"] = changed.fieldsMap.getValue("campaign_name")
      }
      assertViews(mapOf(1L to expected), "new", "old")
    }

  @Test
  fun `ordinary update after publication is visible in Get List and search`() =
    runBlocking<Unit> {
      seedEventGroups(eventGroup(1), eventGroup(2))
      val worker = EventGroupMetadataStore(database.databaseClient)
      val work = worker.start(REQUEST)
      worker.stageNextBatch(work, 40)
      worker.publish(work)
      val updated = struct {
        fields.putAll(expectedMetadata(1).fieldsMap)
        fields["campaign_name"] = value { stringValue = "revised campaign" }
      }
      val writer = OrdinaryMetadataWriter(database.databaseClient)
      database.databaseClient.readOnlyTransaction().use { context ->
        val snapshot = PublishedMetadataReader(context, 1, 1)
        assertThat(snapshot.get(1)).isEqualTo(expectedMetadata(1))
        assertThat(writer.update(1, 1, 1, updated, PROTECTED_KEYS)).isEqualTo(updated)
        assertThat(snapshot.get(1)).isEqualTo(expectedMetadata(1))
        assertThat(snapshot.search("revised")).isEmpty()
      }
      assertViews(mapOf(1L to updated, 2L to expectedMetadata(2)), "new", "old")
      database.databaseClient.readOnlyTransaction().use { context ->
        val reader = PublishedMetadataReader(context, 1, 1)
        assertThat(reader.search("revised")).containsExactly(1L, updated)
        assertThat(reader.search("campaign 1")).isEmpty()
        assertThat(reader.search("new revised")).containsExactly(1L, updated)
      }
    }

  @Test
  fun `ordinary writes and later publications preserve each others metadata`() =
    runBlocking<Unit> {
      seedEventGroups(eventGroup(1), eventGroup(2))
      val original = readMetadata("EventGroups", "EntityMetadata")
      val writer = OrdinaryMetadataWriter(database.databaseClient)
      val firstEdit = struct {
        fields.putAll(original.getValue(1).fieldsMap)
        fields["campaign_name"] = value { stringValue = "initial edit" }
      }
      writer.update(1, 1, 1, firstEdit, PROTECTED_KEYS)
      val worker = EventGroupMetadataStore(database.databaseClient)
      val first = worker.start(REQUEST)
      worker.stageNextBatch(first, 40)
      worker.publish(first)
      val firstPublished = struct {
        fields.putAll(expectedMetadata(1).fieldsMap)
        fields["campaign_name"] = firstEdit.fieldsMap.getValue("campaign_name")
      }
      assertViews(mapOf(1L to firstPublished, 2L to expectedMetadata(2)), "new", "old")
      val secondEdit = struct {
        fields.putAll(firstPublished.fieldsMap)
        fields["campaign_name"] = value { stringValue = "second edit" }
      }
      writer.update(1, 1, 1, secondEdit, PROTECTED_KEYS)
      val next =
        worker.start(
          REQUEST.copy(
            operationId = "next-publication",
            metadata = struct { fields["account_name"] = value { stringValue = "final account" } },
          )
        )
      worker.stageNextBatch(next, 40)
      worker.publish(next)
      val expected =
        mapOf(1L to secondEdit, 2L to expectedMetadata(2)).mapValues { (_, metadata) ->
          struct {
            fields.putAll(metadata.fieldsMap)
            fields["account_name"] = value { stringValue = "final account" }
          }
        }
      assertViews(expected, "final", "new")
      database.databaseClient.readOnlyTransaction().use { context ->
        val reader = PublishedMetadataReader(context, 1, 1)
        assertThat(reader.search("initial")).isEmpty()
        assertThat(reader.search("second final")).containsExactly(1L, expected.getValue(1))
      }
    }

  @Test
  fun `ordinary update validates protected keys against the published metadata`() =
    runBlocking<Unit> {
      seedEventGroups(eventGroup(1))
      val old = readMetadata("EventGroups", "EntityMetadata").getValue(1)
      val worker = EventGroupMetadataStore(database.databaseClient)
      val work = worker.start(REQUEST)
      worker.stageNextBatch(work, 40)
      worker.publish(work)
      val writer = OrdinaryMetadataWriter(database.databaseClient)
      val changedId = struct {
        fields.putAll(expectedMetadata(1).fieldsMap)
        fields["account_id"] = value { stringValue = "Y" }
      }
      val omittedAncestor = struct {
        fields.putAll(expectedMetadata(1).fieldsMap.filterKeys { it != "account_name" })
      }
      for (replacement in listOf(old, changedId, omittedAncestor)) {
        val exception =
          assertFailsWith<StatusRuntimeException> {
            writer.update(1, 1, 1, replacement, PROTECTED_KEYS)
          }
        assertThat(exception.status.code).isEqualTo(Status.Code.FAILED_PRECONDITION)
      }
      assertViews(mapOf(1L to expectedMetadata(1)), "new", "old")
    }

  @Test
  fun `ordinary replacement removes omitted leaf values while retaining shared values`() =
    runBlocking<Unit> {
      seedEventGroups(eventGroup(1), eventGroup(2))
      val worker = EventGroupMetadataStore(database.databaseClient)
      val work = worker.start(REQUEST)
      worker.stageNextBatch(work, 40)
      worker.publish(work)
      val replacement = struct {
        fields.putAll(expectedMetadata(1).fieldsMap.filterKeys { it != "campaign_name" })
      }
      OrdinaryMetadataWriter(database.databaseClient).update(1, 1, 1, replacement, PROTECTED_KEYS)
      assertViews(mapOf(1L to replacement, 2L to expectedMetadata(2)), "new", "old")
      database.databaseClient.readOnlyTransaction().use { context ->
        val reader = PublishedMetadataReader(context, 1, 1)
        assertThat(reader.search("campaign 1")).isEmpty()
        assertThat(reader.search("campaign 2")).containsExactly(2L, expectedMetadata(2))
      }
    }

  @Test
  fun `ordinary update is blocked during staging and succeeds after cancellation`() =
    runBlocking<Unit> {
      seedEventGroups(eventGroup(1), eventGroup(2))
      val worker = EventGroupMetadataStore(database.databaseClient)
      val first = worker.start(REQUEST)
      worker.stageNextBatch(first, 40)
      worker.publish(first)
      val pending =
        worker.start(
          REQUEST.copy(
            operationId = "pending",
            metadata = struct { fields["account_name"] = value { stringValue = "unpublished" } },
          )
        )
      worker.stageNextBatch(pending, 1)
      val replacement = struct {
        fields.putAll(expectedMetadata(1).fieldsMap)
        fields["campaign_name"] = value { stringValue = "replacement" }
      }
      val writer = OrdinaryMetadataWriter(database.databaseClient)
      assertAborted { writer.update(1, 1, 1, replacement, PROTECTED_KEYS) }
      assertThat(worker.cancel(pending.operationId)).isEqualTo(State.CANCELLED)
      writer.update(1, 1, 1, replacement, PROTECTED_KEYS)
      assertViews(mapOf(1L to replacement, 2L to expectedMetadata(2)), "new", "unpublished")
    }

  @Test
  fun `ordinary update rejects derived ancestors and deleted EventGroups`() =
    runBlocking<Unit> {
      seedEventGroups(
        eventGroup(1, path = REQUEST.entityPath, isLeaf = false),
        eventGroup(2, state = EventGroup.State.DELETED),
      )
      val original = readMetadata("EventGroups", "EntityMetadata")
      val writer = OrdinaryMetadataWriter(database.databaseClient)
      for (id in listOf(1L, 2L)) {
        val exception =
          assertFailsWith<StatusRuntimeException> {
            writer.update(1, 1, id, original.getValue(id), PROTECTED_KEYS)
          }
        assertThat(exception.status.code).isEqualTo(Status.Code.FAILED_PRECONDITION)
      }
      assertThat(readMetadata("EventGroups", "EntityMetadata")).isEqualTo(original)
    }

  @Test
  fun `checkWrite rolls back other mutations when the caller transaction affects a locked subtree`() =
    runBlocking<Unit> {
      seedEventGroups(eventGroup(1), eventGroup(2, path = "account/X2/campaign/2/"))
      val original = readMetadata("EventGroups", "EntityMetadata")
      EventGroupMetadataStore(database.databaseClient).start(REQUEST)
      val guard = MetadataWriteGuard(database.databaseClient)
      assertAborted {
        database.databaseClient.readWriteTransaction().run { transaction ->
          transaction.buffer(Mutation.delete("EventGroups", Key.of(1L, 2L)))
          guard.checkWrite(transaction, 1, 1, setOf(1L, 2L))
        }
      }
      assertViews(original, "old", "new")
    }

  @Test
  fun `checkWrite and version allocation roll back with the caller transaction`() =
    runBlocking<Unit> {
      seedEventGroups(eventGroup(1))
      val original = readMetadata("EventGroups", "EntityMetadata")
      val guard = MetadataWriteGuard(database.databaseClient)
      assertFailsWith<IllegalStateException> {
        database.databaseClient.readWriteTransaction().run { transaction ->
          guard.checkWrite(transaction, 1, 1, setOf(1L))
          assertThat(MetadataPublicationSequences.next(transaction, 1, 1)).isEqualTo(1L)
          transaction.buffer(Mutation.delete("EventGroups", Key.of(1L, 1L)))
          error("Abort caller transaction")
        }
      }
      assertViews(original, "old", "new")
      val store = EventGroupMetadataStore(database.databaseClient)
      val work = store.start(REQUEST)
      store.stageNextBatch(work, 40)
      assertThat(store.publish(work)).isEqualTo(1L)
    }

  @Test
  fun `start rolls back the operation and lock with the caller transaction`() =
    runBlocking<Unit> {
      val store = EventGroupMetadataStore(database.databaseClient)
      assertFailsWith<IllegalStateException> {
        database.databaseClient.readWriteTransaction().run { transaction ->
          store.start(transaction, REQUEST)
          error("Abort caller transaction")
        }
      }
      assertFailsWith<IllegalArgumentException> { store.getOperation(REQUEST.operationId) }
      val work = store.start(REQUEST)
      assertThat(store.getOperation(work.operationId).state).isEqualTo(State.RUNNING)
      assertThat(store.cancel(work.operationId)).isEqualTo(State.CANCELLED)
    }

  private suspend fun assertAborted(action: suspend () -> Unit) {
    val exception = assertFailsWith<StatusRuntimeException> { action() }
    assertThat(exception.status.code).isEqualTo(Status.Code.ABORTED)
  }

  private suspend fun assertTerminal(action: suspend () -> Unit) {
    val exception = assertFailsWith<StatusRuntimeException> { action() }
    assertThat(exception.status.code).isEqualTo(Status.Code.FAILED_PRECONDITION)
  }

  private suspend fun assertViews(expected: Map<Long, Struct>, present: String, absent: String) {
    database.databaseClient.readOnlyTransaction().use { context ->
      val reader = PublishedMetadataReader(context, 1, 1)
      assertThat(reader.list()).isEqualTo(expected)
      for ((id, metadata) in expected) {
        assertThat(reader.get(id)).isEqualTo(metadata)
      }
      assertThat(reader.search(present)).isEqualTo(expected)
      assertThat(reader.search(absent)).isEmpty()
    }
  }

  private suspend fun seedEventGroups(vararg eventGroups: Mutation) {
    val certificate =
      Mutation.newInsertBuilder("Certificates")
        .apply {
          set("CertificateId").to(1L)
          set("SubjectKeyIdentifier").to(ByteArray.copyFrom("test-certificate"))
          set("NotValidBefore").to(Timestamp.parseTimestamp("2026-01-01T00:00:00Z"))
          set("NotValidAfter").to(Timestamp.parseTimestamp("2027-01-01T00:00:00Z"))
          set("RevocationState").to(0L)
        }
        .build()
    val consumerIds = eventGroups.map { it.asMap().getValue("MeasurementConsumerId").int64 }.toSet()
    val consumers =
      consumerIds.map { id ->
        Mutation.newInsertBuilder("MeasurementConsumers")
          .apply {
            set("MeasurementConsumerId").to(id)
            set("ExternalMeasurementConsumerId").to(id)
            set("PublicKeyCertificateId").to(1L)
            set("MeasurementConsumerDetails")
              .to(SpannerValue.protoMessage(MeasurementConsumerDetails.getDefaultInstance()))
          }
          .build()
      }
    database.databaseClient.write(listOf(certificate) + consumers + eventGroups)
  }

  private suspend fun readMetadata(table: String, column: String): Map<Long, Struct> {
    return database.databaseClient.singleUse().use { context ->
      context
        .executeQuery(Statement.of("SELECT EventGroupId, $column FROM $table"))
        .toList()
        .associate { row ->
          row.getLong("EventGroupId") to row.getProtoMessage(column, Struct.getDefaultInstance())
        }
    }
  }

  private fun eventGroup(
    id: Long,
    dataProviderId: Long = 1,
    measurementConsumerId: Long = 1,
    path: String? = "${REQUEST.entityPath}campaign/$id/",
    state: EventGroup.State = EventGroup.State.ACTIVE,
    isLeaf: Boolean = true,
  ): Mutation {
    val metadata = struct {
      fields["account_name"] = value { stringValue = "old account" }
      fields["obsolete"] = value { stringValue = "remove me" }
      fields["campaign_name"] = value { stringValue = "campaign $id" }
      fields["account_id"] = value { stringValue = "X" }
    }
    return Mutation.newInsertBuilder("EventGroups")
      .apply {
        set("DataProviderId").to(dataProviderId)
        set("MeasurementConsumerId").to(measurementConsumerId)
        set("EventGroupId").to(id)
        set("ExternalEventGroupId").to(id)
        set("CreateTime").to(SpannerValue.COMMIT_TIMESTAMP)
        set("State").to(state.number.toLong())
        set("EntityPath").to(path)
        set("IsLeaf").to(isLeaf)
        set("EntityMetadata").to(SpannerValue.protoMessage(metadata))
        set("EntityMetadataValues").toStringArray(EntityMetadataValues.flatten(metadata))
      }
      .build()
  }

  private fun expectedMetadata(id: Long): Struct = struct {
    fields.putAll(REQUEST.metadata.fieldsMap)
    fields["campaign_name"] = value { stringValue = "campaign $id" }
    fields["account_id"] = value { stringValue = "X" }
  }

  companion object {
    @get:ClassRule @JvmStatic val spannerEmulator = SpannerEmulatorRule()

    private val REQUEST =
      Request(
        operationId = "operation-1",
        dataProviderId = 1,
        measurementConsumerId = 1,
        entityPath = "account/X/",
        ownedKeys = setOf("account_name", "obsolete"),
        metadata = struct { fields["account_name"] = value { stringValue = "new account" } },
      )

    private val PROTECTED_KEYS = REQUEST.ownedKeys + "account_id"
  }
}
