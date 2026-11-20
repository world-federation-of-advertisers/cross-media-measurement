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

import com.google.common.truth.Truth.assertThat
import com.google.protobuf.Message
import io.grpc.Status
import io.grpc.StatusRuntimeException
import java.util.concurrent.CopyOnWriteArrayList
import kotlin.test.assertFailsWith
import kotlinx.coroutines.runBlocking
import org.junit.ClassRule
import org.junit.Rule
import org.junit.Test
import org.junit.rules.RuleChain
import org.wfanet.measurement.common.IdGenerator
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.common.pack
import org.wfanet.measurement.config.securecomputation.WatchedPathKt.controlPlaneQueueSink
import org.wfanet.measurement.config.securecomputation.watchedPath
import org.wfanet.measurement.edpaggregator.v1alpha.dataAvailabilityCleanupParams
import org.wfanet.measurement.gcloud.spanner.testing.SpannerEmulatorDatabaseRule
import org.wfanet.measurement.gcloud.spanner.testing.SpannerEmulatorRule
import org.wfanet.measurement.internal.securecomputation.controlplane.WorkItemsGrpcKt.WorkItemsCoroutineStub as InternalWorkItemsCoroutineStub
import org.wfanet.measurement.internal.securecomputation.controlplane.createWorkItemRequest as internalCreateWorkItemRequest
import org.wfanet.measurement.internal.securecomputation.controlplane.workItem as internalWorkItem
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItem
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemKt.WorkItemParamsKt.dataPathParams
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemKt.workItemParams
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemsGrpcKt.WorkItemsCoroutineStub
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemsService
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.getWorkItemRequest
import org.wfanet.measurement.securecomputation.datawatcher.DataWatcher
import org.wfanet.measurement.securecomputation.deploy.gcloud.spanner.testing.Schemata
import org.wfanet.measurement.securecomputation.service.internal.WorkItemPublisher
import org.wfanet.measurement.securecomputation.service.internal.testing.TestConfig

/** Checks cleanup intake through both API layers and the persistent WorkItem service. */
class CleanupWorkItemContractTest {
  private val database =
    SpannerEmulatorDatabaseRule(spannerEmulator, Schemata.SECURECOMPUTATION_CHANGELOG_PATH)
  private val published = CopyOnWriteArrayList<WorkItem>()
  private val publisher =
    object : WorkItemPublisher {
      override suspend fun publishMessage(queueName: String, message: Message) {
        published += message as WorkItem
      }
    }
  private val service by lazy {
    SpannerWorkItemsService(
      database.databaseClient,
      TestConfig.QUEUE_MAPPING,
      IdGenerator.Default,
      WorkItemPublicationRunner(database.databaseClient, TestConfig.QUEUE_MAPPING, publisher),
    )
  }
  private val internalServer = GrpcTestServerRule { addService(service) }
  private val publicServer = GrpcTestServerRule {
    addService(WorkItemsService(InternalWorkItemsCoroutineStub(internalServer.channel)))
  }
  @get:Rule val rules = RuleChain.outerRule(database).around(internalServer).around(publicServer)

  @Test
  fun `deletion with generation but no creation time persists through the real APIs`() =
    runBlocking {
      val notification = deletedEvent + (DataWatcher.GENERATION_METADATA_KEY to "1")
      val watcher = watcher()
      watcher.receivePath(PATH, notification)
      // A newer delivery of the same notification can include the previously missing time.
      watcher.receivePath(
        PATH,
        notification + (DataWatcher.CREATE_TIME_METADATA_KEY to CREATE_TIME),
      )
      val persisted = assertPersisted()
      val source =
        persisted.workItemParams.unpack(WorkItem.WorkItemParams::class.java).dataPathParams
      assertThat(source.hasGeneration()).isFalse()
      assertThat(source.hasCreateTime()).isFalse()
    }

  @Test
  fun `legacy deletion with creation time and event ID persists without a generation`() =
    runBlocking {
      val notification = deletedEvent + (DataWatcher.CREATE_TIME_METADATA_KEY to CREATE_TIME)
      val watcher = watcher()
      repeat(2) { watcher.receivePath(PATH, notification) }
      val persisted = assertPersisted()
      val source =
        persisted.workItemParams.unpack(WorkItem.WorkItemParams::class.java).dataPathParams
      assertThat(source.hasGeneration()).isFalse()
      assertThat(source.hasCreateTime()).isFalse()
    }

  @Test
  fun `legacy deletion with only an event ID persists and deduplicates`() = runBlocking {
    val watcher = watcher()
    repeat(2) { watcher.receivePath(PATH, deletedEvent) }
    val persisted = assertPersisted()
    val source = persisted.workItemParams.unpack(WorkItem.WorkItemParams::class.java).dataPathParams
    assertThat(source.hasGeneration()).isFalse()
    assertThat(source.hasCreateTime()).isFalse()
  }

  @Test
  fun `complete deletion source identity still persists`() = runBlocking {
    val notification =
      deletedEvent +
        mapOf(
          DataWatcher.GENERATION_METADATA_KEY to "1",
          DataWatcher.CREATE_TIME_METADATA_KEY to CREATE_TIME,
        )
    val watcher = watcher()
    repeat(2) { watcher.receivePath(PATH, notification) }
    val source =
      assertPersisted().workItemParams.unpack(WorkItem.WorkItemParams::class.java).dataPathParams
    assertThat(source.generation).isEqualTo(1L)
    assertThat(source.hasCreateTime()).isTrue()
  }

  @Test
  fun `ordered deletion still requires creation time`() = runBlocking {
    assertRejected("ordered", 1L, PATH)
  }

  @Test
  fun `finalization is filtered before creating a cleanup WorkItem`() = runBlocking {
    watcher()
      .receivePath(
        PATH,
        mapOf(
          DataWatcher.EVENT_TYPE_METADATA_KEY to DataWatcher.FINALIZED_EVENT_TYPE,
          DataWatcher.GENERATION_METADATA_KEY to "1",
          DataWatcher.CREATE_TIME_METADATA_KEY to CREATE_TIME,
        ),
      )
    assertThat(published).isEmpty()
  }

  @Test
  fun `zero generation remains invalid even with a deletion event ID`() = runBlocking {
    assertRejected("", 0L, PATH)
  }

  @Test
  fun `partial deletion identity still requires a source path`() = runBlocking {
    assertRejected("", 1L, "")
  }

  private suspend fun assertRejected(serializationKey: String, generation: Long, path: String) {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        service.createWorkItem(
          internalCreateWorkItemRequest {
            workItem = internalWorkItem {
              workItemResourceId = "invalid-cleanup"
              queueResourceId = "test-topid-id"
              this.serializationKey = serializationKey
              workItemParams =
                workItemParams {
                    dataPathParams = dataPathParams {
                      dataPath = path
                      this.generation = generation
                    }
                  }
                  .pack()
            }
          }
        )
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(published).isEmpty()
  }

  private suspend fun assertPersisted(): WorkItem {
    assertThat(published).hasSize(1)
    val persisted =
      WorkItemsCoroutineStub(publicServer.channel)
        .getWorkItem(getWorkItemRequest { name = published.single().name })
    assertThat(persisted.state).isEqualTo(WorkItem.State.QUEUED)
    assertThat(persisted.serializationKey).isEmpty()
    assertThat(
        persisted.workItemParams.unpack(WorkItem.WorkItemParams::class.java).dataPathParams.dataPath
      )
      .isEqualTo(PATH)
    return persisted
  }

  private fun watcher() =
    DataWatcher(
      WorkItemsCoroutineStub(publicServer.channel),
      listOf(
        watchedPath {
          identifier = "cleanup"
          sourcePathRegex = "^gs://bucket/.*"
          eventTypes += DataWatcher.DELETED_EVENT_TYPE
          eventTypes += DataWatcher.ARCHIVED_EVENT_TYPE
          controlPlaneQueueSink = controlPlaneQueueSink {
            queue = "test-topid-id"
            allowMissingSourceIdentity = true
            appParams =
              dataAvailabilityCleanupParams { dataProvider = "dataProviders/provider-1" }.pack()
          }
        }
      ),
    )

  private val deletedEvent =
    mapOf(
      DataWatcher.EVENT_TYPE_METADATA_KEY to DataWatcher.DELETED_EVENT_TYPE,
      DataWatcher.EVENT_ID_METADATA_KEY to "legacy-event",
    )

  companion object {
    @ClassRule @JvmField val spannerEmulator = SpannerEmulatorRule()
    private const val PATH = "gs://bucket/path/to/metadata"
    private const val CREATE_TIME = "2026-08-15T10:15:30Z"
  }
}
