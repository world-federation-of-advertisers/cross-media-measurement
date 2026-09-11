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

import com.google.cloud.Timestamp as CloudTimestamp
import com.google.common.truth.Truth.assertThat
import com.google.protobuf.Any
import com.google.protobuf.Message
import java.time.Clock
import java.time.Duration
import java.time.Instant
import java.time.ZoneId
import java.time.ZoneOffset
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.async
import kotlinx.coroutines.flow.single
import kotlinx.coroutines.runBlocking
import org.junit.ClassRule
import org.junit.Rule
import org.junit.Test
import org.wfa.measurement.queue.testing.testWork
import org.wfanet.measurement.common.IdGenerator
import org.wfanet.measurement.gcloud.spanner.bufferUpdateMutation
import org.wfanet.measurement.gcloud.spanner.statement
import org.wfanet.measurement.gcloud.spanner.testing.SpannerEmulatorDatabaseRule
import org.wfanet.measurement.gcloud.spanner.testing.SpannerEmulatorRule
import org.wfanet.measurement.internal.securecomputation.controlplane.WorkItem
import org.wfanet.measurement.internal.securecomputation.controlplane.createWorkItemRequest
import org.wfanet.measurement.internal.securecomputation.controlplane.workItem
import org.wfanet.measurement.securecomputation.deploy.gcloud.spanner.db.claimWorkItemPublication
import org.wfanet.measurement.securecomputation.deploy.gcloud.spanner.db.insertWorkItem
import org.wfanet.measurement.securecomputation.deploy.gcloud.spanner.db.insertWorkItemAttempt
import org.wfanet.measurement.securecomputation.deploy.gcloud.spanner.db.insertWorkItemPublication
import org.wfanet.measurement.securecomputation.deploy.gcloud.spanner.testing.Schemata
import org.wfanet.measurement.securecomputation.service.internal.WorkItemPublisher
import org.wfanet.measurement.securecomputation.service.internal.testing.TestConfig

class WorkItemPublicationRunnerTest {
  @get:Rule
  val spannerDatabase =
    SpannerEmulatorDatabaseRule(spannerEmulator, Schemata.SECURECOMPUTATION_CHANGELOG_PATH)

  @Test
  fun `create succeeds and pending publication is retried`() = runBlocking {
    val publisher = RecordingPublisher(fail = true)
    val clock = MutableClock(Instant.now().plusSeconds(10))
    val runner = newRunner(publisher, clock)
    val service =
      SpannerWorkItemsService(
        spannerDatabase.databaseClient,
        TestConfig.QUEUE_MAPPING,
        IdGenerator.Default,
        runner,
      )

    val created = service.createWorkItem(createRequest("work-item-1"))

    assertThat(created.state).isEqualTo(WorkItem.State.QUEUED)
    assertThat(publisher.callCount).isEqualTo(1)
    assertThat(publicationCount()).isEqualTo(1L)

    publisher.fail = false
    clock.advance(Duration.ofSeconds(2))

    assertThat(runner.publishPendingWorkItems()).isEqualTo(1)
    assertThat(publisher.messages).hasSize(1)
    assertThat((publisher.messages.single() as WorkItem).workItemResourceId)
      .isEqualTo("work-item-1")
    assertThat(publicationCount()).isEqualTo(0L)
  }

  @Test
  fun `active lease prevents concurrent publication`() = runBlocking {
    insertPendingWorkItem(WORK_ITEM_ID, "work-item-1")
    val clock = MutableClock(Instant.now().plusSeconds(10))
    val publisher = BlockingPublisher()
    val firstRunner = newRunner(publisher, clock)
    val secondRunner = newRunner(publisher, clock)

    val firstPublication = async { firstRunner.publishPendingWorkItems() }
    publisher.started.await()

    assertThat(secondRunner.publishPendingWorkItems()).isEqualTo(0)

    publisher.release.complete(Unit)
    assertThat(firstPublication.await()).isEqualTo(1)
    assertThat(publisher.callCount).isEqualTo(1)
    assertThat(publicationCount()).isEqualTo(0L)
  }

  @Test
  fun `expired lease is published by another runner`() = runBlocking {
    insertPendingWorkItem(WORK_ITEM_ID, "work-item-1")
    val clock = MutableClock(Instant.now().plusSeconds(10))
    spannerDatabase.databaseClient.readWriteTransaction().run { transaction ->
      transaction.claimWorkItemPublication(
        TestConfig.QUEUE_MAPPING,
        "stopped-runner",
        clock.instant(),
        clock.instant().plus(Duration.ofMinutes(1)),
      )
    }
    clock.advance(Duration.ofMinutes(1).plusSeconds(1))
    val publisher = RecordingPublisher()

    assertThat(newRunner(publisher, clock).publishPendingWorkItems()).isEqualTo(1)
    assertThat(publisher.callCount).isEqualTo(1)
    assertThat(publicationCount()).isEqualTo(0L)
  }

  @Test
  fun `publishPendingWorkItems continues after retry bookkeeping failure`() = runBlocking {
    insertPendingWorkItem(WORK_ITEM_ID, "work-item-1")
    val clock = FailOnceClock(Instant.now().plusSeconds(10), failOnCall = 2)
    val publisher = RecordingPublisher(fail = true)
    val runner = newRunner(publisher, clock)

    assertThat(runner.publishPendingWorkItems()).isEqualTo(0)
    assertThat(publicationCount()).isEqualTo(1L)

    publisher.fail = false
    clock.advance(Duration.ofMinutes(1).plusSeconds(1))

    assertThat(runner.publishPendingWorkItems()).isEqualTo(1)
    assertThat(publicationCount()).isEqualTo(0L)
  }

  @Test
  fun `publishPendingWorkItems prioritizes unattempted rows after invalid backlog`() = runBlocking {
    val unresolvableCount = 100
    repeat(unresolvableCount) { index ->
      insertPendingWorkItem(
        WORK_ITEM_ID + index,
        "work-item-invalid-$index",
        queueId = Long.MAX_VALUE,
      )
    }
    val clock = MutableClock(Instant.now().plusSeconds(10))
    val publisher = RecordingPublisher()
    val runner = newRunner(publisher, clock)

    assertThat(runner.publishPendingWorkItems()).isEqualTo(0)
    clock.advance(Duration.ofMinutes(1).plusSeconds(1))
    insertPendingWorkItem(WORK_ITEM_ID + unresolvableCount, "work-item-valid")

    assertThat(runner.publishPendingWorkItems()).isEqualTo(1)

    assertThat(publisher.messages).hasSize(1)
    assertThat((publisher.messages.single() as WorkItem).workItemResourceId)
      .isEqualTo("work-item-valid")
    assertThat(publicationCount()).isEqualTo(unresolvableCount.toLong())
  }

  @Test
  fun `eligible retry is not starved by newer unattempted rows`() = runBlocking {
    val clock = MutableClock(Instant.now().plusSeconds(10))
    insertPendingWorkItem(WORK_ITEM_ID, "work-item-retry")
    markPublicationAsRetry(WORK_ITEM_ID, Instant.EPOCH)
    val publisher = RecordingPublisher()
    val runner = newRunner(publisher, clock)
    repeat(100) { index ->
      val workItemId = WORK_ITEM_ID + index + 1
      insertPendingWorkItem(workItemId, "work-item-new-$index")
    }

    assertThat(runner.publishPendingWorkItems(limit = 1)).isEqualTo(1)
    assertThat((publisher.messages.single() as WorkItem).workItemResourceId)
      .isEqualTo("work-item-retry")
  }

  @Test
  fun `starting a WorkItem attempt removes its pending publication`() = runBlocking {
    insertPendingWorkItem(WORK_ITEM_ID, "work-item-1")
    spannerDatabase.databaseClient.readWriteTransaction().run { transaction ->
      transaction.insertWorkItemAttempt(WORK_ITEM_ID, 456L, "attempt-1")
    }
    val publisher = RecordingPublisher()
    val clock = MutableClock(Instant.now().plusSeconds(10))

    assertThat(newRunner(publisher, clock).publishPendingWorkItems()).isEqualTo(0)
    assertThat(publisher.callCount).isEqualTo(0)
    assertThat(publicationCount()).isEqualTo(0L)
  }

  private fun newRunner(publisher: WorkItemPublisher, clock: Clock): WorkItemPublicationRunner {
    return WorkItemPublicationRunner(
      databaseClient = spannerDatabase.databaseClient,
      queueMapping = TestConfig.QUEUE_MAPPING,
      workItemPublisher = publisher,
      clock = clock,
      leaseDuration = Duration.ofMinutes(1),
      initialRetryDelay = Duration.ofSeconds(1),
      maxRetryDelay = Duration.ofMinutes(1),
    )
  }

  private suspend fun insertPendingWorkItem(workItemId: Long, workItemResourceId: String) {
    val queue = checkNotNull(TestConfig.QUEUE_MAPPING.getQueueByResourceId(QUEUE_RESOURCE_ID))
    insertPendingWorkItem(workItemId, workItemResourceId, queue.queueId)
  }

  private suspend fun insertPendingWorkItem(
    workItemId: Long,
    workItemResourceId: String,
    queueId: Long,
  ) {
    spannerDatabase.databaseClient.readWriteTransaction().run { transaction ->
      transaction.insertWorkItem(
        workItemId,
        workItemResourceId,
        queueId,
        Any.pack(testWork { userName = "UserName" }),
      )
      transaction.insertWorkItemPublication(workItemId)
    }
  }

  private suspend fun publicationCount(): Long {
    return spannerDatabase.databaseClient.singleUse().use { readContext ->
      readContext
        .executeQuery(statement("SELECT COUNT(*) AS PublicationCount FROM WorkItemPublications"))
        .single()
        .getLong("PublicationCount")
    }
  }

  private suspend fun markPublicationAsRetry(workItemId: Long, nextAttemptTime: Instant) {
    spannerDatabase.databaseClient.readWriteTransaction().run { transaction ->
      transaction.bufferUpdateMutation("WorkItemPublications") {
        set("WorkItemId").to(workItemId)
        set("NextAttemptTime")
          .to(
            CloudTimestamp.ofTimeSecondsAndNanos(nextAttemptTime.epochSecond, nextAttemptTime.nano)
          )
        set("AttemptCount").to(1L)
      }
    }
  }

  private fun createRequest(workItemResourceId: String) = createWorkItemRequest {
    workItem = workItem {
      this.workItemResourceId = workItemResourceId
      queueResourceId = QUEUE_RESOURCE_ID
      workItemParams = Any.pack(testWork { userName = "UserName" })
    }
  }

  private class RecordingPublisher(var fail: Boolean = false) : WorkItemPublisher {
    var callCount = 0
      private set

    val messages = mutableListOf<Message>()

    override suspend fun publishMessage(queueName: String, message: Message) {
      callCount++
      if (fail) {
        error("Publication failed")
      }
      messages += message
    }
  }

  private class BlockingPublisher : WorkItemPublisher {
    val started = CompletableDeferred<Unit>()
    val release = CompletableDeferred<Unit>()
    var callCount = 0
      private set

    override suspend fun publishMessage(queueName: String, message: Message) {
      callCount++
      started.complete(Unit)
      release.await()
    }
  }

  private class MutableClock(private var currentInstant: Instant) : Clock() {
    override fun getZone(): ZoneId = ZoneOffset.UTC

    override fun withZone(zone: ZoneId): Clock = this

    override fun instant(): Instant = currentInstant

    fun advance(duration: Duration) {
      currentInstant = currentInstant.plus(duration)
    }
  }

  private class FailOnceClock(private var currentInstant: Instant, private val failOnCall: Int) :
    Clock() {
    private var callCount = 0

    override fun getZone(): ZoneId = ZoneOffset.UTC

    override fun withZone(zone: ZoneId): Clock = this

    override fun instant(): Instant {
      callCount++
      if (callCount == failOnCall) {
        error("Clock failure")
      }
      return currentInstant
    }

    fun advance(duration: Duration) {
      currentInstant = currentInstant.plus(duration)
    }
  }

  companion object {
    private const val WORK_ITEM_ID = 123L
    private const val QUEUE_RESOURCE_ID = "test-topid-id"

    @get:ClassRule @JvmStatic val spannerEmulator = SpannerEmulatorRule()
  }
}
