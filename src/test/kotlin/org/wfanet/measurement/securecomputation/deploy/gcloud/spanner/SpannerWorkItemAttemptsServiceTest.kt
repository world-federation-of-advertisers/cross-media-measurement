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

package org.wfanet.measurement.securecomputation.deploy.gcloud.spanner

import com.google.common.truth.Truth.assertThat
import com.google.protobuf.Any
import com.google.protobuf.Message
import io.grpc.Status
import io.grpc.StatusRuntimeException
import java.time.Clock
import java.time.Duration
import java.time.Instant
import java.time.ZoneId
import java.time.ZoneOffset
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.runBlocking
import org.junit.ClassRule
import org.junit.Rule
import org.junit.Test
import org.wfa.measurement.queue.testing.testWork
import org.wfanet.measurement.common.IdGenerator
import org.wfanet.measurement.common.grpc.errorInfo
import org.wfanet.measurement.config.securecomputation.QueuesConfigKt.queueInfo
import org.wfanet.measurement.config.securecomputation.queuesConfig
import org.wfanet.measurement.gcloud.spanner.testing.SpannerEmulatorDatabaseRule
import org.wfanet.measurement.gcloud.spanner.testing.SpannerEmulatorRule
import org.wfanet.measurement.internal.securecomputation.controlplane.WorkItem
import org.wfanet.measurement.internal.securecomputation.controlplane.WorkItemAttempt
import org.wfanet.measurement.internal.securecomputation.controlplane.completeWorkItemAttemptRequest
import org.wfanet.measurement.internal.securecomputation.controlplane.createWorkItemAttemptRequest
import org.wfanet.measurement.internal.securecomputation.controlplane.createWorkItemRequest
import org.wfanet.measurement.internal.securecomputation.controlplane.failWorkItemAttemptRequest
import org.wfanet.measurement.internal.securecomputation.controlplane.failWorkItemRequest
import org.wfanet.measurement.internal.securecomputation.controlplane.getWorkItemRequest
import org.wfanet.measurement.internal.securecomputation.controlplane.listWorkItemAttemptsRequest
import org.wfanet.measurement.internal.securecomputation.controlplane.renewWorkItemAttemptRequest
import org.wfanet.measurement.internal.securecomputation.controlplane.retryWorkItemRequest
import org.wfanet.measurement.internal.securecomputation.controlplane.workItem
import org.wfanet.measurement.internal.securecomputation.controlplane.workItemAttempt
import org.wfanet.measurement.securecomputation.deploy.gcloud.spanner.testing.Schemata
import org.wfanet.measurement.securecomputation.service.internal.Errors
import org.wfanet.measurement.securecomputation.service.internal.QueueMapping
import org.wfanet.measurement.securecomputation.service.internal.WorkItemPublisher
import org.wfanet.measurement.securecomputation.service.internal.testing.TestConfig
import org.wfanet.measurement.securecomputation.service.internal.testing.WorkItemAttemptsServiceTest

class SpannerWorkItemAttemptsServiceTest : WorkItemAttemptsServiceTest() {

  @get:Rule
  val spannerDatabase =
    SpannerEmulatorDatabaseRule(spannerEmulator, Schemata.SECURECOMPUTATION_CHANGELOG_PATH)

  override fun initServices(
    queueMapping: QueueMapping,
    idGenerator: IdGenerator,
    workItemPublisher: WorkItemPublisher,
  ): Services {
    val serviceDispatcher = Dispatchers.Default
    val workItemPublicationRunner =
      WorkItemPublicationRunner(spannerDatabase.databaseClient, queueMapping, workItemPublisher)
    return Services(
      SpannerWorkItemAttemptsService(
        spannerDatabase.databaseClient,
        queueMapping,
        idGenerator,
        serviceDispatcher,
      ),
      SpannerWorkItemsService(
        spannerDatabase.databaseClient,
        queueMapping,
        idGenerator,
        workItemPublicationRunner,
      ),
    )
  }

  @Test
  fun `expired attempt lease is failed and WorkItem is republished at a new generation`() =
    runBlocking {
      val clock = MutableClock(Instant.now().plusSeconds(10))
      val publisher = RecordingPublisher()
      val publicationRunner =
        WorkItemPublicationRunner(
          spannerDatabase.databaseClient,
          TestConfig.QUEUE_MAPPING,
          publisher,
          clock = clock,
        )
      val attemptsService =
        SpannerWorkItemAttemptsService(
          spannerDatabase.databaseClient,
          TestConfig.QUEUE_MAPPING,
          IdGenerator.Default,
          Dispatchers.Default,
          clock = clock,
          attemptLeaseDuration = Duration.ofMinutes(5),
        )
      val workItemsService =
        SpannerWorkItemsService(
          spannerDatabase.databaseClient,
          TestConfig.QUEUE_MAPPING,
          IdGenerator.Default,
          publicationRunner,
        )
      val workItem =
        workItemsService.createWorkItem(
          createWorkItemRequest {
            this.workItem = workItem {
              workItemResourceId = "leased-work-item"
              queueResourceId = "test-topid-id"
              workItemParams = Any.pack(testWork { userName = "UserName" })
            }
          }
        )
      val attempt =
        attemptsService.createWorkItemAttempt(
          createWorkItemAttemptRequest {
            expectedWorkItemGeneration = workItem.generation
            supportsAttemptLease = true
            workItemAttempt = workItemAttempt {
              workItemResourceId = workItem.workItemResourceId
              workItemAttemptResourceId = "leased-attempt"
            }
          }
        )
      assertThat(attempt.leaseExpirationTime.seconds)
        .isEqualTo(clock.instant().plus(Duration.ofMinutes(5)).epochSecond)

      clock.advance(Duration.ofMinutes(4))
      val renewed =
        attemptsService.renewWorkItemAttempt(
          renewWorkItemAttemptRequest {
            workItemResourceId = attempt.workItemResourceId
            workItemAttemptResourceId = attempt.workItemAttemptResourceId
          }
        )
      assertThat(renewed.leaseExpirationTime.seconds)
        .isEqualTo(clock.instant().plus(Duration.ofMinutes(5)).epochSecond)

      val reaper =
        WorkItemAttemptLeaseReaper(spannerDatabase.databaseClient, TestConfig.QUEUE_MAPPING, clock)
      clock.advance(Duration.ofMinutes(2))
      assertThat(reaper.recoverExpiredAttempts()).isEqualTo(0)

      clock.advance(Duration.ofMinutes(4))
      assertThat(reaper.recoverExpiredAttempts()).isEqualTo(1)
      val recoveredWorkItem =
        workItemsService.getWorkItem(
          getWorkItemRequest { workItemResourceId = workItem.workItemResourceId }
        )
      assertThat(recoveredWorkItem.state).isEqualTo(WorkItem.State.QUEUED)
      assertThat(recoveredWorkItem.generation).isEqualTo(workItem.generation + 1L)
      val recoveredAttempt =
        attemptsService.getWorkItemAttempt(
          org.wfanet.measurement.internal.securecomputation.controlplane.getWorkItemAttemptRequest {
            workItemResourceId = attempt.workItemResourceId
            workItemAttemptResourceId = attempt.workItemAttemptResourceId
          }
        )
      assertThat(recoveredAttempt.state).isEqualTo(WorkItemAttempt.State.FAILED)

      assertThat(publicationRunner.publishPendingWorkItems()).isEqualTo(1)
      assertThat((publisher.messages.last() as WorkItem).generation)
        .isEqualTo(workItem.generation + 1L)

      val replacementAttempt =
        attemptsService.createWorkItemAttempt(
          createWorkItemAttemptRequest {
            expectedWorkItemGeneration = recoveredWorkItem.generation
            supportsAttemptLease = true
            workItemAttempt = workItemAttempt {
              workItemResourceId = workItem.workItemResourceId
              workItemAttemptResourceId = "replacement-attempt"
            }
          }
        )
      val completed =
        attemptsService.completeWorkItemAttempt(
          completeWorkItemAttemptRequest {
            workItemResourceId = replacementAttempt.workItemResourceId
            workItemAttemptResourceId = replacementAttempt.workItemAttemptResourceId
          }
        )
      assertThat(replacementAttempt.state).isEqualTo(WorkItemAttempt.State.ACTIVE)
      assertThat(completed.state).isEqualTo(WorkItemAttempt.State.SUCCEEDED)
    }

  @Test
  fun `expired attempt cannot be renewed or completed`() = runBlocking {
    val clock = MutableClock(Instant.now())
    val publicationRunner =
      WorkItemPublicationRunner(
        spannerDatabase.databaseClient,
        TestConfig.QUEUE_MAPPING,
        RecordingPublisher(),
        clock = clock,
      )
    val attemptsService =
      SpannerWorkItemAttemptsService(
        spannerDatabase.databaseClient,
        TestConfig.QUEUE_MAPPING,
        IdGenerator.Default,
        Dispatchers.Default,
        clock = clock,
        attemptLeaseDuration = Duration.ofMinutes(5),
      )
    val workItemsService =
      SpannerWorkItemsService(
        spannerDatabase.databaseClient,
        TestConfig.QUEUE_MAPPING,
        IdGenerator.Default,
        publicationRunner,
      )
    val workItem =
      workItemsService.createWorkItem(
        createWorkItemRequest {
          this.workItem = workItem {
            workItemResourceId = "expired-lease-work-item"
            queueResourceId = QUEUE_RESOURCE_ID
            workItemParams = Any.pack(testWork { userName = "UserName" })
          }
        }
      )
    val attempt =
      attemptsService.createWorkItemAttempt(
        createWorkItemAttemptRequest {
          expectedWorkItemGeneration = workItem.generation
          supportsAttemptLease = true
          workItemAttempt = workItemAttempt {
            workItemResourceId = workItem.workItemResourceId
            workItemAttemptResourceId = "expired-lease-attempt"
          }
        }
      )
    clock.advance(Duration.ofMinutes(5))

    val renewException =
      kotlin.test.assertFailsWith<StatusRuntimeException> {
        attemptsService.renewWorkItemAttempt(
          renewWorkItemAttemptRequest {
            workItemResourceId = attempt.workItemResourceId
            workItemAttemptResourceId = attempt.workItemAttemptResourceId
          }
        )
      }
    val completeException =
      kotlin.test.assertFailsWith<StatusRuntimeException> {
        attemptsService.completeWorkItemAttempt(
          completeWorkItemAttemptRequest {
            workItemResourceId = attempt.workItemResourceId
            workItemAttemptResourceId = attempt.workItemAttemptResourceId
          }
        )
      }

    assertThat(renewException.status.code).isEqualTo(Status.Code.FAILED_PRECONDITION)
    assertThat(completeException.status.code).isEqualTo(Status.Code.FAILED_PRECONDITION)
  }

  @Test
  fun `attempt without lease cannot be renewed`() = runBlocking {
    val clock = MutableClock(Instant.now())
    val publicationRunner =
      WorkItemPublicationRunner(
        spannerDatabase.databaseClient,
        TestConfig.QUEUE_MAPPING,
        RecordingPublisher(),
        clock = clock,
      )
    val attemptsService =
      SpannerWorkItemAttemptsService(
        spannerDatabase.databaseClient,
        TestConfig.QUEUE_MAPPING,
        IdGenerator.Default,
        Dispatchers.Default,
        clock = clock,
      )
    val workItemsService =
      SpannerWorkItemsService(
        spannerDatabase.databaseClient,
        TestConfig.QUEUE_MAPPING,
        IdGenerator.Default,
        publicationRunner,
      )
    val workItem =
      workItemsService.createWorkItem(
        createWorkItemRequest {
          this.workItem = workItem {
            workItemResourceId = "unleased-renewal-work-item"
            queueResourceId = QUEUE_RESOURCE_ID
            workItemParams = Any.pack(testWork { userName = "UserName" })
          }
        }
      )
    val attempt =
      attemptsService.createWorkItemAttempt(
        createWorkItemAttemptRequest {
          expectedWorkItemGeneration = workItem.generation
          workItemAttempt = workItemAttempt {
            workItemResourceId = workItem.workItemResourceId
            workItemAttemptResourceId = "unleased-attempt"
          }
        }
      )

    val exception =
      kotlin.test.assertFailsWith<StatusRuntimeException> {
        attemptsService.renewWorkItemAttempt(
          renewWorkItemAttemptRequest {
            workItemResourceId = attempt.workItemResourceId
            workItemAttemptResourceId = attempt.workItemAttemptResourceId
          }
        )
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.FAILED_PRECONDITION)
  }

  @Test
  fun `attempt from caller without lease support is not reaped`() = runBlocking {
    val clock = MutableClock(Instant.now().plusSeconds(10))
    val publisher = RecordingPublisher()
    val publicationRunner =
      WorkItemPublicationRunner(
        spannerDatabase.databaseClient,
        TestConfig.QUEUE_MAPPING,
        publisher,
        clock = clock,
      )
    val attemptsService =
      SpannerWorkItemAttemptsService(
        spannerDatabase.databaseClient,
        TestConfig.QUEUE_MAPPING,
        IdGenerator.Default,
        Dispatchers.Default,
        clock = clock,
        attemptLeaseDuration = Duration.ofMinutes(5),
      )
    val workItemsService =
      SpannerWorkItemsService(
        spannerDatabase.databaseClient,
        TestConfig.QUEUE_MAPPING,
        IdGenerator.Default,
        publicationRunner,
      )
    val workItem =
      workItemsService.createWorkItem(
        createWorkItemRequest {
          this.workItem = workItem {
            workItemResourceId = "legacy-worker-item"
            queueResourceId = "test-topid-id"
            workItemParams = Any.pack(testWork { userName = "UserName" })
          }
        }
      )
    val attempt =
      attemptsService.createWorkItemAttempt(
        createWorkItemAttemptRequest {
          expectedWorkItemGeneration = workItem.generation
          workItemAttempt = workItemAttempt {
            workItemResourceId = workItem.workItemResourceId
            workItemAttemptResourceId = "legacy-worker-attempt"
          }
        }
      )

    assertThat(attempt.hasLeaseExpirationTime()).isFalse()
    clock.advance(Duration.ofHours(1))
    assertThat(
        WorkItemAttemptLeaseReaper(spannerDatabase.databaseClient, TestConfig.QUEUE_MAPPING, clock)
          .recoverExpiredAttempts()
      )
      .isEqualTo(0)
    assertThat(
        workItemsService
          .getWorkItem(getWorkItemRequest { workItemResourceId = workItem.workItemResourceId })
          .state
      )
      .isEqualTo(WorkItem.State.RUNNING)
  }

  @Test
  fun `failWorkItemAttempt durably republishes leased attempt after retry delay`() = runBlocking {
    val queueMapping = queueMapping(maxWorkItemAttempts = 2)
    val publisher = RecordingPublisher()
    val clock = MutableClock(Instant.now().plusSeconds(10))
    val publicationRunner =
      WorkItemPublicationRunner(
        spannerDatabase.databaseClient,
        queueMapping,
        publisher,
        clock = clock,
      )
    val attemptsService =
      SpannerWorkItemAttemptsService(
        spannerDatabase.databaseClient,
        queueMapping,
        IdGenerator.Default,
        Dispatchers.Default,
        clock = clock,
      )
    val workItemsService =
      SpannerWorkItemsService(
        spannerDatabase.databaseClient,
        queueMapping,
        IdGenerator.Default,
        publicationRunner,
      )
    val workItem =
      workItemsService.createWorkItem(
        createWorkItemRequest {
          this.workItem = workItem {
            workItemResourceId = "failed-leased-work-item"
            queueResourceId = QUEUE_RESOURCE_ID
            workItemParams = Any.pack(testWork { userName = "UserName" })
          }
        }
      )
    publisher.clear()
    val attempt =
      attemptsService.createWorkItemAttempt(
        createWorkItemAttemptRequest {
          expectedWorkItemGeneration = workItem.generation
          supportsAttemptLease = true
          workItemAttempt = workItemAttempt {
            workItemResourceId = workItem.workItemResourceId
            workItemAttemptResourceId = "failed-leased-attempt"
          }
        }
      )

    val failedAttempt =
      attemptsService.failWorkItemAttempt(
        failWorkItemAttemptRequest {
          workItemResourceId = attempt.workItemResourceId
          workItemAttemptResourceId = attempt.workItemAttemptResourceId
          errorMessage = "permanent failure"
        }
      )
    // Retrying after a lost response must preserve the same recovery publication.
    val repeatedFailure =
      attemptsService.failWorkItemAttempt(
        failWorkItemAttemptRequest {
          workItemResourceId = attempt.workItemResourceId
          workItemAttemptResourceId = attempt.workItemAttemptResourceId
          errorMessage = "permanent failure"
        }
      )

    assertThat(failedAttempt.state).isEqualTo(WorkItemAttempt.State.FAILED)
    assertThat(repeatedFailure.state).isEqualTo(WorkItemAttempt.State.FAILED)
    assertThat(failedAttempt.errorMessage).isEqualTo("permanent failure")
    assertThat(repeatedFailure.errorMessage).isEqualTo("permanent failure")
    val recoveredWorkItem =
      workItemsService.getWorkItem(
        getWorkItemRequest { workItemResourceId = workItem.workItemResourceId }
      )
    assertThat(recoveredWorkItem.state).isEqualTo(WorkItem.State.QUEUED)
    assertThat(recoveredWorkItem.generation).isEqualTo(workItem.generation + 1L)

    assertThat(publicationRunner.publishPendingWorkItems()).isEqualTo(0)
    clock.advance(Duration.ofSeconds(1))
    assertThat(publicationRunner.publishPendingWorkItems()).isEqualTo(1)
    assertThat(publisher.queueNames).containsExactly(QUEUE_RESOURCE_ID)
    assertThat((publisher.messages.single() as WorkItem).generation)
      .isEqualTo(workItem.generation + 1L)
  }

  @Test
  fun `failed leased attempt is dead-lettered when attempt limit is reached`() = runBlocking {
    val queueMapping = queueMapping(maxWorkItemAttempts = 1)
    val publisher = RecordingPublisher()
    val publicationRunner =
      WorkItemPublicationRunner(spannerDatabase.databaseClient, queueMapping, publisher)
    val attemptsService =
      SpannerWorkItemAttemptsService(
        spannerDatabase.databaseClient,
        queueMapping,
        IdGenerator.Default,
        Dispatchers.Default,
      )
    val workItemsService =
      SpannerWorkItemsService(
        spannerDatabase.databaseClient,
        queueMapping,
        IdGenerator.Default,
        publicationRunner,
      )
    val workItem =
      workItemsService.createWorkItem(
        createWorkItemRequest {
          this.workItem = workItem {
            workItemResourceId = "dead-lettered-work-item"
            queueResourceId = QUEUE_RESOURCE_ID
            workItemParams = Any.pack(testWork { userName = "UserName" })
          }
        }
      )
    publisher.clear()
    val attempt =
      attemptsService.createWorkItemAttempt(
        createWorkItemAttemptRequest {
          expectedWorkItemGeneration = workItem.generation
          supportsAttemptLease = true
          workItemAttempt = workItemAttempt {
            workItemResourceId = workItem.workItemResourceId
            workItemAttemptResourceId = "dead-lettered-attempt"
          }
        }
      )

    attemptsService.failWorkItemAttempt(
      failWorkItemAttemptRequest {
        workItemResourceId = attempt.workItemResourceId
        workItemAttemptResourceId = attempt.workItemAttemptResourceId
        errorMessage = "permanent failure"
      }
    )

    val pendingDeadLetter =
      workItemsService.getWorkItem(
        getWorkItemRequest { workItemResourceId = workItem.workItemResourceId }
      )
    assertThat(pendingDeadLetter.state).isEqualTo(WorkItem.State.RUNNING)
    assertThat(pendingDeadLetter.generation).isEqualTo(workItem.generation + 1L)
    val retryError =
      kotlin.test.assertFailsWith<StatusRuntimeException> {
        workItemsService.retryWorkItem(
          retryWorkItemRequest {
            workItemResourceId = workItem.workItemResourceId
            expectedWorkItemGeneration = pendingDeadLetter.generation
          }
        )
      }
    assertThat(retryError.status.code).isEqualTo(Status.Code.FAILED_PRECONDITION)
    assertThat(retryError.errorInfo?.reason)
      .isEqualTo(Errors.Reason.WORK_ITEM_PUBLICATION_PENDING.name)
    val staleDeliveryError =
      kotlin.test.assertFailsWith<StatusRuntimeException> {
        attemptsService.createWorkItemAttempt(
          createWorkItemAttemptRequest {
            expectedWorkItemGeneration = workItem.generation
            supportsAttemptLease = true
            workItemAttempt = workItemAttempt {
              workItemResourceId = workItem.workItemResourceId
              workItemAttemptResourceId = "stale-redelivery-attempt"
            }
          }
        )
      }
    assertThat(staleDeliveryError.status.code).isEqualTo(Status.Code.FAILED_PRECONDITION)
    assertThat(staleDeliveryError.errorInfo?.reason)
      .isEqualTo(Errors.Reason.WORK_ITEM_GENERATION_MISMATCH.name)
    val attempts =
      attemptsService.listWorkItemAttempts(
        listWorkItemAttemptsRequest { workItemResourceId = workItem.workItemResourceId }
      )
    assertThat(attempts.workItemAttemptsList).hasSize(1)
    assertThat(publicationRunner.publishPendingWorkItems()).isEqualTo(1)
    assertThat(publisher.queueNames).containsExactly(DEAD_LETTER_QUEUE_RESOURCE_ID)
    assertThat((publisher.messages.single() as WorkItem).generation)
      .isEqualTo(workItem.generation + 1L)
  }

  @Test
  fun `concurrent duplicate deliveries create only one active attempt`() = runBlocking {
    val publisher =
      object : WorkItemPublisher {
        override suspend fun publishMessage(queueName: String, message: Message) {}
      }
    val services = initServices(TestConfig.QUEUE_MAPPING, IdGenerator.Default, publisher)
    val workItem =
      services.workItemsService.createWorkItem(
        createWorkItemRequest {
          this.workItem = workItem {
            workItemResourceId = "duplicate-work-item"
            queueResourceId = "test-topid-id"
            workItemParams = Any.pack(testWork { userName = "UserName" })
          }
        }
      )

    val results =
      listOf("attempt-one", "attempt-two")
        .map { attemptId ->
          async(Dispatchers.Default) {
            runCatching {
              services.service.createWorkItemAttempt(
                createWorkItemAttemptRequest {
                  expectedWorkItemGeneration = workItem.generation
                  this.workItemAttempt = workItemAttempt {
                    workItemResourceId = workItem.workItemResourceId
                    workItemAttemptResourceId = attemptId
                  }
                }
              )
            }
          }
        }
        .awaitAll()

    assertThat(results.count { it.isSuccess }).isEqualTo(1)
    val failure = results.single { it.isFailure }.exceptionOrNull() as StatusRuntimeException
    assertThat(failure.status.code).isEqualTo(Status.Code.FAILED_PRECONDITION)
  }

  @Test
  fun `stale delivery cannot create an attempt for a replacement generation`() = runBlocking {
    val services =
      initServices(
        TestConfig.QUEUE_MAPPING,
        IdGenerator.Default,
        object : WorkItemPublisher {
          override suspend fun publishMessage(queueName: String, message: Message) {}
        },
      )
    val original =
      services.workItemsService.createWorkItem(
        createWorkItemRequest {
          workItem = workItem {
            workItemResourceId = "generation-fenced-work-item"
            queueResourceId = "test-topid-id"
            workItemParams = Any.pack(testWork { userName = "UserName" })
          }
        }
      )
    val originalAttempt =
      services.service.createWorkItemAttempt(
        createWorkItemAttemptRequest {
          expectedWorkItemGeneration = original.generation
          workItemAttempt = workItemAttempt {
            workItemResourceId = original.workItemResourceId
            workItemAttemptResourceId = "original-attempt"
          }
        }
      )
    services.service.failWorkItemAttempt(
      failWorkItemAttemptRequest {
        workItemResourceId = originalAttempt.workItemResourceId
        workItemAttemptResourceId = originalAttempt.workItemAttemptResourceId
      }
    )
    services.workItemsService.failWorkItem(
      failWorkItemRequest {
        workItemResourceId = original.workItemResourceId
        expectedWorkItemGeneration = original.generation
      }
    )
    val replacement =
      services.workItemsService.retryWorkItem(
        retryWorkItemRequest { workItemResourceId = original.workItemResourceId }
      )

    val exception =
      kotlin.test.assertFailsWith<StatusRuntimeException> {
        services.service.createWorkItemAttempt(
          createWorkItemAttemptRequest {
            expectedWorkItemGeneration = original.generation
            workItemAttempt = workItemAttempt {
              workItemResourceId = original.workItemResourceId
              workItemAttemptResourceId = "stale-attempt"
            }
          }
        )
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.FAILED_PRECONDITION)
    assertThat(exception.errorInfo?.reason)
      .isEqualTo(Errors.Reason.WORK_ITEM_GENERATION_MISMATCH.name)
    val afterStaleDelivery =
      services.workItemsService.getWorkItem(
        getWorkItemRequest { workItemResourceId = replacement.workItemResourceId }
      )
    assertThat(afterStaleDelivery.state).isEqualTo(WorkItem.State.QUEUED)
    assertThat(afterStaleDelivery.generation).isEqualTo(replacement.generation)
    val currentAttempt =
      services.service.createWorkItemAttempt(
        createWorkItemAttemptRequest {
          expectedWorkItemGeneration = replacement.generation
          workItemAttempt = workItemAttempt {
            workItemResourceId = replacement.workItemResourceId
            workItemAttemptResourceId = "replacement-attempt"
          }
        }
      )
    val attempts =
      services.service
        .listWorkItemAttempts(
          listWorkItemAttemptsRequest { workItemResourceId = replacement.workItemResourceId }
        )
        .workItemAttemptsList

    assertThat(replacement.generation).isEqualTo(original.generation + 1L)
    assertThat(currentAttempt.state).isEqualTo(WorkItemAttempt.State.ACTIVE)
    assertThat(attempts.count { it.state == WorkItemAttempt.State.ACTIVE }).isEqualTo(1)
  }

  companion object {
    @get:ClassRule @JvmStatic val spannerEmulator = SpannerEmulatorRule()

    private const val QUEUE_RESOURCE_ID = "test-topid-id"
    private const val DEAD_LETTER_QUEUE_RESOURCE_ID = "$QUEUE_RESOURCE_ID-dlq"
  }

  private class RecordingPublisher : WorkItemPublisher {
    val messages = mutableListOf<Message>()
    val queueNames = mutableListOf<String>()

    override suspend fun publishMessage(queueName: String, message: Message) {
      queueNames += queueName
      messages += message
    }

    fun clear() {
      queueNames.clear()
      messages.clear()
    }
  }

  private fun queueMapping(maxWorkItemAttempts: Int): QueueMapping {
    return QueueMapping(
      queuesConfig {
        queueInfos.add(
          queueInfo {
            queueResourceId = QUEUE_RESOURCE_ID
            deadLetterQueueResourceId = DEAD_LETTER_QUEUE_RESOURCE_ID
            this.maxWorkItemAttempts = maxWorkItemAttempts
          }
        )
      }
    )
  }

  private class MutableClock(private var currentInstant: Instant) : Clock() {
    override fun getZone(): ZoneId = ZoneOffset.UTC

    override fun withZone(zone: ZoneId): Clock = this

    override fun instant(): Instant = currentInstant

    fun advance(duration: Duration) {
      currentInstant = currentInstant.plus(duration)
    }
  }
}
