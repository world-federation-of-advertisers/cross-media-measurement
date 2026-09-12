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

package org.wfanet.measurement.securecomputation.service.internal.testing

import com.google.common.truth.Truth.assertThat
import com.google.common.truth.extensions.proto.ProtoTruth.assertThat
import com.google.protobuf.Any
import com.google.protobuf.Message
import com.google.rpc.errorInfo
import io.grpc.Status
import io.grpc.StatusRuntimeException
import java.time.Instant
import java.util.logging.Logger
import kotlin.test.assertFailsWith
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.runBlocking
import org.junit.Before
import org.junit.Rule
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.threeten.bp.Duration
import org.wfa.measurement.queue.testing.TestWork
import org.wfa.measurement.queue.testing.testWork
import org.wfanet.measurement.common.IdGenerator
import org.wfanet.measurement.common.grpc.errorInfo
import org.wfanet.measurement.common.toInstant
import org.wfanet.measurement.gcloud.pubsub.testing.GooglePubSubEmulatorClient
import org.wfanet.measurement.gcloud.pubsub.testing.GooglePubSubEmulatorProvider
import org.wfanet.measurement.internal.securecomputation.controlplane.ListWorkItemsPageTokenKt
import org.wfanet.measurement.internal.securecomputation.controlplane.ListWorkItemsRequest
import org.wfanet.measurement.internal.securecomputation.controlplane.ListWorkItemsResponse
import org.wfanet.measurement.internal.securecomputation.controlplane.WorkItem
import org.wfanet.measurement.internal.securecomputation.controlplane.WorkItemAttempt
import org.wfanet.measurement.internal.securecomputation.controlplane.WorkItemAttemptsGrpcKt.WorkItemAttemptsCoroutineImplBase
import org.wfanet.measurement.internal.securecomputation.controlplane.WorkItemsGrpcKt.WorkItemsCoroutineImplBase
import org.wfanet.measurement.internal.securecomputation.controlplane.completeWorkItemAttemptRequest
import org.wfanet.measurement.internal.securecomputation.controlplane.copy
import org.wfanet.measurement.internal.securecomputation.controlplane.createWorkItemAttemptRequest
import org.wfanet.measurement.internal.securecomputation.controlplane.createWorkItemRequest
import org.wfanet.measurement.internal.securecomputation.controlplane.failWorkItemAttemptRequest
import org.wfanet.measurement.internal.securecomputation.controlplane.failWorkItemRequest
import org.wfanet.measurement.internal.securecomputation.controlplane.getWorkItemRequest
import org.wfanet.measurement.internal.securecomputation.controlplane.listWorkItemAttemptsRequest
import org.wfanet.measurement.internal.securecomputation.controlplane.listWorkItemsPageToken
import org.wfanet.measurement.internal.securecomputation.controlplane.listWorkItemsRequest
import org.wfanet.measurement.internal.securecomputation.controlplane.listWorkItemsResponse
import org.wfanet.measurement.internal.securecomputation.controlplane.retryWorkItemRequest
import org.wfanet.measurement.internal.securecomputation.controlplane.workItem
import org.wfanet.measurement.internal.securecomputation.controlplane.workItemAttempt
import org.wfanet.measurement.securecomputation.deploy.gcloud.publisher.GoogleWorkItemPublisher
import org.wfanet.measurement.securecomputation.service.internal.Errors
import org.wfanet.measurement.securecomputation.service.internal.QueueMapping
import org.wfanet.measurement.securecomputation.service.internal.WorkItemPublisher

@RunWith(JUnit4::class)
abstract class WorkItemsServiceTest {

  @Rule @JvmField val pubSubEmulatorProvider = GooglePubSubEmulatorProvider()

  private val projectId = "test-project-id"
  private val topicId = "test-topid-id"
  private val workItemId = "test-work-item-1"
  private val subscriptionId = "test-subscription-id"
  private lateinit var googlePubSubClient: GooglePubSubEmulatorClient

  protected data class Services(
    /** Service under test. */
    val service: WorkItemsCoroutineImplBase,
    val workItemAttemptsService: WorkItemAttemptsCoroutineImplBase,
  )

  @Before
  fun createGooglePubSubEmulator() {
    googlePubSubClient =
      GooglePubSubEmulatorClient(
        host = pubSubEmulatorProvider.host,
        port = pubSubEmulatorProvider.port,
      )
  }

  private suspend fun deleteSubscriptionAndTopic() {
    googlePubSubClient.deleteSubscription(projectId, subscriptionId)
    googlePubSubClient.deleteTopic(projectId, topicId)
  }

  /** Initializes the service under test. */
  protected abstract fun initServices(
    queueMapping: QueueMapping,
    idGenerator: IdGenerator,
    workItemPublisher: WorkItemPublisher,
  ): Services

  private fun initServices(idGenerator: IdGenerator = IdGenerator.Default): Services {
    val workItemPublisher: WorkItemPublisher =
      GoogleWorkItemPublisher(projectId, googlePubSubClient)
    return initServices(TestConfig.QUEUE_MAPPING, idGenerator, workItemPublisher)
  }

  @Test
  fun `createWorkItem returns created WorkItem`() = runBlocking {
    val services = initServices()

    googlePubSubClient.createTopic(projectId, topicId)
    googlePubSubClient.createSubscription(projectId, subscriptionId, topicId)

    val request = createWorkItemRequest {
      workItem = workItem {
        workItemResourceId = "work_item_resource_id"
        queueResourceId = topicId
        workItemParams =
          Any.pack(
            testWork {
              userName = "UserName"
              userAge = "25"
              userCountry = "US"
            }
          )
      }
    }

    val createResponse: WorkItem = services.service.createWorkItem(request)

    assertThat(createResponse)
      .ignoringFields(
        WorkItem.CREATE_TIME_FIELD_NUMBER,
        WorkItem.UPDATE_TIME_FIELD_NUMBER,
        WorkItem.WORK_ITEM_RESOURCE_ID_FIELD_NUMBER,
      )
      .isEqualTo(
        request.workItem.copy {
          state = WorkItem.State.QUEUED
          generation = 1L
          workItemResourceId = "work_item_resource_id"
        }
      )
    assertThat(createResponse.createTime.toInstant()).isGreaterThan(Instant.now().minusSeconds(10))
    assertThat(createResponse.updateTime).isEqualTo(createResponse.createTime)

    val deferred = CompletableDeferred<String>()
    val subscriber =
      googlePubSubClient.buildSubscriber(
        projectId = projectId,
        subscriptionId = "test-subscription-id",
        ackExtensionPeriod = Duration.ofHours(6),
      ) { message, consumer ->
        try {
          val workItem = WorkItem.parseFrom(message.data.toByteArray())
          val testWork = workItem.workItemParams.unpack(TestWork::class.java)
          deferred.complete(testWork.userName)
          consumer.ack()
        } catch (e: Exception) {
          val stackTrace = e.stackTrace.joinToString("\n")
          logger.info("Subscriber Exception: $stackTrace")
          consumer.nack()
        }
      }
    subscriber.startAsync().awaitRunning()
    val result = deferred.await()
    assertThat(result).isEqualTo("UserName")

    val getRequest = getWorkItemRequest { workItemResourceId = createResponse.workItemResourceId }
    val workItem = services.service.getWorkItem(getRequest)

    assertThat(createResponse).isEqualTo(workItem)

    deleteSubscriptionAndTopic()
  }

  @Test
  fun `createWorkItem throws INVALID_ARGUMENT if queueResourceId is missing`() = runBlocking {
    val services = initServices()

    googlePubSubClient.createTopic(projectId, topicId)
    googlePubSubClient.createSubscription(projectId, subscriptionId, topicId)

    val request = createWorkItemRequest {
      workItem = workItem { workItemResourceId = "work_item_resource_id" }
    }

    val exception =
      assertFailsWith<StatusRuntimeException> { services.service.createWorkItem(request) }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.errorInfo)
      .isEqualTo(
        errorInfo {
          domain = Errors.DOMAIN
          reason = Errors.Reason.REQUIRED_FIELD_NOT_SET.name
          metadata[Errors.Metadata.FIELD_NAME.key] = "queue_resource_id"
        }
      )
    deleteSubscriptionAndTopic()
  }

  @Test
  fun `createWorkItem throws INVALID_ARGUMENT if workItemParams is missing`() = runBlocking {
    val services = initServices()

    googlePubSubClient.createTopic(projectId, topicId)
    googlePubSubClient.createSubscription(projectId, subscriptionId, topicId)

    val request = createWorkItemRequest {
      workItem = workItem {
        workItemResourceId = "work_item_resource_id"
        queueResourceId = topicId
      }
    }

    val exception =
      assertFailsWith<StatusRuntimeException> { services.service.createWorkItem(request) }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.errorInfo)
      .isEqualTo(
        errorInfo {
          domain = Errors.DOMAIN
          reason = Errors.Reason.REQUIRED_FIELD_NOT_SET.name
          metadata[Errors.Metadata.FIELD_NAME.key] = "work_item_params"
        }
      )
    deleteSubscriptionAndTopic()
  }

  @Test
  fun `createWorkItem throws INVALID_ARGUMENT if workItemResourceId is missing`() = runBlocking {
    val services = initServices()

    googlePubSubClient.createTopic(projectId, topicId)
    googlePubSubClient.createSubscription(projectId, subscriptionId, topicId)

    val request = createWorkItemRequest {
      workItem = workItem {
        queueResourceId = topicId
        workItemParams =
          Any.pack(
            testWork {
              userName = "UserName"
              userAge = "25"
              userCountry = "US"
            }
          )
      }
    }

    val exception =
      assertFailsWith<StatusRuntimeException> { services.service.createWorkItem(request) }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.errorInfo)
      .isEqualTo(
        errorInfo {
          domain = Errors.DOMAIN
          reason = Errors.Reason.REQUIRED_FIELD_NOT_SET.name
          metadata[Errors.Metadata.FIELD_NAME.key] = "work_item_resource_id"
        }
      )
    deleteSubscriptionAndTopic()
  }

  @Test
  fun `createWorkItem throws FAILED_PRECONDITION if queue_resource_id not found`() = runBlocking {
    val services = initServices()
    val request = createWorkItemRequest {
      workItem = workItem {
        workItemResourceId = "work_item_resource_id"
        workItemParams =
          Any.pack(
            testWork {
              userName = "UserName"
              userAge = "25"
              userCountry = "US"
            }
          )
        queueResourceId = "non_existing_queue"
      }
    }

    val exception =
      assertFailsWith<StatusRuntimeException> { services.service.createWorkItem(request) }

    assertThat(exception.status.code).isEqualTo(Status.Code.FAILED_PRECONDITION)
    assertThat(exception.errorInfo)
      .isEqualTo(
        errorInfo {
          domain = Errors.DOMAIN
          reason = Errors.Reason.QUEUE_NOT_FOUND.name
          metadata[Errors.Metadata.QUEUE_RESOURCE_ID.key] = "non_existing_queue"
        }
      )
  }

  @Test
  fun `getWorkItem throws INVALID_ARGUMENT if workItemResourceId is missing`() = runBlocking {
    val services = initServices()
    val request = getWorkItemRequest {}

    val exception =
      assertFailsWith<StatusRuntimeException> { services.service.getWorkItem(request) }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.errorInfo)
      .isEqualTo(
        errorInfo {
          domain = Errors.DOMAIN
          reason = Errors.Reason.REQUIRED_FIELD_NOT_SET.name
          metadata[Errors.Metadata.FIELD_NAME.key] = "work_item_resource_id"
        }
      )
  }

  @Test
  fun `getWorkItem throws NOT_FOUND when WorkItem not found`() = runBlocking {
    val services = initServices()
    val request = getWorkItemRequest { workItemResourceId = "123" }

    val exception =
      assertFailsWith<StatusRuntimeException> { services.service.getWorkItem(request) }

    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
    assertThat(exception.errorInfo)
      .isEqualTo(
        errorInfo {
          domain = Errors.DOMAIN
          reason = Errors.Reason.WORK_ITEM_NOT_FOUND.name
          metadata[Errors.Metadata.WORK_ITEM_RESOURCE_ID.key] = "123"
        }
      )
  }

  @Test
  fun `failWorkItem returns WorkItem with updated state`() = runBlocking {
    val services = initServices()

    googlePubSubClient.createTopic(projectId, topicId)
    googlePubSubClient.createSubscription(projectId, subscriptionId, topicId)

    val request = createWorkItemRequest {
      workItem = workItem {
        workItemResourceId = "work_item_resource_id"
        queueResourceId = topicId
        workItemParams =
          Any.pack(
            testWork {
              userName = "UserName"
              userAge = "25"
              userCountry = "US"
            }
          )
      }
    }

    val createResponse: WorkItem = services.service.createWorkItem(request)
    val workItemAttemptRequest = createWorkItemAttemptRequest {
      expectedWorkItemGeneration = createResponse.generation
      workItemAttempt = workItemAttempt {
        workItemResourceId = createResponse.workItemResourceId
        workItemAttemptResourceId = "work_item_attempt_resource_id"
      }
    }

    services.workItemAttemptsService.createWorkItemAttempt(workItemAttemptRequest)

    val failRequest = failWorkItemRequest { workItemResourceId = createResponse.workItemResourceId }
    val workItem = services.service.failWorkItem(failRequest)

    assertThat(workItem)
      .ignoringFields(WorkItem.UPDATE_TIME_FIELD_NUMBER)
      .isEqualTo(createResponse.copy { state = WorkItem.State.FAILED })

    val listWorkItemAttemptsRequest = listWorkItemAttemptsRequest {
      workItemResourceId = workItem.workItemResourceId
    }
    val listWorkItemAttemptsResponse =
      services.workItemAttemptsService.listWorkItemAttempts(listWorkItemAttemptsRequest)

    assertThat(
        listWorkItemAttemptsResponse.workItemAttemptsList.all {
          it.state == WorkItemAttempt.State.FAILED
        }
      )
      .isTrue()
  }

  @Test
  fun `failWorkItem is idempotent for the same generation`() = runBlocking {
    val services = initServicesWithNoOpPublisher()
    val created = createWorkItem(services.service)
    createWorkItemAttempt(services, created, "attempt")
    val request = failWorkItemRequest {
      workItemResourceId = created.workItemResourceId
      expectedWorkItemGeneration = created.generation
    }

    val firstResponse = services.service.failWorkItem(request)
    val secondResponse = services.service.failWorkItem(request)

    assertThat(firstResponse.state).isEqualTo(WorkItem.State.FAILED)
    assertThat(secondResponse.state).isEqualTo(WorkItem.State.FAILED)
    assertThat(secondResponse.generation).isEqualTo(created.generation)
  }

  @Test
  fun `failWorkItem throws INVALID_ARGUMENT if workItemResourceId is missing`() = runBlocking {
    val services = initServices()
    val failRequest = failWorkItemRequest {}

    val exception =
      assertFailsWith<StatusRuntimeException> { services.service.failWorkItem(failRequest) }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.errorInfo)
      .isEqualTo(
        errorInfo {
          domain = Errors.DOMAIN
          reason = Errors.Reason.REQUIRED_FIELD_NOT_SET.name
          metadata[Errors.Metadata.FIELD_NAME.key] = "work_item_resource_id"
        }
      )
  }

  @Test
  fun `retryWorkItem returns failed WorkItem to queue`() = runBlocking {
    var publicationCount = 0
    val services =
      initServices(
        TestConfig.QUEUE_MAPPING,
        IdGenerator.Default,
        object : WorkItemPublisher {
          override suspend fun publishMessage(queueName: String, message: Message) {
            publicationCount++
          }
        },
      )
    val created =
      services.service.createWorkItem(
        createWorkItemRequest {
          workItem = workItem {
            workItemResourceId = workItemId
            queueResourceId = topicId
            workItemParams = Any.pack(testWork { userName = "UserName" })
          }
        }
      )
    val attempt =
      services.workItemAttemptsService.createWorkItemAttempt(
        createWorkItemAttemptRequest {
          expectedWorkItemGeneration = created.generation
          workItemAttempt = workItemAttempt {
            workItemResourceId = created.workItemResourceId
            workItemAttemptResourceId = "attempt"
          }
        }
      )
    services.service.failWorkItem(
      failWorkItemRequest { workItemResourceId = created.workItemResourceId }
    )

    val retried =
      services.service.retryWorkItem(
        retryWorkItemRequest { workItemResourceId = created.workItemResourceId }
      )

    assertThat(retried.state).isEqualTo(WorkItem.State.QUEUED)
    assertThat(retried.generation).isEqualTo(created.generation + 1L)
    assertThat(publicationCount).isEqualTo(2)
    assertThat(
        services.workItemAttemptsService
          .getWorkItemAttempt(
            org.wfanet.measurement.internal.securecomputation.controlplane
              .getWorkItemAttemptRequest {
                workItemResourceId = attempt.workItemResourceId
                workItemAttemptResourceId = attempt.workItemAttemptResourceId
              }
          )
          .state
      )
      .isEqualTo(WorkItemAttempt.State.FAILED)
  }

  @Test
  fun `retryWorkItem republishes queued WorkItem with missing outbox record`() = runBlocking {
    var publicationCount = 0
    val services =
      initServices(
        TestConfig.QUEUE_MAPPING,
        IdGenerator.Default,
        object : WorkItemPublisher {
          override suspend fun publishMessage(queueName: String, message: Message) {
            publicationCount++
          }
        },
      )
    val created =
      services.service.createWorkItem(
        createWorkItemRequest {
          workItem = workItem {
            workItemResourceId = workItemId
            queueResourceId = topicId
            workItemParams = Any.pack(testWork { userName = "UserName" })
          }
        }
      )

    val repaired =
      services.service.retryWorkItem(
        retryWorkItemRequest { workItemResourceId = created.workItemResourceId }
      )

    assertThat(repaired.state).isEqualTo(WorkItem.State.QUEUED)
    assertThat(publicationCount).isEqualTo(2)
  }

  @Test
  fun `retryWorkItem retries abandoned running WorkItem`() = runBlocking {
    var publicationCount = 0
    val services =
      initServices(
        TestConfig.QUEUE_MAPPING,
        IdGenerator.Default,
        object : WorkItemPublisher {
          override suspend fun publishMessage(queueName: String, message: Message) {
            publicationCount++
          }
        },
      )
    val created =
      services.service.createWorkItem(
        createWorkItemRequest {
          workItem = workItem {
            workItemResourceId = workItemId
            queueResourceId = topicId
            workItemParams = Any.pack(testWork { userName = "UserName" })
          }
        }
      )

    val abandonedAttempt =
      services.workItemAttemptsService.createWorkItemAttempt(
        createWorkItemAttemptRequest {
          expectedWorkItemGeneration = created.generation
          workItemAttempt = workItemAttempt {
            workItemResourceId = created.workItemResourceId
            workItemAttemptResourceId = "abandoned-attempt"
          }
        }
      )
    services.workItemAttemptsService.failWorkItemAttempt(
      failWorkItemAttemptRequest {
        workItemResourceId = abandonedAttempt.workItemResourceId
        workItemAttemptResourceId = abandonedAttempt.workItemAttemptResourceId
        errorMessage = "Worker confirmed stopped"
      }
    )

    val retried =
      services.service.retryWorkItem(
        retryWorkItemRequest { workItemResourceId = created.workItemResourceId }
      )
    val failedAttempt =
      services.workItemAttemptsService.getWorkItemAttempt(
        org.wfanet.measurement.internal.securecomputation.controlplane.getWorkItemAttemptRequest {
          workItemResourceId = abandonedAttempt.workItemResourceId
          workItemAttemptResourceId = abandonedAttempt.workItemAttemptResourceId
        }
      )
    val replacementAttempt =
      services.workItemAttemptsService.createWorkItemAttempt(
        createWorkItemAttemptRequest {
          expectedWorkItemGeneration = retried.generation
          workItemAttempt = workItemAttempt {
            workItemResourceId = created.workItemResourceId
            workItemAttemptResourceId = "replacement-attempt"
          }
        }
      )

    assertThat(retried.state).isEqualTo(WorkItem.State.QUEUED)
    assertThat(failedAttempt.state).isEqualTo(WorkItemAttempt.State.FAILED)
    assertThat(replacementAttempt.state).isEqualTo(WorkItemAttempt.State.ACTIVE)
    assertThat(replacementAttempt.attemptNumber).isEqualTo(2)
    assertThat(publicationCount).isEqualTo(2)
  }

  @Test
  fun `stale retry does not fail replacement attempt`() = runBlocking {
    var publicationCount = 0
    val services =
      initServices(
        TestConfig.QUEUE_MAPPING,
        IdGenerator.Default,
        object : WorkItemPublisher {
          override suspend fun publishMessage(queueName: String, message: Message) {
            publicationCount++
          }
        },
      )
    val created =
      services.service.createWorkItem(
        createWorkItemRequest {
          workItem = workItem {
            workItemResourceId = workItemId
            queueResourceId = topicId
            workItemParams = Any.pack(testWork { userName = "UserName" })
          }
        }
      )
    val abandonedAttempt =
      services.workItemAttemptsService.createWorkItemAttempt(
        createWorkItemAttemptRequest {
          expectedWorkItemGeneration = created.generation
          workItemAttempt = workItemAttempt {
            workItemResourceId = created.workItemResourceId
            workItemAttemptResourceId = "abandoned-attempt"
          }
        }
      )
    services.workItemAttemptsService.failWorkItemAttempt(
      failWorkItemAttemptRequest {
        workItemResourceId = abandonedAttempt.workItemResourceId
        workItemAttemptResourceId = abandonedAttempt.workItemAttemptResourceId
      }
    )
    val retried =
      services.service.retryWorkItem(
        retryWorkItemRequest { workItemResourceId = created.workItemResourceId }
      )
    val replacementAttempt =
      services.workItemAttemptsService.createWorkItemAttempt(
        createWorkItemAttemptRequest {
          expectedWorkItemGeneration = retried.generation
          workItemAttempt = workItemAttempt {
            workItemResourceId = created.workItemResourceId
            workItemAttemptResourceId = "replacement-attempt"
          }
        }
      )

    val exception =
      assertFailsWith<StatusRuntimeException> {
        services.service.retryWorkItem(
          retryWorkItemRequest { workItemResourceId = created.workItemResourceId }
        )
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.FAILED_PRECONDITION)
    assertThat(
        services.workItemAttemptsService
          .getWorkItemAttempt(
            org.wfanet.measurement.internal.securecomputation.controlplane
              .getWorkItemAttemptRequest {
                workItemResourceId = replacementAttempt.workItemResourceId
                workItemAttemptResourceId = replacementAttempt.workItemAttemptResourceId
              }
          )
          .state
      )
      .isEqualTo(WorkItemAttempt.State.ACTIVE)
    assertThat(publicationCount).isEqualTo(2)
  }

  @Test
  fun `stale fail does not fail replacement attempt`() = runBlocking {
    val services = initServicesWithNoOpPublisher()
    val created = createWorkItem(services.service)
    createWorkItemAttempt(services, created, "first-attempt")
    services.service.failWorkItem(
      failWorkItemRequest {
        workItemResourceId = created.workItemResourceId
        expectedWorkItemGeneration = created.generation
      }
    )
    val retried =
      services.service.retryWorkItem(
        retryWorkItemRequest { workItemResourceId = created.workItemResourceId }
      )
    val replacementAttempt = createWorkItemAttempt(services, retried, "replacement-attempt")

    val exception =
      assertFailsWith<StatusRuntimeException> {
        services.service.failWorkItem(
          failWorkItemRequest {
            workItemResourceId = created.workItemResourceId
            expectedWorkItemGeneration = created.generation
          }
        )
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.FAILED_PRECONDITION)
    assertThat(exception.errorInfo?.reason)
      .isEqualTo(Errors.Reason.WORK_ITEM_GENERATION_MISMATCH.name)
    assertThat(
        services.workItemAttemptsService
          .getWorkItemAttempt(
            org.wfanet.measurement.internal.securecomputation.controlplane
              .getWorkItemAttemptRequest {
                workItemResourceId = replacementAttempt.workItemResourceId
                workItemAttemptResourceId = replacementAttempt.workItemAttemptResourceId
              }
          )
          .state
      )
      .isEqualTo(WorkItemAttempt.State.ACTIVE)
    val current =
      services.service.getWorkItem(
        getWorkItemRequest { workItemResourceId = created.workItemResourceId }
      )
    assertThat(current.state).isEqualTo(WorkItem.State.RUNNING)
    assertThat(current.generation).isEqualTo(retried.generation)
  }

  @Test
  fun `failWorkItem does not fail succeeded WorkItem`() = runBlocking {
    val services = initServicesWithNoOpPublisher()
    val created = createWorkItem(services.service)
    val attempt = createWorkItemAttempt(services, created, "attempt")
    services.workItemAttemptsService.completeWorkItemAttempt(
      completeWorkItemAttemptRequest {
        workItemResourceId = attempt.workItemResourceId
        workItemAttemptResourceId = attempt.workItemAttemptResourceId
      }
    )

    val exception =
      assertFailsWith<StatusRuntimeException> {
        services.service.failWorkItem(
          failWorkItemRequest {
            workItemResourceId = created.workItemResourceId
            expectedWorkItemGeneration = created.generation
          }
        )
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.FAILED_PRECONDITION)
    assertThat(exception.errorInfo?.reason).isEqualTo(Errors.Reason.INVALID_WORK_ITEM_STATE.name)
    assertThat(
        services.service
          .getWorkItem(getWorkItemRequest { workItemResourceId = created.workItemResourceId })
          .state
      )
      .isEqualTo(WorkItem.State.SUCCEEDED)
  }

  @Test
  fun `listWorkItems returns workItems ordered by create time`() = runBlocking {
    val services = initServices()

    googlePubSubClient.createTopic(projectId, topicId)
    googlePubSubClient.createSubscription(projectId, subscriptionId, topicId)

    val workItems: List<WorkItem> = createWorkItems(services.service, 10)

    val response: ListWorkItemsResponse =
      services.service.listWorkItems(ListWorkItemsRequest.getDefaultInstance())

    assertThat(response).isEqualTo(listWorkItemsResponse { this.workItems += workItems })
    deleteSubscriptionAndTopic()
  }

  @Test
  fun `listWorkItems returns workItems when page size is specified`() = runBlocking {
    val services = initServices()

    googlePubSubClient.createTopic(projectId, topicId)
    googlePubSubClient.createSubscription(projectId, subscriptionId, topicId)

    val workItems: List<WorkItem> = createWorkItems(services.service, 10)

    val response: ListWorkItemsResponse =
      services.service.listWorkItems(listWorkItemsRequest { pageSize = 10 })

    assertThat(response).isEqualTo(listWorkItemsResponse { this.workItems += workItems })
    deleteSubscriptionAndTopic()
  }

  @Test
  fun `listWorkItems returns next page token when there are more results`() = runBlocking {
    val services = initServices()

    googlePubSubClient.createTopic(projectId, topicId)
    googlePubSubClient.createSubscription(projectId, subscriptionId, topicId)

    val workItems: List<WorkItem> = createWorkItems(services.service, 10)

    val request = listWorkItemsRequest { pageSize = 5 }
    val response: ListWorkItemsResponse = services.service.listWorkItems(request)
    assertThat(response)
      .isEqualTo(
        listWorkItemsResponse {
          this.workItems += workItems.take(request.pageSize)
          nextPageToken = listWorkItemsPageToken {
            after =
              ListWorkItemsPageTokenKt.after {
                workItemResourceId = workItems.get(4).workItemResourceId
                createdAfter = workItems.get(4).createTime
              }
          }
        }
      )
    deleteSubscriptionAndTopic()
  }

  @Test
  fun `listWorkItems returns results after page token`() = runBlocking {
    val services = initServices()

    googlePubSubClient.createTopic(projectId, topicId)
    googlePubSubClient.createSubscription(projectId, subscriptionId, topicId)

    val workItems: List<WorkItem> = createWorkItems(services.service, 10)

    val request = listWorkItemsRequest {
      pageSize = 2
      pageToken = listWorkItemsPageToken {
        after =
          ListWorkItemsPageTokenKt.after {
            workItemResourceId = workItems.get(4).workItemResourceId
            createdAfter = workItems.get(4).createTime
          }
      }
    }
    val response: ListWorkItemsResponse = services.service.listWorkItems(request)
    assertThat(response)
      .isEqualTo(
        listWorkItemsResponse {
          this.workItems += workItems.subList(5, 7)
          nextPageToken = listWorkItemsPageToken {
            after =
              ListWorkItemsPageTokenKt.after {
                workItemResourceId = workItems.get(6).workItemResourceId
                createdAfter = workItems.get(6).createTime
              }
          }
        }
      )

    deleteSubscriptionAndTopic()
  }

  private suspend fun createWorkItems(
    service: WorkItemsCoroutineImplBase,
    count: Int,
  ): List<WorkItem> {
    return (1..count).map {
      val workItemResourceId = "work_item_id_$it"
      service.createWorkItem(
        createWorkItemRequest {
          workItem = workItem {
            this.workItemResourceId = workItemResourceId
            queueResourceId = topicId
            workItemParams =
              Any.pack(
                testWork {
                  userName = "UserName"
                  userAge = "25"
                  userCountry = "US"
                }
              )
          }
        }
      )
    }
  }

  private fun initServicesWithNoOpPublisher(): Services {
    return initServices(
      TestConfig.QUEUE_MAPPING,
      IdGenerator.Default,
      object : WorkItemPublisher {
        override suspend fun publishMessage(queueName: String, message: Message) {}
      },
    )
  }

  private suspend fun createWorkItem(service: WorkItemsCoroutineImplBase): WorkItem {
    return service.createWorkItem(
      createWorkItemRequest {
        workItem = workItem {
          workItemResourceId = workItemId
          queueResourceId = topicId
          workItemParams = Any.pack(testWork { userName = "UserName" })
        }
      }
    )
  }

  private suspend fun createWorkItemAttempt(
    services: Services,
    workItem: WorkItem,
    resourceId: String,
  ): WorkItemAttempt {
    return services.workItemAttemptsService.createWorkItemAttempt(
      createWorkItemAttemptRequest {
        expectedWorkItemGeneration = workItem.generation
        workItemAttempt = workItemAttempt {
          workItemResourceId = workItem.workItemResourceId
          workItemAttemptResourceId = resourceId
        }
      }
    )
  }

  companion object {
    private val logger: Logger = Logger.getLogger(this::class.java.name)
  }
}
