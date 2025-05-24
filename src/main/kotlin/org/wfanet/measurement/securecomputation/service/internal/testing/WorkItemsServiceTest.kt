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
import org.wfanet.measurement.internal.securecomputation.controlplane.copy
import org.wfanet.measurement.internal.securecomputation.controlplane.createWorkItemAttemptRequest
import org.wfanet.measurement.internal.securecomputation.controlplane.createWorkItemRequest
import org.wfanet.measurement.internal.securecomputation.controlplane.failWorkItemRequest
import org.wfanet.measurement.internal.securecomputation.controlplane.getWorkItemRequest
import org.wfanet.measurement.internal.securecomputation.controlplane.listWorkItemAttemptsRequest
import org.wfanet.measurement.internal.securecomputation.controlplane.listWorkItemsPageToken
import org.wfanet.measurement.internal.securecomputation.controlplane.listWorkItemsRequest
import org.wfanet.measurement.internal.securecomputation.controlplane.listWorkItemsResponse
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

  companion object {
    private val logger: Logger = Logger.getLogger(this::class.java.name)
  }
}
