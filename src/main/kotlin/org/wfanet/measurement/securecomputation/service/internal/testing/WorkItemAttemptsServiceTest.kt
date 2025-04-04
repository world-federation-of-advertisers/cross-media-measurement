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
import kotlinx.coroutines.runBlocking
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfa.measurement.queue.testing.testWork
import org.wfanet.measurement.common.IdGenerator
import org.wfanet.measurement.common.grpc.errorInfo
import org.wfanet.measurement.common.toInstant
import org.wfanet.measurement.internal.securecomputation.controlplane.ListWorkItemAttemptsPageTokenKt
import org.wfanet.measurement.internal.securecomputation.controlplane.WorkItem
import org.wfanet.measurement.internal.securecomputation.controlplane.WorkItemAttempt
import org.wfanet.measurement.internal.securecomputation.controlplane.WorkItemAttemptsGrpcKt.WorkItemAttemptsCoroutineImplBase
import org.wfanet.measurement.internal.securecomputation.controlplane.WorkItemsGrpcKt.WorkItemsCoroutineImplBase
import org.wfanet.measurement.internal.securecomputation.controlplane.completeWorkItemAttemptRequest
import org.wfanet.measurement.internal.securecomputation.controlplane.copy
import org.wfanet.measurement.internal.securecomputation.controlplane.createWorkItemAttemptRequest
import org.wfanet.measurement.internal.securecomputation.controlplane.createWorkItemRequest
import org.wfanet.measurement.internal.securecomputation.controlplane.failWorkItemAttemptRequest
import org.wfanet.measurement.internal.securecomputation.controlplane.getWorkItemAttemptRequest
import org.wfanet.measurement.internal.securecomputation.controlplane.getWorkItemRequest
import org.wfanet.measurement.internal.securecomputation.controlplane.listWorkItemAttemptsPageToken
import org.wfanet.measurement.internal.securecomputation.controlplane.listWorkItemAttemptsRequest
import org.wfanet.measurement.internal.securecomputation.controlplane.listWorkItemAttemptsResponse
import org.wfanet.measurement.internal.securecomputation.controlplane.workItem
import org.wfanet.measurement.internal.securecomputation.controlplane.workItemAttempt
import org.wfanet.measurement.securecomputation.service.internal.Errors
import org.wfanet.measurement.securecomputation.service.internal.QueueMapping
import org.wfanet.measurement.securecomputation.service.internal.WorkItemPublisher

@RunWith(JUnit4::class)
abstract class WorkItemAttemptsServiceTest {

  protected data class Services(
    /** Service under test. */
    val service: WorkItemAttemptsCoroutineImplBase,
    val workItemsService: WorkItemsCoroutineImplBase,
  )

  /** Initializes the service under test. */
  protected abstract fun initServices(
    queueMapping: QueueMapping,
    idGenerator: IdGenerator,
    workItemPublisher: WorkItemPublisher,
  ): Services

  private fun initServices(idGenerator: IdGenerator = IdGenerator.Default): Services {
    val fakePublisher =
      object : WorkItemPublisher {
        override suspend fun publishMessage(queueName: String, message: Message) {
          logger.info("message published")
        }
      }
    return initServices(TestConfig.QUEUE_MAPPING, idGenerator, fakePublisher)
  }

  @Test
  fun `createWorkAttemptItem returns created WorkItemAttempt`() = runBlocking {
    val services = initServices()
    val workItem = createWorkItem(services.workItemsService)
    val request = createWorkItemAttemptRequest {
      workItemAttempt = workItemAttempt {
        workItemResourceId = workItem.workItemResourceId
        workItemAttemptResourceId = "work_item_attempt_resource_id"
      }
    }
    val workItemAttempt = services.service.createWorkItemAttempt(request)

    assertThat(workItemAttempt)
      .ignoringFields(
        WorkItemAttempt.CREATE_TIME_FIELD_NUMBER,
        WorkItemAttempt.UPDATE_TIME_FIELD_NUMBER,
      )
      .isEqualTo(
        request.workItemAttempt.copy {
          workItemResourceId = workItem.workItemResourceId
          workItemAttemptResourceId = "work_item_attempt_resource_id"
          state = WorkItemAttempt.State.ACTIVE
          attemptNumber = 1
        }
      )
    assertThat(workItemAttempt.createTime.toInstant()).isGreaterThan(Instant.now().minusSeconds(10))
    assertThat(workItemAttempt.updateTime).isEqualTo(workItemAttempt.createTime)

    val getResponse =
      services.service.getWorkItemAttempt(
        getWorkItemAttemptRequest {
          workItemResourceId = workItem.workItemResourceId
          workItemAttemptResourceId = workItemAttempt.workItemAttemptResourceId
        }
      )
    assertThat(getResponse).isEqualTo(workItemAttempt)
  }

  @Test
  fun `createWorkItemAttempt throws INVALID_ARGUMENT if workItemResourceId is missing`() =
    runBlocking {
      val services = initServices()
      val request = createWorkItemAttemptRequest { workItemAttempt = workItemAttempt {} }

      val exception =
        assertFailsWith<StatusRuntimeException> { services.service.createWorkItemAttempt(request) }

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
  fun `createWorkItemAttempt throws INVALID_ARGUMENT if workItemAttemptResourceId is missing`() =
    runBlocking {
      val services = initServices()
      val request = createWorkItemAttemptRequest {
        workItemAttempt = workItemAttempt { workItemResourceId = "123" }
      }

      val exception =
        assertFailsWith<StatusRuntimeException> { services.service.createWorkItemAttempt(request) }

      assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
      assertThat(exception.errorInfo)
        .isEqualTo(
          errorInfo {
            domain = Errors.DOMAIN
            reason = Errors.Reason.REQUIRED_FIELD_NOT_SET.name
            metadata[Errors.Metadata.FIELD_NAME.key] = "work_item_attempt_resource_id"
          }
        )
    }

  @Test
  fun `getWorkItemAttempt throws INVALID_ARGUMENT if workItemResourceId is missing`() =
    runBlocking {
      val services = initServices()
      val request = getWorkItemAttemptRequest {}
      val exception =
        assertFailsWith<StatusRuntimeException> { services.service.getWorkItemAttempt(request) }

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
  fun `getWorkItemAttempt throws INVALID_ARGUMENT if workItemAttemptResourceId is missing`() =
    runBlocking {
      val services = initServices()
      val request = getWorkItemAttemptRequest { workItemResourceId = "123" }
      val exception =
        assertFailsWith<StatusRuntimeException> { services.service.getWorkItemAttempt(request) }

      assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
      assertThat(exception.errorInfo)
        .isEqualTo(
          errorInfo {
            domain = Errors.DOMAIN
            reason = Errors.Reason.REQUIRED_FIELD_NOT_SET.name
            metadata[Errors.Metadata.FIELD_NAME.key] = "work_item_attempt_resource_id"
          }
        )
    }

  @Test
  fun `getWorkItemAttempt throws NOT_FOUND when WorkItemAttempt not found`() = runBlocking {
    val services = initServices()
    val request = getWorkItemAttemptRequest {
      workItemResourceId = "123"
      workItemAttemptResourceId = "123"
    }
    val exception =
      assertFailsWith<StatusRuntimeException> { services.service.getWorkItemAttempt(request) }

    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
    assertThat(exception.errorInfo)
      .isEqualTo(
        errorInfo {
          domain = Errors.DOMAIN
          reason = Errors.Reason.WORK_ITEM_ATTEMPT_NOT_FOUND.name
          metadata[Errors.Metadata.WORK_ITEM_RESOURCE_ID.key] = "123"
          metadata[Errors.Metadata.WORK_ITEM_ATTEMPT_RESOURCE_ID.key] = "123"
        }
      )
  }

  @Test
  fun `failWorkItemAttempt returns WorkItemAttempt with updated state`() = runBlocking {
    val services = initServices()
    val workItem: WorkItem = createWorkItem(services.workItemsService)
    val workItemAttempt =
      createWorkItemAttempts(services.service, workItem.workItemResourceId, 1).get(0)

    val failWorkItemAttemptRequest = failWorkItemAttemptRequest {
      workItemResourceId = workItemAttempt.workItemResourceId
      workItemAttemptResourceId = workItemAttempt.workItemAttemptResourceId
      errorMessage = "ErrorMessage"
    }

    val updatedWorkItemAttempt = services.service.failWorkItemAttempt(failWorkItemAttemptRequest)

    assertThat(workItemAttempt)
      .ignoringFields(WorkItemAttempt.UPDATE_TIME_FIELD_NUMBER, WorkItemAttempt.STATE_FIELD_NUMBER)
      .isEqualTo(updatedWorkItemAttempt)
    assertThat(updatedWorkItemAttempt.state).isEqualTo(WorkItemAttempt.State.FAILED)
  }

  @Test
  fun `failWorkItemAttempt throws INVALID_WORK_ITEM_ATTEMPT_STATE if workItemAttempt state is not ACTIVE`() =
    runBlocking {
      val services = initServices()
      val workItem = createWorkItem(services.workItemsService)
      val workItemAttempt =
        createWorkItemAttempts(services.service, workItem.workItemResourceId, 1).get(0)

      val failWorkItemAttemptRequest = failWorkItemAttemptRequest {
        workItemResourceId = workItemAttempt.workItemResourceId
        workItemAttemptResourceId = workItemAttempt.workItemAttemptResourceId
        errorMessage = "ErrorMessage"
      }

      services.service.failWorkItemAttempt(failWorkItemAttemptRequest)

      val exception =
        assertFailsWith<StatusRuntimeException> {
          services.service.failWorkItemAttempt(failWorkItemAttemptRequest)
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.FAILED_PRECONDITION)
      assertThat(exception.errorInfo)
        .isEqualTo(
          errorInfo {
            domain = Errors.DOMAIN
            reason = Errors.Reason.INVALID_WORK_ITEM_ATTEMPT_STATE.name
            metadata[Errors.Metadata.WORK_ITEM_RESOURCE_ID.key] = "work_item_resource_id"
            metadata[Errors.Metadata.WORK_ITEM_ATTEMPT_RESOURCE_ID.key] =
              "work_item_attempt_resource_id_1"
            metadata[Errors.Metadata.WORK_ITEM_ATTEMPT_STATE.key] = "FAILED"
          }
        )
    }

  @Test
  fun `failWorkItemAttempt throws INVALID_ARGUMENT if workItemResourceId is missing`() =
    runBlocking {
      val services = initServices()
      val failWorkItemAttemptRequest = failWorkItemAttemptRequest {}
      val exception =
        assertFailsWith<StatusRuntimeException> {
          services.service.failWorkItemAttempt(failWorkItemAttemptRequest)
        }

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
  fun `failWorkItemAttempt throws INVALID_ARGUMENT if workItemAttemptResourceId is missing`() =
    runBlocking {
      val services = initServices()
      val failWorkItemAttemptRequest = failWorkItemAttemptRequest { workItemResourceId = "123" }
      val exception =
        assertFailsWith<StatusRuntimeException> {
          services.service.failWorkItemAttempt(failWorkItemAttemptRequest)
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
      assertThat(exception.errorInfo)
        .isEqualTo(
          errorInfo {
            domain = Errors.DOMAIN
            reason = Errors.Reason.REQUIRED_FIELD_NOT_SET.name
            metadata[Errors.Metadata.FIELD_NAME.key] = "work_item_attempt_resource_id"
          }
        )
    }

  @Test
  fun `failWorkItemAttempt throws NOT_FOUND when WorkItemAttempt not found`() = runBlocking {
    val services = initServices()
    val failWorkItemAttemptRequest = failWorkItemAttemptRequest {
      workItemResourceId = "123"
      workItemAttemptResourceId = "123"
    }
    val exception =
      assertFailsWith<StatusRuntimeException> {
        services.service.failWorkItemAttempt(failWorkItemAttemptRequest)
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
    assertThat(exception.errorInfo)
      .isEqualTo(
        errorInfo {
          domain = Errors.DOMAIN
          reason = Errors.Reason.WORK_ITEM_ATTEMPT_NOT_FOUND.name
          metadata[Errors.Metadata.WORK_ITEM_RESOURCE_ID.key] = "123"
          metadata[Errors.Metadata.WORK_ITEM_ATTEMPT_RESOURCE_ID.key] = "123"
        }
      )
  }

  @Test
  fun `completeWorkItemAttempt returns WorkItemAttempt with updated state`() = runBlocking {
    val services = initServices()
    val workItem: WorkItem = createWorkItem(services.workItemsService)
    val workItemAttempt =
      createWorkItemAttempts(services.service, workItem.workItemResourceId, 1).get(0)

    val completeWorkItemAttemptRequest = completeWorkItemAttemptRequest {
      workItemResourceId = workItemAttempt.workItemResourceId
      workItemAttemptResourceId = workItemAttempt.workItemAttemptResourceId
    }

    val updatedWorkItemAttempt =
      services.service.completeWorkItemAttempt(completeWorkItemAttemptRequest)
    assertThat(workItemAttempt)
      .ignoringFields(WorkItemAttempt.UPDATE_TIME_FIELD_NUMBER, WorkItemAttempt.STATE_FIELD_NUMBER)
      .isEqualTo(updatedWorkItemAttempt)

    val getWorkItemRequest = getWorkItemRequest { workItemResourceId = workItem.workItemResourceId }
    val updatedWorkItem = services.workItemsService.getWorkItem(getWorkItemRequest)
    assertThat(updatedWorkItem.state).isEqualTo(WorkItem.State.SUCCEEDED)
  }

  @Test
  fun `completeWorkItemAttempt throws INVALID_WORK_ITEM_ATTEMPT_STATE if workItemAttempt state is not ACTIVE`() =
    runBlocking {
      val services = initServices()
      val workItem = createWorkItem(services.workItemsService)
      val workItemAttempt =
        createWorkItemAttempts(services.service, workItem.workItemResourceId, 1).get(0)

      val completeWorkItemAttemptRequest = completeWorkItemAttemptRequest {
        workItemResourceId = workItemAttempt.workItemResourceId
        workItemAttemptResourceId = workItemAttempt.workItemAttemptResourceId
      }

      services.service.completeWorkItemAttempt(completeWorkItemAttemptRequest)

      val exception =
        assertFailsWith<StatusRuntimeException> {
          services.service.completeWorkItemAttempt(completeWorkItemAttemptRequest)
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.FAILED_PRECONDITION)
      assertThat(exception.errorInfo)
        .isEqualTo(
          errorInfo {
            domain = Errors.DOMAIN
            reason = Errors.Reason.INVALID_WORK_ITEM_ATTEMPT_STATE.name
            metadata[Errors.Metadata.WORK_ITEM_RESOURCE_ID.key] = "work_item_resource_id"
            metadata[Errors.Metadata.WORK_ITEM_ATTEMPT_RESOURCE_ID.key] =
              "work_item_attempt_resource_id_1"
            metadata[Errors.Metadata.WORK_ITEM_ATTEMPT_STATE.key] = "SUCCEEDED"
          }
        )
    }

  @Test
  fun `completeWorkItemAttempt throws INVALID_ARGUMENT if workItemResourceId is missing`() =
    runBlocking {
      val services = initServices()
      val completeWorkItemAttemptRequest = completeWorkItemAttemptRequest {}
      val exception =
        assertFailsWith<StatusRuntimeException> {
          services.service.completeWorkItemAttempt(completeWorkItemAttemptRequest)
        }

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
  fun `completeWorkItemAttempt throws INVALID_ARGUMENT if workItemAttemptResourceId is missing`() =
    runBlocking {
      val services = initServices()
      val completeWorkItemAttemptRequest = completeWorkItemAttemptRequest {
        workItemResourceId = "123"
      }
      val exception =
        assertFailsWith<StatusRuntimeException> {
          services.service.completeWorkItemAttempt(completeWorkItemAttemptRequest)
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
      assertThat(exception.errorInfo)
        .isEqualTo(
          errorInfo {
            domain = Errors.DOMAIN
            reason = Errors.Reason.REQUIRED_FIELD_NOT_SET.name
            metadata[Errors.Metadata.FIELD_NAME.key] = "work_item_attempt_resource_id"
          }
        )
    }

  @Test
  fun `completeWorkItemAttempt throws NOT_FOUND when WorkItemAttempt not found`() = runBlocking {
    val services = initServices()
    val completeWorkItemAttemptRequest = completeWorkItemAttemptRequest {
      workItemResourceId = "123"
      workItemAttemptResourceId = "123"
    }
    val exception =
      assertFailsWith<StatusRuntimeException> {
        services.service.completeWorkItemAttempt(completeWorkItemAttemptRequest)
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
    assertThat(exception.errorInfo)
      .isEqualTo(
        errorInfo {
          domain = Errors.DOMAIN
          reason = Errors.Reason.WORK_ITEM_ATTEMPT_NOT_FOUND.name
          metadata[Errors.Metadata.WORK_ITEM_RESOURCE_ID.key] = "123"
          metadata[Errors.Metadata.WORK_ITEM_ATTEMPT_RESOURCE_ID.key] = "123"
        }
      )
  }

  @Test
  fun `listWorkItemAttempt returns workItemAttempts ordered by create time`() = runBlocking {
    val services = initServices()
    val workItem = createWorkItem(services.workItemsService)
    val workItemAttempts: List<WorkItemAttempt> =
      createWorkItemAttempts(services.service, workItem.workItemResourceId, 10)

    val response =
      services.service.listWorkItemAttempts(
        listWorkItemAttemptsRequest { workItemResourceId = workItem.workItemResourceId }
      )

    assertThat(response)
      .isEqualTo(listWorkItemAttemptsResponse { this.workItemAttempts += workItemAttempts })
  }

  @Test
  fun `listWorkItemAttempts returns workItemAttempts when page size is specified`() = runBlocking {
    val services = initServices()
    val workItem = createWorkItem(services.workItemsService)
    val workItemAttempts: List<WorkItemAttempt> =
      createWorkItemAttempts(services.service, workItem.workItemResourceId, 10)

    val response =
      services.service.listWorkItemAttempts(
        listWorkItemAttemptsRequest {
          workItemResourceId = workItem.workItemResourceId
          pageSize = 10
        }
      )

    assertThat(response)
      .isEqualTo(listWorkItemAttemptsResponse { this.workItemAttempts += workItemAttempts })
  }

  @Test
  fun `listWorkItemAttempts returns next page token when there are more results`() = runBlocking {
    val services = initServices()
    val workItem = createWorkItem(services.workItemsService)
    val workItemAttempts: List<WorkItemAttempt> =
      createWorkItemAttempts(services.service, workItem.workItemResourceId, 10)

    val request = listWorkItemAttemptsRequest {
      workItemResourceId = workItem.workItemResourceId
      pageSize = 5
    }
    val response = services.service.listWorkItemAttempts(request)
    assertThat(response)
      .isEqualTo(
        listWorkItemAttemptsResponse {
          this.workItemAttempts += workItemAttempts.take(request.pageSize)
          nextPageToken = listWorkItemAttemptsPageToken {
            after =
              ListWorkItemAttemptsPageTokenKt.after {
                createdAfter = workItemAttempts.get(4).createTime
                workItemAttemptResourceId = workItemAttempts.get(4).workItemAttemptResourceId
                workItemResourceId = workItemAttempts.get(4).workItemResourceId
              }
          }
        }
      )
  }

  @Test
  fun `listWorkItemAttempts returns results after page token`() = runBlocking {
    val services = initServices()
    val workItem = createWorkItem(services.workItemsService)
    val workItemAttempts: List<WorkItemAttempt> =
      createWorkItemAttempts(services.service, workItem.workItemResourceId, 10)
    workItemAttempts.forEach {}

    val request = listWorkItemAttemptsRequest {
      workItemResourceId = workItem.workItemResourceId
      pageSize = 2
      pageToken = listWorkItemAttemptsPageToken {
        after =
          ListWorkItemAttemptsPageTokenKt.after {
            createdAfter = workItemAttempts.get(4).createTime
            workItemResourceId = workItemAttempts.get(4).workItemResourceId
            workItemAttemptResourceId = workItemAttempts.get(4).workItemAttemptResourceId
          }
      }
    }
    val response = services.service.listWorkItemAttempts(request)
    assertThat(response)
      .isEqualTo(
        listWorkItemAttemptsResponse {
          this.workItemAttempts += workItemAttempts.subList(5, 7)
          nextPageToken = listWorkItemAttemptsPageToken {
            after =
              ListWorkItemAttemptsPageTokenKt.after {
                createdAfter = workItemAttempts.get(6).createTime
                workItemResourceId = workItemAttempts.get(4).workItemResourceId
                workItemAttemptResourceId = workItemAttempts.get(6).workItemAttemptResourceId
              }
          }
        }
      )
  }

  private suspend fun createWorkItem(service: WorkItemsCoroutineImplBase): WorkItem {
    return service.createWorkItem(
      createWorkItemRequest {
        workItem = workItem {
          workItemResourceId = "work_item_resource_id"
          queueResourceId = "test-topid-id"
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

  private suspend fun createWorkItemAttempts(
    service: WorkItemAttemptsCoroutineImplBase,
    workItemResourceId: String,
    count: Int,
  ): List<WorkItemAttempt> {
    return (1..count).map {
      val workItemAttemptResourceId = "work_item_attempt_resource_id_$it"
      service.createWorkItemAttempt(
        createWorkItemAttemptRequest {
          workItemAttempt = workItemAttempt {
            this.workItemResourceId = workItemResourceId
            this.workItemAttemptResourceId = workItemAttemptResourceId
          }
        }
      )
    }
  }

  companion object {
    private val logger: Logger = Logger.getLogger(this::class.java.name)
  }
}
