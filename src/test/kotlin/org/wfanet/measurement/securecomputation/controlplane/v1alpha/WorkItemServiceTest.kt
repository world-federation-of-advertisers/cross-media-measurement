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

package org.wfanet.measurement.securecomputation.controlplane.v1alpha

import com.google.common.truth.Truth.assertThat
import com.google.common.truth.extensions.proto.ProtoTruth.assertThat
import com.google.protobuf.Any
import com.google.rpc.errorInfo
import io.grpc.Status
import io.grpc.StatusRuntimeException
import kotlin.test.assertFailsWith
import kotlinx.coroutines.runBlocking
import org.junit.Before
import org.junit.Rule
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.mockito.kotlin.any
import org.mockito.kotlin.doReturn
import org.mockito.kotlin.doThrow
import org.mockito.kotlin.stub
import org.wfanet.measurement.common.base64UrlEncode
import org.wfanet.measurement.common.grpc.errorInfo
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.common.grpc.testing.mockService
import org.wfanet.measurement.common.testing.verifyProtoArgument
import org.wfanet.measurement.internal.securecomputation.controlplane.ListWorkItemsPageTokenKt as InternalListWorkItemsPageTokenKt
import org.wfanet.measurement.internal.securecomputation.controlplane.WorkItem as InternalWorkItem
import org.wfanet.measurement.internal.securecomputation.controlplane.WorkItemsGrpcKt
import org.wfanet.measurement.internal.securecomputation.controlplane.WorkItemsGrpcKt.WorkItemsCoroutineStub as InternalWorkItemsCoroutineStub
import org.wfanet.measurement.internal.securecomputation.controlplane.createWorkItemRequest as internalCreateWorkItemRequest
import org.wfanet.measurement.internal.securecomputation.controlplane.failWorkItemRequest as internalFailWorkItemRequest
import org.wfanet.measurement.internal.securecomputation.controlplane.getWorkItemRequest as internalGetWorkItemRequest
import org.wfanet.measurement.internal.securecomputation.controlplane.listWorkItemsPageToken as internalListWorkItemsPageToken
import org.wfanet.measurement.internal.securecomputation.controlplane.listWorkItemsRequest as internalListWorkItemsRequest
import org.wfanet.measurement.internal.securecomputation.controlplane.listWorkItemsResponse as internalListWorkItemsResponse
import org.wfanet.measurement.internal.securecomputation.controlplane.workItem as internalWorkItem
import org.wfanet.measurement.securecomputation.service.Errors
import org.wfanet.measurement.securecomputation.service.internal.WorkItemAlreadyExistsException
import org.wfanet.measurement.securecomputation.service.internal.WorkItemNotFoundException

@RunWith(JUnit4::class)
class WorkItemServiceTest {

  private val internalServiceMock = mockService<WorkItemsGrpcKt.WorkItemsCoroutineImplBase>()

  @get:Rule val grpcTestServer = GrpcTestServerRule { addService(internalServiceMock) }

  private lateinit var service: WorkItemsService

  @Before
  fun initService() {
    service = WorkItemsService(InternalWorkItemsCoroutineStub(grpcTestServer.channel))
  }

  @Test
  fun `createWorkItem returns WorkItem`() = runBlocking {
    val internalWorkItem = internalWorkItem {
      workItemResourceId = "workItem"
      queueResourceId = "queueId"
      state = InternalWorkItem.State.QUEUED
    }
    internalServiceMock.stub { onBlocking { createWorkItem(any()) } doReturn internalWorkItem }

    val request = createWorkItemRequest {
      workItem = workItem {
        name = "workItems/${internalWorkItem.workItemResourceId}"
        queue = "queueId"
      }
      workItemId = "workItem"
    }
    val response = service.createWorkItem(request)

    verifyProtoArgument(
        internalServiceMock,
        WorkItemsGrpcKt.WorkItemsCoroutineImplBase::createWorkItem,
      )
      .isEqualTo(
        internalCreateWorkItemRequest {
          this.workItem = internalWorkItem {
            queueResourceId = "queueId"
            workItemResourceId = request.workItemId
            workItemParams = Any.getDefaultInstance()
          }
        }
      )

    assertThat(response)
      .ignoringFields(
        WorkItem.CREATE_TIME_FIELD_NUMBER,
        WorkItem.UPDATE_TIME_FIELD_NUMBER,
        WorkItem.STATE_FIELD_NUMBER,
      )
      .isEqualTo(request.workItem)
    assertThat(response.state).isEqualTo(WorkItem.State.QUEUED)
  }

  @Test
  fun `createWorkItem throws REQUIRED_FIELD_NOT_SET when workItem is not set`() = runBlocking {
    val exception =
      assertFailsWith<StatusRuntimeException> { service.createWorkItem(createWorkItemRequest {}) }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.errorInfo)
      .isEqualTo(
        errorInfo {
          domain = Errors.DOMAIN
          reason = Errors.Reason.REQUIRED_FIELD_NOT_SET.name
          metadata[Errors.Metadata.FIELD_NAME.key] = "work_item"
        }
      )
  }

  @Test
  fun `createWorkItem throws REQUIRED_FIELD_NOT_SET when queue id is not set`() = runBlocking {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        service.createWorkItem(createWorkItemRequest { workItem = workItem {} })
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.errorInfo)
      .isEqualTo(
        errorInfo {
          domain = Errors.DOMAIN
          reason = Errors.Reason.REQUIRED_FIELD_NOT_SET.name
          metadata[Errors.Metadata.FIELD_NAME.key] = "queue"
        }
      )
  }

  @Test
  fun `createWorkItem throws REQUIRED_FIELD_NOT_SET when workItemId id is not set`() = runBlocking {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        service.createWorkItem(createWorkItemRequest { workItem = workItem { queue = "queueId" } })
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.errorInfo)
      .isEqualTo(
        errorInfo {
          domain = Errors.DOMAIN
          reason = Errors.Reason.REQUIRED_FIELD_NOT_SET.name
          metadata[Errors.Metadata.FIELD_NAME.key] = "work_item_id"
        }
      )
  }

  @Test
  fun `createWorkItem throws INVALID_FIELD_VALUE when workItemId is malformed`() = runBlocking {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        service.createWorkItem(
          createWorkItemRequest {
            workItem = workItem {
              name = "workItems/workItem"
              queue = "queueId"
            }
            workItemId = "123"
          }
        )
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.errorInfo)
      .isEqualTo(
        errorInfo {
          domain = Errors.DOMAIN
          reason = Errors.Reason.INVALID_FIELD_VALUE.name
          metadata[Errors.Metadata.FIELD_NAME.key] = "work_item_id"
        }
      )
  }

  @Test
  fun `createWorkItem throws WORK_ITEM_ALREADY_EXISTS from backend`() = runBlocking {
    internalServiceMock.stub {
      onBlocking { createWorkItem(any()) } doThrow
        WorkItemAlreadyExistsException().asStatusRuntimeException(Status.Code.ALREADY_EXISTS)
    }

    val request = createWorkItemRequest {
      workItem = workItem {
        name = "workItems/workItem"
        queue = "queueId"
      }
      workItemId = "workItem"
    }

    val exception = assertFailsWith<StatusRuntimeException> { service.createWorkItem(request) }

    assertThat(exception.status.code).isEqualTo(Status.Code.ALREADY_EXISTS)
    assertThat(exception.errorInfo)
      .isEqualTo(
        errorInfo {
          domain = Errors.DOMAIN
          reason = Errors.Reason.WORK_ITEM_ALREADY_EXISTS.name
          metadata[Errors.Metadata.WORK_ITEM.key] = "workItems/workItem"
        }
      )
  }

  @Test
  fun `getWorkItem returns WorkItem`() = runBlocking {
    val internalWorkItem = internalWorkItem { workItemResourceId = "workItem" }
    internalServiceMock.stub { onBlocking { getWorkItem(any()) } doReturn internalWorkItem }

    val request = getWorkItemRequest { name = "workItems/${internalWorkItem.workItemResourceId}" }
    val response = service.getWorkItem(request)

    verifyProtoArgument(
        internalServiceMock,
        WorkItemsGrpcKt.WorkItemsCoroutineImplBase::getWorkItem,
      )
      .isEqualTo(
        internalGetWorkItemRequest { workItemResourceId = internalWorkItem.workItemResourceId }
      )

    assertThat(response)
      .ignoringFields(WorkItem.CREATE_TIME_FIELD_NUMBER, WorkItem.UPDATE_TIME_FIELD_NUMBER)
      .isEqualTo(workItem { name = request.name })
  }

  @Test
  fun `getWorkItem throws REQUIRED_FIELD_NOT_SET when name is not set`() = runBlocking {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        service.getWorkItem(GetWorkItemRequest.getDefaultInstance())
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.errorInfo)
      .isEqualTo(
        errorInfo {
          domain = Errors.DOMAIN
          reason = Errors.Reason.REQUIRED_FIELD_NOT_SET.name
          metadata[Errors.Metadata.FIELD_NAME.key] = "name"
        }
      )
  }

  @Test
  fun `getWorkItem throws INVALID_FIELD_VALUE when name is malformed`() = runBlocking {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        service.getWorkItem(getWorkItemRequest { name = "workItems" })
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.errorInfo)
      .isEqualTo(
        errorInfo {
          domain = Errors.DOMAIN
          reason = Errors.Reason.INVALID_FIELD_VALUE.name
          metadata[Errors.Metadata.FIELD_NAME.key] = "name"
        }
      )
  }

  @Test
  fun `getWorkItem throws WORK_ITEM_NOT_FOUND from backend`() = runBlocking {
    internalServiceMock.stub {
      onBlocking { getWorkItem(any()) } doThrow
        WorkItemNotFoundException("workItem").asStatusRuntimeException(Status.Code.NOT_FOUND)
    }
    val request = getWorkItemRequest { name = "workItems/workItem" }
    val exception = assertFailsWith<StatusRuntimeException> { service.getWorkItem(request) }

    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
    assertThat(exception.errorInfo)
      .isEqualTo(
        errorInfo {
          domain = Errors.DOMAIN
          reason = Errors.Reason.WORK_ITEM_NOT_FOUND.name
          metadata[Errors.Metadata.WORK_ITEM.key] = request.name
        }
      )
  }

  @Test
  fun `failWorkItem returns WorkItem`() = runBlocking {
    val internalWorkItem = internalWorkItem {
      workItemResourceId = "workItem"
      state = InternalWorkItem.State.FAILED
    }
    internalServiceMock.stub { onBlocking { failWorkItem(any()) } doReturn internalWorkItem }

    val request = failWorkItemRequest { name = "workItems/${internalWorkItem.workItemResourceId}" }
    val response = service.failWorkItem(request)

    val internalRequest = internalFailWorkItemRequest {
      workItemResourceId = internalWorkItem.workItemResourceId
    }
    verifyProtoArgument(
        internalServiceMock,
        WorkItemsGrpcKt.WorkItemsCoroutineImplBase::failWorkItem,
      )
      .isEqualTo(internalRequest)

    assertThat(response.state).isEqualTo(WorkItem.State.FAILED)
  }

  @Test
  fun `failWorkItem throws REQUIRED_FIELD_NOT_SET when name is not set`() = runBlocking {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        service.failWorkItem(FailWorkItemRequest.getDefaultInstance())
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.errorInfo)
      .isEqualTo(
        errorInfo {
          domain = Errors.DOMAIN
          reason = Errors.Reason.REQUIRED_FIELD_NOT_SET.name
          metadata[Errors.Metadata.FIELD_NAME.key] = "name"
        }
      )
  }

  @Test
  fun `failWorkItem throws INVALID_FIELD_VALUE when name is malformed`() = runBlocking {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        service.failWorkItem(failWorkItemRequest { name = "workItems" })
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.errorInfo)
      .isEqualTo(
        errorInfo {
          domain = Errors.DOMAIN
          reason = Errors.Reason.INVALID_FIELD_VALUE.name
          metadata[Errors.Metadata.FIELD_NAME.key] = "name"
        }
      )
  }

  @Test
  fun `failWorkItem throws WORK_ITEM_NOT_FOUND from backend`() = runBlocking {
    internalServiceMock.stub {
      onBlocking { failWorkItem(any()) } doThrow
        WorkItemNotFoundException("workItem").asStatusRuntimeException(Status.Code.NOT_FOUND)
    }
    val request = failWorkItemRequest { name = "workItems/workItem" }
    val exception = assertFailsWith<StatusRuntimeException> { service.failWorkItem(request) }

    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
    assertThat(exception.errorInfo)
      .isEqualTo(
        errorInfo {
          domain = Errors.DOMAIN
          reason = Errors.Reason.WORK_ITEM_NOT_FOUND.name
          metadata[Errors.Metadata.WORK_ITEM.key] = request.name
        }
      )
  }

  @Test
  fun `listWorkItems returns WorkItems`() = runBlocking {
    val internalWorkItemFirst = internalWorkItem {
      workItemResourceId = "workItemOne"
      queueResourceId = "queueId"
      state = InternalWorkItem.State.QUEUED
    }

    val internalListWorkItemsResponse = internalListWorkItemsResponse {
      workItems += internalWorkItemFirst
      nextPageToken = internalListWorkItemsPageToken {
        after = InternalListWorkItemsPageTokenKt.after { workItemResourceId = "workItemTwo" }
      }
    }
    internalServiceMock.stub {
      onBlocking { listWorkItems(any()) } doReturn internalListWorkItemsResponse
    }

    val response = service.listWorkItems(listWorkItemsRequest { pageSize = 1 })

    verifyProtoArgument(
        internalServiceMock,
        WorkItemsGrpcKt.WorkItemsCoroutineImplBase::listWorkItems,
      )
      .isEqualTo(internalListWorkItemsRequest { pageSize = 1 })
    assertThat(response)
      .isEqualTo(
        listWorkItemsResponse {
          workItems += internalWorkItemFirst.toWorkItem()
          nextPageToken =
            internalListWorkItemsResponse.nextPageToken.after.toByteString().base64UrlEncode()
        }
      )
  }

  @Test
  fun `listWorkItems throws INVALID_FIELD_VALUE when page size is invalid`() = runBlocking {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        service.listWorkItems(listWorkItemsRequest { pageSize = -1 })
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.errorInfo)
      .isEqualTo(
        errorInfo {
          domain = Errors.DOMAIN
          reason = Errors.Reason.INVALID_FIELD_VALUE.name
          metadata[Errors.Metadata.FIELD_NAME.key] = "page_size"
        }
      )
  }

  @Test
  fun `listWorkItems throws INVALID_FIELD_VALUE when page token is invalid`() = runBlocking {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        service.listWorkItems(listWorkItemsRequest { pageToken = "1" })
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.errorInfo)
      .isEqualTo(
        errorInfo {
          domain = Errors.DOMAIN
          reason = Errors.Reason.INVALID_FIELD_VALUE.name
          metadata[Errors.Metadata.FIELD_NAME.key] = "page_token"
        }
      )
  }
}
