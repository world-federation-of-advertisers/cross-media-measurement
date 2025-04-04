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
<<<<<<< HEAD
import org.junit.Before
import org.junit.Rule
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.mockito.kotlin.any
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.common.grpc.testing.mockService
import org.wfanet.measurement.internal.securecomputation.controlplane.WorkItemAttemptsGrpcKt.WorkItemAttemptsCoroutineStub as InternalWorkItemAttemptsCoroutineStub
import kotlinx.coroutines.runBlocking
import org.wfanet.measurement.internal.securecomputation.controlplane.WorkItemAttemptsGrpcKt
import org.wfanet.measurement.internal.securecomputation.controlplane.workItemAttempt as internalWorkItemAttempt
import org.wfanet.measurement.internal.securecomputation.controlplane.WorkItemAttempt as InternalWorkItemAttempt
import org.wfanet.measurement.internal.securecomputation.controlplane.WorkItem as InternalWorkItem
import org.wfanet.measurement.internal.securecomputation.controlplane.createWorkItemAttemptRequest as internalCreateWorkItemAttemptRequest
import org.wfanet.measurement.internal.securecomputation.controlplane.getWorkItemAttemptRequest as internalGetWorkItemAttemptRequest
import org.wfanet.measurement.internal.securecomputation.controlplane.failWorkItemAttemptRequest as internalFailWorkItemAttemptRequest
import org.wfanet.measurement.internal.securecomputation.controlplane.completeWorkItemAttemptRequest as internalCompleteWorkItemAttemptRequest
import org.wfanet.measurement.internal.securecomputation.controlplane.listWorkItemAttemptsRequest as internalListWorkItemAttemptsRequest
import org.wfanet.measurement.internal.securecomputation.controlplane.listWorkItemAttemptsResponse as internalListWorkItemAttemptsResponse
import org.wfanet.measurement.internal.securecomputation.controlplane.listWorkItemAttemptsPageToken as internalListWorkItemAttemptsPageToken
import org.wfanet.measurement.internal.securecomputation.controlplane.ListWorkItemAttemptsPageTokenKt as InternalListWorkItemAttemptsPageTokenKt

=======
>>>>>>> main
import com.google.rpc.errorInfo
import io.grpc.Status
import io.grpc.StatusRuntimeException
import kotlin.test.assertFailsWith
<<<<<<< HEAD
import org.junit.Test
=======
import kotlinx.coroutines.runBlocking
import org.junit.Before
import org.junit.Rule
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.mockito.kotlin.any
>>>>>>> main
import org.mockito.kotlin.doReturn
import org.mockito.kotlin.doThrow
import org.mockito.kotlin.stub
import org.wfanet.measurement.common.base64UrlEncode
import org.wfanet.measurement.common.grpc.errorInfo
<<<<<<< HEAD
import org.wfanet.measurement.common.testing.verifyProtoArgument
import org.wfanet.measurement.securecomputation.service.Errors
import org.wfanet.measurement.securecomputation.service.internal.WorkItemInvalidStateException
import org.wfanet.measurement.securecomputation.service.internal.WorkItemAttemptInvalidStateException
import org.wfanet.measurement.securecomputation.service.internal.WorkItemAttemptAlreadyExistsException
import org.wfanet.measurement.securecomputation.service.internal.WorkItemAttemptNotFoundException
=======
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.common.grpc.testing.mockService
import org.wfanet.measurement.common.testing.verifyProtoArgument
import org.wfanet.measurement.internal.securecomputation.controlplane.ListWorkItemAttemptsPageTokenKt as InternalListWorkItemAttemptsPageTokenKt
import org.wfanet.measurement.internal.securecomputation.controlplane.WorkItem as InternalWorkItem
import org.wfanet.measurement.internal.securecomputation.controlplane.WorkItemAttempt as InternalWorkItemAttempt
import org.wfanet.measurement.internal.securecomputation.controlplane.WorkItemAttemptsGrpcKt
import org.wfanet.measurement.internal.securecomputation.controlplane.WorkItemAttemptsGrpcKt.WorkItemAttemptsCoroutineStub as InternalWorkItemAttemptsCoroutineStub
import org.wfanet.measurement.internal.securecomputation.controlplane.completeWorkItemAttemptRequest as internalCompleteWorkItemAttemptRequest
import org.wfanet.measurement.internal.securecomputation.controlplane.createWorkItemAttemptRequest as internalCreateWorkItemAttemptRequest
import org.wfanet.measurement.internal.securecomputation.controlplane.failWorkItemAttemptRequest as internalFailWorkItemAttemptRequest
import org.wfanet.measurement.internal.securecomputation.controlplane.getWorkItemAttemptRequest as internalGetWorkItemAttemptRequest
import org.wfanet.measurement.internal.securecomputation.controlplane.listWorkItemAttemptsPageToken as internalListWorkItemAttemptsPageToken
import org.wfanet.measurement.internal.securecomputation.controlplane.listWorkItemAttemptsRequest as internalListWorkItemAttemptsRequest
import org.wfanet.measurement.internal.securecomputation.controlplane.listWorkItemAttemptsResponse as internalListWorkItemAttemptsResponse
import org.wfanet.measurement.internal.securecomputation.controlplane.workItemAttempt as internalWorkItemAttempt
import org.wfanet.measurement.securecomputation.service.Errors
import org.wfanet.measurement.securecomputation.service.internal.WorkItemAttemptAlreadyExistsException
import org.wfanet.measurement.securecomputation.service.internal.WorkItemAttemptInvalidStateException
import org.wfanet.measurement.securecomputation.service.internal.WorkItemAttemptNotFoundException
import org.wfanet.measurement.securecomputation.service.internal.WorkItemInvalidStateException
>>>>>>> main

@RunWith(JUnit4::class)
class WorkItemAttemptsServiceTest {

<<<<<<< HEAD
  private val internalServiceMock = mockService<WorkItemAttemptsGrpcKt.WorkItemAttemptsCoroutineImplBase>()

  @get:Rule
  val grpcTestServer = GrpcTestServerRule { addService(internalServiceMock) }
=======
  private val internalServiceMock =
    mockService<WorkItemAttemptsGrpcKt.WorkItemAttemptsCoroutineImplBase>()

  @get:Rule val grpcTestServer = GrpcTestServerRule { addService(internalServiceMock) }
>>>>>>> main

  private lateinit var service: WorkItemAttemptsService

  @Before
  fun initService() {
    service = WorkItemAttemptsService(InternalWorkItemAttemptsCoroutineStub(grpcTestServer.channel))
  }

  @Test
  fun `createWorkItemAttempt returns WorkItemAttempt`() = runBlocking {
<<<<<<< HEAD

=======
>>>>>>> main
    val internalWorkItemAttempt = internalWorkItemAttempt {
      workItemResourceId = "workItem"
      workItemAttemptResourceId = "workItemAttempt"
      state = InternalWorkItemAttempt.State.ACTIVE
    }
<<<<<<< HEAD
    internalServiceMock.stub { onBlocking { createWorkItemAttempt(any()) } doReturn internalWorkItemAttempt }
=======
    internalServiceMock.stub {
      onBlocking { createWorkItemAttempt(any()) } doReturn internalWorkItemAttempt
    }
>>>>>>> main

    val request = createWorkItemAttemptRequest {
      parent = "workItems/${internalWorkItemAttempt.workItemResourceId}"
      workItemAttempt = workItemAttempt {
<<<<<<< HEAD
        name = "workItems/${internalWorkItemAttempt.workItemResourceId}/workItemAttempts/${internalWorkItemAttempt.workItemAttemptResourceId}"
=======
        name =
          "workItems/${internalWorkItemAttempt.workItemResourceId}/workItemAttempts/${internalWorkItemAttempt.workItemAttemptResourceId}"
>>>>>>> main
      }
      workItemAttemptId = "workItemAttempt"
    }
    val response = service.createWorkItemAttempt(request)

<<<<<<< HEAD
    verifyProtoArgument(internalServiceMock, WorkItemAttemptsGrpcKt.WorkItemAttemptsCoroutineImplBase::createWorkItemAttempt)
      .isEqualTo(internalCreateWorkItemAttemptRequest {
        internalWorkItemAttempt {
          workItemResourceId = request.parent
          workItemAttemptResourceId = request.workItemAttemptId
        }
      })
=======
    verifyProtoArgument(
        internalServiceMock,
        WorkItemAttemptsGrpcKt.WorkItemAttemptsCoroutineImplBase::createWorkItemAttempt,
      )
      .isEqualTo(
        internalCreateWorkItemAttemptRequest {
          internalWorkItemAttempt {
            workItemResourceId = request.parent
            workItemAttemptResourceId = request.workItemAttemptId
          }
        }
      )
>>>>>>> main

    assertThat(response)
      .ignoringFields(
        WorkItemAttempt.CREATE_TIME_FIELD_NUMBER,
        WorkItemAttempt.UPDATE_TIME_FIELD_NUMBER,
<<<<<<< HEAD
        WorkItemAttempt.STATE_FIELD_NUMBER
=======
        WorkItemAttempt.STATE_FIELD_NUMBER,
>>>>>>> main
      )
      .isEqualTo(request.workItemAttempt)
    assertThat(response.state).isEqualTo(WorkItemAttempt.State.ACTIVE)
  }

  @Test
  fun `createWorkItemAttempt throws REQUIRED_FIELD_NOT_SET when parent is not set`() = runBlocking {
    val exception =
      assertFailsWith<StatusRuntimeException> {
<<<<<<< HEAD
        service.createWorkItemAttempt(
          createWorkItemAttemptRequest {
          }
        )
=======
        service.createWorkItemAttempt(createWorkItemAttemptRequest {})
>>>>>>> main
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.errorInfo)
      .isEqualTo(
        errorInfo {
          domain = Errors.DOMAIN
          reason = Errors.Reason.REQUIRED_FIELD_NOT_SET.name
          metadata[Errors.Metadata.FIELD_NAME.key] = "parent"
        }
      )
  }

  @Test
<<<<<<< HEAD
  fun `createWorkItemAttempt throws REQUIRED_FIELD_NOT_SET when workIteAttemptId is not set`() = runBlocking {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        service.createWorkItemAttempt(
          createWorkItemAttemptRequest {
            parent = "workItems/workItem"
            workItemAttempt = workItemAttempt {
              name = "workItems/workItem/workItemAttempts/workItemAttempt"
            }
          }
        )
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.errorInfo)
      .isEqualTo(
        errorInfo {
          domain = Errors.DOMAIN
          reason = Errors.Reason.REQUIRED_FIELD_NOT_SET.name
          metadata[Errors.Metadata.FIELD_NAME.key] = "work_item_attempt_id"
        }
      )
  }

  @Test
  fun `createWorkItemAttempt throws INVALID_FIELD_VALUE when workItemAttemptId is malformed`() = runBlocking {

    val exception =
      assertFailsWith<StatusRuntimeException> {
        service.createWorkItemAttempt(
          createWorkItemAttemptRequest {
            parent = "workItems/workItem"
            workItemAttempt = workItemAttempt {
              name = "workItems/workItem/workItemAttempts/workItemAttempt"
            }
            workItemAttemptId = "123"
          }
        )
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.errorInfo)
      .isEqualTo(
        errorInfo {
          domain = Errors.DOMAIN
          reason = Errors.Reason.INVALID_FIELD_VALUE.name
          metadata[Errors.Metadata.FIELD_NAME.key] = "work_item_attempt_id"
        }
      )
  }
=======
  fun `createWorkItemAttempt throws REQUIRED_FIELD_NOT_SET when workIteAttemptId is not set`() =
    runBlocking {
      val exception =
        assertFailsWith<StatusRuntimeException> {
          service.createWorkItemAttempt(
            createWorkItemAttemptRequest {
              parent = "workItems/workItem"
              workItemAttempt = workItemAttempt {
                name = "workItems/workItem/workItemAttempts/workItemAttempt"
              }
            }
          )
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
      assertThat(exception.errorInfo)
        .isEqualTo(
          errorInfo {
            domain = Errors.DOMAIN
            reason = Errors.Reason.REQUIRED_FIELD_NOT_SET.name
            metadata[Errors.Metadata.FIELD_NAME.key] = "work_item_attempt_id"
          }
        )
    }

  @Test
  fun `createWorkItemAttempt throws INVALID_FIELD_VALUE when workItemAttemptId is malformed`() =
    runBlocking {
      val exception =
        assertFailsWith<StatusRuntimeException> {
          service.createWorkItemAttempt(
            createWorkItemAttemptRequest {
              parent = "workItems/workItem"
              workItemAttempt = workItemAttempt {
                name = "workItems/workItem/workItemAttempts/workItemAttempt"
              }
              workItemAttemptId = "123"
            }
          )
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
      assertThat(exception.errorInfo)
        .isEqualTo(
          errorInfo {
            domain = Errors.DOMAIN
            reason = Errors.Reason.INVALID_FIELD_VALUE.name
            metadata[Errors.Metadata.FIELD_NAME.key] = "work_item_attempt_id"
          }
        )
    }
>>>>>>> main

  @Test
  fun `createWorkItemAttempt throws WORK_ITEM_ATTEMPT_ALREADY_EXISTS from backend`() = runBlocking {
    internalServiceMock.stub {
      onBlocking { createWorkItemAttempt(any()) } doThrow
        WorkItemAttemptAlreadyExistsException().asStatusRuntimeException(Status.Code.ALREADY_EXISTS)
    }

    val request = createWorkItemAttemptRequest {
      parent = "workItems/workItem"
      workItemAttempt = workItemAttempt {
        name = "workItems/workItem/workItemAttempts/workItemAttempt"
      }
      workItemAttemptId = "workItem"
    }

<<<<<<< HEAD
    val exception = assertFailsWith<StatusRuntimeException> { service.createWorkItemAttempt(request) }
=======
    val exception =
      assertFailsWith<StatusRuntimeException> { service.createWorkItemAttempt(request) }
>>>>>>> main

    assertThat(exception.status.code).isEqualTo(Status.Code.ALREADY_EXISTS)
    assertThat(exception.errorInfo)
      .isEqualTo(
        errorInfo {
          domain = Errors.DOMAIN
          reason = Errors.Reason.WORK_ITEM_ATTEMPT_ALREADY_EXISTS.name
<<<<<<< HEAD
          metadata[Errors.Metadata.WORK_ITEM_ATTEMPT.key] = "workItems/workItem/workItemAttempts/workItemAttempt"
=======
          metadata[Errors.Metadata.WORK_ITEM_ATTEMPT.key] =
            "workItems/workItem/workItemAttempts/workItemAttempt"
>>>>>>> main
        }
      )
  }

  @Test
  fun `createWorkItemAttempt throws INVALID_WORK_ITEM_STATE from backend`() = runBlocking {
    internalServiceMock.stub {
      onBlocking { createWorkItemAttempt(any()) } doThrow
<<<<<<< HEAD
        WorkItemInvalidStateException("workItem", InternalWorkItem.State.SUCCEEDED).asStatusRuntimeException(Status.Code.FAILED_PRECONDITION)

=======
        WorkItemInvalidStateException("workItem", InternalWorkItem.State.SUCCEEDED)
          .asStatusRuntimeException(Status.Code.FAILED_PRECONDITION)
>>>>>>> main
    }

    val request = createWorkItemAttemptRequest {
      parent = "workItems/workItem"
      workItemAttempt = workItemAttempt {
        name = "workItems/workItem/workItemAttempts/workItemAttempt"
      }
      workItemAttemptId = "workItem"
    }

<<<<<<< HEAD
    val exception = assertFailsWith<StatusRuntimeException> { service.createWorkItemAttempt(request) }
=======
    val exception =
      assertFailsWith<StatusRuntimeException> { service.createWorkItemAttempt(request) }
>>>>>>> main

    assertThat(exception.status.code).isEqualTo(Status.Code.FAILED_PRECONDITION)
    assertThat(exception.errorInfo)
      .isEqualTo(
        errorInfo {
          domain = Errors.DOMAIN
          reason = Errors.Reason.INVALID_WORK_ITEM_STATE.name
          metadata[Errors.Metadata.WORK_ITEM.key] = "workItems/workItem"
          metadata[Errors.Metadata.WORK_ITEM_STATE.key] = "SUCCEEDED"
        }
      )
  }

<<<<<<< HEAD
   @Test
    fun `getWorkItemAttempt returns WorkItemAttempt`() = runBlocking {
      val internalWorkItemAttempt = internalWorkItemAttempt {
        workItemResourceId = "workItem"
        workItemAttemptResourceId = "workItemAttempt"
      }
      internalServiceMock.stub { onBlocking { getWorkItemAttempt(any()) } doReturn internalWorkItemAttempt }

      val request = getWorkItemAttemptRequest { name = "workItems/${internalWorkItemAttempt.workItemResourceId}/workItemAttempts/${internalWorkItemAttempt.workItemAttemptResourceId}" }
      val response = service.getWorkItemAttempt(request)

      verifyProtoArgument(internalServiceMock, WorkItemAttemptsGrpcKt.WorkItemAttemptsCoroutineImplBase::getWorkItemAttempt)
        .isEqualTo(internalGetWorkItemAttemptRequest {
          workItemResourceId = internalWorkItemAttempt.workItemResourceId
          workItemAttemptResourceId = internalWorkItemAttempt.workItemAttemptResourceId
        })

      assertThat(response)
        .ignoringFields(
          WorkItemAttempt.CREATE_TIME_FIELD_NUMBER,
          WorkItemAttempt.UPDATE_TIME_FIELD_NUMBER
        )
        .isEqualTo(
          workItemAttempt {
            name = request.name
          }
        )
    }

  @Test
    fun `getWorkItemAttempt throws REQUIRED_FIELD_NOT_SET when name is not set`() = runBlocking {
      val exception =
        assertFailsWith<StatusRuntimeException> {
          service.getWorkItemAttempt(GetWorkItemAttemptRequest.getDefaultInstance())
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
    fun `getWorkItemAttempt throws INVALID_FIELD_VALUE when name is malformed`() = runBlocking {
      val exception =
        assertFailsWith<StatusRuntimeException> {
          service.getWorkItemAttempt(getWorkItemAttemptRequest {
            name = "workItemAttempts"
          })
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
    fun `getWorkItemAttempt throws WORK_ITEM_ATTEMPT_NOT_FOUND from backend`() = runBlocking {
      internalServiceMock.stub {
        onBlocking { getWorkItemAttempt(any()) } doThrow
          WorkItemAttemptNotFoundException("workItem", "workItemAttempt").asStatusRuntimeException(Status.Code.NOT_FOUND)
      }
      val request = getWorkItemAttemptRequest { name = "workItems/workItem/workItemAttempts/workItemAttempt" }
      val exception = assertFailsWith<StatusRuntimeException> { service.getWorkItemAttempt(request) }

      assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
          assertThat(exception.errorInfo)
            .isEqualTo(
              errorInfo {
                domain = Errors.DOMAIN
                reason = Errors.Reason.WORK_ITEM_ATTEMPT_NOT_FOUND.name
                metadata[Errors.Metadata.WORK_ITEM.key] = request.name
              }
            )
    }

   @Test
    fun `failWorkItemAttempt returns WorkItemAttempt`() = runBlocking {

      val internalWorkItemAttempt = internalWorkItemAttempt {
        workItemResourceId = "workItem"
        workItemAttemptResourceId = "workItemAttempt"
        state = InternalWorkItemAttempt.State.FAILED
      }
      internalServiceMock.stub { onBlocking { failWorkItemAttempt(any()) } doReturn internalWorkItemAttempt }

      val request = failWorkItemAttemptRequest {
        name = "workItems/${internalWorkItemAttempt.workItemResourceId}/workItemAttempts/${internalWorkItemAttempt.workItemAttemptResourceId}"
      }
      val response = service.failWorkItemAttempt(request)

      val internalRequest = internalFailWorkItemAttemptRequest {
        workItemResourceId = internalWorkItemAttempt.workItemResourceId
        workItemAttemptResourceId = internalWorkItemAttempt.workItemAttemptResourceId
      }
      verifyProtoArgument(internalServiceMock, WorkItemAttemptsGrpcKt.WorkItemAttemptsCoroutineImplBase::failWorkItemAttempt)
        .isEqualTo(internalRequest)

      assertThat(response.state).isEqualTo(WorkItemAttempt.State.FAILED)
    }

   @Test
    fun `failWorkItemAttempt throws REQUIRED_FIELD_NOT_SET when name is not set`() = runBlocking {
      val exception =
        assertFailsWith<StatusRuntimeException> {
          service.failWorkItemAttempt(FailWorkItemAttemptRequest.getDefaultInstance())
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
    fun `failWorkItemAttempt throws INVALID_FIELD_VALUE when name is malformed`() = runBlocking {
      val exception =
        assertFailsWith<StatusRuntimeException> {
          service.failWorkItemAttempt(failWorkItemAttemptRequest {
            name = "workItems"
          })
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
    fun `failWorkItemAttempt throws WORK_ITEM_ATTEMPT_NOT_FOUND from backend`() = runBlocking {
      internalServiceMock.stub {
        onBlocking { failWorkItemAttempt(any()) } doThrow
          WorkItemAttemptNotFoundException("workItem", "workItemAttempt").asStatusRuntimeException(Status.Code.NOT_FOUND)
      }
      val request = failWorkItemAttemptRequest { name = "workItems/workItem/workItemAttempts/workItemAttempt" }
      val exception = assertFailsWith<StatusRuntimeException> { service.failWorkItemAttempt(request) }

      assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
      assertThat(exception.errorInfo)
        .isEqualTo(
          errorInfo {
            domain = Errors.DOMAIN
            reason = Errors.Reason.WORK_ITEM_ATTEMPT_NOT_FOUND.name
            metadata[Errors.Metadata.WORK_ITEM_ATTEMPT.key] = request.name
          }
        )
    }
=======
  @Test
  fun `getWorkItemAttempt returns WorkItemAttempt`() = runBlocking {
    val internalWorkItemAttempt = internalWorkItemAttempt {
      workItemResourceId = "workItem"
      workItemAttemptResourceId = "workItemAttempt"
    }
    internalServiceMock.stub {
      onBlocking { getWorkItemAttempt(any()) } doReturn internalWorkItemAttempt
    }

    val request = getWorkItemAttemptRequest {
      name =
        "workItems/${internalWorkItemAttempt.workItemResourceId}/workItemAttempts/${internalWorkItemAttempt.workItemAttemptResourceId}"
    }
    val response = service.getWorkItemAttempt(request)

    verifyProtoArgument(
        internalServiceMock,
        WorkItemAttemptsGrpcKt.WorkItemAttemptsCoroutineImplBase::getWorkItemAttempt,
      )
      .isEqualTo(
        internalGetWorkItemAttemptRequest {
          workItemResourceId = internalWorkItemAttempt.workItemResourceId
          workItemAttemptResourceId = internalWorkItemAttempt.workItemAttemptResourceId
        }
      )

    assertThat(response)
      .ignoringFields(
        WorkItemAttempt.CREATE_TIME_FIELD_NUMBER,
        WorkItemAttempt.UPDATE_TIME_FIELD_NUMBER,
      )
      .isEqualTo(workItemAttempt { name = request.name })
  }

  @Test
  fun `getWorkItemAttempt throws REQUIRED_FIELD_NOT_SET when name is not set`() = runBlocking {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        service.getWorkItemAttempt(GetWorkItemAttemptRequest.getDefaultInstance())
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
  fun `getWorkItemAttempt throws INVALID_FIELD_VALUE when name is malformed`() = runBlocking {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        service.getWorkItemAttempt(getWorkItemAttemptRequest { name = "workItemAttempts" })
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
  fun `getWorkItemAttempt throws WORK_ITEM_ATTEMPT_NOT_FOUND from backend`() = runBlocking {
    internalServiceMock.stub {
      onBlocking { getWorkItemAttempt(any()) } doThrow
        WorkItemAttemptNotFoundException("workItem", "workItemAttempt")
          .asStatusRuntimeException(Status.Code.NOT_FOUND)
    }
    val request = getWorkItemAttemptRequest {
      name = "workItems/workItem/workItemAttempts/workItemAttempt"
    }
    val exception = assertFailsWith<StatusRuntimeException> { service.getWorkItemAttempt(request) }

    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
    assertThat(exception.errorInfo)
      .isEqualTo(
        errorInfo {
          domain = Errors.DOMAIN
          reason = Errors.Reason.WORK_ITEM_ATTEMPT_NOT_FOUND.name
          metadata[Errors.Metadata.WORK_ITEM.key] = request.name
        }
      )
  }

  @Test
  fun `failWorkItemAttempt returns WorkItemAttempt`() = runBlocking {
    val internalWorkItemAttempt = internalWorkItemAttempt {
      workItemResourceId = "workItem"
      workItemAttemptResourceId = "workItemAttempt"
      state = InternalWorkItemAttempt.State.FAILED
    }
    internalServiceMock.stub {
      onBlocking { failWorkItemAttempt(any()) } doReturn internalWorkItemAttempt
    }

    val request = failWorkItemAttemptRequest {
      name =
        "workItems/${internalWorkItemAttempt.workItemResourceId}/workItemAttempts/${internalWorkItemAttempt.workItemAttemptResourceId}"
    }
    val response = service.failWorkItemAttempt(request)

    val internalRequest = internalFailWorkItemAttemptRequest {
      workItemResourceId = internalWorkItemAttempt.workItemResourceId
      workItemAttemptResourceId = internalWorkItemAttempt.workItemAttemptResourceId
    }
    verifyProtoArgument(
        internalServiceMock,
        WorkItemAttemptsGrpcKt.WorkItemAttemptsCoroutineImplBase::failWorkItemAttempt,
      )
      .isEqualTo(internalRequest)

    assertThat(response.state).isEqualTo(WorkItemAttempt.State.FAILED)
  }

  @Test
  fun `failWorkItemAttempt throws REQUIRED_FIELD_NOT_SET when name is not set`() = runBlocking {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        service.failWorkItemAttempt(FailWorkItemAttemptRequest.getDefaultInstance())
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
  fun `failWorkItemAttempt throws INVALID_FIELD_VALUE when name is malformed`() = runBlocking {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        service.failWorkItemAttempt(failWorkItemAttemptRequest { name = "workItems" })
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
  fun `failWorkItemAttempt throws WORK_ITEM_ATTEMPT_NOT_FOUND from backend`() = runBlocking {
    internalServiceMock.stub {
      onBlocking { failWorkItemAttempt(any()) } doThrow
        WorkItemAttemptNotFoundException("workItem", "workItemAttempt")
          .asStatusRuntimeException(Status.Code.NOT_FOUND)
    }
    val request = failWorkItemAttemptRequest {
      name = "workItems/workItem/workItemAttempts/workItemAttempt"
    }
    val exception = assertFailsWith<StatusRuntimeException> { service.failWorkItemAttempt(request) }

    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
    assertThat(exception.errorInfo)
      .isEqualTo(
        errorInfo {
          domain = Errors.DOMAIN
          reason = Errors.Reason.WORK_ITEM_ATTEMPT_NOT_FOUND.name
          metadata[Errors.Metadata.WORK_ITEM_ATTEMPT.key] = request.name
        }
      )
  }
>>>>>>> main

  @Test
  fun `failWorkItemAttempt throws INVALID_WORK_ITEM_ATTEMPT_STATE from backend`() = runBlocking {
    internalServiceMock.stub {
      onBlocking { failWorkItemAttempt(any()) } doThrow
<<<<<<< HEAD
        WorkItemAttemptInvalidStateException("workItem", "workItemAttempt", InternalWorkItemAttempt.State.SUCCEEDED).asStatusRuntimeException(Status.Code.FAILED_PRECONDITION)
    }
    val request = failWorkItemAttemptRequest { name = "workItems/workItem/workItemAttempts/workItemAttempt" }
=======
        WorkItemAttemptInvalidStateException(
            "workItem",
            "workItemAttempt",
            InternalWorkItemAttempt.State.SUCCEEDED,
          )
          .asStatusRuntimeException(Status.Code.FAILED_PRECONDITION)
    }
    val request = failWorkItemAttemptRequest {
      name = "workItems/workItem/workItemAttempts/workItemAttempt"
    }
>>>>>>> main
    val exception = assertFailsWith<StatusRuntimeException> { service.failWorkItemAttempt(request) }

    assertThat(exception.status.code).isEqualTo(Status.Code.FAILED_PRECONDITION)
    assertThat(exception.errorInfo)
      .isEqualTo(
        errorInfo {
          domain = Errors.DOMAIN
          reason = Errors.Reason.INVALID_WORK_ITEM_ATTEMPT_STATE.name
          metadata[Errors.Metadata.WORK_ITEM_ATTEMPT.key] = request.name
          metadata[Errors.Metadata.WORK_ITEM_ATTEMPT_STATE.key] = "SUCCEEDED"
        }
      )
  }

  @Test
  fun `completeWorkItemAttempt returns WorkItemAttempt`() = runBlocking {
<<<<<<< HEAD

=======
>>>>>>> main
    val internalWorkItemAttempt = internalWorkItemAttempt {
      workItemResourceId = "workItem"
      workItemAttemptResourceId = "workItemAttempt"
      state = InternalWorkItemAttempt.State.SUCCEEDED
    }
<<<<<<< HEAD
    internalServiceMock.stub { onBlocking { completeWorkItemAttempt(any()) } doReturn internalWorkItemAttempt }

    val request = completeWorkItemAttemptRequest {
      name = "workItems/${internalWorkItemAttempt.workItemResourceId}/workItemAttempts/${internalWorkItemAttempt.workItemAttemptResourceId}"
=======
    internalServiceMock.stub {
      onBlocking { completeWorkItemAttempt(any()) } doReturn internalWorkItemAttempt
    }

    val request = completeWorkItemAttemptRequest {
      name =
        "workItems/${internalWorkItemAttempt.workItemResourceId}/workItemAttempts/${internalWorkItemAttempt.workItemAttemptResourceId}"
>>>>>>> main
    }
    val response = service.completeWorkItemAttempt(request)

    val internalRequest = internalCompleteWorkItemAttemptRequest {
      workItemResourceId = internalWorkItemAttempt.workItemResourceId
      workItemAttemptResourceId = internalWorkItemAttempt.workItemAttemptResourceId
    }
<<<<<<< HEAD
    verifyProtoArgument(internalServiceMock, WorkItemAttemptsGrpcKt.WorkItemAttemptsCoroutineImplBase::completeWorkItemAttempt)
=======
    verifyProtoArgument(
        internalServiceMock,
        WorkItemAttemptsGrpcKt.WorkItemAttemptsCoroutineImplBase::completeWorkItemAttempt,
      )
>>>>>>> main
      .isEqualTo(internalRequest)

    assertThat(response.state).isEqualTo(WorkItemAttempt.State.SUCCEEDED)
  }

  @Test
  fun `completeWorkItemAttempt throws REQUIRED_FIELD_NOT_SET when name is not set`() = runBlocking {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        service.completeWorkItemAttempt(CompleteWorkItemAttemptRequest.getDefaultInstance())
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
  fun `completeWorkItemAttempt throws INVALID_FIELD_VALUE when name is malformed`() = runBlocking {
    val exception =
      assertFailsWith<StatusRuntimeException> {
<<<<<<< HEAD
        service.completeWorkItemAttempt(completeWorkItemAttemptRequest {
          name = "workItems"
        })
=======
        service.completeWorkItemAttempt(completeWorkItemAttemptRequest { name = "workItems" })
>>>>>>> main
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
  fun `completeWorkItemAttempt throws WORK_ITEM_ATTEMPT_NOT_FOUND from backend`() = runBlocking {
    internalServiceMock.stub {
      onBlocking { completeWorkItemAttempt(any()) } doThrow
<<<<<<< HEAD
        WorkItemAttemptNotFoundException("workItem", "workItemAttempt").asStatusRuntimeException(Status.Code.NOT_FOUND)
    }
    val request = completeWorkItemAttemptRequest { name = "workItems/workItem/workItemAttempts/workItemAttempt" }
    val exception = assertFailsWith<StatusRuntimeException> { service.completeWorkItemAttempt(request) }
=======
        WorkItemAttemptNotFoundException("workItem", "workItemAttempt")
          .asStatusRuntimeException(Status.Code.NOT_FOUND)
    }
    val request = completeWorkItemAttemptRequest {
      name = "workItems/workItem/workItemAttempts/workItemAttempt"
    }
    val exception =
      assertFailsWith<StatusRuntimeException> { service.completeWorkItemAttempt(request) }
>>>>>>> main

    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
    assertThat(exception.errorInfo)
      .isEqualTo(
        errorInfo {
          domain = Errors.DOMAIN
          reason = Errors.Reason.WORK_ITEM_ATTEMPT_NOT_FOUND.name
          metadata[Errors.Metadata.WORK_ITEM_ATTEMPT.key] = request.name
        }
      )
  }

  @Test
<<<<<<< HEAD
  fun `completeWorkItemAttempt throws INVALID_WORK_ITEM_ATTEMPT_STATE from backend`() = runBlocking {
    internalServiceMock.stub {
      onBlocking { completeWorkItemAttempt(any()) } doThrow
        WorkItemAttemptInvalidStateException("workItem", "workItemAttempt", InternalWorkItemAttempt.State.SUCCEEDED).asStatusRuntimeException(Status.Code.FAILED_PRECONDITION)
    }
    val request = completeWorkItemAttemptRequest { name = "workItems/workItem/workItemAttempts/workItemAttempt" }
    val exception = assertFailsWith<StatusRuntimeException> { service.completeWorkItemAttempt(request) }

    assertThat(exception.status.code).isEqualTo(Status.Code.FAILED_PRECONDITION)
    assertThat(exception.errorInfo)
      .isEqualTo(
        errorInfo {
          domain = Errors.DOMAIN
          reason = Errors.Reason.INVALID_WORK_ITEM_ATTEMPT_STATE.name
          metadata[Errors.Metadata.WORK_ITEM_ATTEMPT.key] = request.name
          metadata[Errors.Metadata.WORK_ITEM_ATTEMPT_STATE.key] = "SUCCEEDED"
        }
      )
  }
=======
  fun `completeWorkItemAttempt throws INVALID_WORK_ITEM_ATTEMPT_STATE from backend`() =
    runBlocking {
      internalServiceMock.stub {
        onBlocking { completeWorkItemAttempt(any()) } doThrow
          WorkItemAttemptInvalidStateException(
              "workItem",
              "workItemAttempt",
              InternalWorkItemAttempt.State.SUCCEEDED,
            )
            .asStatusRuntimeException(Status.Code.FAILED_PRECONDITION)
      }
      val request = completeWorkItemAttemptRequest {
        name = "workItems/workItem/workItemAttempts/workItemAttempt"
      }
      val exception =
        assertFailsWith<StatusRuntimeException> { service.completeWorkItemAttempt(request) }

      assertThat(exception.status.code).isEqualTo(Status.Code.FAILED_PRECONDITION)
      assertThat(exception.errorInfo)
        .isEqualTo(
          errorInfo {
            domain = Errors.DOMAIN
            reason = Errors.Reason.INVALID_WORK_ITEM_ATTEMPT_STATE.name
            metadata[Errors.Metadata.WORK_ITEM_ATTEMPT.key] = request.name
            metadata[Errors.Metadata.WORK_ITEM_ATTEMPT_STATE.key] = "SUCCEEDED"
          }
        )
    }
>>>>>>> main

  @Test
  fun `listWorkItemAttempts returns WorkItemAttempts`() = runBlocking {
    val internalWorkItemAttemptFirst = internalWorkItemAttempt {
<<<<<<< HEAD
        workItemResourceId = "workItemOne"
        workItemAttemptResourceId = "workItemAttemptOne"
        state = InternalWorkItemAttempt.State.ACTIVE
        attemptNumber = 1
      }
=======
      workItemResourceId = "workItemOne"
      workItemAttemptResourceId = "workItemAttemptOne"
      state = InternalWorkItemAttempt.State.ACTIVE
      attemptNumber = 1
    }
>>>>>>> main

    val internalListWorkItemAttemptsResponse = internalListWorkItemAttemptsResponse {
      workItemAttempts += internalWorkItemAttemptFirst
      nextPageToken = internalListWorkItemAttemptsPageToken {
<<<<<<< HEAD
        after = InternalListWorkItemAttemptsPageTokenKt.after {
          workItemResourceId = "workItemTwo"
          workItemAttemptResourceId = "workItemAttemptTwo"
        }
      }
    }
    internalServiceMock.stub { onBlocking { listWorkItemAttempts(any()) } doReturn internalListWorkItemAttemptsResponse }

    val response = service.listWorkItemAttempts(listWorkItemAttemptsRequest { pageSize = 1 })

    verifyProtoArgument(internalServiceMock, WorkItemAttemptsGrpcKt.WorkItemAttemptsCoroutineImplBase::listWorkItemAttempts)
=======
        after =
          InternalListWorkItemAttemptsPageTokenKt.after {
            workItemResourceId = "workItemTwo"
            workItemAttemptResourceId = "workItemAttemptTwo"
          }
      }
    }
    internalServiceMock.stub {
      onBlocking { listWorkItemAttempts(any()) } doReturn internalListWorkItemAttemptsResponse
    }

    val response = service.listWorkItemAttempts(listWorkItemAttemptsRequest { pageSize = 1 })

    verifyProtoArgument(
        internalServiceMock,
        WorkItemAttemptsGrpcKt.WorkItemAttemptsCoroutineImplBase::listWorkItemAttempts,
      )
>>>>>>> main
      .isEqualTo(internalListWorkItemAttemptsRequest { pageSize = 1 })
    assertThat(response)
      .isEqualTo(
        listWorkItemAttemptsResponse {
          workItemAttempts += internalWorkItemAttemptFirst.toWorkItemAttempt()
          nextPageToken =
<<<<<<< HEAD
            internalListWorkItemAttemptsResponse.nextPageToken.after.toByteString().base64UrlEncode()
=======
            internalListWorkItemAttemptsResponse.nextPageToken.after
              .toByteString()
              .base64UrlEncode()
>>>>>>> main
        }
      )
  }

  @Test
  fun `listWorkItemAttempts throws INVALID_FIELD_VALUE when page size is invalid`() = runBlocking {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        service.listWorkItemAttempts(listWorkItemAttemptsRequest { pageSize = -1 })
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
<<<<<<< HEAD
=======

>>>>>>> main
  @Test
  fun `listWorkItemAttepmts throws INVALID_FIELD_VALUE when page token is invalid`() = runBlocking {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        service.listWorkItemAttempts(listWorkItemAttemptsRequest { pageToken = "1" })
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
<<<<<<< HEAD

=======
>>>>>>> main
}
