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
import org.wfanet.measurement.internal.securecomputation.controlplane.createWorkItemAttemptRequest as internalCreateWorkItemAttemptRequest
import com.google.rpc.errorInfo
import io.grpc.Status
import io.grpc.StatusRuntimeException
import kotlin.test.assertFailsWith
import org.junit.Test
import org.mockito.kotlin.doReturn
import org.mockito.kotlin.doThrow
import org.mockito.kotlin.stub
import org.wfanet.measurement.common.grpc.errorInfo
import org.wfanet.measurement.common.testing.verifyProtoArgument
import org.wfanet.measurement.securecomputation.service.Errors
import org.wfanet.measurement.securecomputation.service.internal.WorkItemAlreadyExistsException
import org.wfanet.measurement.securecomputation.service.internal.WorkItemAttemptAlreadyExistsException

@RunWith(JUnit4::class)
class WorkItemAttemptsServiceTest {

  private val internalServiceMock = mockService<WorkItemAttemptsGrpcKt.WorkItemAttemptsCoroutineImplBase>()

  @get:Rule
  val grpcTestServer = GrpcTestServerRule { addService(internalServiceMock) }

  private lateinit var service: WorkItemAttemptsService

  @Before
  fun initService() {
    service = WorkItemAttemptsService(InternalWorkItemAttemptsCoroutineStub(grpcTestServer.channel))
  }

  @Test
  fun `createWorkItemAttempt returns WorkItemAttempt`() = runBlocking {

    val internalWorkItemAttempt = internalWorkItemAttempt {
      workItemResourceId = "workItem"
      workItemAttemptResourceId = "workItemAttempt"
      state = InternalWorkItemAttempt.State.ACTIVE
    }
    internalServiceMock.stub { onBlocking { createWorkItemAttempt(any()) } doReturn internalWorkItemAttempt }

    val request = createWorkItemAttemptRequest {
      parent = "workItems/${internalWorkItemAttempt.workItemResourceId}"
      workItemAttempt = workItemAttempt {
        name = "workItems/${internalWorkItemAttempt.workItemResourceId}/workItemAttempts/${internalWorkItemAttempt.workItemAttemptResourceId}"
      }
      workItemAttemptId = "workItemAttempt"
    }
    val response = service.createWorkItemAttempt(request)

    verifyProtoArgument(internalServiceMock, WorkItemAttemptsGrpcKt.WorkItemAttemptsCoroutineImplBase::createWorkItemAttempt)
      .isEqualTo(internalCreateWorkItemAttemptRequest {
        internalWorkItemAttempt {
          workItemResourceId = request.parent
          workItemAttemptResourceId = request.workItemAttemptId
        }
      })

    assertThat(response)
      .ignoringFields(
        WorkItemAttempt.CREATE_TIME_FIELD_NUMBER,
        WorkItemAttempt.UPDATE_TIME_FIELD_NUMBER,
        WorkItemAttempt.STATE_FIELD_NUMBER
      )
      .isEqualTo(request.workItemAttempt)
    assertThat(response.state).isEqualTo(WorkItemAttempt.State.ACTIVE)
  }

  @Test
  fun `createWorkItemAttempt throws REQUIRED_FIELD_NOT_SET when parent is not set`() = runBlocking {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        service.createWorkItemAttempt(
          createWorkItemAttemptRequest {
          }
        )
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

    val exception = assertFailsWith<StatusRuntimeException> { service.createWorkItemAttempt(request) }

    assertThat(exception.status.code).isEqualTo(Status.Code.ALREADY_EXISTS)
    assertThat(exception.errorInfo)
      .isEqualTo(
        errorInfo {
          domain = Errors.DOMAIN
          reason = Errors.Reason.WORK_ITEM_ATTEMPT_ALREADY_EXISTS.name
          metadata[Errors.Metadata.WORK_ITEM_ATTEMPT.key] = "workItems/workItem/workItemAttempts/workItemAttempt"
        }
      )
  }

}
