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

import io.grpc.Status
import io.grpc.StatusException
import org.wfanet.measurement.internal.securecomputation.controlplane.CompleteWorkItemAttemptRequest
import org.wfanet.measurement.internal.securecomputation.controlplane.FailWorkItemAttemptRequest
import org.wfanet.measurement.internal.securecomputation.controlplane.GetWorkItemAttemptRequest
import org.wfanet.measurement.internal.securecomputation.controlplane.ListWorkItemAttemptsRequest
import org.wfanet.measurement.internal.securecomputation.controlplane.ListWorkItemAttemptsResponse
import org.wfanet.measurement.internal.securecomputation.controlplane.WorkItemAttempt as InternalWorkItemAttempt
import org.wfanet.measurement.internal.securecomputation.controlplane.workItemAttempt as internalWorkItemAttempt
import org.wfanet.measurement.internal.securecomputation.controlplane.createWorkItemAttemptRequest as internalCreateWorkItemAttemptRequest
import org.wfanet.measurement.internal.securecomputation.controlplane.CreateWorkItemAttemptRequest
import org.wfanet.measurement.internal.securecomputation.controlplane.WorkItemAttempt
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemAttemptsGrpcKt.WorkItemAttemptsCoroutineImplBase
import org.wfanet.measurement.internal.securecomputation.controlplane.workItem
import org.wfanet.measurement.securecomputation.service.RequiredFieldNotSetException
import org.wfanet.measurement.securecomputation.service.internal.Errors

class WorkItemAttemptsService(/*private val internalWorkItemAttemptsStub: InternalWorkItemAttemptsCoroutineImplBase*/) :
  WorkItemAttemptsCoroutineImplBase() {

//  suspend fun createWorkItemAttempt(request: CreateWorkItemAttemptRequest): WorkItemAttempt
//
//  override suspend fun createWorkItemAttempt(request: CreateWorkItemAttemptRequest): WorkItemAttempt {
//    if (!request.hasWorkItemAttempt()) {
//      throw RequiredFieldNotSetException("work_item_attempt")
//        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
//    }
//    if (request.workItemAttempt.workItemAttemptId.isEmpty()) {
//      throw RequiredFieldNotSetException("work_item_attempt_id")
//        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
//    }
//
//    val workItemAttempt = request.workItemAttempt
//
//    val internalResponse: InternalWorkItemAttempt =
//      try {
//        internalWorkItemAttemptsStub.createWorkItemAttempt(
//          internalCreateWorkItemAttemptRequest {
//            internalWorkItemAttempt {
//              workItemResourceId = request.workItem.queue
//              workItemAttemptResourceId =
//            }
//          }
//        )
//      } catch (e: StatusException) {
//        throw when (Errors.getReason(e)) {
//          Errors.Reason.REQUIRED_FIELD_NOT_SET,
//          Errors.Reason.QUEUE_NOT_FOUND,
//          Errors.Reason.QUEUE_NOT_FOUND_FOR_INTERNAL_ID,
//          Errors.Reason.INVALID_WORK_ITEM_PRECONDITION_STATE,
//          Errors.Reason.WORK_ITEM_NOT_FOUND,
//          Errors.Reason.WORK_ITEM_ATTEMPT_NOT_FOUND,
//          Errors.Reason.INVALID_FIELD_VALUE,
//          Errors.Reason.WORK_ITEM_ALREADY_EXISTS,
//          Errors.Reason.WORK_ITEM_ATTEMPT_ALREADY_EXISTS,
//          null -> Status.INTERNAL.withCause(e).asRuntimeException()
//        }
//      }
//
//    return internalResponse.toWorkItem()
//  }

//  override suspend fun getWorkItemAttempt(request: GetWorkItemAttemptRequest): WorkItemAttempt {
//    return super.getWorkItemAttempt(request)
//  }
//
//  override suspend fun failWorkItemAttempt(request: FailWorkItemAttemptRequest): WorkItemAttempt {
//    return super.failWorkItemAttempt(request)
//  }
//
//  override suspend fun completeWorkItemAttempt(request: CompleteWorkItemAttemptRequest): WorkItemAttempt {
//    return super.completeWorkItemAttempt(request)
//  }
//
//  override suspend fun listWorkItemAttempts(request: ListWorkItemAttemptsRequest): ListWorkItemAttemptsResponse {
//    return super.listWorkItemAttempts(request)
//  }

}
