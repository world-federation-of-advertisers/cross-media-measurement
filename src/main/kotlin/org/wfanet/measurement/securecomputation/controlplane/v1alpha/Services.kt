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

import io.grpc.BindableService
import io.grpc.Channel
import kotlin.coroutines.CoroutineContext
import org.wfanet.measurement.internal.securecomputation.controlplane.WorkItemAttemptsGrpcKt as InternalWorkItemAttemptsGrpcKt
import org.wfanet.measurement.internal.securecomputation.controlplane.WorkItemsGrpcKt as InternalWorkItemsGrpcKt

data class Services(
  val workItems: WorkItemsGrpcKt.WorkItemsCoroutineImplBase,
  val workItemAttempts: WorkItemAttemptsGrpcKt.WorkItemAttemptsCoroutineImplBase,
) {
  fun toList(): List<BindableService> = listOf(workItems, workItemAttempts)

  companion object {
    fun build(internalApiChannel: Channel, coroutineContext: CoroutineContext): Services {
      val internalWorkItemsStub = InternalWorkItemsGrpcKt.WorkItemsCoroutineStub(internalApiChannel)
      val internalWorkItemAttemptsStub =
        InternalWorkItemAttemptsGrpcKt.WorkItemAttemptsCoroutineStub(internalApiChannel)

      return Services(
        WorkItemsService(internalWorkItemsStub, coroutineContext),
        WorkItemAttemptsService(internalWorkItemAttemptsStub, coroutineContext),
      )
    }
  }
}
