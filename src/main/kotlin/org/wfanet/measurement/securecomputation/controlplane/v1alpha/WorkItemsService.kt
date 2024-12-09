/*
 * Copyright 2024 The Cross-Media Measurement Authors
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

import com.google.protobuf.Message
import io.grpc.Status
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemsGrpcKt.WorkItemsCoroutineImplBase


abstract class WorkItemsService: WorkItemsCoroutineImplBase() {

  abstract fun publishMessage(queueName: String, message: Message)

  override suspend fun createWorkItem(
    request: CreateWorkItemRequest
  ): WorkItem {

    val workItem = request.workItem
    val topicId = workItem.queue

    try {
      publishMessage(topicId, workItem.workItemParams)
    } catch (e: Exception) {
      throw when {
        e.message?.contains("Topic id: $topicId does not exist") == true -> {
          Status.NOT_FOUND
            .withDescription(e.message)
            .asRuntimeException()
        }

        else -> {
          Status.UNKNOWN
            .withDescription("An unknown error occurred: ${e.message}")
            .asRuntimeException()
        }
      }
    }
    return workItem
  }

}
