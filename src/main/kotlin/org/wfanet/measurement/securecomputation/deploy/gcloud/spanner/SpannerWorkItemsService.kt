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

import io.grpc.Status
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.map
import org.wfanet.measurement.common.grpc.grpcRequire
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.common.identity.IdGenerator
import org.wfanet.measurement.gcloud.spanner.AsyncDatabaseClient
import org.wfanet.measurement.internal.securecomputation.controlplane.CreateWorkItemRequest
import org.wfanet.measurement.internal.securecomputation.controlplane.FailWorkItemRequest
import org.wfanet.measurement.internal.securecomputation.controlplane.GetWorkItemRequest
import org.wfanet.measurement.internal.securecomputation.controlplane.StreamWorkItemsRequest
import org.wfanet.measurement.internal.securecomputation.controlplane.WorkItemsGrpcKt.WorkItemsCoroutineImplBase
import org.wfanet.measurement.internal.securecomputation.controlplane.WorkItem
import org.wfanet.measurement.securecomputation.deploy.gcloud.spanner.common.WorkItemNotFoundException
import org.wfanet.measurement.securecomputation.deploy.gcloud.spanner.queries.StreamWorkItems
import org.wfanet.measurement.securecomputation.deploy.gcloud.spanner.readers.WorkItemReader
import org.wfanet.measurement.securecomputation.deploy.gcloud.spanner.writers.CreateWorkItem
import org.wfanet.measurement.securecomputation.deploy.gcloud.spanner.writers.FailWorkItem


class SpannerWorkItemsService(
  private val idGenerator: IdGenerator,
  private val client: AsyncDatabaseClient,
) : WorkItemsCoroutineImplBase() {

  override suspend fun createWorkItem(request: CreateWorkItemRequest): WorkItem {
    grpcRequire(request.workItem.queueResourceId.isNotEmpty()) { "Queue field of WorkItem is missing." }
    return CreateWorkItem(request.workItem).execute(client, idGenerator)
  }

  override suspend fun getWorkItem(request: GetWorkItemRequest): WorkItem {
    val workItemResourceId = ExternalId(request.workItemResourceId)
    return WorkItemReader()
      .readByResourceId(
        client.singleUse(),
        workItemResourceId,
      )
      ?.workItem
      ?: throw WorkItemNotFoundException(
        workItemResourceId,
      )
        .asStatusRuntimeException(Status.Code.NOT_FOUND, "WorkItem not found.")
  }

  override fun streamWorkItems(request: StreamWorkItemsRequest): Flow<WorkItem> {
    grpcRequire(request.limit >= 0) { "Limit cannot be less than 0" }
    return StreamWorkItems(request.filter, request.limit).execute(client.singleUse()).map {
      it.workItem
    }
  }

  override suspend fun failWorkItem(request: FailWorkItemRequest): WorkItem {
    grpcRequire(request.workItemResourceId != 0L) {
      "work_item_resource_id not specified"
    }
    try {
      return FailWorkItem(request).execute(client, idGenerator)
    } catch (e: WorkItemNotFoundException) {
      throw e.asStatusRuntimeException(Status.Code.NOT_FOUND, e.message ?: "WorkItem not found.")
    }
  }

}
