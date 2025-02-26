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
import org.wfanet.measurement.internal.securecomputation.controlplane.v1alpha.CompleteWorkItemAttemptRequest
import org.wfanet.measurement.internal.securecomputation.controlplane.v1alpha.CreateWorkItemAttemptRequest
import org.wfanet.measurement.internal.securecomputation.controlplane.v1alpha.FailWorkItemAttemptRequest
import org.wfanet.measurement.internal.securecomputation.controlplane.v1alpha.GetWorkItemAttemptRequest
import org.wfanet.measurement.internal.securecomputation.controlplane.v1alpha.StreamWorkItemAttemptsRequest
import org.wfanet.measurement.internal.securecomputation.controlplane.v1alpha.WorkItemAttemptsGrpcKt.WorkItemAttemptsCoroutineImplBase
import org.wfanet.measurement.internal.securecomputation.controlplane.v1alpha.WorkItemAttempt
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.ModelSuiteNotFoundException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.ModelSuiteReader
import org.wfanet.measurement.securecomputation.deploy.gcloud.spanner.common.WorkItemAttemptNotFoundException
import org.wfanet.measurement.securecomputation.deploy.gcloud.spanner.common.WorkItemNotFoundException
import org.wfanet.measurement.securecomputation.deploy.gcloud.spanner.queries.StreamWorkItemAttempts
import org.wfanet.measurement.securecomputation.deploy.gcloud.spanner.queries.StreamWorkItems
import org.wfanet.measurement.securecomputation.deploy.gcloud.spanner.readers.WorkItemAttemptReader
import org.wfanet.measurement.securecomputation.deploy.gcloud.spanner.readers.WorkItemReader
import org.wfanet.measurement.securecomputation.deploy.gcloud.spanner.writers.CompleteWorkItemAttempt
import org.wfanet.measurement.securecomputation.deploy.gcloud.spanner.writers.CreateWorkItemAttempt
import org.wfanet.measurement.securecomputation.deploy.gcloud.spanner.writers.FailWorkItem
import org.wfanet.measurement.securecomputation.deploy.gcloud.spanner.writers.FailWorkItemAttempt

class SpannerWorkItemAttemptsService(
  private val idGenerator: IdGenerator,
  private val client: AsyncDatabaseClient,
) : WorkItemAttemptsCoroutineImplBase() {

  override suspend fun createWorkItemAttempt(request: CreateWorkItemAttemptRequest): WorkItemAttempt {
    return CreateWorkItemAttempt(request.workItemAttempt).execute(client, idGenerator)
  }

  override suspend fun getWorkItemAttempt(request: GetWorkItemAttemptRequest): WorkItemAttempt {
    val externalWorkItemId = ExternalId(request.externalWorkItemId)
    val externalWorkItemAttemptId = ExternalId(request.externalWorkItemAttemptId)
    return WorkItemAttemptReader()
      .readByExternalIds(client.singleUse(), externalWorkItemId, externalWorkItemAttemptId)
      ?.workItemAttempt
      ?: throw WorkItemAttemptNotFoundException(externalWorkItemId, externalWorkItemAttemptId)
        .asStatusRuntimeException(Status.Code.NOT_FOUND, "WorkItemAttempt not found.")
  }

  override fun streamWorkItemAttempts(request: StreamWorkItemAttemptsRequest): Flow<WorkItemAttempt> {
    grpcRequire(request.limit >= 0) { "Limit cannot be less than 0" }
    return StreamWorkItemAttempts(request.filter, request.limit).execute(client.singleUse()).map {
      it.workItemAttempt
    }
  }

  override suspend fun failWorkItemAttempt(request: FailWorkItemAttemptRequest): WorkItemAttempt {
    grpcRequire(request.externalWorkItemId != 0L) {
      "external_work_item_id not specified"
    }
    grpcRequire(request.externalWorkItemAttemptId != 0L) {
      "external_work_item_attempt_id not specified"
    }
    try {
      return FailWorkItemAttempt(request).execute(client, idGenerator)
    } catch (e: WorkItemAttemptNotFoundException) {
      throw e.asStatusRuntimeException(Status.Code.NOT_FOUND, e.message ?: "WorkItemAttempt not found.")
    }
  }

  override suspend fun completeWorkItemAttempt(request: CompleteWorkItemAttemptRequest): WorkItemAttempt {
    grpcRequire(request.externalWorkItemId != 0L) {
      "external_work_item_id not specified"
    }
    grpcRequire(request.externalWorkItemAttemptId != 0L) {
      "external_work_item_attempt_id not specified"
    }
    try {
      return CompleteWorkItemAttempt(request).execute(client, idGenerator)
    } catch (e: WorkItemAttemptNotFoundException) {
      throw e.asStatusRuntimeException(Status.Code.NOT_FOUND, e.message ?: "WorkItemAttempt not found.")
    }
  }

}
