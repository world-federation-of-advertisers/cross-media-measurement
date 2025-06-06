/*
 * Copyright 2023 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.kingdom.deploy.gcloud.spanner

import io.grpc.Status
import kotlin.coroutines.CoroutineContext
import kotlin.coroutines.EmptyCoroutineContext
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.map
import org.wfanet.measurement.common.grpc.failGrpc
import org.wfanet.measurement.common.grpc.grpcRequire
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.common.identity.IdGenerator
import org.wfanet.measurement.gcloud.spanner.AsyncDatabaseClient
import org.wfanet.measurement.internal.kingdom.GetModelSuiteRequest
import org.wfanet.measurement.internal.kingdom.ModelSuite
import org.wfanet.measurement.internal.kingdom.ModelSuitesGrpcKt.ModelSuitesCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.StreamModelSuitesRequest
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.ModelProviderNotFoundException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.ModelSuiteNotFoundException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.queries.StreamModelSuites
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.ModelSuiteReader
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.writers.CreateModelSuite

class SpannerModelSuitesService(
  private val idGenerator: IdGenerator,
  private val client: AsyncDatabaseClient,
  coroutineContext: CoroutineContext = EmptyCoroutineContext,
) : ModelSuitesCoroutineImplBase(coroutineContext) {

  override suspend fun createModelSuite(request: ModelSuite): ModelSuite {
    grpcRequire(request.displayName.isNotEmpty()) { "DisplayName field of ModelSuite is missing." }
    try {
      return CreateModelSuite(request).execute(client, idGenerator)
    } catch (e: ModelProviderNotFoundException) {
      throw e.asStatusRuntimeException(Status.Code.NOT_FOUND, "ModelProvider not found.")
    }
  }

  override suspend fun getModelSuite(request: GetModelSuiteRequest): ModelSuite {
    val externalModelProviderId = ExternalId(request.externalModelProviderId)
    val externalModelSuiteId = ExternalId(request.externalModelSuiteId)
    return ModelSuiteReader()
      .readByExternalModelSuiteId(client.singleUse(), externalModelProviderId, externalModelSuiteId)
      ?.modelSuite
      ?: throw ModelSuiteNotFoundException(externalModelProviderId, externalModelSuiteId)
        .asStatusRuntimeException(Status.Code.NOT_FOUND, "ModelSuite not found.")
  }

  override fun streamModelSuites(request: StreamModelSuitesRequest): Flow<ModelSuite> {
    grpcRequire(request.limit >= 0) { "Limit cannot be less than 0" }
    if (
      request.filter.hasAfter() &&
        (!request.filter.after.hasCreateTime() ||
          request.filter.after.externalModelSuiteId == 0L ||
          request.filter.after.externalModelProviderId == 0L)
    ) {
      failGrpc(Status.INVALID_ARGUMENT) { "Missing After filter fields" }
    }
    return StreamModelSuites(request.filter, request.limit).execute(client.singleUse()).map {
      it.modelSuite
    }
  }
}
