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
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.queries.StreamModelSuites
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.ModelSuiteReader
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.writers.CreateModelSuite

class SpannerModelSuitesService(
  private val idGenerator: IdGenerator,
  private val client: AsyncDatabaseClient
) : ModelSuitesCoroutineImplBase() {

  override suspend fun createModelSuite(request: ModelSuite): ModelSuite {
    grpcRequire(request.displayName.isNotEmpty()) {
      "DisplayName field of ModelSuite is missing."
    }
    return CreateModelSuite(request).execute(client, idGenerator)
  }

  override suspend fun getModelSuite(request: GetModelSuiteRequest): ModelSuite {
    return ModelSuiteReader()
      .readByExternalModelSuiteId(
        client.singleUse(),
        ExternalId(request.externalModelProviderId),
        ExternalId(request.externalModelSuiteId)
      )
      ?.modelSuite
      ?: failGrpc(Status.NOT_FOUND) { "ModelSuite not found" }
  }

  override fun streamModelSuites(request: StreamModelSuitesRequest): Flow<ModelSuite> {
    return StreamModelSuites(request.filter, request.limit).execute(client.singleUse()).map {
      it.modelSuite
    }
  }
}
