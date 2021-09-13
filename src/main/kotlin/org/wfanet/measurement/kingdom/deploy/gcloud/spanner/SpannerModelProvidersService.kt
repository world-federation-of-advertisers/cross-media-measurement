// Copyright 2021 The Cross-Media Measurement Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.wfanet.measurement.kingdom.deploy.gcloud.spanner

import io.grpc.Status
import org.wfanet.measurement.common.grpc.failGrpc
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.common.identity.IdGenerator
import org.wfanet.measurement.gcloud.spanner.AsyncDatabaseClient
import org.wfanet.measurement.internal.kingdom.GetModelProviderRequest
import org.wfanet.measurement.internal.kingdom.ModelProvider
import org.wfanet.measurement.internal.kingdom.ModelProvidersGrpcKt.ModelProvidersCoroutineImplBase
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.ModelProviderReader
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.writers.CreateModelProvider

class SpannerModelProvidersService(
  private val idGenerator: IdGenerator,
  private val client: AsyncDatabaseClient
) : ModelProvidersCoroutineImplBase() {
  override suspend fun createModelProvider(request: ModelProvider): ModelProvider {
    return CreateModelProvider().execute(client, idGenerator)
  }
  override suspend fun getModelProvider(request: GetModelProviderRequest): ModelProvider {
    return ModelProviderReader()
      .readByExternalModelProviderId(
        client.singleUse(),
        ExternalId(request.externalModelProviderId)
      )
      ?.modelProvider
      ?: failGrpc(Status.NOT_FOUND) { "ModelProvider not found" }
  }
}
