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
import kotlin.coroutines.CoroutineContext
import kotlin.coroutines.EmptyCoroutineContext
import org.wfanet.measurement.common.grpc.failGrpc
import org.wfanet.measurement.common.grpc.grpcRequire
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.common.identity.IdGenerator
import org.wfanet.measurement.gcloud.spanner.AsyncDatabaseClient
import org.wfanet.measurement.internal.kingdom.GetModelProviderRequest
import org.wfanet.measurement.internal.kingdom.ListModelProvidersPageTokenKt
import org.wfanet.measurement.internal.kingdom.ListModelProvidersRequest
import org.wfanet.measurement.internal.kingdom.ListModelProvidersResponse
import org.wfanet.measurement.internal.kingdom.ModelProvider
import org.wfanet.measurement.internal.kingdom.ModelProvidersGrpcKt.ModelProvidersCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.listModelProvidersPageToken
import org.wfanet.measurement.internal.kingdom.listModelProvidersResponse
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.ModelProviderReader
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.writers.CreateModelProvider

class SpannerModelProvidersService(
  private val idGenerator: IdGenerator,
  private val client: AsyncDatabaseClient,
  coroutineContext: CoroutineContext = EmptyCoroutineContext,
) : ModelProvidersCoroutineImplBase(coroutineContext) {
  override suspend fun createModelProvider(request: ModelProvider): ModelProvider {
    return CreateModelProvider().execute(client, idGenerator)
  }

  override suspend fun getModelProvider(request: GetModelProviderRequest): ModelProvider {
    return ModelProviderReader()
      .readByExternalModelProviderId(
        client.singleUse(),
        ExternalId(request.externalModelProviderId),
      )
      ?.modelProvider ?: failGrpc(Status.NOT_FOUND) { "ModelProvider not found" }
  }

  override suspend fun listModelProviders(
    request: ListModelProvidersRequest
  ): ListModelProvidersResponse {
    grpcRequire(request.pageSize >= 0) { "Page size cannot be less than 0" }
    val pageSize =
      if (request.pageSize == 0) {
        DEFAULT_PAGE_SIZE
      } else {
        request.pageSize.coerceAtMost(MAX_PAGE_SIZE)
      }

    val after = if (request.hasPageToken()) request.pageToken.after else null

    val modelProviderList =
      ModelProviderReader().readModelProviders(client.singleUse(), pageSize + 1, after).map {
        it.modelProvider
      }

    if (modelProviderList.isEmpty()) {
      return ListModelProvidersResponse.getDefaultInstance()
    }

    return listModelProvidersResponse {
      for ((index, modelProvider) in modelProviderList.withIndex()) {
        if (index == pageSize) {
          nextPageToken = listModelProvidersPageToken {
            this.after =
              ListModelProvidersPageTokenKt.after {
                externalModelProviderId =
                  this@listModelProvidersResponse.modelProviders.last().externalModelProviderId
              }
          }
        } else {
          this.modelProviders += modelProvider
        }
      }
    }
  }

  companion object {
    private const val MAX_PAGE_SIZE = 1000
    private const val DEFAULT_PAGE_SIZE = 50
  }
}
