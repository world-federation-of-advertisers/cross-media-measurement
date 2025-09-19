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

package org.wfanet.measurement.kingdom.service.api.v2alpha

import com.google.protobuf.InvalidProtocolBufferException
import io.grpc.Status
import io.grpc.StatusException
import kotlin.coroutines.CoroutineContext
import kotlin.coroutines.EmptyCoroutineContext
import org.wfanet.measurement.api.v2alpha.DataProviderPrincipal
import org.wfanet.measurement.api.v2alpha.GetModelProviderRequest
import org.wfanet.measurement.api.v2alpha.ListModelProvidersRequest
import org.wfanet.measurement.api.v2alpha.ListModelProvidersResponse
import org.wfanet.measurement.api.v2alpha.MeasurementConsumerPrincipal
import org.wfanet.measurement.api.v2alpha.MeasurementPrincipal
import org.wfanet.measurement.api.v2alpha.ModelProvider
import org.wfanet.measurement.api.v2alpha.ModelProviderKey
import org.wfanet.measurement.api.v2alpha.ModelProviderPrincipal
import org.wfanet.measurement.api.v2alpha.ModelProvidersGrpcKt.ModelProvidersCoroutineImplBase as ModelProvidersCoroutineService
import org.wfanet.measurement.api.v2alpha.listModelProvidersResponse
import org.wfanet.measurement.api.v2alpha.principalFromCurrentContext
import org.wfanet.measurement.common.base64UrlDecode
import org.wfanet.measurement.common.base64UrlEncode
import org.wfanet.measurement.common.grpc.failGrpc
import org.wfanet.measurement.common.grpc.grpcRequire
import org.wfanet.measurement.common.grpc.grpcRequireNotNull
import org.wfanet.measurement.common.identity.apiIdToExternalId
import org.wfanet.measurement.internal.kingdom.ListModelProvidersPageToken as InternalListModelProvidersPageToken
import org.wfanet.measurement.internal.kingdom.ListModelProvidersResponse as InternalListModelProvidersResponse
import org.wfanet.measurement.internal.kingdom.ModelProvidersGrpcKt
import org.wfanet.measurement.internal.kingdom.getModelProviderRequest as internalGetModelProviderRequest
import org.wfanet.measurement.internal.kingdom.listModelProvidersRequest as internalListModelProvidersRequest

class ModelProvidersService(
  private val internalModelProviders: ModelProvidersGrpcKt.ModelProvidersCoroutineStub,
  coroutineContext: CoroutineContext = EmptyCoroutineContext,
) : ModelProvidersCoroutineService(coroutineContext) {
  override suspend fun getModelProvider(request: GetModelProviderRequest): ModelProvider {
    val key =
      grpcRequireNotNull(ModelProviderKey.fromName(request.name)) {
        "Resource name is either unspecified or invalid"
      }

    when (val principal: MeasurementPrincipal = principalFromCurrentContext) {
      is ModelProviderPrincipal -> {
        if (principal.resourceKey.modelProviderId != key.modelProviderId) {
          failGrpc(Status.PERMISSION_DENIED) { "Cannot get others' ModelProvider" }
        }
      }
      is DataProviderPrincipal -> {}
      is MeasurementConsumerPrincipal -> {}
      else -> {
        failGrpc(Status.PERMISSION_DENIED) {
          "Caller does not have permission to get ModelProvider"
        }
      }
    }

    val internalGetModelProviderRequest = internalGetModelProviderRequest {
      externalModelProviderId = apiIdToExternalId(key.modelProviderId)
    }

    try {
      return internalModelProviders
        .getModelProvider(internalGetModelProviderRequest)
        .toModelProvider()
    } catch (e: StatusException) {
      throw when (e.status.code) {
        Status.Code.NOT_FOUND -> Status.NOT_FOUND
        else -> Status.INTERNAL
      }.toExternalStatusRuntimeException(e)
    }
  }

  override suspend fun listModelProviders(
    request: ListModelProvidersRequest
  ): ListModelProvidersResponse {
    grpcRequire(request.pageSize >= 0) { "Page size cannot be less than 0" }

    val internalPageToken: InternalListModelProvidersPageToken? =
      if (request.pageToken.isEmpty()) {
        null
      } else {
        try {
          InternalListModelProvidersPageToken.parseFrom(request.pageToken.base64UrlDecode())
        } catch (e: InvalidProtocolBufferException) {
          throw Status.INVALID_ARGUMENT.withCause(e)
            .withDescription("invalid page token for public ListModelProviders")
            .asRuntimeException()
        }
      }

    val internalListModelProvidersRequest = internalListModelProvidersRequest {
      pageSize = request.pageSize
      if (internalPageToken != null) {
        pageToken = internalPageToken
      }
    }

    val response: InternalListModelProvidersResponse =
      internalModelProviders.listModelProviders(internalListModelProvidersRequest)

    return listModelProvidersResponse {
      modelProviders += response.modelProvidersList.map { it.toModelProvider() }
      if (response.hasNextPageToken()) {
        nextPageToken = response.nextPageToken.toByteString().base64UrlEncode()
      }
    }
  }
}
