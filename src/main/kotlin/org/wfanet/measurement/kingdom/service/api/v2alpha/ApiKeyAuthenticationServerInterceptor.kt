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

package org.wfanet.measurement.kingdom.service.api.v2alpha

import io.grpc.Context
import io.grpc.Contexts
import io.grpc.Metadata
import io.grpc.ServerCall
import io.grpc.ServerCallHandler
import io.grpc.ServerInterceptor
import io.grpc.ServerInterceptors
import io.grpc.ServerServiceDefinition
import io.grpc.Status
import io.grpc.StatusException
import kotlin.coroutines.CoroutineContext
import kotlin.coroutines.EmptyCoroutineContext
import org.wfanet.measurement.api.ApiKeyCredentials
import org.wfanet.measurement.api.v2alpha.MeasurementConsumerKey
import org.wfanet.measurement.api.v2alpha.MeasurementConsumerPrincipal
import org.wfanet.measurement.api.v2alpha.withPrincipal
import org.wfanet.measurement.common.crypto.Hashing
import org.wfanet.measurement.common.grpc.SuspendableServerInterceptor
import org.wfanet.measurement.common.identity.apiIdToExternalId
import org.wfanet.measurement.common.identity.externalIdToApiId
import org.wfanet.measurement.internal.kingdom.ApiKey
import org.wfanet.measurement.internal.kingdom.ApiKeysGrpcKt.ApiKeysCoroutineStub
import org.wfanet.measurement.internal.kingdom.MeasurementConsumer
import org.wfanet.measurement.internal.kingdom.authenticateApiKeyRequest

/**
 * gRPC [ServerInterceptor] to check [ApiKey] credentials from request headers and map them to a
 * [MeasurementConsumerPrincipal] in the [Context].
 */
class ApiKeyAuthenticationServerInterceptor(
  private val internalApiKeysClient: ApiKeysCoroutineStub,
  coroutineContext: CoroutineContext = EmptyCoroutineContext,
) : SuspendableServerInterceptor(coroutineContext) {

  override suspend fun <ReqT : Any, RespT : Any> interceptCallSuspending(
    call: ServerCall<ReqT, RespT>,
    headers: Metadata,
    next: ServerCallHandler<ReqT, RespT>,
  ): ServerCall.Listener<ReqT> {
    val authenticationKey =
      ApiKeyCredentials.fromHeaders(headers)?.apiAuthenticationKey
        ?: return next.startCall(call, headers)

    var context = Context.current()
    try {
      val measurementConsumer = authenticateAuthenticationKey(authenticationKey)
      context =
        context.withPrincipal(
          MeasurementConsumerPrincipal(
            MeasurementConsumerKey(
              externalIdToApiId(measurementConsumer.externalMeasurementConsumerId)
            )
          )
        )
    } catch (e: StatusException) {
      val status =
        when (e.status.code) {
          Status.Code.INVALID_ARGUMENT,
          Status.Code.NOT_FOUND ->
            Status.UNAUTHENTICATED.withDescription("API key is unauthenticated")
          Status.Code.CANCELLED -> Status.CANCELLED
          Status.Code.DEADLINE_EXCEEDED -> Status.DEADLINE_EXCEEDED
          else -> Status.UNKNOWN
        }
      call.close(status.withCause(e), headers)
    }

    return Contexts.interceptCall(context, call, headers, next)
  }

  private suspend fun authenticateAuthenticationKey(
    authenticationKey: String
  ): MeasurementConsumer {
    val keyHash =
      try {
        Hashing.hashSha256(apiIdToExternalId(authenticationKey))
      } catch (e: Exception) {
        throw Status.INVALID_ARGUMENT.withDescription("API key is invalid").asException()
      }
    return internalApiKeysClient.authenticateApiKey(
      authenticateApiKeyRequest { authenticationKeyHash = keyHash }
    )
  }
}

fun ServerServiceDefinition.withApiKeyAuthenticationServerInterceptor(
  internalApiKeysStub: ApiKeysCoroutineStub
): ServerServiceDefinition =
  ServerInterceptors.intercept(this, ApiKeyAuthenticationServerInterceptor(internalApiKeysStub))
