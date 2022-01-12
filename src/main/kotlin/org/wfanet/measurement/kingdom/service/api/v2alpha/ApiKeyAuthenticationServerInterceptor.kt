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

import io.grpc.BindableService
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
import io.grpc.StatusRuntimeException
import io.grpc.stub.AbstractStub
import io.grpc.stub.MetadataUtils
import kotlinx.coroutines.runBlocking
import org.wfanet.measurement.api.ApiKeyConstants
import org.wfanet.measurement.common.crypto.hashSha256
import org.wfanet.measurement.common.identity.apiIdToExternalId
import org.wfanet.measurement.internal.kingdom.ApiKey
import org.wfanet.measurement.internal.kingdom.ApiKeysGrpcKt.ApiKeysCoroutineStub
import org.wfanet.measurement.internal.kingdom.MeasurementConsumer
import org.wfanet.measurement.internal.kingdom.authenticateApiKeyRequest

/** gRPC [ServerInterceptor] to check [ApiKey] credentials coming in from a request. */
class ApiKeyAuthenticationServerInterceptor(
  private val internalApiKeysClient: ApiKeysCoroutineStub
) : ServerInterceptor {
  override fun <ReqT, RespT> interceptCall(
    call: ServerCall<ReqT, RespT>,
    headers: Metadata,
    next: ServerCallHandler<ReqT, RespT>
  ): ServerCall.Listener<ReqT> {
    val authenticationKey = headers.get(ApiKeyConstants.API_AUTHENTICATION_KEY_METADATA_KEY)

    var context = Context.current()
    if (authenticationKey != null) {
      try {
        val measurementConsumer = authenticateAuthenticationKey(authenticationKey)
        context =
          context.withValue(ApiKeyConstants.CONTEXT_MEASUREMENT_CONSUMER_KEY, measurementConsumer)
      } catch (e: Exception) {
        when (e) {
          is StatusRuntimeException, is StatusException -> {}
          else ->
            call.close(Status.UNKNOWN.withDescription("Unknown error when authenticating"), headers)
        }
      }
    }

    return Contexts.interceptCall(context, call, headers, next)
  }

  private fun authenticateAuthenticationKey(authenticationKey: String): MeasurementConsumer =
      runBlocking {
    internalApiKeysClient.authenticateApiKey(
      authenticateApiKeyRequest {
        authenticationKeyHash = hashSha256(apiIdToExternalId(authenticationKey))
      }
    )
  }
}

fun <T : AbstractStub<T>> T.withAuthenticationKey(authenticationKey: String? = null): T {
  val metadata = Metadata()
  authenticationKey?.let { metadata.put(ApiKeyConstants.API_AUTHENTICATION_KEY_METADATA_KEY, it) }
  return MetadataUtils.attachHeaders(this, metadata)
}

fun BindableService.withApiKeyAuthenticationServerInterceptor(
  internalApiKeysStub: ApiKeysCoroutineStub
): ServerServiceDefinition =
  ServerInterceptors.interceptForward(
    this,
    ApiKeyAuthenticationServerInterceptor(internalApiKeysStub)
  )

fun ServerServiceDefinition.withApiKeyAuthenticationServerInterceptor(
  internalApiKeysStub: ApiKeysCoroutineStub
): ServerServiceDefinition =
  ServerInterceptors.interceptForward(
    this,
    ApiKeyAuthenticationServerInterceptor(internalApiKeysStub)
  )

/** Executes [block] with [MeasurementConsumer] installed in a new [Context]. */
fun <T> withMeasurementConsumer(measurementConsumer: MeasurementConsumer, block: () -> T): T {
  return Context.current().withMeasurementConsumer(measurementConsumer).call(block)
}

fun Context.withMeasurementConsumer(measurementConsumer: MeasurementConsumer): Context {
  return withValue(ApiKeyConstants.CONTEXT_MEASUREMENT_CONSUMER_KEY, measurementConsumer)
}
