// Copyright 2020 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.common.identity

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
import io.grpc.stub.AbstractStub
import io.grpc.stub.MetadataUtils

/**
 * Details about an authenticated Duchy.
 *
 * @property[id] Stable identifier for a duchy.
 */
data class DuchyIdentity(val id: String) {
  init {
    require(id in DuchyIds.ALL) {
      "Duchy $id is unknown; known Duchies are ${DuchyIds.ALL}"
    }
  }
}

val duchyIdentityFromContext: DuchyIdentity
  get() = requireNotNull(DUCHY_IDENTITY_CONTEXT_KEY.get()) {
    "gRPC context is missing key $DUCHY_IDENTITY_CONTEXT_KEY"
  }

private const val KEY_NAME = "duchy-identity"
private val DUCHY_IDENTITY_CONTEXT_KEY: Context.Key<DuchyIdentity> = Context.key(KEY_NAME)
private val DUCHY_ID_METADATA_KEY = Metadata.Key.of(KEY_NAME, Metadata.ASCII_STRING_MARSHALLER)

/**
 * Add an interceptor that sets DuchyIdentity in the context.
 *
 * Note that this doesn't provide any guarantees that the Duchy is who it claims to be -- that is
 * still required.
 *
 * To install in a server, wrap a service with:
 *    yourService.withDuchyIdentities()
 *
 * On the client side, use [withDuchyId].
 */
class DuchyServerIdentityInterceptor : ServerInterceptor {
  override fun <ReqT, RespT> interceptCall(
    call: ServerCall<ReqT, RespT>,
    headers: Metadata,
    next: ServerCallHandler<ReqT, RespT>
  ): ServerCall.Listener<ReqT> {
    val duchyId: String? = headers.get(DUCHY_ID_METADATA_KEY)

    if (duchyId == null) {
      call.close(
        Status.UNAUTHENTICATED.withDescription("gRPC metadata missing 'duchy_id' key"),
        Metadata()
      )
      return object : ServerCall.Listener<ReqT>() {}
    }

    val context = Context.current().withValue(DUCHY_IDENTITY_CONTEXT_KEY, DuchyIdentity(duchyId))
    return Contexts.interceptCall(context, call, headers, next)
  }
}

/**
 * Convenience helper for [DuchyServerIdentityInterceptor].
 */
fun BindableService.withDuchyIdentities(): ServerServiceDefinition =
  ServerInterceptors.interceptForward(this, DuchyServerIdentityInterceptor())

/**
 * Convenience helper for [DuchyServerIdentityInterceptor].
 */
fun ServerServiceDefinition.withDuchyIdentities(): ServerServiceDefinition =
  ServerInterceptors.interceptForward(this, DuchyServerIdentityInterceptor())

/**
 * Sets metadata key "duchy_id" on all outgoing requests.
 *
 * Usage:
 *   val someStub = SomeServiceCoroutineStub(channel).withDuchyId("MyDuchyId")
 */
fun <T : AbstractStub<T>> T.withDuchyId(duchyId: String): T {
  val metadata = Metadata()
  metadata.put(DUCHY_ID_METADATA_KEY, duchyId)
  return MetadataUtils.attachHeaders(this, metadata)
}
