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

package org.wfanet.measurement.common.identity.testing

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
import org.wfanet.measurement.common.identity.DUCHY_IDENTITY_CONTEXT_KEY
import org.wfanet.measurement.common.identity.DUCHY_ID_METADATA_KEY
import org.wfanet.measurement.common.identity.DuchyIdentity

/**
 * Add an interceptor that sets DuchyIdentity in the context.
 *
 * The DuchyId is extracted from the metadata of the request. Note that this doesn't provide any
 * guarantees that the Duchy is who it claims to be -- that is still required.
 *
 * To install in a server, wrap a service with:
 * ```
 *    yourService.withMetadataDuchyIdentities()
 * ```
 *
 * On the client side, use [withDuchyId].
 */
class MetadataDuchyIdInterceptor : ServerInterceptor {
  override fun <ReqT, RespT> interceptCall(
    call: ServerCall<ReqT, RespT>,
    headers: Metadata,
    next: ServerCallHandler<ReqT, RespT>,
  ): ServerCall.Listener<ReqT> {
    val duchyId = headers[DUCHY_ID_METADATA_KEY]
    return if (duchyId == null) {
      call.close(Status.UNAUTHENTICATED.withDescription("No duchy Id in the header"), Metadata())
      object : ServerCall.Listener<ReqT>() {}
    } else {
      val context = Context.current().withValue(DUCHY_IDENTITY_CONTEXT_KEY, DuchyIdentity(duchyId))
      Contexts.interceptCall(context, call, headers, next)
    }
  }
}

/** Convenience helper for [MetadataDuchyIdInterceptor]. */
fun BindableService.withMetadataDuchyIdentities(): ServerServiceDefinition =
  ServerInterceptors.interceptForward(this, MetadataDuchyIdInterceptor())
