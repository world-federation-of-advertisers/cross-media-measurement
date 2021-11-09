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
import io.grpc.stub.AbstractStub
import io.grpc.stub.MetadataUtils
import org.wfanet.measurement.api.v2alpha.AccountConstants

private const val ACTIVATE_ACCOUNT_METHOD_NAME = "ActivateAccount"
private const val AUTHENTICATE_METHOD_NAME = "Authenticate"

/** gRPC [ServerInterceptor] to check account credentials coming in from a request. */
class AccountServerInterceptor() : ServerInterceptor {
  override fun <ReqT, RespT> interceptCall(
    call: ServerCall<ReqT, RespT>,
    headers: Metadata,
    next: ServerCallHandler<ReqT, RespT>
  ): ServerCall.Listener<ReqT> {
    val idToken = headers.get(AccountConstants.ID_TOKEN_METADATA_KEY)
    val fullMethodName = call.methodDescriptor.fullMethodName

    if (idToken == null) {
      if (!fullMethodName.contains(AUTHENTICATE_METHOD_NAME)) {
        if (fullMethodName.contains(ACTIVATE_ACCOUNT_METHOD_NAME)) {
          call.close(Status.INVALID_ARGUMENT.withDescription("Id token is missing"), headers)
        } else {
          call.close(Status.UNAUTHENTICATED.withDescription("Id token is missing"), headers)
        }
      }
    } else {
      val context = Context.current().withValue(AccountConstants.CONTEXT_ID_TOKEN_KEY, idToken)
      return Contexts.interceptCall(context, call, headers, next)
    }

    return object : ServerCall.Listener<ReqT>() {}
  }
}

fun <T : AbstractStub<T>> T.withIdToken(idToken: String? = null): T {
  val metadata = Metadata()
  idToken?.let { metadata.put(AccountConstants.ID_TOKEN_METADATA_KEY, it) }
  return MetadataUtils.attachHeaders(this, metadata)
}

fun BindableService.withAccountServerInterceptor(): ServerServiceDefinition =
  ServerInterceptors.interceptForward(this, AccountServerInterceptor())
