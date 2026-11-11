/*
 * Copyright 2026 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.edpaggregator.deploy.common.server

import io.grpc.Metadata
import io.grpc.ServerCall
import io.grpc.ServerCallHandler
import io.grpc.ServerInterceptor
import org.wfanet.measurement.common.grpc.LoggingServerInterceptor

/** Logs RPCs while withholding bearer credentials from request metadata. */
object RedactingLoggingServerInterceptor : ServerInterceptor {
  private val authorization = Metadata.Key.of("authorization", Metadata.ASCII_STRING_MARSHALLER)

  override fun <ReqT, RespT> interceptCall(
    call: ServerCall<ReqT, RespT>,
    headers: Metadata,
    next: ServerCallHandler<ReqT, RespT>,
  ): ServerCall.Listener<ReqT> {
    val safeHeaders =
      Metadata().apply {
        merge(headers)
        discardAll(authorization)
      }
    return LoggingServerInterceptor.interceptCall(
      call,
      safeHeaders,
      ServerCallHandler { wrappedCall, _ -> next.startCall(wrappedCall, headers) },
    )
  }
}
