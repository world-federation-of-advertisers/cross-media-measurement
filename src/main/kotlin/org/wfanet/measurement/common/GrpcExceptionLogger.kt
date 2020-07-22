// Copyright 2020 The Measurement System Authors
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

package org.wfanet.measurement.common

import io.grpc.ForwardingServerCall.SimpleForwardingServerCall
import io.grpc.Metadata
import io.grpc.ServerCall
import io.grpc.ServerCallHandler
import io.grpc.ServerInterceptor
import io.grpc.Status
import java.util.logging.Level
import java.util.logging.Logger

class GrpcExceptionLogger : ServerInterceptor {
  override fun <ReqT, RespT> interceptCall(
    call: ServerCall<ReqT, RespT>,
    headers: Metadata,
    next: ServerCallHandler<ReqT, RespT>
  ): ServerCall.Listener<ReqT> {
    return next.startCall(LoggingCall(call), headers)
  }

  private class LoggingCall<ReqT, RespT>(call: ServerCall<ReqT, RespT>) :
    SimpleForwardingServerCall<ReqT, RespT>(call) {
    companion object {
      private val logger = Logger.getLogger(this::class.java.name)
    }

    override fun close(status: Status, trailers: Metadata) {
      if (status.cause != null) {
        logger.log(Level.SEVERE, "gRPC exception:", status.cause)
      }
      super.close(status, trailers)
    }
  }
}
