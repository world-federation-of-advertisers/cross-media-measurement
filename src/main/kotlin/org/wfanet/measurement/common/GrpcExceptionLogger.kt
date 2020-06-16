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
