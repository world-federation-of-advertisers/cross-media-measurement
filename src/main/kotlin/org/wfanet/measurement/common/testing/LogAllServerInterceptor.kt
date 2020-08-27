package org.wfanet.measurement.common.testing

import io.grpc.BindableService
import io.grpc.ForwardingServerCall.SimpleForwardingServerCall
import io.grpc.ForwardingServerCallListener.SimpleForwardingServerCallListener
import io.grpc.Metadata
import io.grpc.ServerCall
import io.grpc.ServerCallHandler
import io.grpc.ServerInterceptor
import io.grpc.ServerInterceptors
import io.grpc.ServerServiceDefinition
import java.util.logging.Logger

/**
 * Logs all gRPC requests and responses.
 */
class LogAllServerInterceptor : ServerInterceptor {
  override fun <ReqT, RespT> interceptCall(
    call: ServerCall<ReqT, RespT>,
    headers: Metadata?,
    next: ServerCallHandler<ReqT, RespT>
  ): ServerCall.Listener<ReqT> {
    val interceptedCall = object : SimpleForwardingServerCall<ReqT, RespT>(call) {
      override fun sendMessage(message: RespT) {
        logger.info("${call.methodDescriptor.fullMethodName} response: $message")
        super.sendMessage(message)
      }
    }
    val originalListener = next.startCall(interceptedCall, headers)
    return object : SimpleForwardingServerCallListener<ReqT>(originalListener) {
      override fun onMessage(message: ReqT) {
        logger.info("${call.methodDescriptor.fullMethodName} request: $message")
        super.onMessage(message)
      }
    }
  }

  companion object {
    val logger: Logger = Logger.getLogger(this::class.java.name)
  }
}

/**
 * Logs all gRPC requests and responses.
 */
fun BindableService.withVerboseLogging(): ServerServiceDefinition {
  return ServerInterceptors.interceptForward(this, LogAllServerInterceptor())
}

/**
 * Logs all gRPC requests and responses.
 */
fun ServerServiceDefinition.withVerboseLogging(): ServerServiceDefinition {
  return ServerInterceptors.interceptForward(this, LogAllServerInterceptor())
}
