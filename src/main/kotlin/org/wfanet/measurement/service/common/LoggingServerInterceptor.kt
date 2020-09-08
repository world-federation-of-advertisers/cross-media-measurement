package org.wfanet.measurement.service.common

import io.grpc.BindableService
import io.grpc.ForwardingServerCall.SimpleForwardingServerCall
import io.grpc.ForwardingServerCallListener.SimpleForwardingServerCallListener
import io.grpc.Metadata
import io.grpc.ServerCall
import io.grpc.ServerCallHandler
import io.grpc.ServerInterceptor
import io.grpc.ServerInterceptors
import io.grpc.ServerServiceDefinition
import io.grpc.Status
import java.util.logging.Level
import java.util.logging.Logger

/**
 * Logs all gRPC requests and responses.
 */
class LoggingServerInterceptor : ServerInterceptor {
  override fun <ReqT, RespT> interceptCall(
    call: ServerCall<ReqT, RespT>,
    headers: Metadata?,
    next: ServerCallHandler<ReqT, RespT>
  ): ServerCall.Listener<ReqT> {
    val methodName = call.methodDescriptor.fullMethodName
    val interceptedCall = object : SimpleForwardingServerCall<ReqT, RespT>(call) {
      override fun sendMessage(message: RespT) {
        logger.logp(Level.INFO, methodName, "gRPC response", "[$threadName] $message")
        super.sendMessage(message)
      }
      override fun close(status: Status, trailers: Metadata) {
        if (status.cause != null) {
          logger.logp(Level.SEVERE, methodName, "gRPC exception", "[$threadName]", status.cause)
        }
        super.close(status, trailers)
      }
    }
    val originalListener = next.startCall(interceptedCall, headers)
    return object : SimpleForwardingServerCallListener<ReqT>(originalListener) {
      override fun onMessage(message: ReqT) {
        logger.logp(Level.INFO, methodName, "gRPC request", "[$threadName] $headers $message")
        super.onMessage(message)
      }
    }
  }

  companion object {
    private val logger: Logger = Logger.getLogger(this::class.java.name)
    private val threadName: String
      get() = Thread.currentThread().name
  }
}

/**
 * Logs all gRPC requests and responses.
 */
fun BindableService.withVerboseLogging(): ServerServiceDefinition {
  return ServerInterceptors.interceptForward(this, LoggingServerInterceptor())
}

/**
 * Logs all gRPC requests and responses.
 */
fun ServerServiceDefinition.withVerboseLogging(): ServerServiceDefinition {
  return ServerInterceptors.interceptForward(this, LoggingServerInterceptor())
}
