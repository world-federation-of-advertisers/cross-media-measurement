package org.wfanet.measurement.service.common

import io.grpc.CallOptions
import io.grpc.Channel
import io.grpc.ClientCall
import io.grpc.ClientInterceptor
import io.grpc.ClientInterceptors
import io.grpc.ForwardingClientCall
import io.grpc.ForwardingClientCallListener.SimpleForwardingClientCallListener
import io.grpc.Metadata
import io.grpc.MethodDescriptor
import java.util.logging.Logger

/**
 * Logs all gRPC requests and responses for clients.
 */
class LogAllClientInterceptor : ClientInterceptor {
  override fun <ReqT, RespT> interceptCall(
    method: MethodDescriptor<ReqT, RespT>,
    callOptions: CallOptions,
    next: Channel
  ): ClientCall<ReqT, RespT> {
    val nextCall = next.newCall(method, callOptions)
    return object : ForwardingClientCall.SimpleForwardingClientCall<ReqT, RespT>(nextCall) {
      override fun start(responseListener: Listener<RespT>?, headers: Metadata?) {
        val listener = object : SimpleForwardingClientCallListener<RespT>(responseListener) {
          override fun onMessage(message: RespT) {
            logger.info("${method.fullMethodName} response: $message")
            super.onMessage(message)
          }
        }
        super.start(listener, headers)
      }
      override fun sendMessage(message: ReqT) {
        logger.info("${method.fullMethodName} request: $message")
        super.sendMessage(message)
      }
    }
  }

  companion object {
    private val logger: Logger = Logger.getLogger(this::class.java.name)
  }
}

/**
 * Enables [LogAllClientInterceptor] on the returned [Channel].
 *
 * @param enable if true (the default), enables verbose logging
 */
fun Channel.withVerboseLogging(enable: Boolean = true): Channel {
  return if (enable) {
    ClientInterceptors.interceptForward(this, LogAllClientInterceptor())
  } else {
    this
  }
}
