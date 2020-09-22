package org.wfanet.measurement.service.common

import com.google.protobuf.Message
import io.grpc.CallOptions
import io.grpc.Channel
import io.grpc.ClientCall
import io.grpc.ClientInterceptor
import io.grpc.ClientInterceptors
import io.grpc.ForwardingClientCall
import io.grpc.ForwardingClientCallListener.SimpleForwardingClientCallListener
import io.grpc.Metadata
import io.grpc.MethodDescriptor
import java.util.logging.Level
import java.util.logging.Logger
import org.wfanet.measurement.common.truncateByteFields

/**
 * Logs all gRPC requests and responses for clients.
 */
class LoggingClientInterceptor : ClientInterceptor {
  override fun <ReqT, RespT> interceptCall(
    method: MethodDescriptor<ReqT, RespT>,
    callOptions: CallOptions,
    next: Channel
  ): ClientCall<ReqT, RespT> {
    val nextCall = next.newCall(method, callOptions)
    return object : ForwardingClientCall.SimpleForwardingClientCall<ReqT, RespT>(nextCall) {
      override fun start(responseListener: Listener<RespT>?, headers: Metadata?) {
        logger.logp(
          Level.INFO, method.fullMethodName, "gRPC headers", "[$threadName] $headers"
        )
        val listener = object : SimpleForwardingClientCallListener<RespT>(responseListener) {
          override fun onMessage(message: RespT) {
            val messageToLog = (message as Message).truncateByteFields(BYTES_TO_LOG)
            logger.logp(
              Level.INFO, method.fullMethodName, "gRPC response", "[$threadName] $messageToLog"
            )
            super.onMessage(message)
          }
        }
        super.start(listener, headers)
      }
      override fun sendMessage(message: ReqT) {
        val messageToLog = (message as Message).truncateByteFields(BYTES_TO_LOG)
        logger.logp(
          Level.INFO, method.fullMethodName, "gRPC request", "[$threadName] $messageToLog"
        )
        super.sendMessage(message)
      }
    }
  }

  companion object {
    private val logger: Logger = Logger.getLogger(this::class.java.name)
    private const val BYTES_TO_LOG = 100
    private val threadName: String
      get() = Thread.currentThread().name
  }
}

/**
 * Enables [LoggingClientInterceptor] on the returned [Channel].
 *
 * @param enable if true (the default), enables verbose logging
 */
fun Channel.withVerboseLogging(enable: Boolean = true): Channel {
  return if (enable) {
    ClientInterceptors.interceptForward(this, LoggingClientInterceptor())
  } else {
    this
  }
}
