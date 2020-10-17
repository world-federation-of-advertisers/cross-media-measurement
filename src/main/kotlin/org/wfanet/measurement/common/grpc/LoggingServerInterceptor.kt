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

package org.wfanet.measurement.common.grpc

import com.google.protobuf.Message
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
import org.wfanet.measurement.common.truncateByteFields

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
        val messageToLog = (message as Message).truncateByteFields(BYTES_TO_LOG)
        logger.logp(Level.INFO, methodName, "gRPC response", "[$threadName] $messageToLog")
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
        val messageToLog = (message as Message).truncateByteFields(BYTES_TO_LOG)
        logger.logp(
          Level.INFO,
          methodName,
          "gRPC request",
          "[$threadName] $headers $messageToLog"
        )
        super.onMessage(message)
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
 * Logs all gRPC requests and responses.
 */
fun BindableService.withVerboseLogging(enabled: Boolean = true): ServerServiceDefinition {
  if (!enabled) return this.bindService()
  return ServerInterceptors.interceptForward(this, LoggingServerInterceptor())
}

/**
 * Logs all gRPC requests and responses.
 */
fun ServerServiceDefinition.withVerboseLogging(enabled: Boolean = true): ServerServiceDefinition {
  if (!enabled) return this
  return ServerInterceptors.interceptForward(this, LoggingServerInterceptor())
}
