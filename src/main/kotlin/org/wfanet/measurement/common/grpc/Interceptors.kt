/*
 * Copyright 2025 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.common.grpc

import io.grpc.BindableService
import io.grpc.CallOptions
import io.grpc.Channel
import io.grpc.ClientCall
import io.grpc.ClientInterceptor
import io.grpc.ForwardingClientCall
import io.grpc.ForwardingClientCallListener
import io.grpc.Metadata
import io.grpc.MethodDescriptor
import io.grpc.ServerInterceptor
import io.grpc.ServerInterceptors
import io.grpc.ServerServiceDefinition
import io.grpc.Status
import io.opentelemetry.api.common.AttributeKey
import io.opentelemetry.api.common.Attributes
import io.opentelemetry.api.metrics.DoubleHistogram
import io.opentelemetry.api.metrics.Meter
import kotlin.time.TimeSource
import org.wfanet.measurement.common.Instrumentation

/**
 * Creates a new [ServerServiceDefinition] whose [io.grpc.ServerCallHandler]s will call
 * [interceptor] before calling the pre-existing [io.grpc.ServerCallHandler].
 *
 * @see ServerInterceptors.intercept
 */
fun ServerServiceDefinition.withInterceptor(
  interceptor: ServerInterceptor
): ServerServiceDefinition = ServerInterceptors.intercept(this, interceptor)

/**
 * Creates a new [ServerServiceDefinition] whose [io.grpc.ServerCallHandler]s will call
 * [interceptor] before calling the pre-existing [io.grpc.ServerCallHandler].
 *
 * @see ServerInterceptors.intercept
 */
fun BindableService.withInterceptor(interceptor: ServerInterceptor): ServerServiceDefinition =
  ServerInterceptors.intercept(this, interceptor)

/**
 * Creates a new [ServerServiceDefinition] whose [io.grpc.ServerCallHandler]s will call
 * [interceptors] before calling the pre-existing [io.grpc.ServerCallHandler].
 *
 * The last interceptor will have its [ServerInterceptor.interceptCall] called first.
 *
 * @see ServerInterceptors.intercept
 */
fun BindableService.withInterceptors(
  vararg interceptors: ServerInterceptor
): ServerServiceDefinition = ServerInterceptors.intercept(this, *interceptors)

/**
 * OpenTelemetry client interceptor that records `rpc.client.duration` metrics for gRPC calls.
 *
 * Records histogram metric with attributes:
 * - `rpc.service`: Full service name (e.g., "wfanet.measurement.api.v2alpha.EventGroups")
 * - `rpc.method`: Method name (e.g., "CreateEventGroup")
 * - `rpc.grpc.status_code`: gRPC status code (0 = OK)
 *
 * Usage:
 * ```kotlin
 * val channel = buildMutualTlsChannel(...)
 *   .intercept(OpenTelemetryClientInterceptor())
 * ```
 */
class OpenTelemetryClientInterceptor(
  private val meter: Meter = Instrumentation.meter
) : ClientInterceptor {

  private val rpcClientDuration: DoubleHistogram =
    meter
      .histogramBuilder("rpc.client.duration")
      .setDescription("Duration of gRPC client calls")
      .setUnit("s")
      .build()

  override fun <ReqT, RespT> interceptCall(
    method: MethodDescriptor<ReqT, RespT>,
    callOptions: CallOptions,
    next: Channel
  ): ClientCall<ReqT, RespT> {
    val startTime = TimeSource.Monotonic.markNow()

    return object : ForwardingClientCall.SimpleForwardingClientCall<ReqT, RespT>(
      next.newCall(method, callOptions)
    ) {
      override fun start(responseListener: Listener<RespT>, headers: Metadata) {
        val listener = object : ForwardingClientCallListener.SimpleForwardingClientCallListener<RespT>(
          responseListener
        ) {
          override fun onClose(status: Status, trailers: Metadata) {
            // Record RPC duration
            val duration = startTime.elapsedNow().inWholeMilliseconds / 1000.0
            val serviceName = method.serviceName ?: "unknown"
            val methodName = method.bareMethodName ?: "unknown"

            rpcClientDuration.record(
              duration,
              Attributes.of(
                AttributeKey.stringKey("rpc.service"), serviceName,
                AttributeKey.stringKey("rpc.method"), methodName,
                AttributeKey.longKey("rpc.grpc.status_code"), status.code.value().toLong()
              )
            )

            super.onClose(status, trailers)
          }
        }
        super.start(listener, headers)
      }
    }
  }
}
