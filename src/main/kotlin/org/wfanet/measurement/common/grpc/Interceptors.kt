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
import io.grpc.ServerInterceptor
import io.grpc.ServerInterceptors
import io.grpc.ServerServiceDefinition
import io.opentelemetry.api.common.AttributeKey
import io.opentelemetry.api.common.Attributes.of
import io.opentelemetry.api.metrics.DoubleHistogram
import kotlin.time.Duration
import org.wfanet.measurement.common.Instrumentation

object Interceptors {
  const val INSTRUMENTATION_NAMESPACE = "${Instrumentation.ROOT_NAMESPACE}.grpc.interceptor"
  val GRPC_SERVICE_ATTRIBUTE: AttributeKey<String> = AttributeKey.stringKey("rpc.service")
  val GRPC_METHOD_ATTRIBUTE: AttributeKey<String> = AttributeKey.stringKey("rpc.method")
  private val INTERCEPTOR_NAME_ATTRIBUTE =
    AttributeKey.stringKey("${INSTRUMENTATION_NAMESPACE}.name")

  private val serverDuration: DoubleHistogram =
    Instrumentation.meter
      .histogramBuilder("${INSTRUMENTATION_NAMESPACE}.server.duration")
      .setDescription("Server duration of interceptor execution")
      .setUnit("s")
      .build()
  private val clientDuration: DoubleHistogram =
    Instrumentation.meter
      .histogramBuilder("${INSTRUMENTATION_NAMESPACE}.client.duration")
      .setDescription("Client duration of interceptor execution")
      .setUnit("s")
      .build()

  fun recordServerDuration(
    value: Duration,
    interceptorName: String,
    serviceName: String,
    methodName: String,
  ) = serverDuration.record(value, interceptorName, serviceName, methodName)

  fun recordClientDuration(
    value: Duration,
    interceptorName: String,
    serviceName: String,
    methodName: String,
  ) = clientDuration.record(value, interceptorName, serviceName, methodName)

  private fun DoubleHistogram.record(
    duration: Duration,
    interceptorName: String,
    serviceName: String,
    methodName: String,
  ) {
    record(
      duration.inWholeMilliseconds / 1000.0,
      of(
        INTERCEPTOR_NAME_ATTRIBUTE,
        interceptorName,
        GRPC_SERVICE_ATTRIBUTE,
        serviceName,
        GRPC_METHOD_ATTRIBUTE,
        methodName,
      ),
    )
  }
}

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
