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
