/*
 * Copyright 2024 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.access.client.v1alpha

import io.grpc.Context
import org.wfanet.measurement.access.v1alpha.Principal

/** gRPC [Context.Key]s for Access API clients. */
object ContextKeys {
  val PRINCIPAL: Context.Key<Principal> = Context.key("access-principal")
  val SCOPES: Context.Key<Set<String>> = Context.key("auth-scopes")
}

fun Context.withPrincipalAndScopes(principal: Principal, scopes: Set<String>): Context =
  withValue(ContextKeys.PRINCIPAL, principal).withValue(ContextKeys.SCOPES, scopes)
