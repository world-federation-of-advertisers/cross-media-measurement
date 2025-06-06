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

package org.wfanet.measurement.access.service.v1alpha

import io.grpc.BindableService
import io.grpc.Channel
import kotlin.coroutines.CoroutineContext
import kotlin.coroutines.EmptyCoroutineContext
import org.wfanet.measurement.access.v1alpha.PermissionsGrpcKt
import org.wfanet.measurement.access.v1alpha.PoliciesGrpcKt
import org.wfanet.measurement.access.v1alpha.PrincipalsGrpcKt
import org.wfanet.measurement.access.v1alpha.RolesGrpcKt
import org.wfanet.measurement.internal.access.PermissionsGrpcKt as InternalPermissionsGrpcKt
import org.wfanet.measurement.internal.access.PoliciesGrpcKt as InternalPoliciesGrpcKt
import org.wfanet.measurement.internal.access.PrincipalsGrpcKt as InternalPrincipalsGrpcKt
import org.wfanet.measurement.internal.access.RolesGrpcKt as InternalRolesGrpcKt

data class Services(
  val principals: PrincipalsGrpcKt.PrincipalsCoroutineImplBase,
  val permissions: PermissionsGrpcKt.PermissionsCoroutineImplBase,
  val roles: RolesGrpcKt.RolesCoroutineImplBase,
  val policies: PoliciesGrpcKt.PoliciesCoroutineImplBase,
) {
  fun toList(): List<BindableService> = listOf(principals, permissions, roles, policies)

  companion object {
    fun build(
      internalApiChannel: Channel,
      coroutineContext: CoroutineContext = EmptyCoroutineContext,
    ): Services {
      val internalPermissionsStub =
        InternalPermissionsGrpcKt.PermissionsCoroutineStub(internalApiChannel)
      val internalPrincipalsStub =
        InternalPrincipalsGrpcKt.PrincipalsCoroutineStub(internalApiChannel)
      val internalRolesStub = InternalRolesGrpcKt.RolesCoroutineStub(internalApiChannel)
      val internalPoliciesStub = InternalPoliciesGrpcKt.PoliciesCoroutineStub(internalApiChannel)

      return Services(
        PrincipalsService(internalPrincipalsStub, coroutineContext),
        PermissionsService(internalPermissionsStub, coroutineContext),
        RolesService(internalRolesStub, coroutineContext),
        PoliciesService(internalPoliciesStub, coroutineContext),
      )
    }
  }
}
