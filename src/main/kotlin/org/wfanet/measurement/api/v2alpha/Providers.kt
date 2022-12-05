// Copyright 2021 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.api.v2alpha

import io.grpc.Status
import org.wfanet.measurement.common.api.ResourceKey
import org.wfanet.measurement.common.grpc.failGrpc
import org.wfanet.measurement.common.grpc.grpcRequire
import org.wfanet.measurement.common.identity.apiIdToExternalId
import org.wfanet.measurement.internal.common.Provider
import org.wfanet.measurement.internal.common.provider

fun ResourceKey.toProvider(): Provider? {
  return when (this) {
    is DataProviderKey ->
      provider {
        type = Provider.Type.DATA_PROVIDER
        externalId = apiIdToExternalId(dataProviderId)
      }
    is ModelProviderKey ->
      provider {
        type = Provider.Type.MODEL_PROVIDER
        externalId = apiIdToExternalId(modelProviderId)
      }
    else ->
      failGrpc(Status.UNAUTHENTICATED) {
        "Caller identity is neither DataProvider nor ModelProvider"
      }
  }
}

fun getProviderFromContext(): Provider {
  return checkNotNull(principalFromCurrentContext.resourceKey.toProvider())
}

fun validateRequestProvider(requestParent: String): Provider {
  val contextProvider = getProviderFromContext()
  grpcRequire(
    contextProvider == MeasurementPrincipal.fromName(requestParent)?.resourceKey?.toProvider()
  ) {
    "Principal from authentication does not match request"
  }
  return contextProvider
}
