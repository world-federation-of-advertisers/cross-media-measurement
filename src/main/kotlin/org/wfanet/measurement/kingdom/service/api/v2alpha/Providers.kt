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

package org.wfanet.measurement.kingdom.service.api.v2alpha

import io.grpc.Status
import org.wfanet.measurement.api.v2alpha.DataProviderKey
import org.wfanet.measurement.api.v2alpha.ModelProviderKey
import org.wfanet.measurement.api.v2alpha.RecurringExchangeKey
import org.wfanet.measurement.common.grpc.failGrpc
import org.wfanet.measurement.common.grpc.grpcRequire
import org.wfanet.measurement.common.identity.apiIdToExternalId
import org.wfanet.measurement.common.identity.externalIdToApiId
import org.wfanet.measurement.internal.kingdom.Provider
import org.wfanet.measurement.internal.kingdom.provider

/** Returns a [Provider] as implied by the current gRPC context. */
fun getProviderFromContext(): Provider {
  return provider {
    when (val key = principalFromCurrentContext.resourceKey) {
      is DataProviderKey -> {
        type = Provider.Type.DATA_PROVIDER
        externalId = apiIdToExternalId(key.dataProviderId)
      }
      is ModelProviderKey -> {
        type = Provider.Type.MODEL_PROVIDER
        externalId = apiIdToExternalId(key.modelProviderId)
      }
      else ->
        failGrpc(Status.UNAUTHENTICATED) {
          "Caller identity is neither DataProvider nor ModelProvider"
        }
    }
  }
}

fun validateRequestProvider(requestModelProvider: String, requestDataProvider: String): Provider {
  val provider = getProviderFromContext()
  @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA") // Proto enums are not null.
  val parentFromRequest: String =
    when (provider.type) {
      Provider.Type.MODEL_PROVIDER -> requestModelProvider
      Provider.Type.DATA_PROVIDER -> requestDataProvider
      Provider.Type.TYPE_UNSPECIFIED, Provider.Type.UNRECOGNIZED -> error("Unsupported Principal")
    }
  grpcRequire(externalIdToApiId(provider.externalId) == parentFromRequest) {
    "Principal from authentication does not match request"
  }
  return provider
}
