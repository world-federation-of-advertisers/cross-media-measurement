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

package org.wfanet.measurement.kingdom.service.internal

import org.wfanet.measurement.common.grpc.failGrpc
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.internal.common.Provider

/**
 * If [Provider] is for a DataProvider, returns the DataProvider's [ExternalId].
 *
 * If the [Provider] is invalid, throws a `StatusRuntimeException` signaling an invalid argument.
 * Otherwise, returns null.
 */
val Provider.externalDataProviderId: ExternalId?
  get() = getExternalIdOfType(Provider.Type.DATA_PROVIDER)

/**
 * If [Provider] is for a ModelProvider, returns the ModelProvider's [ExternalId].
 *
 * If the [Provider] is invalid, throws a `StatusRuntimeException` signaling an invalid argument.
 * Otherwise, returns null.
 */
val Provider.externalModelProviderId: ExternalId?
  get() = getExternalIdOfType(Provider.Type.MODEL_PROVIDER)

private fun Provider.getExternalIdOfType(desiredType: Provider.Type): ExternalId? {
  @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA")
  return when (type) {
    desiredType -> ExternalId(externalId)
    Provider.Type.MODEL_PROVIDER,
    Provider.Type.DATA_PROVIDER -> null
    Provider.Type.UNRECOGNIZED,
    Provider.Type.TYPE_UNSPECIFIED -> failGrpc { "Invalid Provider: $this" }
  }
}
