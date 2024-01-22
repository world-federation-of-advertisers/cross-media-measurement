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

import org.wfanet.measurement.common.ResourceNameParser
import org.wfanet.measurement.common.api.ChildResourceKey
import org.wfanet.measurement.common.api.ResourceKey

/** [ResourceKey] of the parent of a requisitions resource collection. */
sealed interface RequisitionParentKey : ResourceKey

/** [ResourceKey] of a Requisition. */
sealed interface RequisitionKey : ChildResourceKey {
  override val parentKey: RequisitionParentKey

  val requisitionId: String

  companion object FACTORY : ResourceKey.Factory<RequisitionKey> {
    override fun fromName(resourceName: String): RequisitionKey? {
      return CanonicalRequisitionKey.fromName(resourceName)
        ?: MeasurementRequisitionKey.fromName(resourceName)
    }
  }
}

/** Canonical [ResourceKey] of a Requisition. */
data class CanonicalRequisitionKey(
  override val parentKey: DataProviderKey,
  override val requisitionId: String,
) : RequisitionKey {
  constructor(
    dataProviderId: String,
    requisitionId: String,
  ) : this(DataProviderKey(dataProviderId), requisitionId)

  val dataProviderId: String
    get() = parentKey.dataProviderId

  override fun toName(): String {
    return parser.assembleName(
      mapOf(IdVariable.DATA_PROVIDER to dataProviderId, IdVariable.REQUISITION to requisitionId)
    )
  }

  companion object FACTORY : ResourceKey.Factory<CanonicalRequisitionKey> {
    private val parser =
      ResourceNameParser("dataProviders/{data_provider}/requisitions/{requisition}")

    val defaultValue = CanonicalRequisitionKey("", "")

    override fun fromName(resourceName: String): CanonicalRequisitionKey? {
      return parser.parseIdVars(resourceName)?.let {
        CanonicalRequisitionKey(
          it.getValue(IdVariable.DATA_PROVIDER),
          it.getValue(IdVariable.REQUISITION),
        )
      }
    }
  }
}
