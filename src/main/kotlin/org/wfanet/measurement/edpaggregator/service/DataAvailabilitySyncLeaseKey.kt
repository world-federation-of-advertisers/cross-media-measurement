/*
 * Copyright 2026 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.edpaggregator.service

import org.wfanet.measurement.api.v2alpha.DataProviderKey
import org.wfanet.measurement.common.ResourceNameParser
import org.wfanet.measurement.common.api.ChildResourceKey
import org.wfanet.measurement.common.api.ResourceKey

/** [ResourceKey] of a DataAvailabilitySyncLease. */
data class DataAvailabilitySyncLeaseKey(
  override val parentKey: DataProviderKey,
  val dataAvailabilitySyncLeaseId: String,
) : ChildResourceKey {
  constructor(
    dataProviderId: String,
    dataAvailabilitySyncLeaseId: String,
  ) : this(DataProviderKey(dataProviderId), dataAvailabilitySyncLeaseId)

  val dataProviderId: String
    get() = parentKey.dataProviderId

  override fun toName(): String =
    parser.assembleName(
      mapOf(
        IdVariable.DATA_PROVIDER to dataProviderId,
        IdVariable.DATA_AVAILABILITY_SYNC_LEASE to dataAvailabilitySyncLeaseId,
      )
    )

  companion object FACTORY : ResourceKey.Factory<DataAvailabilitySyncLeaseKey> {
    const val PATTERN =
      "${DataProviderKey.PATTERN}/dataAvailabilitySyncLeases/{data_availability_sync_lease}"
    private val parser = ResourceNameParser(PATTERN)

    override fun fromName(resourceName: String): DataAvailabilitySyncLeaseKey? {
      val idVars = parser.parseIdVars(resourceName) ?: return null
      return DataAvailabilitySyncLeaseKey(
        idVars.getValue(IdVariable.DATA_PROVIDER),
        idVars.getValue(IdVariable.DATA_AVAILABILITY_SYNC_LEASE),
      )
    }
  }
}
