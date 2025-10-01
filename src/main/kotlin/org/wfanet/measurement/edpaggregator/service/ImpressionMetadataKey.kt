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

package org.wfanet.measurement.edpaggregator.service

import org.wfanet.measurement.api.v2alpha.DataProviderKey
import org.wfanet.measurement.common.ResourceNameParser
import org.wfanet.measurement.common.api.ChildResourceKey
import org.wfanet.measurement.common.api.ResourceKey

/** [ResourceKey] of a ImpressionMetadata. */
data class ImpressionMetadataKey(
  override val parentKey: DataProviderKey,
  val impressionMetadataId: String,
) : ChildResourceKey {
  constructor(
    dataProviderId: String,
    impressionMetadataId: String,
  ) : this(DataProviderKey(dataProviderId), impressionMetadataId)

  val dataProviderId: String
    get() = parentKey.dataProviderId

  override fun toName(): String {
    return parser.assembleName(
      mapOf(
        IdVariable.DATA_PROVIDER to dataProviderId,
        IdVariable.IMPRESSION_METADATA to impressionMetadataId,
      )
    )
  }

  companion object FACTORY : ResourceKey.Factory<ImpressionMetadataKey> {
    const val PATTERN = "${DataProviderKey.PATTERN}/impressionMetadata/{impression_metadata}"
    private val parser = ResourceNameParser(PATTERN)

    override fun fromName(resourceName: String): ImpressionMetadataKey? {
      val idVars: Map<IdVariable, String> = parser.parseIdVars(resourceName) ?: return null
      return ImpressionMetadataKey(
        idVars.getValue(IdVariable.DATA_PROVIDER),
        idVars.getValue(IdVariable.IMPRESSION_METADATA),
      )
    }
  }
}
