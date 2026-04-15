// Copyright 2026 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.edpaggregator.service

import org.wfanet.measurement.api.v2alpha.DataProviderKey
import org.wfanet.measurement.common.ResourceNameParser
import org.wfanet.measurement.common.api.ChildResourceKey
import org.wfanet.measurement.common.api.ResourceKey

/** [ResourceKey] of a RawImpressionMetadataBatch. */
data class RawImpressionMetadataBatchKey(
  override val parentKey: DataProviderKey,
  val rawImpressionMetadataBatchId: String,
) : ChildResourceKey {
  constructor(
    dataProviderId: String,
    rawImpressionMetadataBatchId: String,
  ) : this(DataProviderKey(dataProviderId), rawImpressionMetadataBatchId)

  val dataProviderId: String
    get() = parentKey.dataProviderId

  override fun toName(): String {
    return parser.assembleName(
      mapOf(
        IdVariable.DATA_PROVIDER to dataProviderId,
        IdVariable.RAW_IMPRESSION_METADATA_BATCH to rawImpressionMetadataBatchId,
      )
    )
  }

  companion object FACTORY : ResourceKey.Factory<RawImpressionMetadataBatchKey> {
    const val PATTERN =
      "${DataProviderKey.PATTERN}/rawImpressionMetadataBatches/{raw_impression_metadata_batch}"
    private val parser = ResourceNameParser(PATTERN)

    override fun fromName(resourceName: String): RawImpressionMetadataBatchKey? {
      val idVars: Map<IdVariable, String> = parser.parseIdVars(resourceName) ?: return null
      return RawImpressionMetadataBatchKey(
        idVars.getValue(IdVariable.DATA_PROVIDER),
        idVars.getValue(IdVariable.RAW_IMPRESSION_METADATA_BATCH),
      )
    }
  }
}

/** [ResourceKey] of a RawImpressionMetadataBatchFile. */
data class RawImpressionMetadataBatchFileKey(
  override val parentKey: RawImpressionMetadataBatchKey,
  val fileId: String,
) : ChildResourceKey {
  constructor(
    dataProviderId: String,
    rawImpressionMetadataBatchId: String,
    fileId: String,
  ) : this(RawImpressionMetadataBatchKey(dataProviderId, rawImpressionMetadataBatchId), fileId)

  val dataProviderId: String
    get() = parentKey.dataProviderId

  val rawImpressionMetadataBatchId: String
    get() = parentKey.rawImpressionMetadataBatchId

  override fun toName(): String {
    return parser.assembleName(
      mapOf(
        IdVariable.DATA_PROVIDER to dataProviderId,
        IdVariable.RAW_IMPRESSION_METADATA_BATCH to rawImpressionMetadataBatchId,
        IdVariable.FILE to fileId,
      )
    )
  }

  companion object FACTORY : ResourceKey.Factory<RawImpressionMetadataBatchFileKey> {
    const val PATTERN = "${RawImpressionMetadataBatchKey.PATTERN}/files/{file}"
    private val parser = ResourceNameParser(PATTERN)

    override fun fromName(resourceName: String): RawImpressionMetadataBatchFileKey? {
      val idVars: Map<IdVariable, String> = parser.parseIdVars(resourceName) ?: return null
      return RawImpressionMetadataBatchFileKey(
        idVars.getValue(IdVariable.DATA_PROVIDER),
        idVars.getValue(IdVariable.RAW_IMPRESSION_METADATA_BATCH),
        idVars.getValue(IdVariable.FILE),
      )
    }
  }
}
