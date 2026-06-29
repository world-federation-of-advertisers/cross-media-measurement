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

/** [ResourceKey] of a RawImpressionUpload. */
data class RawImpressionUploadKey(
  override val parentKey: DataProviderKey,
  val rawImpressionUploadId: String,
) : ChildResourceKey {
  constructor(
    dataProviderId: String,
    rawImpressionUploadId: String,
  ) : this(DataProviderKey(dataProviderId), rawImpressionUploadId)

  val dataProviderId: String
    get() = parentKey.dataProviderId

  override fun toName(): String {
    return parser.assembleName(
      mapOf(
        IdVariable.DATA_PROVIDER to dataProviderId,
        IdVariable.RAW_IMPRESSION_UPLOAD to rawImpressionUploadId,
      )
    )
  }

  companion object FACTORY : ResourceKey.Factory<RawImpressionUploadKey> {
    const val PATTERN = "${DataProviderKey.PATTERN}/rawImpressionUploads/{raw_impression_upload}"
    private val parser = ResourceNameParser(PATTERN)

    override fun fromName(resourceName: String): RawImpressionUploadKey? {
      val idVars: Map<IdVariable, String> = parser.parseIdVars(resourceName) ?: return null
      return RawImpressionUploadKey(
        idVars.getValue(IdVariable.DATA_PROVIDER),
        idVars.getValue(IdVariable.RAW_IMPRESSION_UPLOAD),
      )
    }
  }
}
