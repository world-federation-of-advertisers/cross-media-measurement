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

import org.wfanet.measurement.common.ResourceNameParser
import org.wfanet.measurement.common.api.ChildResourceKey
import org.wfanet.measurement.common.api.ResourceKey

/** [ResourceKey] of a RankerJob. */
data class RankerJobKey(override val parentKey: RawImpressionUploadKey, val rankerJobId: String) :
  ChildResourceKey {
  constructor(
    dataProviderId: String,
    rawImpressionUploadId: String,
    rankerJobId: String,
  ) : this(RawImpressionUploadKey(dataProviderId, rawImpressionUploadId), rankerJobId)

  val dataProviderId: String
    get() = parentKey.dataProviderId

  val rawImpressionUploadId: String
    get() = parentKey.rawImpressionUploadId

  override fun toName(): String {
    return parser.assembleName(
      mapOf(
        IdVariable.DATA_PROVIDER to dataProviderId,
        IdVariable.RAW_IMPRESSION_UPLOAD to rawImpressionUploadId,
        IdVariable.RANKER_JOB to rankerJobId,
      )
    )
  }

  companion object FACTORY : ResourceKey.Factory<RankerJobKey> {
    const val PATTERN = "${RawImpressionUploadKey.PATTERN}/rankerJobs/{ranker_job}"
    private val parser = ResourceNameParser(PATTERN)

    override fun fromName(resourceName: String): RankerJobKey? {
      val idVars: Map<IdVariable, String> = parser.parseIdVars(resourceName) ?: return null
      return RankerJobKey(
        idVars.getValue(IdVariable.DATA_PROVIDER),
        idVars.getValue(IdVariable.RAW_IMPRESSION_UPLOAD),
        idVars.getValue(IdVariable.RANKER_JOB),
      )
    }
  }
}
