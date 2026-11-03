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

/** [ResourceKey] of a RawImpressionUploadCorrectionCandidate. */
data class RawImpressionUploadCorrectionCandidateKey(
  override val parentKey: DataProviderKey,
  val rawImpressionUploadCorrectionCandidateId: String,
) : ChildResourceKey {
  constructor(
    dataProviderId: String,
    rawImpressionUploadCorrectionCandidateId: String,
  ) : this(DataProviderKey(dataProviderId), rawImpressionUploadCorrectionCandidateId)

  val dataProviderId: String
    get() = parentKey.dataProviderId

  override fun toName(): String =
    parser.assembleName(
      mapOf(
        IdVariable.DATA_PROVIDER to dataProviderId,
        IdVariable.RAW_IMPRESSION_UPLOAD_CORRECTION_CANDIDATE to
          rawImpressionUploadCorrectionCandidateId,
      )
    )

  companion object FACTORY : ResourceKey.Factory<RawImpressionUploadCorrectionCandidateKey> {
    const val PATTERN =
      "${DataProviderKey.PATTERN}/rawImpressionUploadCorrectionCandidates/" +
        "{raw_impression_upload_correction_candidate}"
    private val parser = ResourceNameParser(PATTERN)

    override fun fromName(resourceName: String): RawImpressionUploadCorrectionCandidateKey? {
      val idVars = parser.parseIdVars(resourceName) ?: return null
      return RawImpressionUploadCorrectionCandidateKey(
        idVars.getValue(IdVariable.DATA_PROVIDER),
        idVars.getValue(IdVariable.RAW_IMPRESSION_UPLOAD_CORRECTION_CANDIDATE),
      )
    }
  }
}
