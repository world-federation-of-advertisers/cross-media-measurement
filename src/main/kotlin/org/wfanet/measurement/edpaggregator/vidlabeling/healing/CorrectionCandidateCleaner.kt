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

package org.wfanet.measurement.edpaggregator.vidlabeling.healing

import org.wfanet.measurement.api.v2alpha.DataProviderKey
import org.wfanet.measurement.internal.edpaggregator.RawImpressionUploadCorrectionCandidateServiceGrpcKt.RawImpressionUploadCorrectionCandidateServiceCoroutineStub
import org.wfanet.measurement.internal.edpaggregator.purgeExpiredRawImpressionUploadCorrectionCandidatesRequest

/** Removes expired terminal correction candidates. */
fun interface CorrectionCandidateCleaner {
  suspend fun purge(dataProvider: String)
}

/** Drains expired correction candidates through the internal API. */
class GrpcCorrectionCandidateCleaner(
  private val candidatesStub: RawImpressionUploadCorrectionCandidateServiceCoroutineStub,
  private val pageSize: Int = DEFAULT_PAGE_SIZE,
) : CorrectionCandidateCleaner {
  init {
    require(pageSize in 1..MAX_PAGE_SIZE) { "pageSize must be between 1 and $MAX_PAGE_SIZE" }
  }

  override suspend fun purge(dataProvider: String) {
    val dataProviderId =
      requireNotNull(DataProviderKey.fromName(dataProvider)) {
          "Invalid DataProvider: $dataProvider"
        }
        .dataProviderId
    do {
      val response =
        candidatesStub.purgeExpiredRawImpressionUploadCorrectionCandidates(
          purgeExpiredRawImpressionUploadCorrectionCandidatesRequest {
            dataProviderResourceId = dataProviderId
            pageSize = this@GrpcCorrectionCandidateCleaner.pageSize
          }
        )
      check(response.purgedCount in 0..pageSize) {
        "Purge response exceeded the requested page size"
      }
    } while (response.purgedCount == pageSize)
  }

  companion object {
    private const val DEFAULT_PAGE_SIZE = 100
    private const val MAX_PAGE_SIZE = 1000
  }
}
