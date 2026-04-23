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

package org.wfanet.measurement.edpaggregator.benchmark

import com.google.cloud.ByteArray as SpannerByteArray
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.db.RankEntry

interface RankTableStorage : AutoCloseable {
  suspend fun initializePoolCounter(
    dataProvider: String,
    modelRelease: String,
    poolId: String,
    rankedSize: Long,
  )

  suspend fun lookupRanks(
    dataProvider: String,
    modelRelease: String,
    fingerprints: List<SpannerByteArray>,
  ): Map<SpannerByteArray, RankEntry>

  suspend fun lookupKnownFingerprints(
    dataProvider: String,
    modelRelease: String,
    fingerprints: List<SpannerByteArray>,
  ): Set<SpannerByteArray>

  suspend fun lookupRankValues(
    dataProvider: String,
    modelRelease: String,
    fingerprints: List<SpannerByteArray>,
  ): Map<SpannerByteArray, Long> {
    return lookupRanks(dataProvider, modelRelease, fingerprints)
      .mapValues { it.value.rankValue }
  }

  suspend fun lookupRankValuesStale(
    dataProvider: String,
    modelRelease: String,
    fingerprints: List<SpannerByteArray>,
    maxStalenessSeconds: Long = 15,
  ): Map<SpannerByteArray, Long> = lookupRankValues(dataProvider, modelRelease, fingerprints)

  suspend fun allocateRanks(
    dataProvider: String,
    modelRelease: String,
    poolId: String,
    count: Int,
  ): Long

  suspend fun writeRanks(entries: List<RankEntry>)

  override fun close()
}
