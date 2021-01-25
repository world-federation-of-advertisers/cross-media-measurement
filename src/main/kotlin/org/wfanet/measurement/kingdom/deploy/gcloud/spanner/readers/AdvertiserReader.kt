// Copyright 2020 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers

import com.google.cloud.spanner.Struct
import org.wfanet.measurement.internal.kingdom.Advertiser

class AdvertiserReader : SpannerReader<AdvertiserReader.Result>() {
  data class Result(
    val advertiser: Advertiser,
    val advertiserId: Long
  )

  override val baseSql: String =
    """
    SELECT
      Advertisers.AdvertiserId,
      Advertisers.ExternalAdvertiserId,
      Advertisers.AdvertiserDetails,
      Advertisers.AdvertiserDetailsJson
    FROM Advertisers
    """.trimIndent()

  override val externalIdColumn: String = "Advertisers.ExternalAdvertiserId"

  override suspend fun translate(struct: Struct): Result =
    Result(buildAdvertiser(struct), struct.getLong("AdvertiserId"))

  private fun buildAdvertiser(struct: Struct): Advertiser = Advertiser.newBuilder().apply {
    externalAdvertiserId = struct.getLong("ExternalAdvertiserId")
  }.build()
}
