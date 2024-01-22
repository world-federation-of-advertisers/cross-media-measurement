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

package org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers

import com.google.cloud.Timestamp
import com.google.cloud.spanner.Struct
import com.google.protobuf.ByteString
import kotlinx.coroutines.flow.singleOrNull
import org.wfanet.measurement.common.identity.InternalId
import org.wfanet.measurement.gcloud.common.toGcloudByteArray
import org.wfanet.measurement.gcloud.spanner.AsyncDatabaseClient
import org.wfanet.measurement.gcloud.spanner.appendClause
import org.wfanet.measurement.gcloud.spanner.bind

class MeasurementConsumerCreationTokenReader :
  SpannerReader<MeasurementConsumerCreationTokenReader.Result>() {
  data class Result(val createTime: Timestamp, val measurementConsumerCreationTokenId: InternalId)

  override val baseSql: String =
    """
    SELECT
      MeasurementConsumerCreationTokens.MeasurementConsumerCreationTokenId,
      MeasurementConsumerCreationTokens.MeasurementConsumerCreationTokenHash,
      MeasurementConsumerCreationTokens.CreateTime,
    FROM MeasurementConsumerCreationTokens
    """
      .trimIndent()

  override suspend fun translate(struct: Struct): Result =
    Result(
      struct.getTimestamp("CreateTime"),
      InternalId(struct.getLong("MeasurementConsumerCreationTokenId")),
    )

  suspend fun readByMeasurementConsumerCreationTokenHash(
    readContext: AsyncDatabaseClient.ReadContext,
    measurementConsumerCreationTokenHash: ByteString,
  ): Result? {
    return fillStatementBuilder {
        appendClause(
          """
          WHERE MeasurementConsumerCreationTokens.MeasurementConsumerCreationTokenHash
            = @measurementConsumerCreationTokenHash
          """
            .trimIndent()
        )
        bind(
          "measurementConsumerCreationTokenHash" to
            measurementConsumerCreationTokenHash.toGcloudByteArray()
        )
      }
      .execute(readContext)
      .singleOrNull()
  }
}
