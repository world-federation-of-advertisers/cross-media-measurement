// Copyright 2022 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.duchy.deploy.gcloud.spanner.daemon.herald

import com.google.cloud.spanner.Mutation
import com.google.cloud.spanner.Statement
import com.google.cloud.spanner.Struct
import com.google.cloud.spanner.Value
import kotlinx.coroutines.flow.singleOrNull
import org.wfanet.measurement.duchy.daemon.herald.ContinuationTokenStore
import org.wfanet.measurement.duchy.deploy.gcloud.spanner.computation.SqlBasedQuery
import org.wfanet.measurement.gcloud.spanner.AsyncDatabaseClient

class SpannerContinuationTokenStore(
  val databaseClient: AsyncDatabaseClient,
  val duchyName: String
) : SqlBasedQuery<ContinuationTokenReaderResult>, ContinuationTokenStore {
  companion object {
    private const val parameterizedQueryString =
      """
      SELECT DuchyName, ContinuationToken
      FROM HeraldContinuationTokens
      WHERE DuchyName = @duchyName
      Limit 1
    """
  }
  override val sql: Statement =
    Statement.newBuilder(parameterizedQueryString).bind("duchyName").to(duchyName).build()

  override fun asResult(struct: Struct): ContinuationTokenReaderResult =
    ContinuationTokenReaderResult(
      duchyName = struct.getString("DuchyName"),
      continuationToken = struct.getString("ContinuationToken")
    )

  override suspend fun readContinuationToken(): String {
    return execute(databaseClient).singleOrNull()?.continuationToken ?: ""
  }

  override suspend fun updateContinuationToken(continuationToken: String) {
    val m = Mutation.newInsertOrUpdateBuilder("HeraldContinuationTokens")
    m.set("DuchyName").to(duchyName)
    m.set("ContinuationToken").to(continuationToken)
    m.set("CreationTime").to(Value.COMMIT_TIMESTAMP)
    val continuationTokenRow = m.build()

    databaseClient.write(continuationTokenRow)
  }
}

/** @see [SpannerContinuationTokenStore.asResult] . */
data class ContinuationTokenReaderResult(val duchyName: String, val continuationToken: String)
