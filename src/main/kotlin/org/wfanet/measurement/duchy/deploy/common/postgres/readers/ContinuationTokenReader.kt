// Copyright 2023 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.duchy.deploy.common.postgres.readers

import kotlinx.coroutines.flow.firstOrNull
import org.wfanet.measurement.common.db.r2dbc.ReadContext
import org.wfanet.measurement.common.db.r2dbc.ResultRow
import org.wfanet.measurement.common.db.r2dbc.boundStatement

/** Performs read operations on HeraldContinuationTokens tables */
class ContinuationTokenReader {
  companion object {
    private const val parameterizedQueryString =
      """
      SELECT ContinuationToken
      FROM HeraldContinuationTokens
      Limit 1
    """
  }

  data class Result(val continuationToken: String)

  fun translate(row: ResultRow): Result = Result(row["ContinuationToken"])

  /**
   * Reads a ContinuationToken from the HeraldContinuationTokens table.
   *
   * @return [Result] when a ContinuationToken is found, or null.
   */
  suspend fun getContinuationToken(readContext: ReadContext): Result? {
    val statement = boundStatement(parameterizedQueryString)
    return readContext.executeQuery(statement).consume(::translate).firstOrNull()
  }
}
