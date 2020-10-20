// Copyright 2020 The Measurement System Authors
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

package org.wfanet.measurement.duchy.deploy.gcloud.spanner.computation

import com.google.cloud.spanner.Statement
import com.google.cloud.spanner.Struct
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.map
import org.wfanet.measurement.gcloud.spanner.AsyncDatabaseClient

/**
 * Wrapper around an SQL based query to the Spanner database that abstracts away spanner
 * result sets and spanner structs.
 */
interface SqlBasedQuery<out Result> {
  val sql: Statement

  /** Transmogrify a single resulting row [Struct] in the [Result] type. */
  fun asResult(struct: Struct): Result

  /**
   *  Runs this query using a singleUse query in the database client, returning a [Sequence]
   *  of the [Result]s.
   */
  fun execute(databaseClient: AsyncDatabaseClient): Flow<Result> =
    execute(databaseClient.singleUse())

  /** Runs this query using a read context, returning a [Flow] of the [Result]s. */
  fun execute(readContext: AsyncDatabaseClient.ReadContext): Flow<Result> =
    readContext.executeQuery(sql).map { asResult(it) }
}
