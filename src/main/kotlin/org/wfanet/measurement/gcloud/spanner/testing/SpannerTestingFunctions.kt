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

package org.wfanet.measurement.gcloud.spanner.testing

import com.google.cloud.spanner.Statement
import com.google.cloud.spanner.Struct
import com.google.common.truth.Truth.assertThat
import com.google.common.truth.Truth.assertWithMessage
import kotlinx.coroutines.flow.toList
import org.wfanet.measurement.gcloud.spanner.AsyncDatabaseClient

/**
 * Returns the results of a spanner query as a list of [Struct].
 */
suspend fun queryForResults(dbClient: AsyncDatabaseClient, sqlQuery: String): List<Struct> {
  return dbClient.singleUse().executeQuery(Statement.of(sqlQuery)).toList()
}

/** Asserts that a query returns the expected results. */
suspend fun assertQueryReturns(
  dbClient: AsyncDatabaseClient,
  sqlQuery: String,
  expected: Iterable<Struct>
) {
  val expectedColumns = expected.map { it.type.toString() }.toSet()
  require(expectedColumns.size == 1) {
    "All 'expected: Struct' object should have the same column headings, but was " +
      expectedColumns.joinToString("\n")
  }

  val results: List<Struct> = queryForResults(dbClient, sqlQuery)
  val resultsColumns = results.map { it.type.toString() }.toSet()
  assertWithMessage("All query results should have the same column headings").that(resultsColumns)
    .hasSize(1)
  assertThat(resultsColumns).isEqualTo(expectedColumns)
  assertWithMessage(
    """
    Query did not return expected results:
    '$sqlQuery'

    Columns:
    $expectedColumns
    """.trimIndent()
  ).that(results).containsExactlyElementsIn(expected).inOrder()
}

/** Asserts that a query returns the expected results. */
suspend fun assertQueryReturns(
  dbClient: AsyncDatabaseClient,
  sqlQuery: String,
  vararg expected: Struct
) {
  assertQueryReturns(dbClient, sqlQuery, expected.asIterable())
}
