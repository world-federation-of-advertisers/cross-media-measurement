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

package org.wfanet.measurement.duchy.deploy.postgres

import com.google.common.truth.Truth.assertThat
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.common.db.r2dbc.ResultRow
import org.wfanet.measurement.common.db.r2dbc.boundStatement
import org.wfanet.measurement.common.db.r2dbc.postgres.testing.EmbeddedPostgresDatabaseProvider
import org.wfanet.measurement.duchy.deploy.common.postgres.testing.Schemata.DUCHY_CHANGELOG_PATH

@RunWith(JUnit4::class)
class DuchySchemaTest {
  val databaseClient = EmbeddedPostgresDatabaseProvider(DUCHY_CHANGELOG_PATH).createNewDatabase()

  private fun translate(row: ResultRow): String = row["table_name"]

  @Test
  fun `all duchy tables are created`(): Unit = runBlocking {
    val query = "SELECT table_name FROM information_schema.tables;"
    val duchyTableNames =
      listOf(
        "computations",
        "requisitions",
        "computationstages",
        "computationblobreferences",
        "computationstageattempts",
        "computationstats",
        "heraldcontinuationtokens",
      )

    val resList = mutableListOf<String>()
    databaseClient
      .readTransaction()
      .executeQuery(boundStatement(query))
      .consume(::translate)
      .toList(resList)

    assertThat(resList).containsAtLeastElementsIn(duchyTableNames)
  }
}
