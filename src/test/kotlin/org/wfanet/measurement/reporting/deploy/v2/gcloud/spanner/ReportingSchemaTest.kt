/*
 * Copyright 2025 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.reporting.deploy.v2.gcloud.spanner

import com.google.cloud.spanner.Struct
import com.google.common.truth.Truth.assertThat
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.runBlocking
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.gcloud.spanner.statement
import org.wfanet.measurement.gcloud.spanner.testing.UsingSpannerEmulator
import org.wfanet.measurement.reporting.deploy.v2.gcloud.spanner.testing.Schemata

@RunWith(JUnit4::class)
class ReportingSchemaTest : UsingSpannerEmulator(Schemata.REPORTING_CHANGELOG_PATH) {
  @Test
  fun `database is created`() {
    val spannerDatabaseClient = spannerDatabase.databaseClient
    val transactionRunner = spannerDatabaseClient.readWriteTransaction()
    runBlocking {
      transactionRunner.run { txn ->
        val sql =
          """
          SELECT
            COUNT(*) as tableCount
          FROM
            information_schema.tables
          WHERE
            table_name IN ("MeasurementConsumers", "BasicReports")
          """
            .trimIndent()

        val row: Struct = txn.executeQuery(statement(sql)).first()

        assertThat(row.getLong("tableCount")).isEqualTo(2)
      }
    }
  }
}
