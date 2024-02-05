/*
 * Copyright 2022 The Cross-Media Measurement Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package org.wfanet.measurement.eventdataprovider.privacybudgetmanagement.deploy.common.postgres

import java.sql.Connection
import java.sql.Statement
import org.junit.ClassRule
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.testcontainers.containers.PostgreSQLContainer
import org.wfanet.measurement.eventdataprovider.privacybudgetmanagement.PrivacyBudgetLedgerBackingStore
import org.wfanet.measurement.eventdataprovider.privacybudgetmanagement.deploy.common.postgres.testing.POSTGRES_LEDGER_SCHEMA_FILE
import org.wfanet.measurement.eventdataprovider.privacybudgetmanagement.testing.AbstractPrivacyBudgetLedgerStoreTest

@RunWith(JUnit4::class)
class PostgresBackingStoreTest : AbstractPrivacyBudgetLedgerStoreTest() {
  private fun createConnection(): Connection = postgresContainer.createConnection("")

  override fun recreateSchema() {
    createConnection().use { connection ->
      connection.createStatement().use { statement: Statement ->
        // clear database schema before re-creating
        statement.executeUpdate(
          """
          DROP TABLE IF EXISTS LedgerEntries CASCADE;
          DROP TABLE IF EXISTS PrivacyBucketAcdpCharges CASCADE;
          DROP TYPE IF EXISTS Gender CASCADE;
          DROP TYPE IF EXISTS AgeGroup CASCADE;
        """
            .trimIndent()
        )
        statement.executeUpdate(SCHEMA)
      }
    }
  }

  override fun createBackingStore(): PrivacyBudgetLedgerBackingStore {
    return PostgresBackingStore(::createConnection)
  }

  companion object {
    private const val POSTGRES_IMAGE_NAME = "postgres:15"

    private val SCHEMA by lazy { POSTGRES_LEDGER_SCHEMA_FILE.readText() }

    /**
     * PostgreSQL test container.
     *
     * TODO(@uakyol): Use [org.wfanet.measurement.common.db.r2dbc.postgres.testing.PostgresDatabaseProviderRule]
     *   instead of referencing TestContainers directly.
     */
    @get:ClassRule
    @JvmStatic
    val postgresContainer = PostgreSQLContainer<Nothing>(POSTGRES_IMAGE_NAME)
  }
}
