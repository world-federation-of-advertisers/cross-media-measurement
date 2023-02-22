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
package org.wfanet.measurement.eventdataprovider.privacybudgetmanagement.deploy.postgres

import com.opentable.db.postgres.embedded.EmbeddedPostgres
import java.sql.Connection
import java.sql.Statement
import org.junit.AfterClass
import org.junit.BeforeClass
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.eventdataprovider.privacybudgetmanagement.PrivacyBudgetLedgerBackingStore
import org.wfanet.measurement.eventdataprovider.privacybudgetmanagement.testing.AbstractPrivacyBudgetLedgerStoreTest
import src.main.kotlin.org.wfanet.measurement.integration.deploy.postgres.POSTGRES_LEDGER_SCHEMA_FILE

@RunWith(JUnit4::class)
class PostgresBackingStoreTest : AbstractPrivacyBudgetLedgerStoreTest() {
  companion object {
    private const val TEST_DB = "junit_testing"
    private const val PG_USER_NAME = "postgres"

    @JvmStatic private val schema = POSTGRES_LEDGER_SCHEMA_FILE.readText()
    @JvmStatic private lateinit var embeddedPostgres: EmbeddedPostgres

    @JvmStatic
    @BeforeClass
    fun initDatabase() {
      embeddedPostgres = EmbeddedPostgres.start()
      embeddedPostgres.postgresDatabase.connection.use { connection ->
        connection.createStatement().use { statement: Statement ->
          statement.executeUpdate("CREATE DATABASE $TEST_DB")
        }
      }
    }

    @JvmStatic
    @AfterClass
    fun closeDataBase() {
      embeddedPostgres.close()
    }
  }

  private fun createConnection(): Connection =
    embeddedPostgres.getDatabase(PG_USER_NAME, TEST_DB).connection

  override fun recreateSchema() {
    createConnection().use { connection ->
      connection.createStatement().use { statement: Statement ->
        // clear database schema before re-creating
        statement.executeUpdate(
          """
          DROP TABLE IF EXISTS LedgerEntries CASCADE;
          DROP TABLE IF EXISTS PrivacyBucketCharges CASCADE;
          DROP TYPE IF EXISTS Gender CASCADE;
          DROP TYPE IF EXISTS AgeGroup CASCADE;
        """
            .trimIndent()
        )
        statement.executeUpdate(schema)
      }
    }
  }

  override fun createBackingStore(): PrivacyBudgetLedgerBackingStore {
    return PostgresBackingStore(::createConnection)
  }
}
