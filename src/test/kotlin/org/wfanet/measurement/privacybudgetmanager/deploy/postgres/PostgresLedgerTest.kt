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

package org.wfanet.measurement.privacybudgetmanager.deploy.postgres

import com.google.common.truth.Truth.assertThat
import java.sql.Connection
import java.sql.Statement
import java.sql.Timestamp
import java.time.Instant
import java.time.LocalDate
import kotlin.test.assertFailsWith
import kotlinx.coroutines.runBlocking
import org.junit.After
import org.junit.Before
import org.junit.ClassRule
import org.junit.FixMethodOrder
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.junit.runners.MethodSorters
import org.testcontainers.containers.PostgreSQLContainer
import org.wfanet.measurement.privacybudgetmanager.LedgerException
import org.wfanet.measurement.privacybudgetmanager.LedgerExceptionType
import org.wfanet.measurement.privacybudgetmanager.LedgerRowKey
import org.wfanet.measurement.privacybudgetmanager.Query
import org.wfanet.measurement.privacybudgetmanager.Slice
import org.wfanet.measurement.privacybudgetmanager.deploy.postgres.testing.POSTGRES_LEDGER_SCHEMA_FILE
import org.wfanet.measurement.privacybudgetmanager.query

@RunWith(JUnit4::class)
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
class PostgresLedgerTest {

  private fun createConnection(): Connection = postgresContainer.createConnection("")

  private lateinit var dbConnection: Connection

  @Before
  fun setupTest() {
    dbConnection = createConnection()
    recreateSchema()
  }

  @After
  fun teardownTest() {
    dbConnection.close()
    // Clean up any test-specific data if needed
  }

  private fun recreateSchema() {
    dbConnection.createStatement().use { statement: Statement ->
      // clear database schema before re-creating
      statement.executeUpdate(
        """
          DROP TYPE IF EXISTS ChargesTableState CASCADE;
          DROP TABLE IF EXISTS PrivacyChargesMetadata CASCADE;
          DROP TABLE IF EXISTS PrivacyCharges CASCADE;
          DROP TABLE IF EXISTS LedgerEntries CASCADE;
          """
          .trimIndent()
      )
      statement.executeUpdate(SCHEMA)
    }
  }

  private fun markLandscapeState(landscapeId: String, state: String) {
    dbConnection
      .prepareStatement(
        """
          INSERT INTO PrivacyChargesMetadata (PrivacyLandscapeName, State, CreateTime)
          VALUES (?, CAST(? AS ChargesTableState), ?)
          ON CONFLICT (PrivacyLandscapeName) DO UPDATE
          SET State = CAST(? AS ChargesTableState), DeleteTime = NULL
            """
          .trimIndent()
      )
      .use { preparedStatement ->
        preparedStatement.setString(1, landscapeId)
        preparedStatement.setString(2, state)
        preparedStatement.setTimestamp(3, Timestamp.from(Instant.now()))
        preparedStatement.setString(4, state) // For the UPDATE part

        preparedStatement.executeUpdate()
      }
  }

  @Test(timeout = 1500)
  fun `readQueries fails for queries that target different landscape`() = runBlocking {
    val ledger = PostgresLedger(::createConnection, ACTIVE_LANDSCAPE_ID)
    val tx: PostgresTransactionContext = ledger.startTransaction()
    val query: Query = query { privacyLandscapeIdentifier = "inactive-privacy-landscape" }
    val exception = assertFailsWith<LedgerException> { tx.readQueries(listOf(query)) }

    assertThat(exception.errorType).isEqualTo(LedgerExceptionType.INVALID_PRIVACY_LANDSCAPE_IDS)
    tx.commit()
  }

  @Test(timeout = 1500)
  fun `readQueries fails when active landscape is not in metadata table`() = runBlocking {
    val ledger = PostgresLedger(::createConnection, ACTIVE_LANDSCAPE_ID)
    val tx: PostgresTransactionContext = ledger.startTransaction()
    val query: Query = query { privacyLandscapeIdentifier = ACTIVE_LANDSCAPE_ID }
    val exception = assertFailsWith<LedgerException> { tx.readQueries(listOf(query)) }

    assertThat(exception.errorType).isEqualTo(LedgerExceptionType.TABLE_METADATA_DOESNT_EXIST)
    tx.commit()
  }

  @Test(timeout = 1500)
  fun `readQueries fails when active landscape is backfilling`() = runBlocking {
    markLandscapeState(ACTIVE_LANDSCAPE_ID, "BACKFILLING")
    val ledger = PostgresLedger(::createConnection, ACTIVE_LANDSCAPE_ID)
    val tx: PostgresTransactionContext = ledger.startTransaction()
    val query: Query = query { privacyLandscapeIdentifier = ACTIVE_LANDSCAPE_ID }
    val exception = assertFailsWith<LedgerException> { tx.readQueries(listOf(query)) }
    assertThat(exception.errorType).isEqualTo(LedgerExceptionType.TABLE_NOT_READY)
    tx.commit()
  }

  @Test(timeout = 1500)
  fun `readChargeRows fails when active landscape is not in metadata table`() = runBlocking {
    val ledger = PostgresLedger(::createConnection, ACTIVE_LANDSCAPE_ID)
    val tx: PostgresTransactionContext = ledger.startTransaction()
    val ledgerRowKey = LedgerRowKey("mcid", "egid", LocalDate.parse("2025-07-01"))
    val exception = assertFailsWith<LedgerException> { tx.readChargeRows(listOf(ledgerRowKey)) }

    assertThat(exception.errorType).isEqualTo(LedgerExceptionType.TABLE_METADATA_DOESNT_EXIST)
    tx.commit()
  }

  @Test(timeout = 1500)
  fun `readChargeRows fails when active landscape is backfilling`() = runBlocking {
    markLandscapeState(ACTIVE_LANDSCAPE_ID, "BACKFILLING")
    val ledger = PostgresLedger(::createConnection, ACTIVE_LANDSCAPE_ID)
    val tx: PostgresTransactionContext = ledger.startTransaction()
    val ledgerRowKey = LedgerRowKey("mcid", "egid", LocalDate.parse("2025-07-01"))
    val exception = assertFailsWith<LedgerException> { tx.readChargeRows(listOf(ledgerRowKey)) }
    assertThat(exception.errorType).isEqualTo(LedgerExceptionType.TABLE_NOT_READY)
    tx.commit()
  }

  @Test(timeout = 1500)
  fun `write fails for queries that target different landscape`() = runBlocking {
    val ledger = PostgresLedger(::createConnection, ACTIVE_LANDSCAPE_ID)
    val tx: PostgresTransactionContext = ledger.startTransaction()
    val query: Query = query { privacyLandscapeIdentifier = "inactive-privacy-landscape" }
    val exception = assertFailsWith<LedgerException> { tx.write(Slice(), listOf(query)) }

    assertThat(exception.errorType).isEqualTo(LedgerExceptionType.INVALID_PRIVACY_LANDSCAPE_IDS)
    tx.commit()
  }

  @Test(timeout = 1500)
  fun `write fails when active landscape is not in metadata table`() = runBlocking {
    val ledger = PostgresLedger(::createConnection, ACTIVE_LANDSCAPE_ID)
    val tx: PostgresTransactionContext = ledger.startTransaction()
    val query: Query = query { privacyLandscapeIdentifier = ACTIVE_LANDSCAPE_ID }
    val exception = assertFailsWith<LedgerException> { tx.write(Slice(), listOf(query)) }

    assertThat(exception.errorType).isEqualTo(LedgerExceptionType.TABLE_METADATA_DOESNT_EXIST)
    tx.commit()
  }

  @Test(timeout = 1500)
  fun `write fails when active landscape is backfilling`() = runBlocking {
    markLandscapeState(ACTIVE_LANDSCAPE_ID, "BACKFILLING")
    val ledger = PostgresLedger(::createConnection, ACTIVE_LANDSCAPE_ID)
    val tx: PostgresTransactionContext = ledger.startTransaction()
    val query: Query = query { privacyLandscapeIdentifier = ACTIVE_LANDSCAPE_ID }
    val exception = assertFailsWith<LedgerException> { tx.write(Slice(), listOf(query)) }
    assertThat(exception.errorType).isEqualTo(LedgerExceptionType.TABLE_NOT_READY)
    tx.commit()
    
  }

  companion object {
    private const val POSTGRES_IMAGE_NAME = "postgres:15"
    private const val ACTIVE_LANDSCAPE_ID = "active-privacy-landsapce"
    private val SCHEMA by lazy { POSTGRES_LEDGER_SCHEMA_FILE.readText() }

    @get:ClassRule
    @JvmStatic
    val postgresContainer = PostgreSQLContainer<Nothing>(POSTGRES_IMAGE_NAME)
  }
}
