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
import java.time.LocalDateTime
import java.time.ZoneId
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
import org.wfanet.measurement.common.toProtoTime
import org.wfanet.measurement.privacybudgetmanager.Charges
import org.wfanet.measurement.privacybudgetmanager.ChargesKt
import org.wfanet.measurement.privacybudgetmanager.LedgerException
import org.wfanet.measurement.privacybudgetmanager.LedgerExceptionType
import org.wfanet.measurement.privacybudgetmanager.LedgerRowKey
import org.wfanet.measurement.privacybudgetmanager.Query
import org.wfanet.measurement.privacybudgetmanager.Slice
import org.wfanet.measurement.privacybudgetmanager.acdpCharge
import org.wfanet.measurement.privacybudgetmanager.charges
import org.wfanet.measurement.privacybudgetmanager.copy
import org.wfanet.measurement.privacybudgetmanager.deploy.postgres.testing.POSTGRES_LEDGER_SCHEMA_FILE
import org.wfanet.measurement.privacybudgetmanager.query
import org.wfanet.measurement.privacybudgetmanager.queryIdentifiers

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

  @Test(timeout = 1500)
  fun `readQueries fails for queries that target different landscape`() = runBlocking {
    val ledger = PostgresLedger(::createConnection, ACTIVE_LANDSCAPE_ID)
    val query: Query = query { privacyLandscapeIdentifier = "inactive-privacy-landscape" }
    ledger.startTransaction().use { tx: PostgresTransactionContext ->
      val exception = assertFailsWith<LedgerException> { tx.readQueries(listOf(query)) }
      assertThat(exception.errorType).isEqualTo(LedgerExceptionType.INVALID_PRIVACY_LANDSCAPE_IDS)
    }
  }

  @Test(timeout = 1500)
  fun `readQueries fails when active landscape is not in metadata table`() = runBlocking {
    val ledger = PostgresLedger(::createConnection, ACTIVE_LANDSCAPE_ID)
    val query: Query = query { privacyLandscapeIdentifier = ACTIVE_LANDSCAPE_ID }
    ledger.startTransaction().use { tx: PostgresTransactionContext ->
      val exception = assertFailsWith<LedgerException> { tx.readQueries(listOf(query)) }
      assertThat(exception.errorType).isEqualTo(LedgerExceptionType.TABLE_METADATA_DOESNT_EXIST)
    }
  }

  @Test(timeout = 1500)
  fun `readQueries fails when active landscape is backfilling`() = runBlocking {
    markLandscapeState(dbConnection, ACTIVE_LANDSCAPE_ID, BACKFILLING_STATE)
    val ledger = PostgresLedger(::createConnection, ACTIVE_LANDSCAPE_ID)
    val query: Query = query { privacyLandscapeIdentifier = ACTIVE_LANDSCAPE_ID }
    ledger.startTransaction().use { tx: PostgresTransactionContext ->
      val exception = assertFailsWith<LedgerException> { tx.readQueries(listOf(query)) }
      assertThat(exception.errorType).isEqualTo(LedgerExceptionType.TABLE_NOT_READY)

      tx.commit()
    }
  }

  @Test(timeout = 1500)
  fun `readQueries returns empty list for empty table`() = runBlocking {
    markLandscapeState(dbConnection, ACTIVE_LANDSCAPE_ID, READY_STATE)
    val ledger = PostgresLedger(::createConnection, ACTIVE_LANDSCAPE_ID)
    val query: Query = query { privacyLandscapeIdentifier = ACTIVE_LANDSCAPE_ID }

    ledger.startTransaction().use { tx: PostgresTransactionContext ->
      val queriesRead = tx.readQueries(listOf(query))
      assertThat(queriesRead).isEqualTo(emptyList<Query>())

      tx.commit()
    }
  }

  @Test(timeout = 1500)
  fun `readQueries returns only the queries in the table correctly`() = runBlocking {
    markLandscapeState(dbConnection, ACTIVE_LANDSCAPE_ID, READY_STATE)
    val ledger = PostgresLedger(::createConnection, ACTIVE_LANDSCAPE_ID)
    // val tx: PostgresTransactionContext = ledger.startTransaction()
    val query1: Query = query {
      queryIdentifiers = queryIdentifiers {
        eventDataProviderId = "edp1"
        externalReferenceId = "ref1"
        measurementConsumerId = "mc1"
        isRefund = false
      }
      privacyLandscapeIdentifier = ACTIVE_LANDSCAPE_ID
    }

    val query2: Query = query {
      queryIdentifiers = queryIdentifiers {
        eventDataProviderId = "edp1"
        externalReferenceId = "ref2"
        measurementConsumerId = "mc1"
        isRefund = false
      }
      privacyLandscapeIdentifier = ACTIVE_LANDSCAPE_ID
    }

    insertQueries(dbConnection, listOf(query1))

    val expectedQuery =
      query1.copy {
        queryIdentifiers =
          queryIdentifiers.copy { createTime = CREATE_TIME.toInstant().toProtoTime() }
      }

    ledger.startTransaction().use { tx: PostgresTransactionContext ->
      val queriesRead = tx.readQueries(listOf(query1, query2))
      assertThat(queriesRead).isEqualTo(listOf(expectedQuery))
    }
  }

  @Test(timeout = 1500)
  fun `readChargeRows fails when active landscape is not in metadata table`() = runBlocking {
    val ledger = PostgresLedger(::createConnection, ACTIVE_LANDSCAPE_ID)
    val ledgerRowKey = LedgerRowKey("edpid", "mcid", "egid", LocalDate.parse("2025-07-01"))

    ledger.startTransaction().use { tx: PostgresTransactionContext ->
      val exception = assertFailsWith<LedgerException> { tx.readChargeRows(listOf(ledgerRowKey)) }
      assertThat(exception.errorType).isEqualTo(LedgerExceptionType.TABLE_METADATA_DOESNT_EXIST)
    }
  }

  @Test(timeout = 1500)
  fun `readChargeRows fails when active landscape is backfilling`() = runBlocking {
    markLandscapeState(dbConnection, ACTIVE_LANDSCAPE_ID, BACKFILLING_STATE)
    val ledger = PostgresLedger(::createConnection, ACTIVE_LANDSCAPE_ID)
    val ledgerRowKey = LedgerRowKey("edpid", "mcid", "egid", LocalDate.parse("2025-07-01"))

    ledger.startTransaction().use { tx: PostgresTransactionContext ->
      val exception = assertFailsWith<LedgerException> { tx.readChargeRows(listOf(ledgerRowKey)) }
      assertThat(exception.errorType).isEqualTo(LedgerExceptionType.TABLE_NOT_READY)
    }
  }

  @Test(timeout = 1500)
  fun `readChargeRows succeeds in reading correct rows and returns correct slice`() = runBlocking {
    markLandscapeState(dbConnection, ACTIVE_LANDSCAPE_ID, READY_STATE)
    val ledger = PostgresLedger(::createConnection, ACTIVE_LANDSCAPE_ID)
    val charges1 = charges {
      populationIndexToCharges[456] =
        ChargesKt.intervalCharges {
          vidIntervalIndexToCharges[5] = acdpCharge {
            rho = 12.13f
            theta = 14.15f
          }
        }
    }
    val charges2 = charges {
      populationIndexToCharges[32] =
        ChargesKt.intervalCharges {
          vidIntervalIndexToCharges[1] = acdpCharge {
            rho = 12.13f
            theta = 14.15f
          }
        }
    }
    val ledgerRowKey1 = LedgerRowKey("edpid", "mcid", "egid", LocalDate.parse("2025-07-01"))
    val ledgerRowKey2 =
      LedgerRowKey("edpid", "othermcid", "otheregid", LocalDate.parse("2025-07-01"))

    insertPrivacyCharges(dbConnection, ledgerRowKey1, charges1)
    insertPrivacyCharges(dbConnection, ledgerRowKey2, charges2)

    ledger.startTransaction().use { tx: PostgresTransactionContext ->
      val slice = tx.readChargeRows(listOf(ledgerRowKey1, ledgerRowKey2))
      assertThat(slice.getLedgerRowKeys().toSet()).isEqualTo(setOf(ledgerRowKey1, ledgerRowKey2))
      assertThat(slice.get(ledgerRowKey1)).isEqualTo(charges1)
      assertThat(slice.get(ledgerRowKey2)).isEqualTo(charges2)
    }
  }

  @Test(timeout = 1500)
  fun `write fails for queries that target different landscape`() = runBlocking {
    val ledger = PostgresLedger(::createConnection, ACTIVE_LANDSCAPE_ID)
    val query: Query = query { privacyLandscapeIdentifier = "inactive-privacy-landscape" }
    ledger.startTransaction().use { tx: PostgresTransactionContext ->
      val exception = assertFailsWith<LedgerException> { tx.write(Slice(), listOf(query)) }
      assertThat(exception.errorType).isEqualTo(LedgerExceptionType.INVALID_PRIVACY_LANDSCAPE_IDS)
    }
  }

  @Test(timeout = 1500)
  fun `write fails when active landscape is not in metadata table`() = runBlocking {
    val ledger = PostgresLedger(::createConnection, ACTIVE_LANDSCAPE_ID)
    val query: Query = query { privacyLandscapeIdentifier = ACTIVE_LANDSCAPE_ID }
    ledger.startTransaction().use { tx: PostgresTransactionContext ->
      val exception = assertFailsWith<LedgerException> { tx.write(Slice(), listOf(query)) }
      assertThat(exception.errorType).isEqualTo(LedgerExceptionType.TABLE_METADATA_DOESNT_EXIST)
    }
  }

  @Test(timeout = 1500)
  fun `write fails when active landscape is backfilling`() = runBlocking {
    markLandscapeState(dbConnection, ACTIVE_LANDSCAPE_ID, BACKFILLING_STATE)
    val ledger = PostgresLedger(::createConnection, ACTIVE_LANDSCAPE_ID)
    val query: Query = query { privacyLandscapeIdentifier = ACTIVE_LANDSCAPE_ID }
    ledger.startTransaction().use { tx: PostgresTransactionContext ->
      val exception = assertFailsWith<LedgerException> { tx.write(Slice(), listOf(query)) }
      assertThat(exception.errorType).isEqualTo(LedgerExceptionType.TABLE_NOT_READY)
    }
  }

  @Test(timeout = 1500)
  fun `write succeeds with multipls queries and slice batched`() = runBlocking {
    markLandscapeState(dbConnection, ACTIVE_LANDSCAPE_ID, READY_STATE)
    val ledger = PostgresLedger(::createConnection, ACTIVE_LANDSCAPE_ID)
    val query1: Query = query {
      queryIdentifiers = queryIdentifiers {
        eventDataProviderId = "edp1"
        externalReferenceId = "ref1"
        measurementConsumerId = "mc1"
        isRefund = false
      }
      privacyLandscapeIdentifier = ACTIVE_LANDSCAPE_ID
    }

    val query2: Query = query {
      queryIdentifiers = queryIdentifiers {
        eventDataProviderId = "edp1"
        externalReferenceId = "ref2"
        measurementConsumerId = "mc1"
        isRefund = false
      }
      privacyLandscapeIdentifier = ACTIVE_LANDSCAPE_ID
    }

    val query3: Query = query {
      queryIdentifiers = queryIdentifiers {
        eventDataProviderId = "edp2"
        externalReferenceId = "ref1"
        measurementConsumerId = "mc3"
        isRefund = false
      }
      privacyLandscapeIdentifier = ACTIVE_LANDSCAPE_ID
    }

    val charges1 = charges {
      populationIndexToCharges[456] =
        ChargesKt.intervalCharges {
          vidIntervalIndexToCharges[5] = acdpCharge {
            rho = 12.13f
            theta = 14.15f
          }
        }
    }
    val charges2 = charges {
      populationIndexToCharges[32] =
        ChargesKt.intervalCharges {
          vidIntervalIndexToCharges[1] = acdpCharge {
            rho = 12.13f
            theta = 14.15f
          }
        }
    }
    val ledgerRowKey1 = LedgerRowKey("edpid", "mcid", "egid", LocalDate.parse("2025-07-01"))
    val ledgerRowKey2 =
      LedgerRowKey("edpid", "othermcid", "otheregid", LocalDate.parse("2025-07-01"))
    val ledgerRowKey3 =
      LedgerRowKey("edpid3", "othermcid", "otheregid", LocalDate.parse("2025-07-01"))

    val slice = Slice()
    slice.merge(ledgerRowKey1, charges1)
    slice.merge(ledgerRowKey2, charges2)
    slice.merge(ledgerRowKey3, charges2)
    var returnedQueries = emptyList<Query>()
    ledger.startTransaction().use { tx: PostgresTransactionContext ->
      returnedQueries = tx.write(slice, listOf(query1, query2, query3), 2)
      tx.commit()
    }

    assertThat(returnedQueries).hasSize(3)
    assertThat(returnedQueries[0].queryIdentifiers.createTime).isNotNull()
    assertThat(returnedQueries[1].queryIdentifiers.createTime).isNotNull()
    assertThat(returnedQueries[2].queryIdentifiers.createTime).isNotNull()

    assertThat(readPrivacyCharges(dbConnection, ledgerRowKey1)).isEqualTo(charges1)
    assertThat(readPrivacyCharges(dbConnection, ledgerRowKey2)).isEqualTo(charges2)
    assertThat(readPrivacyCharges(dbConnection, ledgerRowKey3)).isEqualTo(charges2)
  }

  private companion object {
    const val POSTGRES_IMAGE_NAME = "postgres:15"
    const val ACTIVE_LANDSCAPE_ID = "active-privacy-landsapce"
    const val READY_STATE = "READY"
    const val BACKFILLING_STATE = "BACKFILLING"
    val CREATE_TIME =
      Timestamp.valueOf(
        LocalDateTime.of(2025, 7, 1, 0, 0, 0).atZone(ZoneId.systemDefault()).toLocalDateTime()
      )
    val SCHEMA by lazy { POSTGRES_LEDGER_SCHEMA_FILE.readText() }

    fun markLandscapeState(connection: Connection, landscapeId: String, state: String) {
      connection
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
          preparedStatement.setString(4, state)
          preparedStatement.executeUpdate()
        }
    }

    fun insertPrivacyCharges(connection: Connection, ledgerRowKey: LedgerRowKey, charges: Charges) {
      val sql =
        """
          INSERT INTO PrivacyCharges (EdpId, MeasurementConsumerId, EventGroupReferenceId, Date, Charges)
          VALUES (?, ?, ?, ?, ?)
      """

      connection.prepareStatement(sql).use { preparedStatement ->
        preparedStatement.setString(1, ledgerRowKey.edpId)
        preparedStatement.setString(2, ledgerRowKey.measurementConsumerId)
        preparedStatement.setString(3, ledgerRowKey.eventGroupReferenceId)
        preparedStatement.setDate(4, java.sql.Date.valueOf(ledgerRowKey.date))
        preparedStatement.setBytes(5, charges.toByteString().toByteArray())
        preparedStatement.executeUpdate()
      }
    }

    fun readPrivacyCharges(connection: Connection, ledgerRowKey: LedgerRowKey): Charges {
      val sql =
        """
          Select Charges 
          from PrivacyCharges
          WHERE EdpId = ?
          AND MeasurementConsumerId = ?
          AND EventGroupReferenceId = ?
          AND Date = ?
      """
      var charges: Charges = charges {}
      connection.prepareStatement(sql).use { preparedStatement ->
        preparedStatement.setString(1, ledgerRowKey.edpId)
        preparedStatement.setString(2, ledgerRowKey.measurementConsumerId)
        preparedStatement.setString(3, ledgerRowKey.eventGroupReferenceId)
        preparedStatement.setDate(4, java.sql.Date.valueOf(ledgerRowKey.date))

        preparedStatement.executeQuery().use { resultSet ->
          if (resultSet.next()) {
            val chargesBytes = resultSet.getBytes("Charges")

            charges =
              if (chargesBytes != null && chargesBytes.isNotEmpty()) {
                Charges.parseFrom(chargesBytes)
              } else {
                charges {}
              }
          }
        }
      }
      return charges
    }

    fun insertQueries(connection: Connection, queries: List<Query>) {
      val insertStatement =
        """
            INSERT INTO LedgerEntries (EdpId, MeasurementConsumerId, ExternalReferenceId, IsRefund, CreateTime)
            VALUES (?, ?, ?, ?, ?)
        """
      connection.prepareStatement(insertStatement).use { preparedStatement ->
        for (query in queries) {
          val identifiers = query.queryIdentifiers
          preparedStatement.setString(1, identifiers.eventDataProviderId)
          preparedStatement.setString(2, identifiers.measurementConsumerId)
          preparedStatement.setString(3, identifiers.externalReferenceId)
          preparedStatement.setBoolean(4, identifiers.isRefund)
          preparedStatement.setTimestamp(5, CREATE_TIME)
          preparedStatement.addBatch()
        }
        preparedStatement.executeBatch()
      }
    }

    @get:ClassRule
    @JvmStatic
    val postgresContainer =
      PostgreSQLContainer<Nothing>(POSTGRES_IMAGE_NAME).apply {
        withLogConsumer { outputFrame ->
          println("PostgreSQL Container Log: ${outputFrame.getUtf8String()}")
        }
      }
  }
}
