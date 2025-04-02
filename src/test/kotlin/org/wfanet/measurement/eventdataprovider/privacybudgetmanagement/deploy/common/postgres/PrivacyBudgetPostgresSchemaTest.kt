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

package org.wfanet.measurement.eventdataprovider.privacybudgetmanagement.deploy.common.postgres

import java.sql.ResultSet
import java.sql.Statement
import kotlin.test.assertEquals
import org.junit.Before
import org.junit.ClassRule
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.testcontainers.containers.PostgreSQLContainer
import org.wfanet.measurement.eventdataprovider.privacybudgetmanagement.deploy.common.postgres.testing.POSTGRES_LEDGER_SCHEMA_FILE

@RunWith(JUnit4::class)
class PrivacyBudgetPostgresSchemaTest {
  @Before
  fun resetDatabase() {
    createConnection().use { connection ->
      connection.createStatement().use {
        it.execute("DROP SCHEMA public CASCADE; CREATE SCHEMA public;")
      }
    }
  }

  @Test
  fun `privacy budget ledger sql file is valid for postgres`() {
    createConnection().use { connection ->
      val statement: Statement = connection.createStatement()
      statement.execute(SCHEMA)
    }
  }

  @Test
  fun `privacy budget acdp balance can be written and read`() {
    createConnection().use { connection ->
      val statement = connection.createStatement()
      val insertSql =
        """
      INSERT INTO PrivacyBucketAcdpCharges (
        MeasurementConsumerId,
        Date,
        AgeGroup,
        Gender,
        VidStart,
        Rho,
        Theta
      ) VALUES (
        'MC1',
        '2022-01-01',
        '18_34',
        'F',
        100,
        0.1,
        0.01
      );
      """
      val selectSql =
        """
      SELECT Gender, Rho from PrivacyBucketAcdpCharges
      """
      statement.execute(SCHEMA)
      statement.execute(insertSql)
      val result: ResultSet = statement.executeQuery(selectSql)
      result.next()
      assertEquals("F", result.getString("gender"))
      assertEquals(0.1f, result.getFloat("rho"))
    }
  }

  @Test
  fun `privacy budget ledger can be written and read`() {
    createConnection().use { connection ->
      val statement = connection.createStatement()
      val insertSql =
        """
      INSERT INTO LedgerEntries (
        MeasurementConsumerId,
        ReferenceId,
        IsRefund,
        CreateTime
      ) VALUES (
        'MC1',
        'Ref1',
        false,
        NOW()
      );
      """
      val selectSql =
        """
      SELECT MeasurementConsumerId, ReferenceId, IsRefund from LedgerEntries
      """
      statement.execute(SCHEMA)
      statement.execute(insertSql)
      val result: ResultSet = statement.executeQuery(selectSql)
      result.next()
      assertEquals("MC1", result.getString("measurementConsumerId"))
      assertEquals("Ref1", result.getString("referenceId"))
      assertEquals(false, result.getBoolean("isRefund"))
    }
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

    private fun createConnection() = postgresContainer.createConnection("")
  }
}
