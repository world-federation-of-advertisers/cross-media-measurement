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

package org.wfanet.measurement.integration.postgres

import com.opentable.db.postgres.junit.EmbeddedPostgresRules
import com.opentable.db.postgres.junit.SingleInstancePostgresRule
import java.sql.Connection
import java.sql.ResultSet
import java.sql.Statement
import kotlin.test.assertEquals
import org.junit.Rule
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import src.main.kotlin.org.wfanet.measurement.integration.deploy.postgres.POSTGRES_LEDGER_SCHEMA_FILE

@RunWith(JUnit4::class)
class PrivacyBudgetPostgresSchemaTest {
  val schema = POSTGRES_LEDGER_SCHEMA_FILE.readText()

  @get:Rule val pg: SingleInstancePostgresRule = EmbeddedPostgresRules.singleInstance()

  @Test
  fun `privacy budget ledger sql file is valid for postgres`() {
    val connection: Connection = pg.embeddedPostgres.postgresDatabase.connection
    val statement: Statement = connection.createStatement()
    statement.execute(schema)
  }

  @Test
  fun `privacy budget balance can be written and read`() {
    val connection: Connection = pg.embeddedPostgres.postgresDatabase.connection
    val statement = connection.createStatement()
    val insertSql =
      """
      INSERT INTO PrivacyBucketCharges (
        MeasurementConsumerId,
        Date,
        AgeGroup,
        Gender,
        VidStart,
        Delta,
        Epsilon,
        RepetitionCount
      ) VALUES (
        'MC1',
        '2022-01-01',
        '18_34',
        'F',
        100,
        0.1,
        0.01,
        10
      );
      """
    val selectSql = """
      SELECT Gender, Delta from PrivacyBucketCharges
      """
    statement.execute(schema)
    statement.execute(insertSql)
    val result: ResultSet = statement.executeQuery(selectSql)
    result.next()
    assertEquals("F", result.getString("gender"))
    assertEquals(0.1f, result.getFloat("delta"))
  }

  @Test
  fun `privacy budget ledger can be written and read`() {
    val connection: Connection = pg.embeddedPostgres.postgresDatabase.connection
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
    statement.execute(schema)
    statement.execute(insertSql)
    val result: ResultSet = statement.executeQuery(selectSql)
    result.next()
    assertEquals("MC1", result.getString("measurementConsumerId"))
    assertEquals("Ref1", result.getString("referenceId"))
    assertEquals(false, result.getBoolean("isRefund"))
  }
}
