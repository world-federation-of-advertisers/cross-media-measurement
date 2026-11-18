/*
 * Copyright 2023 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.reporting.deploy.v2.gcloud.server

import com.google.common.truth.Truth.assertThat
import io.r2dbc.pool.ConnectionPool
import io.r2dbc.pool.ConnectionPoolConfiguration
import io.r2dbc.spi.ConnectionFactories
import io.r2dbc.spi.ConnectionFactoryOptions
import java.time.Duration
import java.util.UUID
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds
import kotlinx.coroutines.TimeoutCancellationException
import kotlinx.coroutines.flow.single
import kotlinx.coroutines.reactive.awaitFirstOrNull
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import org.junit.After
import org.junit.AfterClass
import org.junit.Before
import org.junit.BeforeClass
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.testcontainers.postgresql.PostgreSQLContainer
import org.wfanet.measurement.common.db.r2dbc.boundStatement
import org.wfanet.measurement.common.db.r2dbc.postgres.PostgresDatabaseClient

@RunWith(JUnit4::class)
class PooledPostgresDatabaseClientTest {
  private lateinit var pool: ConnectionPool
  private lateinit var client: PostgresDatabaseClient
  private lateinit var table: String

  @Before
  fun setUp(): Unit = runBlocking {
    val factory =
      ConnectionFactories.get(
        ConnectionFactoryOptions.builder()
          .option(ConnectionFactoryOptions.DRIVER, "postgresql")
          .option(ConnectionFactoryOptions.HOST, postgres.host)
          .option(
            ConnectionFactoryOptions.PORT,
            postgres.getMappedPort(PostgreSQLContainer.POSTGRESQL_PORT),
          )
          .option(ConnectionFactoryOptions.USER, postgres.username)
          .option(ConnectionFactoryOptions.PASSWORD, postgres.password)
          .option(ConnectionFactoryOptions.DATABASE, postgres.databaseName)
          .build()
      )
    pool =
      ConnectionPool(
        ConnectionPoolConfiguration.builder(factory)
          .initialSize(0)
          .maxSize(1)
          .maxAcquireTime(Duration.ofSeconds(5))
          .build()
      )
    client = PostgresDatabaseClient.fromConnectionFactory(pool)
    table = "pool_test_" + UUID.randomUUID().toString().replace("-", "")
    write("CREATE TABLE $table (value INTEGER PRIMARY KEY)")
  }

  @After fun tearDown(): Unit = runBlocking { pool.disposeLater().awaitFirstOrNull() }

  @Test
  fun `read and write transactions reuse a connection without retaining read only state`(): Unit =
    runBlocking {
      val firstPid = readInt("SELECT pg_backend_pid() AS value")
      write("INSERT INTO $table VALUES (1)")
      assertThat(readInt("SELECT COUNT(*)::INTEGER AS value FROM $table")).isEqualTo(1)
      assertThat(readInt("SELECT pg_backend_pid() AS value")).isEqualTo(firstPid)
    }

  @Test
  fun `closing an uncommitted write rolls it back before the next borrower`(): Unit = runBlocking {
    val transaction = client.readWriteTransaction()
    try {
      transaction.executeStatement(boundStatement("INSERT INTO $table VALUES (1)"))
    } finally {
      transaction.close()
    }
    assertThat(readInt("SELECT COUNT(*)::INTEGER AS value FROM $table")).isEqualTo(0)
    write("INSERT INTO $table VALUES (1)")
    assertThat(readInt("SELECT COUNT(*)::INTEGER AS value FROM $table")).isEqualTo(1)
  }

  @Test
  fun `a failed transaction does not poison the next borrower`(): Unit = runBlocking {
    write("INSERT INTO $table VALUES (1)")
    val transaction = client.readWriteTransaction()
    var failed = false
    try {
      transaction.executeStatement(boundStatement("INSERT INTO $table VALUES (1)"))
    } catch (_: io.r2dbc.spi.R2dbcException) {
      failed = true
    } finally {
      transaction.close()
    }
    assertThat(failed).isTrue()
    write("INSERT INTO $table VALUES (2)")
    assertThat(readInt("SELECT COUNT(*)::INTEGER AS value FROM $table")).isEqualTo(2)
  }

  @Test
  fun `cancelling a waiting borrower does not leak the next available connection`(): Unit =
    runBlocking {
      val held = client.readTransaction()
      var timedOut = false
      try {
        try {
          withTimeout(100.milliseconds) { client.singleUse() }
        } catch (_: TimeoutCancellationException) {
          timedOut = true
        }
      } finally {
        held.close()
      }
      assertThat(timedOut).isTrue()
      withTimeout(5.seconds) { write("INSERT INTO $table VALUES (1)") }
      assertThat(readInt("SELECT COUNT(*)::INTEGER AS value FROM $table")).isEqualTo(1)
    }

  private suspend fun write(sql: String) {
    val transaction = client.readWriteTransaction()
    try {
      transaction.executeStatement(boundStatement(sql))
      transaction.commit()
    } finally {
      transaction.close()
    }
  }

  private suspend fun readInt(sql: String): Int {
    return client
      .singleUse()
      .executeQuery(boundStatement(sql))
      .consume { row -> row.get<Int>("value") }
      .single()
  }

  companion object {
    private val postgres = PostgreSQLContainer("postgres:16")

    @JvmStatic
    @BeforeClass
    fun startPostgres() {
      postgres.start()
    }

    @JvmStatic
    @AfterClass
    fun stopPostgres() {
      postgres.stop()
    }
  }
}
