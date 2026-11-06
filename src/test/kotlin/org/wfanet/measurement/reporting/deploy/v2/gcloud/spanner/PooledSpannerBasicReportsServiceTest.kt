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

import io.r2dbc.pool.ConnectionPool
import io.r2dbc.pool.ConnectionPoolConfiguration
import io.r2dbc.spi.ConnectionFactories
import io.r2dbc.spi.ConnectionFactoryOptions
import java.time.Duration
import java.util.UUID
import kotlinx.coroutines.reactive.awaitFirstOrNull
import kotlinx.coroutines.runBlocking
import org.junit.After
import org.junit.AfterClass
import org.junit.BeforeClass
import org.junit.ClassRule
import org.junit.Rule
import org.testcontainers.postgresql.PostgreSQLContainer
import org.wfanet.measurement.common.db.liquibase.Liquibase
import org.wfanet.measurement.common.db.r2dbc.postgres.PostgresDatabaseClient
import org.wfanet.measurement.common.identity.IdGenerator
import org.wfanet.measurement.gcloud.spanner.testing.SpannerEmulatorDatabaseRule
import org.wfanet.measurement.gcloud.spanner.testing.SpannerEmulatorRule
import org.wfanet.measurement.reporting.deploy.v2.gcloud.spanner.testing.Schemata
import org.wfanet.measurement.reporting.deploy.v2.postgres.PostgresMeasurementConsumersService
import org.wfanet.measurement.reporting.deploy.v2.postgres.PostgresReportingSetsService
import org.wfanet.measurement.reporting.deploy.v2.postgres.testing.Schemata as PostgresSchemata
import org.wfanet.measurement.reporting.service.internal.testing.v2.BasicReportsServiceTest

class PooledSpannerBasicReportsServiceTest : BasicReportsServiceTest<SpannerBasicReportsService>() {
  @get:Rule
  val spannerDatabase =
    SpannerEmulatorDatabaseRule(spannerEmulator, Schemata.REPORTING_CHANGELOG_PATH)

  private val pools = mutableListOf<ConnectionPool>()

  override fun newServices(idGenerator: IdGenerator): Services<SpannerBasicReportsService> {
    val databaseName = "pooled_report_" + UUID.randomUUID().toString().replace("-", "")
    postgres.createConnection("").use { connection ->
      connection.createStatement().use { it.execute("CREATE DATABASE $databaseName") }
    }
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
          .option(ConnectionFactoryOptions.DATABASE, databaseName)
          .build()
      )
    val pool =
      ConnectionPool(
        ConnectionPoolConfiguration.builder(factory)
          .initialSize(0)
          .maxSize(4)
          .maxAcquireTime(Duration.ofSeconds(5))
          .build()
      )
    pools.add(pool)
    val postgresClient = PostgresDatabaseClient.fromConnectionFactory(pool)
    val spannerClient = spannerDatabase.databaseClient
    return Services(
      basicReportsService =
        SpannerBasicReportsService(
          spannerClient = spannerClient,
          postgresClient = postgresClient,
          impressionQualificationFilterMapping = impressionQualificationFilterMapping,
        ),
      PostgresMeasurementConsumersService(idGenerator, postgresClient),
      PostgresReportingSetsService(idGenerator, postgresClient),
      SpannerReportResultsService(
        spannerClient = spannerClient,
        impressionQualificationFilterMapping = impressionQualificationFilterMapping,
      ),
    )
  }

  @After
  fun disposePools(): Unit = runBlocking {
    for (pool in pools) {
      pool.disposeLater().awaitFirstOrNull()
    }
  }

  companion object {
    @get:ClassRule @JvmStatic val spannerEmulator = SpannerEmulatorRule()
    private val postgres = PostgreSQLContainer("postgres:16").withDatabaseName("template1")

    @JvmStatic
    @BeforeClass
    fun startPostgres() {
      postgres.start()
      postgres.createConnection("").use { connection ->
        Liquibase.update(connection, PostgresSchemata.REPORTING_CHANGELOG_PATH)
      }
    }

    @JvmStatic
    @AfterClass
    fun stopPostgres() {
      postgres.stop()
    }
  }
}
