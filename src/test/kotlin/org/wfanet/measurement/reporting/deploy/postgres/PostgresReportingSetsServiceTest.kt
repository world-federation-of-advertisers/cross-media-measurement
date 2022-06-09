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

package org.wfanet.measurement.reporting.deploy.postgres

import com.opentable.db.postgres.embedded.LiquibasePreparer
import com.opentable.db.postgres.junit.EmbeddedPostgresRules
import com.opentable.db.postgres.junit.PreparedDbRule
import io.r2dbc.spi.Connection
import io.r2dbc.spi.ConnectionFactories
import io.r2dbc.spi.ConnectionFactoryOptions
import kotlinx.coroutines.reactive.awaitFirst
import org.junit.Rule
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.common.db.r2dbc.postgres.PostgresDatabaseClient
import org.wfanet.measurement.common.identity.IdGenerator
import org.wfanet.measurement.reporting.deploy.postgres.testing.Schemata.REPORTING_CHANGELOG_PATH
import org.wfanet.measurement.reporting.service.internal.testing.ReportingSetsServiceTest
import reactor.core.publisher.Mono

@RunWith(JUnit4::class)
class PostgresReportingSetsServiceTest : ReportingSetsServiceTest<PostgresReportingSetsService>() {
  @get:Rule
  val db: PreparedDbRule =
    EmbeddedPostgresRules.preparedDatabase(
      LiquibasePreparer.forClasspathLocation(REPORTING_CHANGELOG_PATH.toString())
    )

  private suspend fun getConnection(): Connection {
    val connectionInfo = db.connectionInfo

    val regex = Regex(":${connectionInfo.port}/.*")

    val connectionFactoryOptions =
      ConnectionFactoryOptions.builder()
        .option(ConnectionFactoryOptions.DRIVER, "postgresql")
        .option(ConnectionFactoryOptions.HOST, connectionInfo.host)
        .option(ConnectionFactoryOptions.PORT, connectionInfo.port)
        .option(ConnectionFactoryOptions.USER, connectionInfo.user)
        .option(ConnectionFactoryOptions.PASSWORD, connectionInfo.password)
        .option(
          ConnectionFactoryOptions.DATABASE,
          regex.find(connectionInfo.url)!!.value.split("/")[1].split("?")[0]
        )
        .build()

    val connectionFactory = ConnectionFactories.get(connectionFactoryOptions)
    return Mono.from(connectionFactory.create()).awaitFirst()
  }

  override fun newService(
    idGenerator: IdGenerator,
  ): PostgresReportingSetsService {
    return PostgresReportingSetsService(idGenerator, PostgresDatabaseClient(::getConnection))
  }
}
