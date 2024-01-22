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

package org.wfanet.measurement.reporting.deploy.v2.postgres

import org.junit.ClassRule
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.common.db.r2dbc.postgres.PostgresDatabaseClient
import org.wfanet.measurement.common.db.r2dbc.postgres.testing.PostgresDatabaseProviderRule
import org.wfanet.measurement.common.identity.IdGenerator
import org.wfanet.measurement.reporting.deploy.v2.postgres.testing.Schemata
import org.wfanet.measurement.reporting.service.internal.testing.v2.ReportingSetsServiceTest

@RunWith(JUnit4::class)
class PostgresReportingSetsServiceTest : ReportingSetsServiceTest<PostgresReportingSetsService>() {
  override fun newServices(idGenerator: IdGenerator): Services<PostgresReportingSetsService> {
    val client: PostgresDatabaseClient = databaseProvider.createDatabase()
    return Services(
      PostgresReportingSetsService(idGenerator, client),
      PostgresMeasurementConsumersService(idGenerator, client),
    )
  }

  companion object {
    @get:ClassRule
    @JvmStatic
    val databaseProvider = PostgresDatabaseProviderRule(Schemata.REPORTING_CHANGELOG_PATH)
  }
}
