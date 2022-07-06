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

import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.common.db.r2dbc.postgres.testing.EmbeddedPostgresDatabaseProvider
import org.wfanet.measurement.common.identity.IdGenerator
import org.wfanet.measurement.reporting.deploy.postgres.testing.Schemata.REPORTING_CHANGELOG_PATH
import org.wfanet.measurement.reporting.service.internal.testing.ReportsServiceTest

@RunWith(JUnit4::class)
class PostgresReportsServiceTest : ReportsServiceTest<PostgresReportsService>() {
  override fun newServices(
    idGenerator: IdGenerator,
  ): Services<PostgresReportsService> {
    val client = EmbeddedPostgresDatabaseProvider(REPORTING_CHANGELOG_PATH).createNewDatabase()
    return Services(
      PostgresReportsService(idGenerator, client),
      PostgresMeasurementsService(idGenerator, client),
      PostgresReportingSetsService(idGenerator, client),
    )
  }
}
