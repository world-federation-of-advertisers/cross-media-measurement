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

import org.junit.ClassRule
import org.junit.Rule
import org.junit.rules.TestRule
import org.wfanet.measurement.common.db.r2dbc.postgres.testing.PostgresDatabaseProviderRule
import org.wfanet.measurement.common.identity.IdGenerator
import org.wfanet.measurement.common.testing.chainRulesSequentially
import org.wfanet.measurement.gcloud.spanner.testing.SpannerEmulatorDatabaseRule
import org.wfanet.measurement.gcloud.spanner.testing.SpannerEmulatorRule
import org.wfanet.measurement.reporting.deploy.v2.gcloud.spanner.testing.Schemata
import org.wfanet.measurement.reporting.deploy.v2.postgres.PostgresMeasurementConsumersService
import org.wfanet.measurement.reporting.deploy.v2.postgres.PostgresReportingSetsService
import org.wfanet.measurement.reporting.deploy.v2.postgres.testing.Schemata as PostgresSchemata
import org.wfanet.measurement.reporting.service.internal.testing.v2.BasicReportsServiceTest

class SpannerBasicReportsServiceTest : BasicReportsServiceTest<SpannerBasicReportsService>() {

  @get:Rule
  val spannerDatabase =
    SpannerEmulatorDatabaseRule(spannerEmulator, Schemata.REPORTING_CHANGELOG_PATH)

  override fun newServices(idGenerator: IdGenerator): Services<SpannerBasicReportsService> {
    val spannerDatabaseClient = spannerDatabase.databaseClient
    val postgresDatabaseClient = postgresDatabaseProvider.createDatabase()
    return Services(
      basicReportsService =
        SpannerBasicReportsService(
          spannerClient = spannerDatabaseClient,
          postgresClient = postgresDatabaseClient,
          impressionQualificationFilterMapping = impressionQualificationFilterMapping,
        ),
      PostgresMeasurementConsumersService(idGenerator, postgresDatabaseClient),
      PostgresReportingSetsService(idGenerator, postgresDatabaseClient),
      SpannerReportResultsService(
        spannerClient = spannerDatabaseClient,
        impressionQualificationFilterMapping = impressionQualificationFilterMapping,
      ),
    )
  }

  companion object {
    @JvmStatic val spannerEmulator = SpannerEmulatorRule()

    @JvmStatic
    val postgresDatabaseProvider =
      PostgresDatabaseProviderRule(PostgresSchemata.REPORTING_CHANGELOG_PATH)

    @get:ClassRule
    @JvmStatic
    val ruleChain: TestRule = chainRulesSequentially(spannerEmulator, postgresDatabaseProvider)
  }
}
