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

package org.wfanet.measurement.integration.deploy.gcloud

import java.time.Clock
import org.junit.ClassRule
import org.wfanet.measurement.common.db.r2dbc.postgres.testing.PostgresDatabaseProviderRule
import org.wfanet.measurement.common.identity.RandomIdGenerator
import org.wfanet.measurement.duchy.deploy.common.postgres.testing.Schemata.DUCHY_CHANGELOG_PATH
import org.wfanet.measurement.integration.common.ALL_DUCHY_NAMES
import org.wfanet.measurement.integration.common.reporting.v2.InProcessLifeOfAReportIntegrationTest
import org.wfanet.measurement.integration.deploy.common.postgres.PostgresDuchyDependencyProviderRule
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.testing.KingdomDataServicesProviderRule
import org.wfanet.measurement.reporting.deploy.v2.common.server.postgres.PostgresServices
import org.wfanet.measurement.reporting.deploy.v2.postgres.testing.Schemata.REPORTING_CHANGELOG_PATH

/** Implementation of [InProcessLifeOfAReportIntegrationTest] for Google Cloud. */
class GCloudInProcessLifeOfAReportV2IntegrationTest :
  InProcessLifeOfAReportIntegrationTest(
    KingdomDataServicesProviderRule(),
    PostgresDuchyDependencyProviderRule(duchyDatabaseProvider, ALL_DUCHY_NAMES),
  ) {
  override val internalReportingServerServices by lazy {
    PostgresServices.create(
      RandomIdGenerator(Clock.systemUTC()),
      reportingDatabaseProvider.createDatabase(),
    )
  }

  companion object {
    @get:ClassRule
    @JvmStatic
    val reportingDatabaseProvider = PostgresDatabaseProviderRule(REPORTING_CHANGELOG_PATH)

    @get:ClassRule
    @JvmStatic
    val duchyDatabaseProvider = PostgresDatabaseProviderRule(DUCHY_CHANGELOG_PATH)
  }
}
