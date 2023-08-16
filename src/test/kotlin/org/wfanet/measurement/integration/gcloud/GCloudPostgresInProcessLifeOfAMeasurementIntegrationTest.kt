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

package org.wfanet.measurement.integration.gcloud

import kotlinx.coroutines.debug.junit4.CoroutinesTimeout
import org.junit.ClassRule
import org.junit.Rule
import org.wfanet.measurement.common.db.r2dbc.postgres.testing.PostgresDatabaseProviderRule
import org.wfanet.measurement.duchy.deploy.common.postgres.testing.Schemata
import org.wfanet.measurement.integration.common.ALL_DUCHY_NAMES
import org.wfanet.measurement.integration.common.InProcessLifeOfAMeasurementIntegrationTest
import org.wfanet.measurement.integration.common.duchy.PostgresDuchyDependencyProviderRule

/**
 * Implementation of [InProcessLifeOfAMeasurementIntegrationTest] for GCloud backends with Postgres
 * database.
 */
class GCloudPostgresInProcessLifeOfAMeasurementIntegrationTest :
  InProcessLifeOfAMeasurementIntegrationTest(
    KingdomDataServicesProviderRule(),
    PostgresDuchyDependencyProviderRule(databaseProvider, ALL_DUCHY_NAMES)
  ) {

  @get:Rule val timeout = CoroutinesTimeout.seconds(90)

  companion object {
    @JvmStatic
    @get:ClassRule
    val databaseProvider = PostgresDatabaseProviderRule(Schemata.DUCHY_CHANGELOG_PATH)
  }
}
