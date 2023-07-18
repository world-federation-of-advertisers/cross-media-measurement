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

package org.wfanet.measurement.integration.postgres

import com.google.cloud.storage.contrib.nio.testing.LocalStorageHelper
import java.time.Clock
import org.wfanet.measurement.common.db.r2dbc.postgres.testing.EmbeddedPostgresDatabaseProvider
import org.wfanet.measurement.common.identity.RandomIdGenerator
import org.wfanet.measurement.common.testing.ProviderRule
import org.wfanet.measurement.gcloud.gcs.GcsStorageClient
import org.wfanet.measurement.integration.common.ALL_DUCHY_NAMES
import org.wfanet.measurement.integration.common.InProcessDuchy
import org.wfanet.measurement.integration.common.duchy.PostgresDuchyDependencyProviderRule
import org.wfanet.measurement.integration.common.reporting.v2.InProcessLifeOfAReportIntegrationTest
import org.wfanet.measurement.integration.gcloud.KingdomDataServicesProviderRule
import org.wfanet.measurement.kingdom.deploy.common.service.DataServices
import org.wfanet.measurement.reporting.deploy.v2.common.server.postgres.PostgresServices
import org.wfanet.measurement.reporting.deploy.v2.postgres.testing.Schemata
import org.wfanet.measurement.storage.StorageClient
import org.wfanet.measurement.system.v1alpha.ComputationLogEntriesGrpcKt.ComputationLogEntriesCoroutineStub

/** Implementation of [InProcessLifeOfAReportIntegrationTest] for Postgres. */
class V2PostgresInProcessLifeOfAReportIntegrationTest : InProcessLifeOfAReportIntegrationTest() {
  override val kingdomDataServicesRule: ProviderRule<DataServices> by lazy {
    KingdomDataServicesProviderRule()
  }

  /** Provides a function from Duchy to the dependencies needed to start the Duchy to the test. */
  override val duchyDependenciesRule:
    ProviderRule<
      (String, ComputationLogEntriesCoroutineStub) -> InProcessDuchy.DuchyDependencies
    > by lazy { PostgresDuchyDependencyProviderRule(ALL_DUCHY_NAMES) }

  override val storageClient: StorageClient by lazy {
    GcsStorageClient(LocalStorageHelper.getOptions().service, "bucket-simulator")
  }

  override val internalReportingServerServices by lazy {
    PostgresServices.create(
      RandomIdGenerator(Clock.systemUTC()),
      EmbeddedPostgresDatabaseProvider(Schemata.REPORTING_CHANGELOG_PATH).createNewDatabase()
    )
  }
}
