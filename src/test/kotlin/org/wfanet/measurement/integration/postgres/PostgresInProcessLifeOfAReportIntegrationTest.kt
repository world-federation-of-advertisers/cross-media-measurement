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

import com.google.cloud.storage.contrib.nio.testing.LocalStorageHelper
import java.time.Clock
import org.wfanet.measurement.common.db.r2dbc.postgres.testing.EmbeddedPostgresDatabaseProvider
import org.wfanet.measurement.common.identity.RandomIdGenerator
import org.wfanet.measurement.gcloud.gcs.GcsStorageClient
import org.wfanet.measurement.integration.common.ALL_DUCHY_NAMES
import org.wfanet.measurement.integration.common.reporting.InProcessLifeOfAReportIntegrationTest
import org.wfanet.measurement.integration.gcloud.DuchyDependencyProviderRule
import org.wfanet.measurement.integration.gcloud.KingdomDataServicesProviderRule
import org.wfanet.measurement.reporting.deploy.postgres.PostgresDataServices
import org.wfanet.measurement.reporting.deploy.postgres.testing.Schemata
import org.wfanet.measurement.storage.StorageClient

/** Implementation of [InProcessLifeOfAReportIntegrationTest] for Postgres. */
class PostgresInProcessLifeOfAReportIntegrationTest : InProcessLifeOfAReportIntegrationTest() {
  override val reportingServerDataServices by lazy {
    PostgresDataServices(
      RandomIdGenerator(Clock.systemUTC()),
      EmbeddedPostgresDatabaseProvider(Schemata.REPORTING_CHANGELOG_PATH).createNewDatabase()
    )
  }
  override val kingdomDataServicesRule by lazy { KingdomDataServicesProviderRule() }
  override val duchyDependenciesRule by lazy { DuchyDependencyProviderRule(ALL_DUCHY_NAMES) }
  override val storageClient: StorageClient by lazy {
    GcsStorageClient(LocalStorageHelper.getOptions().service, "bucket-simulator")
  }
}
