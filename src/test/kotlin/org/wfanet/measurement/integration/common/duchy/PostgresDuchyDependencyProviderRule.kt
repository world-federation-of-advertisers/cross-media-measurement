// Copyright 2023 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.integration.common.duchy

import com.google.cloud.storage.contrib.nio.testing.LocalStorageHelper
import java.time.Clock
import org.junit.runner.Description
import org.junit.runners.model.Statement
import org.wfanet.measurement.common.db.r2dbc.postgres.PostgresDatabaseClient
import org.wfanet.measurement.common.testing.ProviderRule
import org.wfanet.measurement.gcloud.gcs.GcsStorageClient
import org.wfanet.measurement.integration.common.InProcessDuchy
import org.wfanet.measurement.system.v1alpha.ComputationLogEntriesGrpcKt.ComputationLogEntriesCoroutineStub
import org.wfanet.measurement.common.db.r2dbc.postgres.testing.EmbeddedPostgresDatabaseProvider
import org.wfanet.measurement.common.identity.RandomIdGenerator
import org.wfanet.measurement.duchy.deploy.common.service.PostgresDuchyDataServices
import org.wfanet.measurement.duchy.deploy.common.postgres.testing.Schemata

class PostgresDuchyDependencyProviderRule(duchies: Iterable<String>) :
  ProviderRule<(String, ComputationLogEntriesCoroutineStub) -> InProcessDuchy.DuchyDependencies> {
  private val computationsDatabases: Map<String, PostgresDatabaseClient> =
    duchies.associateWith { EmbeddedPostgresDatabaseProvider(Schemata.DUCHY_CHANGELOG_PATH).createNewDatabase() }

  private fun buildDuchyDependencies(
    duchyId: String,
    logEntryClient: ComputationLogEntriesCoroutineStub
  ): InProcessDuchy.DuchyDependencies {
    val computationsDatabase =
      computationsDatabases[duchyId]
        ?: error("Missing Computations Spanner database for duchy $duchyId")
    val storageClient = GcsStorageClient(LocalStorageHelper.getOptions().service, "bucket-$duchyId")

    val duchyDataServices = PostgresDuchyDataServices.create(
      storageClient = storageClient,
      computationLogEntriesClient = logEntryClient,
      duchyName = duchyId,
      idGenerator = RandomIdGenerator(Clock.systemUTC()),
      client = computationsDatabase,
    )
    return InProcessDuchy.DuchyDependencies(
      duchyDataServices,
      storageClient
    )
  }

  override val value: (String, ComputationLogEntriesCoroutineStub) -> InProcessDuchy.DuchyDependencies
    get() = ::buildDuchyDependencies

  override fun apply(base: Statement, description: Description): Statement {
    return base
  }
}
