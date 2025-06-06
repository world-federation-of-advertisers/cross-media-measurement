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

package org.wfanet.measurement.integration.deploy.common.postgres

import java.time.Clock
import kotlinx.coroutines.Dispatchers
import org.junit.rules.TemporaryFolder
import org.junit.runner.Description
import org.junit.runners.model.Statement
import org.wfanet.measurement.common.db.r2dbc.postgres.PostgresDatabaseClient
import org.wfanet.measurement.common.db.r2dbc.postgres.testing.PostgresDatabaseProvider
import org.wfanet.measurement.common.identity.RandomIdGenerator
import org.wfanet.measurement.common.testing.ProviderRule
import org.wfanet.measurement.duchy.deploy.common.service.PostgresDuchyDataServices
import org.wfanet.measurement.integration.common.InProcessDuchy
import org.wfanet.measurement.storage.StorageClient
import org.wfanet.measurement.storage.filesystem.FileSystemStorageClient
import org.wfanet.measurement.system.v1alpha.ComputationLogEntriesGrpcKt.ComputationLogEntriesCoroutineStub

/** [TestRule] which provides [InProcessDuchy.DuchyDependencies] factories using Postgres. */
class PostgresDuchyDependencyProviderRule(
  private val databaseProvider: PostgresDatabaseProvider,
  private val duchies: Iterable<String>,
) : ProviderRule<(String, ComputationLogEntriesCoroutineStub) -> InProcessDuchy.DuchyDependencies> {
  private val temporaryFolder = TemporaryFolder()
  private val idGenerator = RandomIdGenerator(Clock.systemUTC())

  private lateinit var computationsDatabases: Map<String, PostgresDatabaseClient>
  private lateinit var storageClient: StorageClient

  override val value:
    (String, ComputationLogEntriesCoroutineStub) -> InProcessDuchy.DuchyDependencies
    get() = ::buildDuchyDependencies

  private fun buildDuchyDependencies(
    duchyId: String,
    logEntryClient: ComputationLogEntriesCoroutineStub,
  ): InProcessDuchy.DuchyDependencies {
    val computationsDatabase =
      computationsDatabases[duchyId]
        ?: error("Missing Computations Spanner database for duchy $duchyId")

    val duchyDataServices =
      PostgresDuchyDataServices.create(
        storageClient = storageClient,
        computationLogEntriesClient = logEntryClient,
        duchyName = duchyId,
        idGenerator = idGenerator,
        client = computationsDatabase,
        coroutineContext = Dispatchers.Default,
      )
    return InProcessDuchy.DuchyDependencies(duchyDataServices, storageClient)
  }

  override fun apply(base: Statement, description: Description): Statement {
    val dbStatement =
      object : Statement() {
        override fun evaluate() {
          computationsDatabases = duchies.associateWith { databaseProvider.createDatabase() }
          storageClient = FileSystemStorageClient(temporaryFolder.root)
          base.evaluate()
        }
      }
    return temporaryFolder.apply(dbStatement, description)
  }
}
