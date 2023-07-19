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

package org.wfanet.measurement.duchy.deploy.common.server.postgres

import java.time.Clock
import kotlinx.coroutines.runBlocking
import org.wfanet.measurement.common.db.postgres.PostgresFlags
import org.wfanet.measurement.common.db.r2dbc.postgres.PostgresDatabaseClient
import org.wfanet.measurement.common.identity.RandomIdGenerator
import org.wfanet.measurement.duchy.deploy.common.server.DuchyDataServer
import org.wfanet.measurement.duchy.deploy.common.service.PostgresDuchyDataServices
import org.wfanet.measurement.storage.StorageClient
import picocli.CommandLine

/** Implementation of [DuchyDataServer] using Postgres. */
@CommandLine.Command(
  name = "PostgresDuchyDataServer",
  description = ["Start the internal Duchy data-layer services in a single blocking server."],
  mixinStandardHelpOptions = true,
  showDefaultValues = true
)
abstract class PostgresDuchyDataServer : DuchyDataServer() {
  @CommandLine.Mixin private lateinit var postgresFlags: PostgresFlags

  protected fun run(storageClient: StorageClient) = runBlocking {
    val clock = Clock.systemUTC()
    val idGenerator = RandomIdGenerator(clock)

    val client = PostgresDatabaseClient.fromFlags(postgresFlags)

    run(
      PostgresDuchyDataServices.create(
        storageClient,
        computationLogEntriesClient,
        duchyFlags.duchyName,
        idGenerator,
        client
      )
    )
  }
}
