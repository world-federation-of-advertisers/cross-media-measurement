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

package org.wfanet.measurement.duchy.deploy.gcloud.server

import java.time.Clock
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.runBlocking
import org.wfanet.measurement.common.commandLineMain
import org.wfanet.measurement.common.db.r2dbc.postgres.PostgresDatabaseClient
import org.wfanet.measurement.common.grpc.ServiceFlags
import org.wfanet.measurement.common.identity.RandomIdGenerator
import org.wfanet.measurement.duchy.deploy.common.server.DuchyDataServer
import org.wfanet.measurement.duchy.deploy.common.service.PostgresDuchyDataServices
import org.wfanet.measurement.gcloud.gcs.GcsFromFlags
import org.wfanet.measurement.gcloud.gcs.GcsStorageClient
import org.wfanet.measurement.gcloud.postgres.PostgresConnectionFactories
import org.wfanet.measurement.gcloud.postgres.PostgresFlags as GCloudPostgresFlags
import picocli.CommandLine

/**
 * Implementation of [DuchyDataServer] using Google Cloud Postgres and Google Cloud Storage (GCS).
 */
@CommandLine.Command(
  name = "GcsPostgresDuchyDataServer",
  description = ["Server daemon for ${DuchyDataServer.SERVICE_NAME} service."],
  mixinStandardHelpOptions = true,
  showDefaultValues = true,
)
class GcsPostgresDuchyDataServer : DuchyDataServer() {
  @CommandLine.Mixin private lateinit var serviceFlags: ServiceFlags
  @CommandLine.Mixin private lateinit var gcsFlags: GcsFromFlags.Flags
  @CommandLine.Mixin private lateinit var gCloudPostgresFlags: GCloudPostgresFlags

  override fun run() = runBlocking {
    val clock = Clock.systemUTC()
    val idGenerator = RandomIdGenerator(clock)
    val storageClient = GcsStorageClient.fromFlags(GcsFromFlags(gcsFlags))

    val factory = PostgresConnectionFactories.buildConnectionFactory(gCloudPostgresFlags)
    val client = PostgresDatabaseClient.fromConnectionFactory(factory)

    run(
      PostgresDuchyDataServices.create(
        storageClient,
        computationLogEntriesClient,
        duchyFlags.duchyName,
        idGenerator,
        client,
        serviceFlags.executor.asCoroutineDispatcher(),
      )
    )
  }
}

fun main(args: Array<String>) = commandLineMain(GcsPostgresDuchyDataServer(), args)
