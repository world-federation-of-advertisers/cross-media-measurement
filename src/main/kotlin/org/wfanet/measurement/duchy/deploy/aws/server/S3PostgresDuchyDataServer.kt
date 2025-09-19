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

package org.wfanet.measurement.duchy.deploy.aws.server

import java.time.Clock
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.runBlocking
import org.wfanet.measurement.aws.postgres.PostgresConnectionFactories
import org.wfanet.measurement.aws.postgres.PostgresFlags as AwsPostgresFlags
import org.wfanet.measurement.aws.s3.S3Flags
import org.wfanet.measurement.aws.s3.S3StorageClient
import org.wfanet.measurement.common.commandLineMain
import org.wfanet.measurement.common.db.r2dbc.postgres.PostgresDatabaseClient
import org.wfanet.measurement.common.grpc.ServiceFlags
import org.wfanet.measurement.common.identity.RandomIdGenerator
import org.wfanet.measurement.duchy.deploy.common.server.DuchyDataServer
import org.wfanet.measurement.duchy.deploy.common.service.PostgresDuchyDataServices
import picocli.CommandLine

/** Implementation of [DuchyDataServer] using AWS RDS Postgres and AWS S3. */
@CommandLine.Command(
  name = "S3PostgresDuchyDataServer",
  description = ["Server daemon for ${DuchyDataServer.SERVICE_NAME} service."],
  mixinStandardHelpOptions = true,
  showDefaultValues = true,
)
class S3PostgresDuchyDataServer : DuchyDataServer() {
  @CommandLine.Mixin private lateinit var serviceFlags: ServiceFlags
  @CommandLine.Mixin private lateinit var postgresFlags: AwsPostgresFlags
  @CommandLine.Mixin private lateinit var s3Flags: S3Flags

  override fun run() = runBlocking {
    val clock = Clock.systemUTC()
    val idGenerator = RandomIdGenerator(clock)

    val factory = PostgresConnectionFactories.buildConnectionFactory(postgresFlags)
    val databaseClient = PostgresDatabaseClient.fromConnectionFactory(factory)
    val storageClient = S3StorageClient.fromFlags(s3Flags)

    run(
      PostgresDuchyDataServices.create(
        storageClient,
        computationLogEntriesClient,
        duchyFlags.duchyName,
        idGenerator,
        databaseClient,
        serviceFlags.executor.asCoroutineDispatcher(),
      )
    )
  }
}

fun main(args: Array<String>) = commandLineMain(S3PostgresDuchyDataServer(), args)
