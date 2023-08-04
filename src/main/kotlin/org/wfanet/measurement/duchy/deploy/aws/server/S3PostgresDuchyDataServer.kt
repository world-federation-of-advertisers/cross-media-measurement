// Copyright 2020 The Cross-Media Measurement Authors
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

import kotlinx.coroutines.runBlocking
import org.wfanet.measurement.aws.s3.S3StorageClient
import org.wfanet.measurement.common.commandLineMain
import org.wfanet.measurement.common.db.postgres.PostgresFlags
import org.wfanet.measurement.common.db.r2dbc.postgres.PostgresDatabaseClient
import org.wfanet.measurement.common.identity.RandomIdGenerator
import org.wfanet.measurement.duchy.deploy.common.server.DuchyDataServer
import org.wfanet.measurement.duchy.deploy.common.service.PostgresDuchyDataServices
import picocli.CommandLine
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.S3AsyncClient
import java.time.Clock

/** Implementation of [DuchyDataServer] using Google Cloud Postgres and Google Cloud Storage (GCS). */
@CommandLine.Command(
  name = "S3PostgresDuchyDataServer",
  description = ["Server daemon for ${DuchyDataServer.SERVICE_NAME} service."],
  mixinStandardHelpOptions = true,
  showDefaultValues = true
)
class S3PostgresDuchyDataServer : DuchyDataServer() {
  @CommandLine.Mixin
  private lateinit var postgresFlags: PostgresFlags

  @CommandLine.Mixin
  private lateinit var s3Flags: S3Flags

  override fun run() = runBlocking {
    val clock = Clock.systemUTC()
    val idGenerator = RandomIdGenerator(clock)

    val databaseClient = PostgresDatabaseClient.fromFlags(postgresFlags)
    val storageClient =
      S3StorageClient(
        S3AsyncClient.builder().region(Region.of(s3Flags.s3Region)).build(),
        s3Flags.s3Bucket
      )

    run(
      PostgresDuchyDataServices.create(
        storageClient,
        computationLogEntriesClient,
        flags.duchy.duchyName,
        idGenerator,
        databaseClient
      )
    )
  }
}

fun main(args: Array<String>) = commandLineMain(S3PostgresDuchyDataServer(), args)
