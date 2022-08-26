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

package org.wfanet.measurement.reporting.deploy.gcloud.postgres.server

import java.time.Clock
import kotlinx.coroutines.runBlocking
import org.wfanet.measurement.common.commandLineMain
import org.wfanet.measurement.common.db.r2dbc.postgres.PostgresDatabaseClient
import org.wfanet.measurement.common.identity.RandomIdGenerator
import org.wfanet.measurement.gcloud.postgres.PostgresConnectionFactories
import org.wfanet.measurement.gcloud.postgres.PostgresFlags as GCloudPostgresFlags
import org.wfanet.measurement.reporting.deploy.common.server.ReportingDataServer
import org.wfanet.measurement.reporting.deploy.common.server.postgres.PostgresServices
import picocli.CommandLine

/** Implementation of [ReportingDataServer] using Google Cloud Postgres. */
@CommandLine.Command(
  name = "GCloudPostgresReportingDataServer",
  description = ["Start the internal Reporting data-layer services in a single blocking server."],
  mixinStandardHelpOptions = true,
  showDefaultValues = true
)
class GCloudPostgresReportingDataServer : ReportingDataServer() {
  @CommandLine.Mixin private lateinit var gCloudPostgresFlags: GCloudPostgresFlags

  override fun run() = runBlocking {
    val clock = Clock.systemUTC()
    val idGenerator = RandomIdGenerator(clock)

    val factory = PostgresConnectionFactories.buildConnectionFactory(gCloudPostgresFlags)
    val client = PostgresDatabaseClient.fromConnectionFactory(factory)

    run(PostgresServices.create(idGenerator, client))
  }
}

fun main(args: Array<String>) = commandLineMain(GCloudPostgresReportingDataServer(), args)
