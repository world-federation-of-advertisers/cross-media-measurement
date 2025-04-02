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

package org.wfanet.measurement.reporting.deploy.v2.gcloud.server

import java.time.Clock
import kotlinx.coroutines.runBlocking
import org.wfanet.measurement.common.commandLineMain
import org.wfanet.measurement.common.db.r2dbc.postgres.PostgresDatabaseClient
import org.wfanet.measurement.common.identity.RandomIdGenerator
import org.wfanet.measurement.gcloud.postgres.PostgresConnectionFactories
import org.wfanet.measurement.gcloud.postgres.PostgresFlags as GCloudPostgresFlags
import org.wfanet.measurement.reporting.deploy.v2.common.SpannerFlags
import org.wfanet.measurement.reporting.deploy.v2.common.server.AbstractInternalReportingServer
import org.wfanet.measurement.reporting.deploy.v2.common.service.DataServices
import org.wfanet.measurement.reporting.deploy.v2.common.usingSpanner
import picocli.CommandLine

/** Implementation of [AbstractInternalReportingServer] using Google Cloud Postgres. */
@CommandLine.Command(
  name = "GCloudInternalReportingServer",
  description = ["Start the internal Reporting data-layer services in a single blocking server."],
  mixinStandardHelpOptions = true,
  showDefaultValues = true,
)
class GCloudInternalReportingServer : AbstractInternalReportingServer() {
  @CommandLine.Mixin private lateinit var gCloudPostgresFlags: GCloudPostgresFlags
  @CommandLine.Mixin private lateinit var spannerFlags: SpannerFlags

  override fun run() = runBlocking {
    val clock = Clock.systemUTC()
    val idGenerator = RandomIdGenerator(clock)

    val factory = PostgresConnectionFactories.buildConnectionFactory(gCloudPostgresFlags)
    val postgresClient = PostgresDatabaseClient.fromConnectionFactory(factory)

    if (basicReportsEnabled) {
      if (spannerFlags.projectName.isEmpty() || spannerFlags.instanceName.isEmpty() || spannerFlags.databaseName.isEmpty()) {
        throw CommandLine.MissingParameterException(
          spec.commandLine(), spec.args(),
          "--spanner-project, --spanner-instance, and --spanner-database are all required if --basic-reports-enabled is set to true"
        )
      }

      spannerFlags.usingSpanner { spanner ->
        val spannerClient = spanner.databaseClient
        run(DataServices.create(idGenerator, postgresClient, spannerClient))
      }
    } else {
      run(DataServices.create(idGenerator, postgresClient, null))
    }
  }
}

fun main(args: Array<String>) = commandLineMain(GCloudInternalReportingServer(), args)
