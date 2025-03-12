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

package org.wfanet.measurement.reporting.deploy.v2.postgres.server

import java.time.Clock
import kotlinx.coroutines.runBlocking
import org.wfanet.measurement.common.commandLineMain
import org.wfanet.measurement.common.db.postgres.PostgresFlags
import org.wfanet.measurement.common.db.r2dbc.postgres.PostgresDatabaseClient
import org.wfanet.measurement.common.identity.RandomIdGenerator
import org.wfanet.measurement.gcloud.spanner.SpannerFlags
import org.wfanet.measurement.gcloud.spanner.usingSpanner
import org.wfanet.measurement.reporting.deploy.v2.common.server.InternalReportingServer
import org.wfanet.measurement.reporting.deploy.v2.common.service.DataServices
import picocli.CommandLine

/** Implementation of [InternalReportingServer] using Postgres. */
@CommandLine.Command(
  name = "PostgresInternalReportingServer",
  description = ["Start the internal Reporting data-layer services in a single blocking server."],
  mixinStandardHelpOptions = true,
  showDefaultValues = true,
)
class PostgresInternalReportingServer : InternalReportingServer() {
  @CommandLine.Mixin private lateinit var postgresFlags: PostgresFlags
  @CommandLine.Mixin private lateinit var spannerFlags: SpannerFlags

  override fun run() = runBlocking {
    val clock = Clock.systemUTC()
    val idGenerator = RandomIdGenerator(clock)

    val postgresClient = PostgresDatabaseClient.fromFlags(postgresFlags)

    spannerFlags.usingSpanner { spanner ->
      val spannerClient = spanner.databaseClient

      run(DataServices.create(idGenerator, postgresClient, spannerClient))
    }
  }
}

fun main(args: Array<String>) = commandLineMain(PostgresInternalReportingServer(), args)
