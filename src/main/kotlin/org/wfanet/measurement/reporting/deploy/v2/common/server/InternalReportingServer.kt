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

package org.wfanet.measurement.reporting.deploy.v2.common.server

import io.grpc.BindableService
import java.time.Clock
import kotlin.reflect.full.declaredMemberProperties
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.runInterruptible
import org.wfanet.measurement.common.commandLineMain
import org.wfanet.measurement.common.db.postgres.PostgresFlags
import org.wfanet.measurement.common.db.r2dbc.postgres.PostgresDatabaseClient
import org.wfanet.measurement.common.grpc.CommonServer
import org.wfanet.measurement.common.identity.RandomIdGenerator
import org.wfanet.measurement.gcloud.spanner.SpannerFlags
import org.wfanet.measurement.gcloud.spanner.usingSpanner
import org.wfanet.measurement.reporting.deploy.v2.common.service.DataServices
import org.wfanet.measurement.reporting.deploy.v2.common.service.Services
import picocli.CommandLine

@CommandLine.Command(
  name = "InternalReportingServer",
  description = ["Start the internal Reporting data-layer services in a single blocking server."],
  mixinStandardHelpOptions = true,
  showDefaultValues = true,
)
open class InternalReportingServer : Runnable {
  @CommandLine.Mixin protected lateinit var postgresFlags: PostgresFlags
  @CommandLine.Mixin protected lateinit var spannerFlags: SpannerFlags
  @CommandLine.Mixin private lateinit var serverFlags: CommonServer.Flags

  protected suspend fun run(services: Services) {
    val server = CommonServer.fromFlags(serverFlags, this::class.simpleName!!, services.toList())

    runInterruptible { server.start().blockUntilShutdown() }
  }

  override fun run() = runBlocking {
    val clock = Clock.systemUTC()
    val idGenerator = RandomIdGenerator(clock)

    val postgresClient = PostgresDatabaseClient.fromFlags(postgresFlags)

    spannerFlags.usingSpanner { spanner ->
      val spannerClient = spanner.databaseClient

      run(DataServices.create(idGenerator, postgresClient, spannerClient))
    }
  }

  companion object {
    fun Services.toList(): List<BindableService> {
      return Services::class.declaredMemberProperties.map { it.get(this) as BindableService }
    }
  }
}

fun main(args: Array<String>) = commandLineMain(InternalReportingServer(), args)
