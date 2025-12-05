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

import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.runBlocking
import org.wfanet.measurement.common.RandomIdGenerator
import org.wfanet.measurement.common.commandLineMain
import org.wfanet.measurement.common.db.r2dbc.postgres.PostgresDatabaseClient
import org.wfanet.measurement.common.grpc.ServiceFlags
import org.wfanet.measurement.common.parseTextProto
import org.wfanet.measurement.config.reporting.ImpressionQualificationFilterConfig
import org.wfanet.measurement.gcloud.postgres.PostgresConnectionFactories
import org.wfanet.measurement.gcloud.postgres.PostgresFlags as GCloudPostgresFlags
import org.wfanet.measurement.gcloud.spanner.SpannerDatabaseConnector
import org.wfanet.measurement.gcloud.spanner.usingSpanner
import org.wfanet.measurement.reporting.deploy.v2.common.server.AbstractInternalReportingServer
import org.wfanet.measurement.reporting.deploy.v2.common.service.DataServices
import org.wfanet.measurement.reporting.service.internal.ImpressionQualificationFilterMapping
import picocli.CommandLine

/** Implementation of [AbstractInternalReportingServer] using Google Cloud Postgres. */
@CommandLine.Command(
  name = "GCloudInternalReportingServer",
  description = ["Start the internal Reporting data-layer services in a single blocking server."],
  mixinStandardHelpOptions = true,
  showDefaultValues = true,
)
class GCloudInternalReportingServer : AbstractInternalReportingServer() {
  @CommandLine.Mixin private lateinit var serviceFlags: ServiceFlags
  @CommandLine.Mixin private lateinit var gCloudPostgresFlags: GCloudPostgresFlags

  override fun run() = runBlocking {
    validateCommandLine()
    val idGenerator = RandomIdGenerator()
    val serviceDispatcher: CoroutineDispatcher = serviceFlags.executor.asCoroutineDispatcher()

    val factory = PostgresConnectionFactories.buildConnectionFactory(gCloudPostgresFlags)
    val postgresClient = PostgresDatabaseClient.fromConnectionFactory(factory)

    if (basicReportsEnabled) {
      val impressionQualificationFilterConfig =
        parseTextProto(
          impressionQualificationFilterConfigFile,
          ImpressionQualificationFilterConfig.getDefaultInstance(),
        )

      val eventMessageDescriptor = getEventMessageDescriptor()

      val impressionQualificationFilterMapping =
        ImpressionQualificationFilterMapping(
          impressionQualificationFilterConfig,
          eventMessageDescriptor,
        )

      spannerFlags.usingSpanner { spanner: SpannerDatabaseConnector ->
        val spannerClient = spanner.databaseClient
        run(
          DataServices.create(
            idGenerator,
            postgresClient,
            spannerClient,
            impressionQualificationFilterMapping,
            eventMessageDescriptor,
            disableMetricsReuse,
            serviceDispatcher,
          )
        )
      }
    } else {
      run(
        DataServices.create(
          idGenerator,
          postgresClient,
          spannerClient = null,
          impressionQualificationFilterMapping = null,
          eventMessageDescriptor = null,
          disableMetricsReuse,
          serviceDispatcher,
        )
      )
    }
  }
}

fun main(args: Array<String>) = commandLineMain(GCloudInternalReportingServer(), args)
