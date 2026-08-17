/*
 * Copyright 2026 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.reporting.deploy.v2.gcloud.spanner.tools

import com.google.protobuf.Timestamp
import com.google.protobuf.util.Timestamps
import java.time.Instant
import java.time.format.DateTimeParseException
import kotlinx.coroutines.runBlocking
import org.wfanet.measurement.common.RandomIdGenerator
import org.wfanet.measurement.common.commandLineMain
import org.wfanet.measurement.common.db.r2dbc.postgres.PostgresDatabaseClient
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.common.identity.IdGenerator
import org.wfanet.measurement.common.identity.InternalId
import org.wfanet.measurement.gcloud.postgres.PostgresConnectionFactories
import org.wfanet.measurement.gcloud.postgres.PostgresFlags as GCloudPostgresFlags
import org.wfanet.measurement.gcloud.spanner.SpannerDatabaseConnector
import org.wfanet.measurement.gcloud.spanner.usingSpanner
import org.wfanet.measurement.reporting.deploy.v2.common.SpannerFlags
import picocli.CommandLine

/**
 * Backfills `ReportingUnitComponentSummary.external_reporting_set_id` on stored `BasicReport`s.
 *
 * `BasicReport`s written via the internal `InsertBasicReport` method before the field was validated
 * have it unset, which makes them unserveable on the public read path.
 */
@CommandLine.Command(
  name = "BackfillBasicReportReportingSets",
  description =
    ["Backfills ReportingUnitComponentSummary.external_reporting_set_id on stored BasicReports."],
  mixinStandardHelpOptions = true,
  showDefaultValues = true,
)
class BackfillBasicReportReportingSets : Runnable {
  @CommandLine.Spec private lateinit var spec: CommandLine.Model.CommandSpec

  @CommandLine.Mixin private lateinit var spannerFlags: SpannerFlags

  @CommandLine.Mixin private lateinit var postgresFlags: GCloudPostgresFlags

  @CommandLine.Option(
    names = ["--dry-run"],
    description = ["Report what would change without writing to any database."],
  )
  private var dryRun: Boolean = false

  @CommandLine.Option(
    names = ["--create-time-after"],
    description =
      [
        "Only examine BasicReports created after this RFC 3339 time, e.g. 2026-06-01T00:00:00Z.",
        "When unset, all SUCCEEDED BasicReports are examined.",
      ],
  )
  private var createTimeAfter: String? = null

  override fun run() {
    runBlocking {
      val postgresClient =
        PostgresDatabaseClient.fromConnectionFactory(
          PostgresConnectionFactories.buildConnectionFactory(postgresFlags)
        )

      spannerFlags.usingSpanner { spanner: SpannerDatabaseConnector ->
        BasicReportReportingSetBackfiller(
            spannerClient = spanner.databaseClient,
            postgresClient = postgresClient,
            idGenerator = LegacyRandomIdGenerator(),
            dryRun = dryRun,
            createTimeAfter = parseCreateTimeAfter(),
          )
          .run()
      }
    }
  }

  private fun parseCreateTimeAfter(): Timestamp? {
    val value = createTimeAfter ?: return null
    return try {
      Timestamps.fromMillis(Instant.parse(value).toEpochMilli())
    } catch (e: DateTimeParseException) {
      throw CommandLine.ParameterException(
        spec.commandLine(),
        "Invalid --create-time-after '$value': expected an RFC 3339 time such as " +
          "2026-06-01T00:00:00Z",
      )
    }
  }

  /** Adapts [RandomIdGenerator] to the [IdGenerator] that PostgresWriter requires. */
  private class LegacyRandomIdGenerator : IdGenerator {
    private val delegate = RandomIdGenerator()

    override fun generateInternalId() = InternalId(delegate.generateId())

    override fun generateExternalId() = ExternalId(delegate.generateId())
  }
}

fun main(args: Array<String>) = commandLineMain(BackfillBasicReportReportingSets(), args)
