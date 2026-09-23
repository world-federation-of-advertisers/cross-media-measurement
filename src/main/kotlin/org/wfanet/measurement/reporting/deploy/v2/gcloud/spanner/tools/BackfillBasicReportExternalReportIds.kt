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
import kotlin.properties.Delegates
import kotlinx.coroutines.runBlocking
import org.wfanet.measurement.common.commandLineMain
import org.wfanet.measurement.common.db.r2dbc.postgres.PostgresDatabaseClient
import org.wfanet.measurement.gcloud.postgres.PostgresConnectionFactories
import org.wfanet.measurement.gcloud.postgres.PostgresFlags as GCloudPostgresFlags
import org.wfanet.measurement.gcloud.spanner.SpannerDatabaseConnector
import org.wfanet.measurement.gcloud.spanner.usingSpanner
import org.wfanet.measurement.reporting.deploy.v2.common.SpannerFlags
import picocli.CommandLine

/** Backfills `BasicReport.external_report_id` from the associated Postgres `Report`. */
@CommandLine.Command(
  name = "BackfillBasicReportExternalReportIds",
  description = ["Backfills external_report_id on stored BasicReports."],
  mixinStandardHelpOptions = true,
  showDefaultValues = true,
)
class BackfillBasicReportExternalReportIds : Runnable {
  @CommandLine.Spec private lateinit var spec: CommandLine.Model.CommandSpec

  @CommandLine.Mixin private lateinit var spannerFlags: SpannerFlags

  @CommandLine.Mixin private lateinit var postgresFlags: GCloudPostgresFlags

  @set:CommandLine.Option(
    names = ["--dry-run"],
    description = ["Report what would change without writing to either database."],
    defaultValue = "false",
  )
  private var dryRun by Delegates.notNull<Boolean>()

  @CommandLine.Option(
    names = ["--create-time-after"],
    description =
      [
        "Only examine BasicReports created after this RFC 3339 time, e.g. 2026-06-01T00:00:00Z.",
        "When unset, all SUCCEEDED BasicReports are examined.",
      ],
  )
  private var createTimeAfter: String? = null

  @CommandLine.Option(
    names = ["--cmms-measurement-consumer-id"],
    description = ["Only examine this MeasurementConsumer ID. May be repeated or comma-separated."],
    split = ",",
  )
  private var cmmsMeasurementConsumerIds: Array<String> = emptyArray()

  @set:CommandLine.Option(
    names = ["--match-external-basic-report-id"],
    description =
      [
        "Use external_basic_report_id when a Report with the same ID exists.",
        "Only for integrations that deliberately reuse the Report ID as the BasicReport ID.",
        "Requires --cmms-measurement-consumer-id.",
      ],
    defaultValue = "false",
  )
  private var matchExternalBasicReportId by Delegates.notNull<Boolean>()

  override fun run() {
    if (matchExternalBasicReportId && cmmsMeasurementConsumerIds.isEmpty()) {
      throw CommandLine.ParameterException(
        spec.commandLine(),
        "--match-external-basic-report-id requires --cmms-measurement-consumer-id",
      )
    }
    runBlocking {
      val postgresClient =
        PostgresDatabaseClient.fromConnectionFactory(
          PostgresConnectionFactories.buildConnectionFactory(postgresFlags)
        )

      spannerFlags.usingSpanner { spanner: SpannerDatabaseConnector ->
        BasicReportExternalReportIdBackfiller(
            spannerClient = spanner.databaseClient,
            postgresClient = postgresClient,
            dryRun = dryRun,
            createTimeAfter = parseCreateTimeAfter(),
            cmmsMeasurementConsumerIds = cmmsMeasurementConsumerIds.toSet(),
            matchExternalBasicReportId = matchExternalBasicReportId,
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
}

fun main(args: Array<String>) = commandLineMain(BackfillBasicReportExternalReportIds(), args)
