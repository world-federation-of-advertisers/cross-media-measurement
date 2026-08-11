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

import kotlinx.coroutines.runBlocking
import org.wfanet.measurement.common.RandomIdGenerator
import org.wfanet.measurement.common.commandLineMain
import org.wfanet.measurement.common.db.postgres.PostgresFlags
import org.wfanet.measurement.common.db.r2dbc.postgres.PostgresDatabaseClient
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.common.identity.IdGenerator
import org.wfanet.measurement.common.identity.InternalId
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
  @CommandLine.Mixin private lateinit var spannerFlags: SpannerFlags

  @CommandLine.Mixin private lateinit var postgresFlags: PostgresFlags

  @CommandLine.Option(
    names = ["--dry-run"],
    description = ["Report what would change without writing to any database."],
  )
  private var dryRun: Boolean = false

  override fun run() {
    runBlocking {
      val postgresClient = PostgresDatabaseClient.fromFlags(postgresFlags)

      spannerFlags.usingSpanner { spanner: SpannerDatabaseConnector ->
        BasicReportReportingSetBackfiller(
            spannerClient = spanner.databaseClient,
            postgresClient = postgresClient,
            idGenerator = LegacyRandomIdGenerator(),
            dryRun = dryRun,
          )
          .run()
      }
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
