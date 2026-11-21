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

import java.time.Duration
import java.time.Instant
import kotlin.properties.Delegates
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import org.wfanet.measurement.common.commandLineMain
import org.wfanet.measurement.common.toDuration
import org.wfanet.measurement.gcloud.spanner.SpannerFlags
import org.wfanet.measurement.gcloud.spanner.usingSpanner
import org.wfanet.measurement.reporting.deploy.v2.gcloud.spanner.db.UnassociatedReportResult
import org.wfanet.measurement.reporting.deploy.v2.gcloud.spanner.db.discardReportResult
import org.wfanet.measurement.reporting.deploy.v2.gcloud.spanner.db.getMeasurementConsumerByCmmsMeasurementConsumerId
import org.wfanet.measurement.reporting.deploy.v2.gcloud.spanner.db.readUnassociatedReportResults
import picocli.CommandLine

/** Deletes old, unassociated report-result trees during an assembly maintenance window. */
@CommandLine.Command(
  name = "CleanupReportResults",
  description = ["Deletes old report-result trees that are not attached to a BasicReport."],
  mixinStandardHelpOptions = true,
  showDefaultValues = true,
)
class CleanupReportResults : Runnable {
  @CommandLine.Spec private lateinit var spec: CommandLine.Model.CommandSpec

  @CommandLine.Mixin private lateinit var spannerFlags: SpannerFlags

  @CommandLine.Option(
    names = ["--cmms-measurement-consumer-id"],
    required = true,
    description = ["MeasurementConsumer whose unassociated result trees may be removed."],
  )
  private lateinit var cmmsMeasurementConsumerId: String

  @CommandLine.ArgGroup(exclusive = true, multiplicity = "1") private lateinit var cutoff: Cutoff

  private class Cutoff {
    @CommandLine.Option(
      names = ["--created-before"],
      description = ["Exclusive creation-time cutoff in RFC 3339 format."],
    )
    var createdBefore: Instant? = null

    @CommandLine.Option(
      names = ["--older-than"],
      converter = [AgeConverter::class],
      description =
        ["Minimum age of result trees, e.g. 7d. Suitable for repeated maintenance runs."],
    )
    var olderThan: Duration? = null
  }

  private class AgeConverter : CommandLine.ITypeConverter<Duration> {
    override fun convert(value: String): Duration {
      require(AGE_PATTERN.matches(value)) { "Expected a duration such as 7d or 12h" }
      return value.toDuration()
    }

    companion object {
      private val AGE_PATTERN = Regex("(?:\\d+(?:ns|ms|s|m|h|d))+")
    }
  }

  @set:CommandLine.Option(
    names = ["--dry-run"],
    description = ["List eligible result trees without deleting them."],
    defaultValue = "true",
    arity = "0..1",
    fallbackValue = "true",
  )
  private var dryRun by Delegates.notNull<Boolean>()

  @set:CommandLine.Option(
    names = ["--assembly-paused"],
    description =
      ["Confirm all assembly workers are paused and in-flight assembly RPCs have finished."],
    defaultValue = "false",
  )
  private var assemblyPaused by Delegates.notNull<Boolean>()

  @set:CommandLine.Option(
    names = ["--limit"],
    description = ["Maximum number of eligible result trees to examine in this run."],
    defaultValue = "1000",
  )
  private var limit by Delegates.notNull<Int>()

  override fun run() {
    if (cmmsMeasurementConsumerId.isEmpty()) {
      invalid("--cmms-measurement-consumer-id must not be empty")
    }
    if (limit <= 0) {
      invalid("--limit must be positive")
    }
    if (!dryRun && !assemblyPaused) {
      invalid(
        "Deletion requires --assembly-paused after pausing workers and draining assembly RPCs"
      )
    }
    val now = Instant.now()
    val age = cutoff.olderThan
    if (age != null && (age.isNegative || age.isZero)) {
      invalid("--older-than must be positive")
    }
    val createdBefore: Instant = cutoff.createdBefore ?: now.minus(requireNotNull(age))
    if (createdBefore > now) {
      invalid("--created-before must not be in the future")
    }

    var examined = 0
    var deleted = 0
    var skipped = 0
    runBlocking {
      spannerFlags.usingSpanner { spanner ->
        val client = spanner.databaseClient
        client
          .singleUse()
          .getMeasurementConsumerByCmmsMeasurementConsumerId(cmmsMeasurementConsumerId)
        var after: UnassociatedReportResult? = null
        try {
          while (examined < limit) {
            val candidates =
              client.readOnlyTransaction().use { txn ->
                txn
                  .readUnassociatedReportResults(
                    cmmsMeasurementConsumerId,
                    createdBefore,
                    minOf(PAGE_SIZE, limit - examined),
                    after,
                  )
                  .toList()
              }
            if (candidates.isEmpty()) {
              break
            }
            for (candidate in candidates) {
              examined++
              if (dryRun) {
                println(
                  "would_delete external_report_result_id=${candidate.externalReportResultId} create_time=${candidate.createTime}"
                )
              } else {
                val removed =
                  client.readWriteTransaction().run { txn ->
                    txn.discardReportResult(
                      cmmsMeasurementConsumerId,
                      candidate.externalReportResultId,
                      createdBefore,
                    )
                  }
                if (removed) {
                  deleted++
                  println("deleted external_report_result_id=${candidate.externalReportResultId}")
                } else {
                  skipped++
                  println("skipped external_report_result_id=${candidate.externalReportResultId}")
                }
              }
            }
            after = candidates.last()
          }
        } finally {
          println(
            "examined=$examined deleted=$deleted skipped=$skipped dry_run=$dryRun limit_reached=${examined == limit}"
          )
        }
      }
    }
  }

  private fun invalid(message: String): Nothing {
    throw CommandLine.ParameterException(spec.commandLine(), message)
  }

  companion object {
    private const val PAGE_SIZE = 100

    @JvmStatic fun main(args: Array<String>) = commandLineMain(CleanupReportResults(), args)
  }
}
