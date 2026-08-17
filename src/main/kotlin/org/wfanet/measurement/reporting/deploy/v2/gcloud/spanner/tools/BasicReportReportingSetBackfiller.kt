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
import java.util.UUID
import java.util.logging.Logger
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.toList
import org.wfanet.measurement.common.db.r2dbc.postgres.PostgresDatabaseClient
import org.wfanet.measurement.common.identity.IdGenerator
import org.wfanet.measurement.gcloud.spanner.AsyncDatabaseClient
import org.wfanet.measurement.internal.reporting.v2.BasicReport
import org.wfanet.measurement.internal.reporting.v2.BasicReportResultDetails
import org.wfanet.measurement.internal.reporting.v2.ListBasicReportsRequestKt
import org.wfanet.measurement.internal.reporting.v2.ReportingSet
import org.wfanet.measurement.internal.reporting.v2.ReportingSetKt
import org.wfanet.measurement.internal.reporting.v2.StreamReportingSetsRequestKt
import org.wfanet.measurement.internal.reporting.v2.createReportingSetRequest
import org.wfanet.measurement.internal.reporting.v2.reportingSet as internalReportingSet
import org.wfanet.measurement.internal.reporting.v2.streamReportingSetsRequest
import org.wfanet.measurement.reporting.deploy.v2.gcloud.spanner.db.BasicReportResult
import org.wfanet.measurement.reporting.deploy.v2.gcloud.spanner.db.readBasicReports
import org.wfanet.measurement.reporting.deploy.v2.gcloud.spanner.db.updateBasicReportResultDetails
import org.wfanet.measurement.reporting.deploy.v2.postgres.readers.ReportingSetReader
import org.wfanet.measurement.reporting.deploy.v2.postgres.writers.CreateReportingSet

/**
 * Backfills `ReportingUnitComponentSummary.external_reporting_set_id` on stored `BasicReport`s.
 *
 * Every SUCCEEDED `BasicReport`, across all `MeasurementConsumer`s, is examined. Each component
 * summary missing the field is resolved to the Campaign Group child `ReportingSet` whose membership
 * equals the component's `event_group_summaries`; a `ReportingSet` is created for a membership that
 * has no match.
 *
 * A `BasicReport` is written only when all of its component summaries resolve.
 *
 * @param dryRun when true, no write is issued to either database
 */
class BasicReportReportingSetBackfiller(
  private val spannerClient: AsyncDatabaseClient,
  private val postgresClient: PostgresDatabaseClient,
  private val idGenerator: IdGenerator,
  private val dryRun: Boolean,
) {
  /** Identifies a Campaign Group. `ReportingSet` IDs are unique per `MeasurementConsumer`. */
  private data class CampaignGroupKey(
    val cmmsMeasurementConsumerId: String,
    val externalCampaignGroupId: String,
  )

  /** Outcome counts for a single [run]. */
  data class Result(
    val examined: Int,
    val alreadyValid: Int,
    val updated: Int,
    val skipped: Int,
    val reportingSetsReused: Int,
    val reportingSetsCreated: Int,
    val unresolvedComponents: Int,
    val unresolvableMetricSetComponents: Int,
    /** Earliest create_time among the backfilled BasicReports, if any. */
    val earliestCreateTime: Timestamp?,
    /** Latest create_time among the backfilled BasicReports, if any. */
    val latestCreateTime: Timestamp?,
  ) {
    // Timestamp's own toString is multi-line, which would break this onto several lines.
    override fun toString(): String =
      "Result(examined=$examined, alreadyValid=$alreadyValid, updated=$updated, " +
        "skipped=$skipped, reportingSetsReused=$reportingSetsReused, " +
        "reportingSetsCreated=$reportingSetsCreated, " +
        "unresolvedComponents=$unresolvedComponents, " +
        "unresolvableMetricSetComponents=$unresolvableMetricSetComponents, " +
        "earliestCreateTime=${formatTime(earliestCreateTime)}, " +
        "latestCreateTime=${formatTime(latestCreateTime)})"
  }

  private var alreadyValid = 0
  private var updated = 0
  private var skipped = 0
  private var reportingSetsReused = 0
  private var reportingSetsCreated = 0
  private var unresolvedComponents = 0
  private var unresolvableMetricSetComponents = 0
  private var earliestCreateTime: Timestamp? = null
  private var latestCreateTime: Timestamp? = null

  /** ReportingSet membership index per Campaign Group, reused across `BasicReport`s. */
  private val reportingSetIdsByCampaignGroup:
    MutableMap<CampaignGroupKey, MutableMap<Set<ReportingSet.Primitive.EventGroupKey>, String>> =
    mutableMapOf()

  suspend fun run(): Result {
    val basicReportResults: List<BasicReportResult> =
      spannerClient.readOnlyTransaction().use { txn ->
        txn
          .readBasicReports(
            ListBasicReportsRequestKt.filter { state = BasicReport.State.SUCCEEDED }
          )
          .toList()
      }
    logger.info { "Examining ${basicReportResults.size} SUCCEEDED BasicReport(s)" }

    for (basicReportResult in basicReportResults) {
      backfillBasicReport(basicReportResult)
    }

    val result =
      Result(
        examined = basicReportResults.size,
        alreadyValid = alreadyValid,
        updated = updated,
        skipped = skipped,
        reportingSetsReused = reportingSetsReused,
        reportingSetsCreated = reportingSetsCreated,
        unresolvedComponents = unresolvedComponents,
        unresolvableMetricSetComponents = unresolvableMetricSetComponents,
        earliestCreateTime = earliestCreateTime,
        latestCreateTime = latestCreateTime,
      )
    printSummary(result)
    logger.info { result.toString() }
    return result
  }

  private suspend fun backfillBasicReport(basicReportResult: BasicReportResult) {
    val basicReport: BasicReport = basicReportResult.basicReport
    val externalBasicReportId: String = basicReport.externalBasicReportId

    val unresolvableComponents: Int =
      basicReport.resultDetails.resultGroupsList.sumOf { resultGroup ->
        resultGroup.resultsList.sumOf { result ->
          result.metricSet.reportingSetComponentsList.count { it.externalReportingSetId.isEmpty() }
        }
      }
    if (unresolvableComponents > 0) {
      logger.warning {
        "BasicReport $externalBasicReportId has $unresolvableComponents " +
          "metric_set.reporting_set_components entries without external_reporting_set_id. " +
          "These carry no membership and cannot be backfilled."
      }
      unresolvableMetricSetComponents += unresolvableComponents
    }

    val resultDetailsBuilder: BasicReportResultDetails.Builder =
      basicReport.resultDetails.toBuilder()
    val emptyComponentSummaries =
      resultDetailsBuilder.resultGroupsBuilderList
        .flatMap { it.resultsBuilderList }
        .flatMap {
          it.metadataBuilder.reportingUnitSummaryBuilder.reportingUnitComponentSummaryBuilderList
        }
        .filter { it.externalReportingSetId.isEmpty() }

    if (emptyComponentSummaries.isEmpty()) {
      alreadyValid++
      return
    }

    val campaignGroupKey =
      CampaignGroupKey(basicReport.cmmsMeasurementConsumerId, basicReport.externalCampaignGroupId)
    val reportingSetIdsByMembership =
      reportingSetIdsByCampaignGroup.getOrPut(campaignGroupKey) {
        readCampaignGroupMembershipIndex(campaignGroupKey)
      }

    var unresolved = 0
    for (componentSummary in emptyComponentSummaries) {
      // The component's own EventGroups are what the BasicReport was computed over. The internal
      // EventGroupSummary carries only the EventGroup ID, so the DataProvider comes from the
      // component summary.
      @Suppress("DEPRECATION") // Only source of membership for these BasicReports.
      val membership: Set<ReportingSet.Primitive.EventGroupKey> =
        componentSummary.eventGroupSummariesList
          .map {
            ReportingSetKt.PrimitiveKt.eventGroupKey {
              cmmsDataProviderId = componentSummary.cmmsDataProviderId
              cmmsEventGroupId = it.cmmsEventGroupId
            }
          }
          .toSet()

      if (membership.isEmpty()) {
        logger.warning {
          "BasicReport $externalBasicReportId has a component summary for DataProvider " +
            "${componentSummary.cmmsDataProviderId} with no EventGroups. Cannot resolve."
        }
        unresolved++
        continue
      }

      val existingId: String? = reportingSetIdsByMembership[membership]
      if (existingId != null) {
        componentSummary.externalReportingSetId = existingId
        reportingSetsReused++
        continue
      }

      val mintedId: String = mintReportingSet(campaignGroupKey, membership)
      reportingSetIdsByMembership[membership] = mintedId
      componentSummary.externalReportingSetId = mintedId
      reportingSetsCreated++
    }

    unresolvedComponents += unresolved
    if (unresolved > 0) {
      logger.warning {
        "BasicReport $externalBasicReportId left with $unresolved unresolved component(s); " +
          "not updating."
      }
      skipped++
      return
    }

    recordCreateTime(basicReport.createTime)
    if (dryRun) {
      updated++
      return
    }

    val resultDetails: BasicReportResultDetails = resultDetailsBuilder.build()
    spannerClient.readWriteTransaction().run { txn ->
      txn.updateBasicReportResultDetails(
        measurementConsumerId = basicReportResult.measurementConsumerId,
        basicReportId = basicReportResult.basicReportId,
        resultDetails = resultDetails,
      )
    }
    updated++
  }

  private fun recordCreateTime(createTime: Timestamp) {
    val earliest = earliestCreateTime
    if (earliest == null || Timestamps.compare(createTime, earliest) < 0) {
      earliestCreateTime = createTime
    }
    val latest = latestCreateTime
    if (latest == null || Timestamps.compare(createTime, latest) > 0) {
      latestCreateTime = createTime
    }
  }

  /**
   * Prints a summary of [result] to stdout.
   *
   * The create_time range covers only the backfilled BasicReports, so it can be compared against
   * the window in which the affected BasicReports were known to have been created.
   */
  private fun printSummary(result: Result) {
    val verb = if (dryRun) "would be" else "were"
    val rows =
      listOf(
        "BasicReports examined" to result.examined.toString(),
        "Already valid" to result.alreadyValid.toString(),
        "Backfilled" to result.updated.toString(),
        "Skipped (unresolved components)" to result.skipped.toString(),
        "Backfilled create_time from" to formatTime(result.earliestCreateTime),
        "Backfilled create_time to" to formatTime(result.latestCreateTime),
        "ReportingSets reused" to result.reportingSetsReused.toString(),
        "ReportingSets created" to result.reportingSetsCreated.toString(),
        "Unresolved components" to result.unresolvedComponents.toString(),
        "Unresolvable metric_set components" to result.unresolvableMetricSetComponents.toString(),
      )
    val width = rows.maxOf { it.first.length }
    println()
    println(if (dryRun) "Backfill summary (dry run, nothing written)" else "Backfill summary")
    for ((label, value) in rows) {
      println("  ${label.padEnd(width)}  $value")
    }
    println("  ${result.updated} BasicReport(s) $verb backfilled.")
    println()
  }

  /** Indexes the Campaign Group's unfiltered primitive children by their EventGroup membership. */
  private suspend fun readCampaignGroupMembershipIndex(
    campaignGroupKey: CampaignGroupKey
  ): MutableMap<Set<ReportingSet.Primitive.EventGroupKey>, String> {
    val readContext = postgresClient.singleUse()
    val reportingSets: List<ReportingSet> =
      try {
        ReportingSetReader(readContext)
          .readReportingSets(
            streamReportingSetsRequest {
              filter =
                StreamReportingSetsRequestKt.filter {
                  cmmsMeasurementConsumerId = campaignGroupKey.cmmsMeasurementConsumerId
                  externalCampaignGroupId = campaignGroupKey.externalCampaignGroupId
                }
              limit = Int.MAX_VALUE
            }
          )
          .map { it.reportingSet }
          .toList()
      } finally {
        readContext.close()
      }

    // Matches the read path: unfiltered primitives only, keyed by exact EventGroup membership.
    return reportingSets
      .filter { it.filter.isEmpty() && it.hasPrimitive() }
      .associateTo(mutableMapOf()) {
        it.primitive.eventGroupKeysList.toSet() to it.externalReportingSetId
      }
  }

  private suspend fun mintReportingSet(
    campaignGroupKey: CampaignGroupKey,
    membership: Set<ReportingSet.Primitive.EventGroupKey>,
  ): String {
    val newExternalReportingSetId = "a${UUID.randomUUID()}"
    if (dryRun) {
      logger.info {
        "[dry run] Would create ReportingSet $newExternalReportingSetId under Campaign Group " +
          "${campaignGroupKey.externalCampaignGroupId} with ${membership.size} EventGroup(s)"
      }
      return newExternalReportingSetId
    }

    val reportingSet: ReportingSet =
      CreateReportingSet(
          createReportingSetRequest {
            externalReportingSetId = newExternalReportingSetId
            reportingSet = internalReportingSet {
              cmmsMeasurementConsumerId = campaignGroupKey.cmmsMeasurementConsumerId
              externalCampaignGroupId = campaignGroupKey.externalCampaignGroupId
              primitive = ReportingSetKt.primitive { eventGroupKeys += membership }
            }
          }
        )
        .execute(postgresClient, idGenerator)
    logger.info {
      "Created ReportingSet ${reportingSet.externalReportingSetId} under Campaign Group " +
        "${campaignGroupKey.externalCampaignGroupId} with ${membership.size} EventGroup(s)"
    }
    return reportingSet.externalReportingSetId
  }

  companion object {
    private val logger: Logger = Logger.getLogger(this::class.java.name)

    private fun formatTime(timestamp: Timestamp?): String =
      if (timestamp == null) "-"
      else Instant.ofEpochSecond(timestamp.seconds, timestamp.nanos.toLong()).toString()
  }
}
