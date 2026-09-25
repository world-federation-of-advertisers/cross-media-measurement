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
import java.util.logging.Logger
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.toList
import org.wfanet.measurement.common.db.r2dbc.postgres.PostgresDatabaseClient
import org.wfanet.measurement.gcloud.spanner.AsyncDatabaseClient
import org.wfanet.measurement.gcloud.spanner.statement
import org.wfanet.measurement.internal.reporting.v2.BasicReport
import org.wfanet.measurement.internal.reporting.v2.ListBasicReportsPageToken
import org.wfanet.measurement.internal.reporting.v2.ListBasicReportsPageTokenKt
import org.wfanet.measurement.internal.reporting.v2.ListBasicReportsRequestKt
import org.wfanet.measurement.internal.reporting.v2.listBasicReportsPageToken
import org.wfanet.measurement.reporting.deploy.v2.gcloud.spanner.db.BasicReportResult
import org.wfanet.measurement.reporting.deploy.v2.gcloud.spanner.db.getBasicReportByExternalId
import org.wfanet.measurement.reporting.deploy.v2.gcloud.spanner.db.readBasicReports
import org.wfanet.measurement.reporting.deploy.v2.gcloud.spanner.db.updateExternalReportIdIfEmpty
import org.wfanet.measurement.reporting.deploy.v2.postgres.readers.ReportReader
import org.wfanet.measurement.reporting.service.api.v2alpha.BasicReportKey

/**
 * Backfills `BasicReport.external_report_id` from the associated Postgres `Report`.
 *
 * Only `BasicReport`s in the `SUCCEEDED` state are eligible for updates. Other rows participate in
 * conflict detection so an existing or inferred link cannot be duplicated.
 *
 * @param dryRun when true, no database write is issued
 * @param createTimeAfter when set, only BasicReports created after this time are eligible for
 *   updates; older rows still participate in conflict detection
 * @param cmmsMeasurementConsumerIds when non-empty, only BasicReports belonging to these
 *   MeasurementConsumers are scanned
 * @param matchExternalBasicReportId when true, an `external_basic_report_id` is considered a match
 *   only when a Postgres Report with the same external ID exists. This must only be enabled for
 *   integrations that deliberately use the same external ID for both resources.
 */
class BasicReportExternalReportIdBackfiller(
  private val spannerClient: AsyncDatabaseClient,
  private val postgresClient: PostgresDatabaseClient,
  private val dryRun: Boolean,
  private val createTimeAfter: Timestamp?,
  private val cmmsMeasurementConsumerIds: Set<String>,
  private val matchExternalBasicReportId: Boolean,
) {
  /** Outcome counts for a single [run]. */
  data class Result(
    val examined: Int,
    val alreadyValid: Int,
    val updated: Int,
    val skipped: Int,
    val matchedByBasicReportName: Int,
    val matchedByCreateReportRequestId: Int,
    val matchedByExternalBasicReportId: Int,
    val unresolved: Int,
    val ambiguous: Int,
    /** Earliest create_time among the backfilled BasicReports, if any. */
    val earliestCreateTime: Timestamp?,
    /** Latest create_time among the backfilled BasicReports, if any. */
    val latestCreateTime: Timestamp?,
  ) {
    // Timestamp's own toString is multi-line, which would break this onto several lines.
    override fun toString(): String =
      "Result(examined=$examined, alreadyValid=$alreadyValid, updated=$updated, " +
        "skipped=$skipped, matchedByBasicReportName=$matchedByBasicReportName, " +
        "matchedByCreateReportRequestId=$matchedByCreateReportRequestId, " +
        "matchedByExternalBasicReportId=$matchedByExternalBasicReportId, " +
        "unresolved=$unresolved, ambiguous=$ambiguous, " +
        "earliestCreateTime=${formatTime(earliestCreateTime)}, " +
        "latestCreateTime=${formatTime(latestCreateTime)})"
  }

  private data class ReportLinkIndex(
    val externalReportIds: Set<String>,
    val externalReportIdsByBasicReportName: Map<String, Set<String>>,
    val externalReportIdsByCreateRequestId: Map<String, Set<String>>,
    val basicReportNamesByExternalReportId: Map<String, Set<String>>,
  )

  private enum class MatchSource(val description: String) {
    BASIC_REPORT_NAME("Report.details.basic_report"),
    CREATE_REPORT_REQUEST_ID("create_report_request_id"),
    EXTERNAL_BASIC_REPORT_ID("same external ID"),
  }

  private data class BasicReportReference(
    val measurementConsumerId: Long,
    val basicReportId: Long,
    val cmmsMeasurementConsumerId: String,
    val externalBasicReportId: String,
    val createTime: Timestamp,
  ) {
    val name: String
      get() = BasicReportKey(cmmsMeasurementConsumerId, externalBasicReportId).toName()
  }

  private data class PlannedUpdate(
    val basicReport: BasicReportReference,
    val externalReportId: String,
    val matchSources: Set<MatchSource>,
  )

  private data class MeasurementConsumerPlan(
    val updates: List<PlannedUpdate>,
    val conflictMessages: List<String>,
  )

  private sealed class UpdateOutcome {
    data object Updated : UpdateOutcome()

    data class AlreadySet(val externalReportId: String) : UpdateOutcome()
  }

  private var examined = 0
  private var alreadyValid = 0
  private var updated = 0
  private var skipped = 0
  private var matchedByBasicReportName = 0
  private var matchedByCreateReportRequestId = 0
  private var matchedByExternalBasicReportId = 0
  private var unresolved = 0
  private var ambiguous = 0
  private var earliestCreateTime: Timestamp? = null
  private var latestCreateTime: Timestamp? = null

  /**
   * Executes the backfill and returns outcome counts.
   *
   * @throws IllegalArgumentException if same-ID matching has no MeasurementConsumer scope or a
   *   requested MeasurementConsumer does not exist
   * @throws IllegalStateException if any candidate mapping is not one-to-one
   */
  suspend fun run(): Result {
    require(!matchExternalBasicReportId || cmmsMeasurementConsumerIds.isNotEmpty()) {
      "Matching by external BasicReport ID requires a MeasurementConsumer scope"
    }

    // BasicReports is interleaved in MeasurementConsumers, so scoping each query to a
    // MeasurementConsumer turns a full table scan into a key-range read.
    val availableCmmsMeasurementConsumerIds: List<String> =
      spannerClient.readOnlyTransaction().use { transaction ->
        transaction
          .executeQuery(statement("SELECT CmmsMeasurementConsumerId FROM MeasurementConsumers"))
          .map { it.getString("CmmsMeasurementConsumerId") }
          .toList()
      }
    val selectedCmmsMeasurementConsumerIds: List<String> =
      if (cmmsMeasurementConsumerIds.isEmpty()) {
        availableCmmsMeasurementConsumerIds
      } else {
        val unknownCmmsMeasurementConsumerIds =
          cmmsMeasurementConsumerIds - availableCmmsMeasurementConsumerIds.toSet()
        require(unknownCmmsMeasurementConsumerIds.isEmpty()) {
          "MeasurementConsumer(s) not found: ${unknownCmmsMeasurementConsumerIds.sorted()}"
        }
        availableCmmsMeasurementConsumerIds.filter { it in cmmsMeasurementConsumerIds }
      }
    logger.info { "Scanning ${selectedCmmsMeasurementConsumerIds.size} MeasurementConsumer(s)" }

    val plans: List<MeasurementConsumerPlan> =
      selectedCmmsMeasurementConsumerIds.map { cmmsMeasurementConsumerId ->
        planMeasurementConsumer(cmmsMeasurementConsumerId)
      }
    val conflictMessages: List<String> = plans.flatMap { it.conflictMessages }
    if (conflictMessages.isNotEmpty()) {
      val message =
        "Conflicting BasicReport-to-Report mappings:\n" + conflictMessages.joinToString("\n")
      if (!dryRun) {
        error(message)
      }
      for (conflictMessage in conflictMessages) {
        logger.warning { conflictMessage }
      }
    }

    for (plan in plans) {
      for (update in plan.updates) {
        applyUpdate(update)
      }
    }

    val result =
      Result(
        examined = examined,
        alreadyValid = alreadyValid,
        updated = updated,
        skipped = skipped,
        matchedByBasicReportName = matchedByBasicReportName,
        matchedByCreateReportRequestId = matchedByCreateReportRequestId,
        matchedByExternalBasicReportId = matchedByExternalBasicReportId,
        unresolved = unresolved,
        ambiguous = ambiguous,
        earliestCreateTime = earliestCreateTime,
        latestCreateTime = latestCreateTime,
      )
    logger.info { result.toString() }
    return result
  }

  private suspend fun planMeasurementConsumer(
    cmmsMeasurementConsumerId: String
  ): MeasurementConsumerPlan {
    val reportLinkIndex: ReportLinkIndex = readReportLinkIndex(cmmsMeasurementConsumerId)
    val claimsByExternalReportId = mutableMapOf<String, MutableMap<String, MutableSet<String>>>()
    for ((externalReportId, basicReportNames) in
      reportLinkIndex.basicReportNamesByExternalReportId) {
      for (basicReportName in basicReportNames) {
        addClaim(
          claimsByExternalReportId,
          externalReportId,
          basicReportName,
          MatchSource.BASIC_REPORT_NAME.description,
        )
      }
    }

    val candidateMatchesByBasicReportName = mutableMapOf<String, Map<String, Set<MatchSource>>>()
    val selectedBasicReportsByName = mutableMapOf<String, BasicReportReference>()
    var pageToken: ListBasicReportsPageToken? = null
    do {
      val page: List<BasicReportResult> =
        spannerClient.readOnlyTransaction().use { transaction ->
          transaction
            .readBasicReports(
              filter =
                ListBasicReportsRequestKt.filter {
                  this.cmmsMeasurementConsumerId = cmmsMeasurementConsumerId
                },
              limit = PAGE_SIZE + 1,
              pageToken = pageToken,
            )
            .toList()
        }

      val hasNextPage = page.size == PAGE_SIZE + 1
      val basicReportResults = if (hasNextPage) page.subList(0, PAGE_SIZE) else page
      for (basicReportResult in basicReportResults) {
        val basicReport: BasicReport = basicReportResult.basicReport
        val basicReportName =
          BasicReportKey(basicReport.cmmsMeasurementConsumerId, basicReport.externalBasicReportId)
            .toName()
        val isSelected =
          basicReport.state == BasicReport.State.SUCCEEDED &&
            (createTimeAfter == null ||
              Timestamps.compare(basicReport.createTime, createTimeAfter) > 0)
        if (isSelected) {
          examined++
        }
        val candidateMatches: Map<String, Set<MatchSource>> =
          findCandidateMatches(basicReport, basicReportName, reportLinkIndex)
        if (basicReport.externalReportId.isNotEmpty()) {
          addClaim(
            claimsByExternalReportId,
            basicReport.externalReportId,
            basicReportName,
            "stored BasicReport.external_report_id",
          )
          if (isSelected) {
            alreadyValid++
          }
        }

        for ((externalReportId, matchSources) in candidateMatches) {
          for (matchSource in matchSources) {
            addClaim(
              claimsByExternalReportId,
              externalReportId,
              basicReportName,
              matchSource.description,
            )
          }
        }
        if (basicReport.externalReportId.isNotEmpty()) {
          continue
        }
        if (!isSelected) {
          continue
        }
        candidateMatchesByBasicReportName[basicReportName] = candidateMatches
        selectedBasicReportsByName[basicReportName] =
          BasicReportReference(
            measurementConsumerId = basicReportResult.measurementConsumerId,
            basicReportId = basicReportResult.basicReportId,
            cmmsMeasurementConsumerId = basicReport.cmmsMeasurementConsumerId,
            externalBasicReportId = basicReport.externalBasicReportId,
            createTime = basicReport.createTime,
          )
        if (candidateMatches.isEmpty()) {
          logger.warning { "BasicReport $basicReportName has no associated Report in Postgres" }
          unresolved++
          skipped++
        }
      }

      pageToken =
        if (hasNextPage) {
          val last = basicReportResults.last().basicReport
          listBasicReportsPageToken {
            lastBasicReport =
              ListBasicReportsPageTokenKt.previousPageEnd {
                createTime = last.createTime
                this.cmmsMeasurementConsumerId = last.cmmsMeasurementConsumerId
                externalBasicReportId = last.externalBasicReportId
              }
          }
        } else {
          null
        }
    } while (pageToken != null)

    val conflictMessages = mutableListOf<String>()
    for ((basicReportName, candidateMatches) in candidateMatchesByBasicReportName) {
      if (candidateMatches.size > 1) {
        conflictMessages +=
          "BasicReport $basicReportName resolves to multiple Reports in Postgres: " +
            formatCandidateMatches(candidateMatches)
      }
    }

    val selectedCandidateExternalReportIds: Set<String> =
      candidateMatchesByBasicReportName.values.flatMapTo(mutableSetOf()) { it.keys }
    val conflictingExternalReportIds = mutableSetOf<String>()
    for ((externalReportId, claimsByBasicReportName) in claimsByExternalReportId) {
      if (
        externalReportId in selectedCandidateExternalReportIds && claimsByBasicReportName.size > 1
      ) {
        conflictingExternalReportIds += externalReportId
        val formattedClaims =
          claimsByBasicReportName.toSortedMap().entries.joinToString { (name, sources) ->
            "$name via ${sources.sorted().joinToString()}"
          }
        conflictMessages +=
          "Report $externalReportId resolves to multiple BasicReports: $formattedClaims"
      }
    }

    val updates = mutableListOf<PlannedUpdate>()
    for ((basicReportName, basicReportReference) in selectedBasicReportsByName) {
      val candidateMatches: Map<String, Set<MatchSource>> =
        candidateMatchesByBasicReportName.getValue(basicReportName)
      if (candidateMatches.isEmpty()) {
        continue
      }
      if (
        candidateMatches.size > 1 ||
          candidateMatches.keys.any { it in conflictingExternalReportIds }
      ) {
        ambiguous++
        skipped++
        continue
      }
      val (externalReportId, matchSources) = candidateMatches.entries.single()
      updates += PlannedUpdate(basicReportReference, externalReportId, matchSources)
    }

    return MeasurementConsumerPlan(updates, conflictMessages)
  }

  private fun findCandidateMatches(
    basicReport: BasicReport,
    basicReportName: String,
    reportLinkIndex: ReportLinkIndex,
  ): Map<String, Set<MatchSource>> {
    val matches = mutableMapOf<String, MutableSet<MatchSource>>()

    fun addMatches(externalReportIds: Set<String>, matchSource: MatchSource) {
      for (externalReportId in externalReportIds) {
        matches.getOrPut(externalReportId) { mutableSetOf() } += matchSource
      }
    }

    addMatches(
      reportLinkIndex.externalReportIdsByBasicReportName[basicReportName].orEmpty(),
      MatchSource.BASIC_REPORT_NAME,
    )
    if (basicReport.createReportRequestId.isNotEmpty()) {
      addMatches(
        reportLinkIndex.externalReportIdsByCreateRequestId[basicReport.createReportRequestId]
          .orEmpty(),
        MatchSource.CREATE_REPORT_REQUEST_ID,
      )
    }
    if (
      matchExternalBasicReportId &&
        basicReport.externalBasicReportId in reportLinkIndex.externalReportIds
    ) {
      addMatches(setOf(basicReport.externalBasicReportId), MatchSource.EXTERNAL_BASIC_REPORT_ID)
    }
    return matches.mapValues { it.value.toSet() }
  }

  private suspend fun applyUpdate(update: PlannedUpdate) {
    if (!dryRun) {
      val outcome: UpdateOutcome =
        spannerClient.readWriteTransaction().run { transaction ->
          if (
            transaction.updateExternalReportIdIfEmpty(
              measurementConsumerId = update.basicReport.measurementConsumerId,
              basicReportId = update.basicReport.basicReportId,
              externalReportId = update.externalReportId,
            )
          ) {
            UpdateOutcome.Updated
          } else {
            UpdateOutcome.AlreadySet(
              transaction
                .getBasicReportByExternalId(
                  update.basicReport.cmmsMeasurementConsumerId,
                  update.basicReport.externalBasicReportId,
                )
                .basicReport
                .externalReportId
            )
          }
        }
      when (outcome) {
        UpdateOutcome.Updated -> Unit
        is UpdateOutcome.AlreadySet -> {
          if (outcome.externalReportId == update.externalReportId) {
            alreadyValid++
          } else {
            logger.warning {
              "Skipping BasicReport ${update.basicReport.name}: external_report_id changed after " +
                "planning to '${outcome.externalReportId}'; planned '${update.externalReportId}'"
            }
            skipped++
          }
          return
        }
      }
    }
    if (MatchSource.BASIC_REPORT_NAME in update.matchSources) {
      matchedByBasicReportName++
    }
    if (MatchSource.CREATE_REPORT_REQUEST_ID in update.matchSources) {
      matchedByCreateReportRequestId++
    }
    if (MatchSource.EXTERNAL_BASIC_REPORT_ID in update.matchSources) {
      matchedByExternalBasicReportId++
    }
    recordCreateTime(update.basicReport.createTime)
    updated++
  }

  private suspend fun readReportLinkIndex(cmmsMeasurementConsumerId: String): ReportLinkIndex {
    val readContext = postgresClient.singleUse()
    val links: List<ReportReader.BasicReportLink> =
      try {
        ReportReader(readContext).readBasicReportLinks(cmmsMeasurementConsumerId)
      } finally {
        readContext.close()
      }

    return ReportLinkIndex(
      externalReportIds = links.mapTo(mutableSetOf()) { it.externalReportId },
      externalReportIdsByBasicReportName =
        links
          .filter { it.basicReport.isNotEmpty() }
          .groupBy({ it.basicReport }, { it.externalReportId })
          .mapValues { it.value.toSet() },
      externalReportIdsByCreateRequestId =
        links
          .filter { it.createReportRequestId.isNotEmpty() }
          .groupBy({ it.createReportRequestId }, { it.externalReportId })
          .mapValues { it.value.toSet() },
      basicReportNamesByExternalReportId =
        links
          .filter { it.basicReport.isNotEmpty() }
          .groupBy({ it.externalReportId }, { it.basicReport })
          .mapValues { it.value.toSet() },
    )
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

  private fun addClaim(
    claimsByExternalReportId: MutableMap<String, MutableMap<String, MutableSet<String>>>,
    externalReportId: String,
    basicReportName: String,
    source: String,
  ) {
    claimsByExternalReportId
      .getOrPut(externalReportId) { mutableMapOf() }
      .getOrPut(basicReportName) { mutableSetOf() }
      .add(source)
  }

  private fun formatCandidateMatches(candidateMatches: Map<String, Set<MatchSource>>): String {
    return candidateMatches.toSortedMap().entries.joinToString { (externalReportId, sources) ->
      "$externalReportId via ${sources.map { it.description }.sorted().joinToString()}"
    }
  }

  companion object {
    private const val PAGE_SIZE = 20

    private val logger: Logger = Logger.getLogger(this::class.java.name)

    private fun formatTime(timestamp: Timestamp?): String =
      if (timestamp == null) "-"
      else Instant.ofEpochSecond(timestamp.seconds, timestamp.nanos.toLong()).toString()
  }
}
