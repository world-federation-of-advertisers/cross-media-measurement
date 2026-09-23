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
import org.wfanet.measurement.reporting.deploy.v2.gcloud.spanner.db.readBasicReports
import org.wfanet.measurement.reporting.deploy.v2.gcloud.spanner.db.updateExternalReportId
import org.wfanet.measurement.reporting.deploy.v2.postgres.readers.ReportReader
import org.wfanet.measurement.reporting.service.api.v2alpha.BasicReportKey

/**
 * Backfills `BasicReport.external_report_id` from the associated Postgres `Report`.
 *
 * Only `BasicReport`s in the `SUCCEEDED` state are examined.
 *
 * @param dryRun when true, no database write is issued
 * @param createTimeAfter when set, only BasicReports created after this time are examined
 * @param cmmsMeasurementConsumerIds when non-empty, only BasicReports belonging to these
 *   MeasurementConsumers are examined
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
  )

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

  /** Executes the backfill and returns outcome counts. */
  suspend fun run(): Result {
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

    for (cmmsMeasurementConsumerId in selectedCmmsMeasurementConsumerIds) {
      backfillMeasurementConsumer(cmmsMeasurementConsumerId)
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

  private suspend fun backfillMeasurementConsumer(cmmsMeasurementConsumerId: String) {
    val reportLinkIndex: ReportLinkIndex = readReportLinkIndex(cmmsMeasurementConsumerId)
    var pageToken: ListBasicReportsPageToken? = null
    do {
      val page: List<BasicReportResult> =
        spannerClient.readOnlyTransaction().use { transaction ->
          transaction
            .readBasicReports(
              filter =
                ListBasicReportsRequestKt.filter {
                  this.cmmsMeasurementConsumerId = cmmsMeasurementConsumerId
                  state = BasicReport.State.SUCCEEDED
                  if (this@BasicReportExternalReportIdBackfiller.createTimeAfter != null) {
                    createTimeAfter = this@BasicReportExternalReportIdBackfiller.createTimeAfter
                  }
                },
              limit = PAGE_SIZE + 1,
              pageToken = pageToken,
            )
            .toList()
        }

      val hasNextPage = page.size == PAGE_SIZE + 1
      val basicReportResults = if (hasNextPage) page.subList(0, PAGE_SIZE) else page
      for (basicReportResult in basicReportResults) {
        examined++
        backfillBasicReport(basicReportResult, reportLinkIndex)
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
  }

  private suspend fun backfillBasicReport(
    basicReportResult: BasicReportResult,
    reportLinkIndex: ReportLinkIndex,
  ) {
    val basicReport: BasicReport = basicReportResult.basicReport
    if (basicReport.externalReportId.isNotEmpty()) {
      alreadyValid++
      return
    }

    val basicReportName =
      BasicReportKey(basicReport.cmmsMeasurementConsumerId, basicReport.externalBasicReportId)
        .toName()
    val matchesByBasicReportName: Set<String> =
      reportLinkIndex.externalReportIdsByBasicReportName[basicReportName].orEmpty()
    val matchesByCreateReportRequestId: Set<String> =
      if (basicReport.createReportRequestId.isEmpty()) {
        emptySet()
      } else {
        reportLinkIndex.externalReportIdsByCreateRequestId[basicReport.createReportRequestId]
          .orEmpty()
      }
    val matchesByExternalBasicReportId: Set<String> =
      if (
        matchExternalBasicReportId &&
          basicReport.externalBasicReportId in reportLinkIndex.externalReportIds
      ) {
        setOf(basicReport.externalBasicReportId)
      } else {
        emptySet()
      }
    val externalReportIds: Set<String> =
      matchesByBasicReportName + matchesByCreateReportRequestId + matchesByExternalBasicReportId

    if (externalReportIds.isEmpty()) {
      logger.warning {
        "BasicReport ${basicReport.externalBasicReportId} has no associated Report in Postgres"
      }
      unresolved++
      skipped++
      return
    }
    if (externalReportIds.size > 1) {
      logger.warning {
        "BasicReport ${basicReport.externalBasicReportId} resolves to multiple Reports in Postgres"
      }
      ambiguous++
      skipped++
      return
    }

    val externalReportId: String = externalReportIds.single()
    if (externalReportId in matchesByBasicReportName) {
      matchedByBasicReportName++
    }
    if (externalReportId in matchesByCreateReportRequestId) {
      matchedByCreateReportRequestId++
    }
    if (externalReportId in matchesByExternalBasicReportId) {
      matchedByExternalBasicReportId++
    }
    recordCreateTime(basicReport.createTime)
    if (!dryRun) {
      spannerClient.readWriteTransaction().run { transaction ->
        transaction.updateExternalReportId(
          measurementConsumerId = basicReportResult.measurementConsumerId,
          basicReportId = basicReportResult.basicReportId,
          externalReportId = externalReportId,
        )
      }
    }
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

  companion object {
    private const val PAGE_SIZE = 20

    private val logger: Logger = Logger.getLogger(this::class.java.name)

    private fun formatTime(timestamp: Timestamp?): String =
      if (timestamp == null) "-"
      else Instant.ofEpochSecond(timestamp.seconds, timestamp.nanos.toLong()).toString()
  }
}
