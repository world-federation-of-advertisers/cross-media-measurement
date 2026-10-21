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
import kotlinx.coroutines.flow.single
import kotlinx.coroutines.flow.toList
import org.wfanet.measurement.common.db.r2dbc.postgres.PostgresDatabaseClient
import org.wfanet.measurement.common.identity.IdGenerator
import org.wfanet.measurement.gcloud.spanner.AsyncDatabaseClient
import org.wfanet.measurement.gcloud.spanner.statement
import org.wfanet.measurement.internal.reporting.v2.BasicReport
import org.wfanet.measurement.internal.reporting.v2.BasicReportResultDetails
import org.wfanet.measurement.internal.reporting.v2.ListBasicReportsPageToken
import org.wfanet.measurement.internal.reporting.v2.ListBasicReportsPageTokenKt
import org.wfanet.measurement.internal.reporting.v2.ListBasicReportsRequestKt
import org.wfanet.measurement.internal.reporting.v2.ReportingSet
import org.wfanet.measurement.internal.reporting.v2.ReportingSetKt
import org.wfanet.measurement.internal.reporting.v2.StreamReportingSetsRequestKt
import org.wfanet.measurement.internal.reporting.v2.batchGetReportingSetsRequest
import org.wfanet.measurement.internal.reporting.v2.createReportingSetRequest
import org.wfanet.measurement.internal.reporting.v2.ensureSynthesizedCampaignGroupReportingSetRequest
import org.wfanet.measurement.internal.reporting.v2.listBasicReportsPageToken
import org.wfanet.measurement.internal.reporting.v2.reportingSet as internalReportingSet
import org.wfanet.measurement.internal.reporting.v2.streamReportingSetsRequest
import org.wfanet.measurement.reporting.deploy.v2.gcloud.spanner.db.BasicReportResult
import org.wfanet.measurement.reporting.deploy.v2.gcloud.spanner.db.readBasicReports
import org.wfanet.measurement.reporting.deploy.v2.gcloud.spanner.db.updateBasicReportResultDetails
import org.wfanet.measurement.reporting.deploy.v2.postgres.readers.ReportingSetReader
import org.wfanet.measurement.reporting.deploy.v2.postgres.writers.CreateReportingSet
import org.wfanet.measurement.reporting.deploy.v2.postgres.writers.EnsureSynthesizedCampaignGroupReportingSet
import org.wfanet.measurement.reporting.service.internal.CampaignGroupInvalidException
import org.wfanet.measurement.reporting.service.internal.ReportingSetNotFoundException

/**
 * Backfills `ReportingUnitComponentSummary.external_reporting_set_id` on stored `BasicReport`s.
 *
 * Every SUCCEEDED `BasicReport`, across all `MeasurementConsumer`s, is examined. Each component
 * summary missing the field is resolved to the Campaign Group child `ReportingSet` whose membership
 * equals the component's `event_group_summaries`; a `ReportingSet` is created for a membership that
 * has no match.
 *
 * No Campaign Group or component `ReportingSet` is created and no `BasicReport` is updated unless
 * all of its component summaries resolve. Campaign Group synthesis is idempotent.
 *
 * Concurrent invocations are not safe. Each run builds its own in-memory view of the existing
 * `ReportingSet`s, and the minted IDs are random, so two runs against the same environment can
 * create duplicate `ReportingSet`s with identical membership without either reporting an error.
 *
 * @param dryRun when true, no write is issued to either database
 */
class BasicReportReportingSetBackfiller(
  private val spannerClient: AsyncDatabaseClient,
  private val postgresClient: PostgresDatabaseClient,
  private val idGenerator: IdGenerator,
  private val dryRun: Boolean,
  /** When set, only BasicReports created after this are examined. */
  private val createTimeAfter: Timestamp? = null,
) {
  /** Identifies a Campaign Group. `ReportingSet` IDs are unique per `MeasurementConsumer`. */
  private data class CampaignGroupKey(
    val cmmsMeasurementConsumerId: String,
    val externalCampaignGroupId: String,
  )

  private data class CampaignGroupResolution(
    val effectiveKey: CampaignGroupKey,
    val reportingSetIdsByMembership: MutableMap<Set<ReportingSet.Primitive.EventGroupKey>, String>,
  )

  private data class CampaignGroupSource(
    val originalKey: CampaignGroupKey,
    val eventGroupKeys: Set<ReportingSet.Primitive.EventGroupKey>,
    val reportingSetResult: ReportingSetReader.Result?,
  )

  private data class ComponentMembershipResolution(
    val membership: Set<ReportingSet.Primitive.EventGroupKey>?,
    val reason: String?,
  )

  private data class EffectiveCampaignGroup(
    val key: CampaignGroupKey,
    val dryRunReportingSet: ReportingSet?,
  )

  /** Outcome counts for a single [run]. */
  data class Result(
    val examined: Int,
    val alreadyValid: Int,
    val updated: Int,
    val skipped: Int,
    val componentSummariesFilled: Int,
    val reportingSetsReused: Int,
    val reportingSetsCreated: Int,
    val unresolvedComponents: Int,
    val unresolvableMetricSetComponents: Int,
    val primitiveCampaignGroupsMigrated: Int,
    val compositeCampaignGroupsMigrated: Int,
    val unresolvableCampaignGroupReferences: Int,
    /** Whether the temporary read-path fallback can be removed after this run. */
    val readyToRemoveReadPathFallback: Boolean,
    /** Earliest create_time among the backfilled BasicReports, if any. */
    val earliestCreateTime: Timestamp?,
    /** Latest create_time among the backfilled BasicReports, if any. */
    val latestCreateTime: Timestamp?,
  ) {
    // Timestamp's own toString is multi-line, which would break this onto several lines.
    override fun toString(): String =
      "Result(examined=$examined, alreadyValid=$alreadyValid, updated=$updated, " +
        "skipped=$skipped, componentSummariesFilled=$componentSummariesFilled, " +
        "reportingSetsReused=$reportingSetsReused, " +
        "reportingSetsCreated=$reportingSetsCreated, " +
        "unresolvedComponents=$unresolvedComponents, " +
        "unresolvableMetricSetComponents=$unresolvableMetricSetComponents, " +
        "primitiveCampaignGroupsMigrated=$primitiveCampaignGroupsMigrated, " +
        "compositeCampaignGroupsMigrated=$compositeCampaignGroupsMigrated, " +
        "unresolvableCampaignGroupReferences=$unresolvableCampaignGroupReferences, " +
        "readyToRemoveReadPathFallback=$readyToRemoveReadPathFallback, " +
        "earliestCreateTime=${formatTime(earliestCreateTime)}, " +
        "latestCreateTime=${formatTime(latestCreateTime)})"
  }

  private var alreadyValid = 0
  private var updated = 0
  private var skipped = 0
  private var componentSummariesFilled = 0
  private val reusedReportingSetIds = mutableSetOf<String>()
  private val mintedReportingSetIds = mutableSetOf<String>()
  private var reportingSetsCreated = 0
  private var unresolvedComponents = 0
  private var unresolvableMetricSetComponents = 0
  private var primitiveCampaignGroupsMigrated = 0
  private var compositeCampaignGroupsMigrated = 0
  private var unresolvableCampaignGroupReferences = 0
  private var earliestCreateTime: Timestamp? = null
  private var latestCreateTime: Timestamp? = null
  private var examined = 0

  private val campaignGroupSources = mutableMapOf<CampaignGroupKey, CampaignGroupSource>()

  /** ReportingSet membership index per Campaign Group, reused across `BasicReport`s. */
  private val campaignGroupResolutions = mutableMapOf<CampaignGroupKey, CampaignGroupResolution>()

  private val unresolvableCampaignGroupKeys = mutableSetOf<CampaignGroupKey>()

  suspend fun run(): Result {
    // BasicReports is interleaved in MeasurementConsumers, so scoping each query to a
    // MeasurementConsumer turns a full table scan into a key-range read.
    val cmmsMeasurementConsumerIds: List<String> =
      spannerClient.readOnlyTransaction().use { txn ->
        txn
          .executeQuery(statement("SELECT CmmsMeasurementConsumerId FROM MeasurementConsumers"))
          .map { it.getString("CmmsMeasurementConsumerId") }
          .toList()
      }
    logger.info { "Scanning ${cmmsMeasurementConsumerIds.size} MeasurementConsumer(s)" }

    for (cmmsMeasurementConsumerId in cmmsMeasurementConsumerIds) {
      backfillMeasurementConsumer(cmmsMeasurementConsumerId)
      // Campaign Groups belong to a single MeasurementConsumer.
      campaignGroupSources.clear()
      campaignGroupResolutions.clear()
    }

    val result =
      Result(
        examined = examined,
        alreadyValid = alreadyValid,
        updated = updated,
        skipped = skipped,
        componentSummariesFilled = componentSummariesFilled,
        reportingSetsReused = reusedReportingSetIds.size,
        reportingSetsCreated = reportingSetsCreated,
        unresolvedComponents = unresolvedComponents,
        unresolvableMetricSetComponents = unresolvableMetricSetComponents,
        primitiveCampaignGroupsMigrated = primitiveCampaignGroupsMigrated,
        compositeCampaignGroupsMigrated = compositeCampaignGroupsMigrated,
        unresolvableCampaignGroupReferences = unresolvableCampaignGroupReferences,
        readyToRemoveReadPathFallback =
          createTimeAfter == null &&
            skipped == 0 &&
            (!dryRun ||
              (updated == 0 &&
                primitiveCampaignGroupsMigrated == 0 &&
                compositeCampaignGroupsMigrated == 0)),
        earliestCreateTime = earliestCreateTime,
        latestCreateTime = latestCreateTime,
      )
    printSummary(result)
    logger.info { result.toString() }
    return result
  }

  /** Pages through one MeasurementConsumer's SUCCEEDED BasicReports. */
  private suspend fun backfillMeasurementConsumer(measurementConsumerId: String) {
    var pageToken: ListBasicReportsPageToken? = null
    do {
      val page: List<BasicReportResult> =
        spannerClient.readOnlyTransaction().use { txn ->
          txn
            .readBasicReports(
              filter =
                ListBasicReportsRequestKt.filter {
                  cmmsMeasurementConsumerId = measurementConsumerId
                  state = BasicReport.State.SUCCEEDED
                  if (this@BasicReportReportingSetBackfiller.createTimeAfter != null) {
                    createTimeAfter = this@BasicReportReportingSetBackfiller.createTimeAfter
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
        backfillBasicReport(basicReportResult)
      }

      pageToken =
        if (hasNextPage) {
          val last = basicReportResults.last().basicReport
          listBasicReportsPageToken {
            lastBasicReport =
              ListBasicReportsPageTokenKt.previousPageEnd {
                createTime = last.createTime
                cmmsMeasurementConsumerId = last.cmmsMeasurementConsumerId
                externalBasicReportId = last.externalBasicReportId
              }
          }
        } else {
          null
        }
    } while (pageToken != null)
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
    val componentSummaries =
      resultDetailsBuilder.resultGroupsBuilderList
        .flatMap { it.resultsBuilderList }
        .flatMap {
          it.metadataBuilder.reportingUnitSummaryBuilder.reportingUnitComponentSummaryBuilderList
        }
    val emptyComponentSummaries = componentSummaries.filter { it.externalReportingSetId.isEmpty() }
    if (emptyComponentSummaries.isEmpty()) {
      alreadyValid++
      return
    }

    val campaignGroupKey =
      CampaignGroupKey(basicReport.cmmsMeasurementConsumerId, basicReport.externalCampaignGroupId)
    if (campaignGroupKey in unresolvableCampaignGroupKeys) {
      recordUnresolvableCampaignGroup(externalBasicReportId, campaignGroupKey)
      return
    }
    val campaignGroupSource =
      try {
        campaignGroupSources.getOrPut(campaignGroupKey) {
          readCampaignGroupSource(campaignGroupKey)
        }
      } catch (e: CampaignGroupInvalidException) {
        recordUnresolvableCampaignGroup(externalBasicReportId, campaignGroupKey)
        return
      } catch (e: ReportingSetNotFoundException) {
        recordUnresolvableCampaignGroup(externalBasicReportId, campaignGroupKey)
        return
      }

    val campaignGroupChanged = campaignGroupSource.reportingSetResult != null

    // Existing component IDs refer to the old Campaign Group. Re-resolve all of them when the
    // Campaign Group changes so the migrated BasicReport cannot mix old and new associations.
    val componentSummariesToResolve =
      if (campaignGroupChanged) componentSummaries else emptyComponentSummaries

    // Resolution is separated from minting so that the Postgres writes are gated on the same
    // all-or-nothing guarantee as the Spanner write. Minting inline would leave a ReportingSet
    // orphaned whenever a later component summary in the same BasicReport turns out
    // unresolvable, since the BasicReport itself is then skipped.
    //
    // The component's own EventGroups are what the BasicReport was computed over. The internal
    // EventGroupSummary carries only the EventGroup ID. Prefer the component's DataProvider when
    // present; otherwise infer it only when the Campaign Group universe has exactly one match.
    val membershipResolutions: List<ComponentMembershipResolution> =
      componentSummariesToResolve.map { componentSummary ->
        @Suppress("DEPRECATION") // Only source of membership for these BasicReports.
        resolveComponentMembership(
          componentSummary.cmmsDataProviderId,
          componentSummary.eventGroupSummariesList.map { it.cmmsEventGroupId },
          campaignGroupSource.eventGroupKeys,
        )
      }

    var unresolved = 0
    for ((componentSummary, resolution) in componentSummariesToResolve.zip(membershipResolutions)) {
      if (resolution.membership == null) {
        logger.warning {
          "BasicReport $externalBasicReportId has an unresolvable component summary for " +
            "DataProvider ${componentSummary.cmmsDataProviderId}: ${resolution.reason}."
        }
        unresolved++
      }
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

    val campaignGroupResolution =
      try {
        campaignGroupResolutions.getOrPut(campaignGroupKey) {
          materializeCampaignGroup(campaignGroupSource)
        }
      } catch (e: CampaignGroupInvalidException) {
        recordUnresolvableCampaignGroup(externalBasicReportId, campaignGroupKey)
        return
      } catch (e: ReportingSetNotFoundException) {
        recordUnresolvableCampaignGroup(externalBasicReportId, campaignGroupKey)
        return
      }
    val effectiveCampaignGroupKey = campaignGroupResolution.effectiveKey
    val reportingSetIdsByMembership = campaignGroupResolution.reportingSetIdsByMembership

    // Every component summary resolved, so minting cannot strand a ReportingSet.
    for ((componentSummary, resolution) in componentSummariesToResolve.zip(membershipResolutions)) {
      val membership: Set<ReportingSet.Primitive.EventGroupKey> =
        checkNotNull(resolution.membership)
      val existingId: String? = reportingSetIdsByMembership[membership]
      if (existingId != null) {
        componentSummary.externalReportingSetId = existingId
        componentSummariesFilled++
        // A ReportingSet this run created and then found again in the index is not a reuse.
        if (existingId !in mintedReportingSetIds) {
          reusedReportingSetIds.add(existingId)
        }
        continue
      }

      val mintedId: String = mintReportingSet(effectiveCampaignGroupKey, membership)
      reportingSetIdsByMembership[membership] = mintedId
      componentSummary.externalReportingSetId = mintedId
      componentSummariesFilled++
      mintedReportingSetIds.add(mintedId)
      reportingSetsCreated++
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
        externalCampaignGroupId = effectiveCampaignGroupKey.externalCampaignGroupId,
        resultDetails = resultDetails,
      )
    }
    updated++
  }

  private fun recordUnresolvableCampaignGroup(
    externalBasicReportId: String,
    campaignGroupKey: CampaignGroupKey,
  ) {
    unresolvableCampaignGroupKeys.add(campaignGroupKey)
    unresolvableCampaignGroupReferences++
    skipped++
    logger.warning {
      "BasicReport $externalBasicReportId references ReportingSet " +
        "${campaignGroupKey.externalCampaignGroupId} as its Campaign Group, but it cannot be " +
        "repaired automatically. Leaving the BasicReport unchanged and marking the environment " +
        "not ready to remove the read-path fallback."
    }
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

  private fun resolveComponentMembership(
    cmmsDataProviderId: String,
    cmmsEventGroupIds: List<String>,
    campaignGroupEventGroupKeys: Set<ReportingSet.Primitive.EventGroupKey>,
  ): ComponentMembershipResolution {
    if (cmmsEventGroupIds.isEmpty()) {
      return ComponentMembershipResolution(null, "it has no EventGroups")
    }

    val membership = mutableSetOf<ReportingSet.Primitive.EventGroupKey>()
    for (cmmsEventGroupId in cmmsEventGroupIds) {
      if (cmmsDataProviderId.isNotEmpty()) {
        membership +=
          ReportingSetKt.PrimitiveKt.eventGroupKey {
            this.cmmsDataProviderId = cmmsDataProviderId
            this.cmmsEventGroupId = cmmsEventGroupId
          }
        continue
      }

      val matchingEventGroupKeys: List<ReportingSet.Primitive.EventGroupKey> =
        campaignGroupEventGroupKeys.filter { it.cmmsEventGroupId == cmmsEventGroupId }
      if (matchingEventGroupKeys.size != 1) {
        val reason =
          if (matchingEventGroupKeys.isEmpty()) {
            "EventGroup $cmmsEventGroupId is not in the Campaign Group universe"
          } else {
            "EventGroup $cmmsEventGroupId belongs to multiple DataProviders in the Campaign " +
              "Group universe"
          }
        return ComponentMembershipResolution(null, reason)
      }
      membership += matchingEventGroupKeys.single()
    }

    val eventGroupKeysOutsideCampaignGroup: Set<ReportingSet.Primitive.EventGroupKey> =
      membership - campaignGroupEventGroupKeys
    if (eventGroupKeysOutsideCampaignGroup.isNotEmpty()) {
      return ComponentMembershipResolution(
        null,
        "its EventGroups are not all in the Campaign Group universe",
      )
    }

    return ComponentMembershipResolution(membership, null)
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
        "Unbackfilled (blocking)" to result.skipped.toString(),
        "Component summaries filled" to result.componentSummariesFilled.toString(),
        "Backfilled create_time from" to formatTime(result.earliestCreateTime),
        "Backfilled create_time to" to formatTime(result.latestCreateTime),
        "ReportingSets reused" to result.reportingSetsReused.toString(),
        "ReportingSets created" to result.reportingSetsCreated.toString(),
        "Unresolved components" to result.unresolvedComponents.toString(),
        "Unresolvable metric_set components" to result.unresolvableMetricSetComponents.toString(),
        "Primitive Campaign Groups migrated" to result.primitiveCampaignGroupsMigrated.toString(),
        "Composite Campaign Groups migrated" to result.compositeCampaignGroupsMigrated.toString(),
        "Unresolvable Campaign Group references" to
          result.unresolvableCampaignGroupReferences.toString(),
        "Ready to remove read-path fallback" to
          if (result.readyToRemoveReadPathFallback) "yes" else "no",
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

  private suspend fun readCampaignGroupSource(
    campaignGroupKey: CampaignGroupKey
  ): CampaignGroupSource {
    val reportingSetResult: ReportingSetReader.Result = readReportingSet(campaignGroupKey)
    val reportingSet: ReportingSet = reportingSetResult.reportingSet
    if (
      reportingSet.externalCampaignGroupId == reportingSet.externalReportingSetId &&
        reportingSet.hasPrimitive() &&
        reportingSet.primitive.eventGroupKeysCount > 0
    ) {
      return CampaignGroupSource(
        originalKey = campaignGroupKey,
        eventGroupKeys = reportingSet.primitive.eventGroupKeysList.toSet(),
        reportingSetResult = null,
      )
    }
    if (reportingSet.externalCampaignGroupId.isNotEmpty()) {
      throw CampaignGroupInvalidException(
        campaignGroupKey.cmmsMeasurementConsumerId,
        campaignGroupKey.externalCampaignGroupId,
      )
    }
    val eventGroupKeys: Set<ReportingSet.Primitive.EventGroupKey> =
      when {
        reportingSet.hasPrimitive() -> reportingSet.primitive.eventGroupKeysList.toSet()
        reportingSet.hasComposite() ->
          collectPrimitiveEventGroupKeys(
            campaignGroupKey,
            reportingSet,
            mutableMapOf(),
            mutableSetOf(),
          )
        else -> emptySet()
      }
    if (eventGroupKeys.isEmpty()) {
      throw CampaignGroupInvalidException(
        campaignGroupKey.cmmsMeasurementConsumerId,
        campaignGroupKey.externalCampaignGroupId,
      )
    }
    return CampaignGroupSource(
      originalKey = campaignGroupKey,
      eventGroupKeys = eventGroupKeys,
      reportingSetResult = reportingSetResult,
    )
  }

  private suspend fun materializeCampaignGroup(
    source: CampaignGroupSource
  ): CampaignGroupResolution {
    val effectiveCampaignGroup: EffectiveCampaignGroup =
      if (source.reportingSetResult == null) {
        EffectiveCampaignGroup(source.originalKey, null)
      } else if (source.reportingSetResult.reportingSet.hasPrimitive()) {
        migratePrimitiveCampaignGroup(source)
      } else {
        migrateCompositeCampaignGroup(source)
      }
    return readCampaignGroupMembershipIndex(source, effectiveCampaignGroup)
  }

  /** Indexes the Campaign Group's unfiltered primitive children by their EventGroup membership. */
  private suspend fun readCampaignGroupMembershipIndex(
    source: CampaignGroupSource,
    effectiveCampaignGroup: EffectiveCampaignGroup,
  ): CampaignGroupResolution {
    val readContext = postgresClient.readTransaction()
    val reportingSets: MutableList<ReportingSet> =
      try {
        ReportingSetReader(readContext)
          .readReportingSets(
            streamReportingSetsRequest {
              filter =
                StreamReportingSetsRequestKt.filter {
                  cmmsMeasurementConsumerId = effectiveCampaignGroup.key.cmmsMeasurementConsumerId
                  externalCampaignGroupId = effectiveCampaignGroup.key.externalCampaignGroupId
                }
              limit = Int.MAX_VALUE
            }
          )
          .map { it.reportingSet }
          .toList()
          .toMutableList()
      } finally {
        readContext.close()
      }
    if (effectiveCampaignGroup.dryRunReportingSet != null) {
      reportingSets.add(effectiveCampaignGroup.dryRunReportingSet)
      reportingSets.sortBy { it.externalReportingSetId }
    }

    val campaignGroupReportingSet: ReportingSet =
      reportingSets.singleOrNull {
        it.externalReportingSetId == effectiveCampaignGroup.key.externalCampaignGroupId &&
          it.hasPrimitive()
      }
        ?: throw CampaignGroupInvalidException(
          source.originalKey.cmmsMeasurementConsumerId,
          source.originalKey.externalCampaignGroupId,
        )
    val campaignGroupEventGroupKeys: Set<ReportingSet.Primitive.EventGroupKey> =
      campaignGroupReportingSet.primitive.eventGroupKeysList.toSet()
    if (campaignGroupEventGroupKeys != source.eventGroupKeys) {
      throw CampaignGroupInvalidException(
        source.originalKey.cmmsMeasurementConsumerId,
        source.originalKey.externalCampaignGroupId,
      )
    }

    // Matches the read path: unfiltered primitives only, keyed by exact EventGroup membership.
    return CampaignGroupResolution(
      effectiveKey = effectiveCampaignGroup.key,
      reportingSetIdsByMembership =
        reportingSets
          .filter { it.filter.isEmpty() && it.hasPrimitive() }
          .associateTo(mutableMapOf()) {
            it.primitive.eventGroupKeysList.toSet() to it.externalReportingSetId
          },
    )
  }

  private suspend fun migratePrimitiveCampaignGroup(
    source: CampaignGroupSource
  ): EffectiveCampaignGroup {
    val reportingSetResult: ReportingSetReader.Result = checkNotNull(source.reportingSetResult)
    val effectiveCampaignGroup: EffectiveCampaignGroup =
      synthesizeCampaignGroup(source, reportingSetResult)
    primitiveCampaignGroupsMigrated++
    logger.info {
      val prefix = if (dryRun) "[dry run] Would migrate" else "Migrated"
      "$prefix primitive Campaign Group reference ${source.originalKey.externalCampaignGroupId} " +
        "to Campaign Group ${effectiveCampaignGroup.key.externalCampaignGroupId}"
    }
    return effectiveCampaignGroup
  }

  private suspend fun migrateCompositeCampaignGroup(
    source: CampaignGroupSource
  ): EffectiveCampaignGroup {
    val reportingSetResult: ReportingSetReader.Result = checkNotNull(source.reportingSetResult)
    val effectiveCampaignGroup: EffectiveCampaignGroup =
      synthesizeCampaignGroup(source, reportingSetResult)
    compositeCampaignGroupsMigrated++
    logger.info {
      val prefix = if (dryRun) "[dry run] Would migrate" else "Migrated"
      "$prefix composite Campaign Group reference ${source.originalKey.externalCampaignGroupId} " +
        "to primitive Campaign Group ${effectiveCampaignGroup.key.externalCampaignGroupId}"
    }
    return effectiveCampaignGroup
  }

  private suspend fun synthesizeCampaignGroup(
    source: CampaignGroupSource,
    reportingSetResult: ReportingSetReader.Result,
  ): EffectiveCampaignGroup {
    val newExternalReportingSetId = "a${UUID.randomUUID()}"
    val synthesizedReportingSet = internalReportingSet {
      cmmsMeasurementConsumerId = source.originalKey.cmmsMeasurementConsumerId
      externalReportingSetId = newExternalReportingSetId
      externalCampaignGroupId = newExternalReportingSetId
      primitive = ReportingSetKt.primitive { eventGroupKeys += source.eventGroupKeys }
    }

    val equivalentCampaignGroupReadContext = postgresClient.readTransaction()
    val existingExternalReportingSetId: String? =
      try {
        ReportingSetReader(equivalentCampaignGroupReadContext)
          .readCampaignGroupByEventGroups(
            reportingSetResult.measurementConsumerId,
            source.eventGroupKeys,
          )
          ?.externalReportingSetId
      } finally {
        equivalentCampaignGroupReadContext.close()
      }

    val effectiveReportingSet: ReportingSet
    val dryRunReportingSet: ReportingSet?
    if (dryRun) {
      effectiveReportingSet =
        if (existingExternalReportingSetId == null) synthesizedReportingSet
        else
          readReportingSet(
              source.originalKey.copy(externalCampaignGroupId = existingExternalReportingSetId)
            )
            .reportingSet
      dryRunReportingSet =
        if (existingExternalReportingSetId == null) synthesizedReportingSet else null
    } else {
      effectiveReportingSet =
        EnsureSynthesizedCampaignGroupReportingSet(
            ensureSynthesizedCampaignGroupReportingSetRequest {
              externalReportingSetId = newExternalReportingSetId
              reportingSet = synthesizedReportingSet
            }
          )
          .execute(postgresClient, idGenerator)
      dryRunReportingSet = null
    }

    return EffectiveCampaignGroup(
      CampaignGroupKey(
        source.originalKey.cmmsMeasurementConsumerId,
        effectiveReportingSet.externalReportingSetId,
      ),
      dryRunReportingSet,
    )
  }

  /**
   * Collects the EventGroup universe reachable from [reportingSet]. Set operations and filters
   * affect population semantics, not which EventGroups belong to the Campaign Group universe.
   */
  private suspend fun collectPrimitiveEventGroupKeys(
    campaignGroupKey: CampaignGroupKey,
    reportingSet: ReportingSet,
    membershipByReportingSetId: MutableMap<String, Set<ReportingSet.Primitive.EventGroupKey>>,
    visitingReportingSetIds: MutableSet<String>,
  ): Set<ReportingSet.Primitive.EventGroupKey> {
    membershipByReportingSetId[reportingSet.externalReportingSetId]?.let {
      return it
    }
    if (!visitingReportingSetIds.add(reportingSet.externalReportingSetId)) {
      throw CampaignGroupInvalidException(
        campaignGroupKey.cmmsMeasurementConsumerId,
        campaignGroupKey.externalCampaignGroupId,
      )
    }

    val membership =
      when {
        reportingSet.hasPrimitive() -> reportingSet.primitive.eventGroupKeysList.toSet()
        reportingSet.hasComposite() ->
          collectExpressionEventGroupKeys(
            campaignGroupKey,
            reportingSet.composite,
            membershipByReportingSetId,
            visitingReportingSetIds,
          )
        else ->
          throw CampaignGroupInvalidException(
            campaignGroupKey.cmmsMeasurementConsumerId,
            campaignGroupKey.externalCampaignGroupId,
          )
      }

    visitingReportingSetIds.remove(reportingSet.externalReportingSetId)
    membershipByReportingSetId[reportingSet.externalReportingSetId] = membership
    return membership
  }

  private suspend fun collectExpressionEventGroupKeys(
    campaignGroupKey: CampaignGroupKey,
    expression: ReportingSet.SetExpression,
    membershipByReportingSetId: MutableMap<String, Set<ReportingSet.Primitive.EventGroupKey>>,
    visitingReportingSetIds: MutableSet<String>,
  ): Set<ReportingSet.Primitive.EventGroupKey> {
    val lhs: Set<ReportingSet.Primitive.EventGroupKey> =
      collectOperandEventGroupKeys(
        campaignGroupKey,
        expression.lhs,
        membershipByReportingSetId,
        visitingReportingSetIds,
      )
    val rhs: Set<ReportingSet.Primitive.EventGroupKey> =
      when (expression.rhs.operandCase) {
        ReportingSet.SetExpression.Operand.OperandCase.EXTERNAL_REPORTING_SET_ID,
        ReportingSet.SetExpression.Operand.OperandCase.EXPRESSION ->
          collectOperandEventGroupKeys(
            campaignGroupKey,
            expression.rhs,
            membershipByReportingSetId,
            visitingReportingSetIds,
          )
        ReportingSet.SetExpression.Operand.OperandCase.OPERAND_NOT_SET,
        null -> emptySet()
      }
    return lhs + rhs
  }

  private suspend fun collectOperandEventGroupKeys(
    campaignGroupKey: CampaignGroupKey,
    operand: ReportingSet.SetExpression.Operand,
    membershipByReportingSetId: MutableMap<String, Set<ReportingSet.Primitive.EventGroupKey>>,
    visitingReportingSetIds: MutableSet<String>,
  ): Set<ReportingSet.Primitive.EventGroupKey> {
    return when (operand.operandCase) {
      ReportingSet.SetExpression.Operand.OperandCase.EXTERNAL_REPORTING_SET_ID -> {
        val reportingSet =
          readReportingSet(
              campaignGroupKey.copy(externalCampaignGroupId = operand.externalReportingSetId)
            )
            .reportingSet
        collectPrimitiveEventGroupKeys(
          campaignGroupKey,
          reportingSet,
          membershipByReportingSetId,
          visitingReportingSetIds,
        )
      }
      ReportingSet.SetExpression.Operand.OperandCase.EXPRESSION ->
        collectExpressionEventGroupKeys(
          campaignGroupKey,
          operand.expression,
          membershipByReportingSetId,
          visitingReportingSetIds,
        )
      ReportingSet.SetExpression.Operand.OperandCase.OPERAND_NOT_SET,
      null ->
        throw CampaignGroupInvalidException(
          campaignGroupKey.cmmsMeasurementConsumerId,
          campaignGroupKey.externalCampaignGroupId,
        )
    }
  }

  private suspend fun readReportingSet(
    reportingSetKey: CampaignGroupKey
  ): ReportingSetReader.Result {
    val readContext = postgresClient.readTransaction()
    return try {
      ReportingSetReader(readContext)
        .batchGetReportingSets(
          batchGetReportingSetsRequest {
            cmmsMeasurementConsumerId = reportingSetKey.cmmsMeasurementConsumerId
            externalReportingSetIds += reportingSetKey.externalCampaignGroupId
          }
        )
        .single()
    } finally {
      readContext.close()
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
    // A page is held in memory as parsed protos, so peak heap scales with this. Measured
    // over 20k BasicReports with result details up to 1.5 MiB: 100 peaked at 4.7 GiB of
    // resident memory, 20 at 1.0 GiB, for a 2.4x increase in wall time.
    private const val PAGE_SIZE = 20

    private val logger: Logger = Logger.getLogger(this::class.java.name)

    private fun formatTime(timestamp: Timestamp?): String =
      if (timestamp == null) "-"
      else Instant.ofEpochSecond(timestamp.seconds, timestamp.nanos.toLong()).toString()
  }
}
