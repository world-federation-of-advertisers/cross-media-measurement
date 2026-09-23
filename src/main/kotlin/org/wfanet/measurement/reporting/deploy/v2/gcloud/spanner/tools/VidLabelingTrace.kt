// Copyright 2026 The Cross-Media Measurement Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.wfanet.measurement.edpaggregator.tools

import java.nio.file.Files
import java.nio.file.Path
import java.security.MessageDigest
import java.time.Clock
import java.time.Duration
import java.time.Instant
import java.time.format.DateTimeParseException
import java.util.concurrent.Callable
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.runBlocking
import org.wfanet.measurement.common.telemetry.CloudLogCollectionTruncatedException
import org.wfanet.measurement.common.telemetry.CloudLogEntry
import org.wfanet.measurement.common.telemetry.CloudLogReader
import org.wfanet.measurement.common.telemetry.CloudTelemetrySourceStatus
import org.wfanet.measurement.common.telemetry.CloudTraceReader
import org.wfanet.measurement.common.telemetry.CloudTraceSpan
import org.wfanet.measurement.common.telemetry.GoogleCloudLogReader
import org.wfanet.measurement.common.telemetry.GoogleCloudTraceReader
import org.wfanet.measurement.common.telemetry.SafeTelemetryText
import org.wfanet.measurement.common.telemetry.buildCloudTelemetryLoggingOptions
import org.wfanet.measurement.common.throttler.MaximumRateThrottler
import picocli.CommandLine
import picocli.CommandLine.Option

internal enum class VidLabelingRoute {
  MEMOIZED,
  NON_MEMOIZED,
  UNKNOWN,
}

internal enum class VidLabelingTraceStatus {
  COMPLETE,
  PARTIAL,
  FAILED,
}

internal enum class VidLabelingExecutionStatus {
  SUCCEEDED,
  FAILED,
  IN_PROGRESS,
  SUPERSEDED,
  NO_WORK,
  UNKNOWN,
}

internal data class VidLabelingStageCoverage(
  val name: String,
  val present: Boolean,
  val evidenceCount: Int,
)

internal data class VidLabelingModelLineGraph(
  val modelLine: String,
  val route: VidLabelingRoute,
  val stages: List<VidLabelingStageCoverage>,
) {
  val missingStages: List<String>
    get() = stages.filterNot { it.present }.map { it.name }
}

internal data class VidLabelingEvidence(
  val timestamp: Instant,
  val sourceProject: String,
  val source: String,
  val service: String,
  val stage: String?,
  val outcome: String?,
  val identifiers: Map<String, String>,
  val traceId: String? = null,
)

internal data class VidLabelingTraceCollection(
  val rawImpressionUpload: String,
  val traceStatus: VidLabelingTraceStatus,
  val executionStatus: VidLabelingExecutionStatus,
  val modelLines: List<VidLabelingModelLineGraph>,
  val evidence: List<VidLabelingEvidence>,
  val sourceStatuses: List<CloudTelemetrySourceStatus>,
  val warnings: List<String>,
)

internal data class VidLabelingTraceRequest(
  val rawImpressionUpload: String,
  val projects: List<String>,
  val startTime: Instant,
  val endTime: Instant,
  val entryLimit: Int = 5_000,
  val expansionRounds: Int = 4,
  val correlationValueLimit: Int = 500,
  val traceIdLimit: Int = 500,
)

/** Collects a bounded, metadata-only view of one VID-labeling workflow. */
internal class VidLabelingTraceCollector(
  private val logReaderFactory: (String) -> CloudLogReader,
  private val spanReader: CloudTraceReader,
) {
  suspend fun collect(request: VidLabelingTraceRequest): VidLabelingTraceCollection {
    require(RAW_UPLOAD_PATTERN.matches(request.rawImpressionUpload)) {
      "Invalid RawImpressionUpload resource name"
    }
    require(request.projects.isNotEmpty()) { "At least one observability project is required" }
    require(request.startTime <= request.endTime) { "startTime must not be after endTime" }
    require(request.entryLimit > 0) { "entryLimit must be positive" }
    require(request.expansionRounds > 0) { "expansionRounds must be positive" }
    require(request.correlationValueLimit > 0) { "correlationValueLimit must be positive" }
    require(request.traceIdLimit > 0) { "traceIdLimit must be positive" }

    val evidence = linkedSetOf<VidLabelingEvidence>()
    val sourceStatuses = mutableListOf<CloudTelemetrySourceStatus>()
    val correlationValues = linkedSetOf(request.rawImpressionUpload)
    val traceIds = linkedSetOf<String>()
    val warnings = mutableListOf<String>()
    var correlationValuesCapped = false
    var traceIdsCapped = false

    var remainingRounds = request.expansionRounds
    while (remainingRounds-- > 0) {
      val originalCorrelationCount = correlationValues.size
      val originalTraceCount = traceIds.size
      for (project in request.projects.distinct()) {
        val logResult = readLogs(project, correlationValues, request, sourceStatuses, warnings)
        evidence += logResult.map { it.toEvidence() }
        traceIdsCapped =
          addBounded(
            traceIds,
            logResult.mapNotNull { it.trace }.map { it.substringAfterLast('/') },
            request.traceIdLimit,
          ) || traceIdsCapped

        val traceLogResult = readTraceLogs(project, traceIds, request, sourceStatuses, warnings)
        evidence += traceLogResult.map { it.toEvidence() }

        val spans =
          readSpans(project, correlationValues, traceIds, request, sourceStatuses, warnings)
        evidence += spans.map { it.toEvidence() }
        traceIdsCapped =
          addBounded(traceIds, spans.map { it.traceId }, request.traceIdLimit) || traceIdsCapped
      }
      val relatedEvidence = retainRelatedEvidence(request.rawImpressionUpload, evidence)
      evidence.clear()
      evidence += relatedEvidence
      correlationValuesCapped =
        addBounded(
          correlationValues,
          evidence
            .flatMap { item ->
              item.identifiers.filterKeys { it in UNIQUE_CORRELATION_FIELDS }.values
            }
            .filter(::isCorrelationValue),
          request.correlationValueLimit,
        ) || correlationValuesCapped
      if (
        correlationValues.size == originalCorrelationCount && traceIds.size == originalTraceCount
      ) {
        break
      }
    }

    if (correlationValuesCapped || traceIdsCapped) {
      val capped = buildList {
        if (correlationValuesCapped) add("correlation values")
        if (traceIdsCapped) add("trace IDs")
      }
      warnings += "Discovery limit reached for " + capped.joinToString(" and ") + "."
      sourceStatuses +=
        CloudTelemetrySourceStatus(
          "collector",
          "correlation discovery",
          "truncated",
          correlationValues.size + traceIds.size,
          correlationValues.size + traceIds.size,
          "Configured " + capped.joinToString(" and ") + " cap reached.",
        )
    }

    val orderedEvidence = evidence.sortedBy { it.timestamp }
    val modelLines = buildModelLineGraphs(orderedEvidence)
    val sourceFailures = sourceStatuses.any { it.status != "complete" }
    val executionStatus = inferExecutionStatus(orderedEvidence, modelLines)
    if (
      executionStatus == VidLabelingExecutionStatus.SUCCEEDED &&
        orderedEvidence.any { it.outcome?.lowercase() in FAILED_OUTCOMES }
    ) {
      warnings += "Earlier failed attempts were followed by successful terminal evidence."
    }
    val traceStatus =
      when {
        orderedEvidence.isEmpty() -> VidLabelingTraceStatus.FAILED
        sourceFailures ||
          modelLines.isEmpty() ||
          modelLines.any { it.missingStages.isNotEmpty() } -> VidLabelingTraceStatus.PARTIAL
        else -> VidLabelingTraceStatus.COMPLETE
      }
    if (modelLines.isEmpty()) {
      warnings += "No model-line correlation evidence was found."
    }
    for (graph in modelLines) {
      if (graph.missingStages.isNotEmpty()) {
        warnings +=
          "Missing " +
            graph.route.name.lowercase().replace('_', '-') +
            " evidence for " +
            graph.modelLine +
            ": " +
            graph.missingStages.joinToString()
      }
    }
    return VidLabelingTraceCollection(
      request.rawImpressionUpload,
      traceStatus,
      executionStatus,
      modelLines,
      orderedEvidence,
      sourceStatuses.distinct(),
      warnings.distinct(),
    )
  }

  private suspend fun readLogs(
    project: String,
    correlationValues: Set<String>,
    request: VidLabelingTraceRequest,
    statuses: MutableList<CloudTelemetrySourceStatus>,
    warnings: MutableList<String>,
  ): List<CloudLogEntry> {
    return try {
      val entries =
        logReaderFactory(project)
          .read(correlationValues, request.startTime, request.endTime, request.entryLimit)
      statuses +=
        CloudTelemetrySourceStatus(project, "Cloud Logging", "complete", entries.size, entries.size)
      entries
    } catch (e: CloudLogCollectionTruncatedException) {
      statuses +=
        CloudTelemetrySourceStatus(
          project,
          "Cloud Logging",
          "truncated",
          e.rawEntriesExamined,
          e.partialEntries.size,
          "The configured read bound was reached.",
        )
      warnings += "Cloud Logging results were truncated for project " + project + "."
      e.partialEntries
    } catch (e: CancellationException) {
      throw e
    } catch (e: Exception) {
      statuses +=
        CloudTelemetrySourceStatus(
          project,
          "Cloud Logging",
          "failed",
          0,
          0,
          e::class.simpleName.orEmpty(),
        )
      warnings += "Cloud Logging collection failed for project " + project + "."
      emptyList()
    }
  }

  private suspend fun readTraceLogs(
    project: String,
    traceIds: Set<String>,
    request: VidLabelingTraceRequest,
    statuses: MutableList<CloudTelemetrySourceStatus>,
    warnings: MutableList<String>,
  ): List<CloudLogEntry> {
    if (traceIds.isEmpty()) return emptyList()
    return try {
      val entries =
        logReaderFactory(project)
          .readTraceIds(traceIds, request.startTime, request.endTime, request.entryLimit)
      statuses +=
        CloudTelemetrySourceStatus(
          project,
          "Cloud Logging by trace ID",
          "complete",
          entries.size,
          entries.size,
        )
      entries
    } catch (e: CloudLogCollectionTruncatedException) {
      statuses +=
        CloudTelemetrySourceStatus(
          project,
          "Cloud Logging by trace ID",
          "truncated",
          e.rawEntriesExamined,
          e.partialEntries.size,
          "The configured read bound was reached.",
        )
      warnings += "Trace-ID log results were truncated for project " + project + "."
      e.partialEntries
    } catch (e: CancellationException) {
      throw e
    } catch (e: Exception) {
      statuses +=
        CloudTelemetrySourceStatus(
          project,
          "Cloud Logging by trace ID",
          "failed",
          0,
          0,
          e::class.simpleName.orEmpty(),
        )
      warnings += "Trace-ID log collection failed for project " + project + "."
      emptyList()
    }
  }

  private suspend fun readSpans(
    project: String,
    correlationValues: Set<String>,
    traceIds: Set<String>,
    request: VidLabelingTraceRequest,
    statuses: MutableList<CloudTelemetrySourceStatus>,
    warnings: MutableList<String>,
  ): List<CloudTraceSpan> {
    return try {
      val spans =
        spanReader.read(
          project,
          correlationValues,
          traceIds,
          request.startTime,
          request.endTime,
          request.entryLimit,
        )
      val truncated = spans.size > request.entryLimit
      val retained = spans.take(request.entryLimit)
      statuses +=
        CloudTelemetrySourceStatus(
          project,
          "Cloud Trace",
          if (truncated) "truncated" else "complete",
          spans.size,
          retained.size,
          if (truncated) "The configured read bound was reached." else "",
        )
      if (truncated) warnings += "Cloud Trace results were truncated for project " + project + "."
      retained
    } catch (e: CancellationException) {
      throw e
    } catch (e: Exception) {
      statuses +=
        CloudTelemetrySourceStatus(
          project,
          "Cloud Trace",
          "failed",
          0,
          0,
          e::class.simpleName.orEmpty(),
        )
      warnings += "Cloud Trace collection failed for project " + project + "."
      emptyList()
    }
  }

  private fun buildModelLineGraphs(
    evidence: List<VidLabelingEvidence>
  ): List<VidLabelingModelLineGraph> {
    val modelLines =
      evidence
        .mapNotNull { it.identifiers[MODEL_LINE] }
        .filter(MODEL_LINE_PATTERN::matches)
        .distinct()
        .sorted()
    return modelLines.map { modelLine ->
      val matching =
        evidence.filter { item ->
          modelLine in item.identifiers.values ||
            item.identifiers.values.any { value -> value.startsWith(modelLine + "/") } ||
            (item.stage == "upload_registration" && item.identifiers.containsKey(RAW_UPLOAD))
        }
      val route = inferRoute(matching)
      val expectedStages =
        when (route) {
          VidLabelingRoute.MEMOIZED -> MEMOIZED_STAGES
          VidLabelingRoute.NON_MEMOIZED -> NON_MEMOIZED_STAGES
          VidLabelingRoute.UNKNOWN -> COMMON_STAGES
        }
      VidLabelingModelLineGraph(
        modelLine,
        route,
        expectedStages.map { stage ->
          val count = matching.count { it.stage == stage }
          VidLabelingStageCoverage(stage, count > 0, count)
        },
      )
    }
  }

  private fun inferRoute(evidence: List<VidLabelingEvidence>): VidLabelingRoute {
    val explicitRoute = evidence.mapNotNull { it.identifiers[ROUTE] }.lastOrNull()?.lowercase()
    return when {
      explicitRoute == "memoized" -> VidLabelingRoute.MEMOIZED
      explicitRoute == "non_memoized" || explicitRoute == "non-memoized" ->
        VidLabelingRoute.NON_MEMOIZED
      evidence.any { it.stage in PHASE_ONE_STAGES } -> VidLabelingRoute.MEMOIZED
      evidence.any { it.stage == "label" } -> VidLabelingRoute.NON_MEMOIZED
      else -> VidLabelingRoute.UNKNOWN
    }
  }

  private fun inferExecutionStatus(
    evidence: List<VidLabelingEvidence>,
    modelLines: List<VidLabelingModelLineGraph>,
  ): VidLabelingExecutionStatus {
    val latestByUnit =
      evidence
        .filter { it.stage != null }
        .groupBy { item -> item.stage to durableUnit(item) }
        .mapValues { (_, attempts) -> attempts.maxBy { it.timestamp } }
        .values
    val latestOutcomes = latestByUnit.mapNotNull { it.outcome?.lowercase() }.toSet()
    val publishedModelLines =
      latestByUnit
        .filter {
          it.stage == "data_availability_publish" && it.outcome?.lowercase() in SUCCEEDED_OUTCOMES
        }
        .mapNotNull { it.identifiers[MODEL_LINE] }
        .toSet()
    return when {
      modelLines.isNotEmpty() && modelLines.all { it.modelLine in publishedModelLines } ->
        VidLabelingExecutionStatus.SUCCEEDED
      latestOutcomes.any { it in FAILED_OUTCOMES } -> VidLabelingExecutionStatus.FAILED
      "started" in latestOutcomes || "in_progress" in latestOutcomes ->
        VidLabelingExecutionStatus.IN_PROGRESS
      "superseded" in latestOutcomes || "stale" in latestOutcomes ->
        VidLabelingExecutionStatus.SUPERSEDED
      "no_work" in latestOutcomes -> VidLabelingExecutionStatus.NO_WORK
      else -> VidLabelingExecutionStatus.UNKNOWN
    }
  }

  private fun durableUnit(item: VidLabelingEvidence): String {
    return listOf(
        IMPRESSION_METADATA,
        "xmm.edpa.vid_labeling_job.name",
        "xmm.edpa.ranker_job.name",
        "xmm.edpa.pool_assignment_job.name",
        RAW_UPLOAD_MODEL_LINE,
        "xmm.edpa.rank_index_blob.name",
        "xmm.edpa.recovery_work_item.name",
        MODEL_LINE,
        RAW_UPLOAD,
      )
      .firstNotNullOfOrNull { item.identifiers[it] } ?: item.service
  }

  private fun retainRelatedEvidence(
    rawImpressionUpload: String,
    candidates: Collection<VidLabelingEvidence>,
  ): List<VidLabelingEvidence> {
    val retained = linkedSetOf<VidLabelingEvidence>()
    val identifiers = linkedSetOf(rawImpressionUpload)
    val traceIds = linkedSetOf<String>()
    var changed: Boolean
    do {
      changed = false
      for (candidate in candidates) {
        if (candidate in retained) continue
        val candidateUpload = candidate.identifiers[RAW_UPLOAD]
        if (candidateUpload != null && candidateUpload != rawImpressionUpload) continue
        val uniqueValues =
          candidate.identifiers.filterKeys { it in UNIQUE_CORRELATION_FIELDS }.values
        val directlyRelated =
          uniqueValues.any { it == rawImpressionUpload || it.startsWith(rawImpressionUpload + "/") }
        if (
          directlyRelated ||
            uniqueValues.any { it in identifiers } ||
            (candidate.traceId != null && candidate.traceId in traceIds)
        ) {
          retained += candidate
          identifiers += uniqueValues
          candidate.traceId?.let { traceIds += it }
          changed = true
        }
      }
    } while (changed)
    return retained.toList()
  }

  private fun CloudLogEntry.toEvidence(): VidLabelingEvidence {
    val fields = SAFE_TEXT.fields(message)
    return VidLabelingEvidence(
      timestamp,
      sourceProject,
      "log",
      service,
      fields[STAGE] ?: inferStage(fields["event"]),
      fields[OUTCOME],
      fields.filterKeys { it in SAFE_IDENTIFIER_FIELDS || it == ROUTE },
      trace?.substringAfterLast('/'),
    )
  }

  private fun CloudTraceSpan.toEvidence(): VidLabelingEvidence {
    return VidLabelingEvidence(
      startTime,
      sourceProject,
      "span",
      service,
      attributes[STAGE] ?: inferStage(name),
      attributes[OUTCOME],
      attributes.filterKeys { it in SAFE_IDENTIFIER_FIELDS || it == ROUTE },
      traceId,
    )
  }

  private fun isCorrelationValue(value: String): Boolean {
    return value.length in 2..1_024 && (value.contains('/') || value.length >= 16)
  }

  private fun inferStage(value: String?): String? {
    val normalized = value.orEmpty().lowercase()
    return when {
      "upload" in normalized && ("register" in normalized || "registration" in normalized) ->
        "upload_registration"
      "pool" in normalized && ("final" in normalized || "parent" in normalized) ->
        "pool_assignment_finalize"
      "pool" in normalized -> "pool_assignment"
      "rank" in normalized && ("final" in normalized || "parent" in normalized) -> "rank_finalize"
      "rank" in normalized -> "rank"
      "label" in normalized && ("final" in normalized || "complete" in normalized) ->
        "label_finalize"
      "label" in normalized -> "label"
      "data_watcher" in normalized -> "data_watcher"
      "availability" in normalized && ("publish" in normalized || "interval" in normalized) ->
        "data_availability_publish"
      "availability" in normalized -> "data_availability_metadata"
      "dispatch" in normalized -> "dispatch"
      else -> null
    }
  }

  private fun addBounded(
    values: LinkedHashSet<String>,
    candidates: Collection<String>,
    limit: Int,
  ): Boolean {
    var capped = false
    for (candidate in candidates.distinct().sorted()) {
      if (candidate in values) continue
      if (values.size == limit) {
        capped = true
      } else {
        values += candidate
      }
    }
    return capped
  }

  companion object {
    private val RAW_UPLOAD_PATTERN = Regex("^dataProviders/([^/]+)/rawImpressionUploads/([^/]+)$")
    private const val STAGE = "xmm.lifecycle.stage"
    private const val OUTCOME = "xmm.outcome"
    private const val ROUTE = "xmm.edpa.label.route"
    private const val MODEL_LINE = "xmm.model_line.name"
    private const val RAW_UPLOAD = "xmm.edpa.raw_impression_upload.name"
    private const val RAW_UPLOAD_MODEL_LINE = "xmm.edpa.raw_impression_upload_model_line.name"
    private const val IMPRESSION_METADATA = "xmm.edpa.impression_metadata.name"

    private val SAFE_IDENTIFIER_FIELDS =
      setOf(
        "xmm.data_provider.name",
        MODEL_LINE,
        "xmm.model_line.names",
        RAW_UPLOAD,
        RAW_UPLOAD_MODEL_LINE,
        "xmm.edpa.pool_assignment_job.name",
        "xmm.edpa.ranker_job.name",
        "xmm.edpa.vid_labeling_job.name",
        "xmm.edpa.rank_index_blob.name",
        "xmm.edpa.rank_index_blob.type",
        IMPRESSION_METADATA,
        "xmm.edpa.recovery_work_item.name",
        "xmm.edpa.pipeline.phase",
        "xmm.gcs.object.generation",
        "xmm.gcs.object.path_hash",
        "xmm.edpa.pool_offset",
        "xmm.edpa.shard_index",
        "xmm.edpa.rank.allocated",
        "xmm.edpa.rank.renewed",
        "xmm.edpa.rank.overflow",
        "xmm.edpa.rank.freed",
        "xmm.edpa.rank.backfill_reused",
        "xmm.edpa.rank.backfill_collisions",
        "xmm.edpa.label.input_file_count",
        "xmm.edpa.label.output_type",
        "xmm.edpa.label.event_date",
        "xmm.edpa.label.expected_finalizations",
        "xmm.edpa.label.done_objects_written",
        "xmm.edpa.label.parents_completed",
        "xmm.edpa.impression_metadata.action",
        "xmm.edpa.availability.interval_start",
        "xmm.edpa.availability.interval_end",
      )
    private val UNIQUE_CORRELATION_FIELDS =
      setOf(
        RAW_UPLOAD,
        RAW_UPLOAD_MODEL_LINE,
        "xmm.edpa.pool_assignment_job.name",
        "xmm.edpa.ranker_job.name",
        "xmm.edpa.vid_labeling_job.name",
        "xmm.edpa.rank_index_blob.name",
        IMPRESSION_METADATA,
        "xmm.edpa.recovery_work_item.name",
      )
    private val MODEL_LINE_PATTERN =
      Regex("^modelProviders/[^/]+/modelSuites/[^/]+/modelLines/[^/]+$")
    private val SAFE_TEXT =
      SafeTelemetryText(SAFE_IDENTIFIER_FIELDS + setOf("event", STAGE, OUTCOME, ROUTE))

    private val PHASE_ONE_STAGES =
      setOf("pool_assignment", "pool_assignment_finalize", "rank", "rank_finalize")
    private val COMMON_STAGES =
      listOf(
        "upload_registration",
        "dispatch",
        "label",
        "label_finalize",
        "data_watcher",
        "data_availability_metadata",
        "data_availability_publish",
      )
    private val NON_MEMOIZED_STAGES = COMMON_STAGES
    private val MEMOIZED_STAGES =
      listOf("upload_registration", "dispatch") + PHASE_ONE_STAGES.sorted() + COMMON_STAGES.drop(2)
    private val FAILED_OUTCOMES = setOf("failed", "error", "aborted")
    private val SUCCEEDED_OUTCOMES = setOf("succeeded", "completed", "published", "resolved")
  }
}

/** Renders only allowlisted operational metadata. Raw log payloads are never included. */
internal object VidLabelingTraceOutput {
  fun render(collection: VidLabelingTraceCollection): String = buildString {
    appendLine("# VID labeling trace")
    appendLine()
    appendLine("- Raw impression upload: `" + safe(collection.rawImpressionUpload) + "`")
    appendLine("- Evidence status: **" + collection.traceStatus.name.lowercase() + "**")
    appendLine("- Execution status: **" + collection.executionStatus.name.lowercase() + "**")
    appendLine()
    appendLine("## Sources")
    appendLine()
    appendLine("| Project | Source | Status | Retained | Note |")
    appendLine("|---|---|---:|---:|---|")
    for (status in collection.sourceStatuses) {
      appendLine(
        "| " +
          safe(status.project) +
          " | " +
          safe(status.source) +
          " | " +
          safe(status.status) +
          " | " +
          status.retained +
          " | " +
          safe(status.note) +
          " |"
      )
    }
    appendLine()
    appendLine("## Route graph")
    appendLine()
    for (graph in collection.modelLines) {
      appendLine("### `" + safe(graph.modelLine) + "` (" + graph.route.name.lowercase() + ")")
      for (stage in graph.stages) {
        appendLine(
          "- [" +
            (if (stage.present) "x" else " ") +
            "] " +
            safe(stage.name) +
            " (" +
            stage.evidenceCount +
            ")"
        )
      }
      appendLine()
    }
    appendLine("## Timeline")
    appendLine()
    appendLine("| Time | Project | Source | Service | Stage | Outcome | Identifiers |")
    appendLine("|---|---|---|---|---|---|---|")
    for (item in collection.evidence) {
      val identifiers =
        item.identifiers.entries
          .sortedBy { it.key }
          .joinToString("<br>") { entry -> safe(entry.key) + "=" + safe(entry.value) }
      appendLine(
        "| " +
          item.timestamp +
          " | " +
          safe(item.sourceProject) +
          " | " +
          safe(item.source) +
          " | " +
          safe(item.service) +
          " | " +
          safe(item.stage.orEmpty()) +
          " | " +
          safe(item.outcome.orEmpty()) +
          " | " +
          identifiers +
          " |"
      )
    }
    if (collection.warnings.isNotEmpty()) {
      appendLine()
      appendLine("## Warnings")
      appendLine()
      for (warning in collection.warnings) appendLine("- " + safe(warning))
    }
  }

  fun renderFailure(rawImpressionUpload: String, throwable: Throwable): String = buildString {
    appendLine("# VID labeling trace")
    appendLine()
    appendLine("- Raw impression upload: `" + safe(rawImpressionUpload) + "`")
    appendLine("- Evidence status: **failed**")
    appendLine("- Failure type: `" + safe(throwable::class.simpleName.orEmpty()) + "`")
  }

  fun artifactFileName(rawImpressionUpload: String): String {
    val match = RAW_UPLOAD_PATTERN.matchEntire(rawImpressionUpload)
    if (match != null) return match.groupValues[1] + "__" + match.groupValues[2] + ".md"
    val digest =
      MessageDigest.getInstance("SHA-256")
        .digest(rawImpressionUpload.toByteArray(Charsets.UTF_8))
        .take(8)
        .joinToString("") { byte -> (byte.toInt() and 0xff).toString(16).padStart(2, '0') }
    return "invalid__" + digest + ".md"
  }

  private fun safe(value: String): String {
    return SafeTelemetryText.markdown(value)
  }

  private val RAW_UPLOAD_PATTERN = Regex("^dataProviders/([^/]+)/rawImpressionUploads/([^/]+)$")
}

internal suspend fun collectVidLabelingTraceBatch(
  rawImpressionUploads: Collection<String>,
  allowPartial: Boolean,
  collect: suspend (String) -> VidLabelingTraceCollection,
  write: (String, String) -> Unit,
): Boolean {
  var failed = false
  for (rawImpressionUpload in rawImpressionUploads.distinct()) {
    try {
      val collection = collect(rawImpressionUpload)
      write(
        VidLabelingTraceOutput.artifactFileName(rawImpressionUpload),
        VidLabelingTraceOutput.render(collection),
      )
      if (!allowPartial && collection.traceStatus != VidLabelingTraceStatus.COMPLETE) failed = true
    } catch (e: CancellationException) {
      throw e
    } catch (e: Exception) {
      failed = true
      write(
        VidLabelingTraceOutput.artifactFileName(rawImpressionUpload),
        VidLabelingTraceOutput.renderFailure(rawImpressionUpload, e),
      )
    }
  }
  return failed
}

@CommandLine.Command(
  name = "vid-labeling-trace",
  mixinStandardHelpOptions = true,
  description = ["Collect a metadata-only VID-labeling trace rooted at a RawImpressionUpload."],
)
internal class VidLabelingTrace : Callable<Int> {
  @Option(
    names = ["--raw-impression-upload"],
    required = true,
    description = ["RawImpressionUpload resource name. Repeat for batch collection."],
  )
  private lateinit var rawImpressionUploads: List<String>

  @Option(
    names = ["--observability-project"],
    required = true,
    description = ["Google Cloud project containing trace or log evidence. Repeat as needed."],
  )
  private lateinit var observabilityProjects: List<String>

  @Option(names = ["--output-dir"], description = ["Artifact directory. Required for a batch."])
  private var outputDirectory: Path? = null

  @Option(names = ["--start-time"], description = ["Inclusive ISO-8601 start time."])
  private var startTime: String? = null

  @Option(names = ["--end-time"], description = ["Inclusive ISO-8601 end time."])
  private var endTime: String? = null

  @Option(names = ["--lookback"], defaultValue = "P14D", description = ["ISO-8601 lookback."])
  private lateinit var lookback: String

  @Option(names = ["--entry-limit"], defaultValue = "5000") private var entryLimit: Int = 5_000

  @Option(names = ["--expansion-rounds"], defaultValue = "4") private var expansionRounds: Int = 4

  @Option(names = ["--correlation-value-limit"], defaultValue = "500")
  private var correlationValueLimit: Int = 500

  @Option(names = ["--trace-id-limit"], defaultValue = "500") private var traceIdLimit: Int = 500

  @Option(names = ["--trace-concurrency"], defaultValue = "4") private var traceConcurrency: Int = 4

  @Option(names = ["--trace-requests-per-second"], defaultValue = "8.0")
  private var traceRequestsPerSecond: Double = 8.0

  @Option(names = ["--logging-requests-per-second"], defaultValue = "4.0")
  private var loggingRequestsPerSecond: Double = 4.0

  @Option(names = ["--include-grpc-payloads"], defaultValue = "false")
  private var includeGrpcPayloads: Boolean = false

  @Option(names = ["--allow-partial"], defaultValue = "false")
  private var allowPartial: Boolean = false

  override fun call(): Int = runBlocking {
    require(rawImpressionUploads.size == 1 || outputDirectory != null) {
      "--output-dir is required when collecting more than one upload"
    }
    val now = Clock.systemUTC().instant()
    val resolvedEndTime = parseInstant(endTime) ?: now
    val resolvedStartTime =
      parseInstant(startTime) ?: resolvedEndTime.minus(Duration.parse(lookback))
    val outputRoot = outputDirectory?.toAbsolutePath()?.normalize()
    outputRoot?.let { Files.createDirectories(it) }
    val spanReader =
      GoogleCloudTraceReader(::vidTraceAttributesFor)
        .withMaxConcurrency(traceConcurrency)
        .withRequestThrottlerFactory { MaximumRateThrottler(traceRequestsPerSecond) }
    val collector =
      VidLabelingTraceCollector(
        logReaderFactory = { project ->
          GoogleCloudLogReader(
              project,
              buildCloudTelemetryLoggingOptions(project).service,
              VID_SAFE_LOG_FIELDS,
              VID_CORRELATION_FIELDS,
              includeGrpcPayloads,
            )
            .withRequestThrottler(MaximumRateThrottler(loggingRequestsPerSecond))
        },
        spanReader = spanReader,
      )
    val failed =
      collectVidLabelingTraceBatch(
        rawImpressionUploads,
        allowPartial,
        collect = { rawImpressionUpload ->
          collector.collect(
            VidLabelingTraceRequest(
              rawImpressionUpload,
              observabilityProjects,
              resolvedStartTime,
              resolvedEndTime,
              entryLimit,
              expansionRounds,
              correlationValueLimit,
              traceIdLimit,
            )
          )
        },
        write = { fileName, contents -> writeArtifact(outputRoot, fileName, contents) },
      )
    if (failed) 1 else 0
  }

  private fun writeArtifact(outputRoot: Path?, fileName: String, contents: String) {
    if (outputRoot == null) {
      print(contents)
      return
    }
    val outputPath = outputRoot.resolve(fileName).normalize()
    check(outputPath.parent == outputRoot) { "Output path escapes --output-dir" }
    Files.writeString(outputPath, contents)
  }

  private fun parseInstant(value: String?): Instant? {
    if (value == null) return null
    return try {
      Instant.parse(value)
    } catch (e: DateTimeParseException) {
      throw IllegalArgumentException("Invalid ISO-8601 instant", e)
    }
  }
}

private val VID_SAFE_LOG_FIELDS =
  setOf(
    "event",
    "xmm.lifecycle.stage",
    "xmm.outcome",
    "xmm.error.type",
    "xmm.error.code",
    "xmm.data_provider.name",
    "xmm.model_line.name",
    "xmm.model_line.names",
    "xmm.edpa.raw_impression_upload.name",
    "xmm.edpa.raw_impression_upload_model_line.name",
    "xmm.edpa.pool_assignment_job.name",
    "xmm.edpa.ranker_job.name",
    "xmm.edpa.vid_labeling_job.name",
    "xmm.edpa.rank_index_blob.name",
    "xmm.edpa.rank_index_blob.type",
    "xmm.edpa.impression_metadata.name",
    "xmm.edpa.recovery_work_item.name",
    "xmm.edpa.pipeline.phase",
    "xmm.edpa.label.route",
    "xmm.gcs.object.generation",
    "xmm.gcs.object.path_hash",
    "xmm.edpa.pool_offset",
    "xmm.edpa.shard_index",
    "xmm.edpa.rank.allocated",
    "xmm.edpa.rank.renewed",
    "xmm.edpa.rank.overflow",
    "xmm.edpa.rank.freed",
    "xmm.edpa.rank.backfill_reused",
    "xmm.edpa.rank.backfill_collisions",
    "xmm.edpa.label.input_file_count",
    "xmm.edpa.label.output_type",
    "xmm.edpa.label.event_date",
    "xmm.edpa.label.expected_finalizations",
    "xmm.edpa.label.done_objects_written",
    "xmm.edpa.label.parents_completed",
    "xmm.edpa.impression_metadata.action",
    "xmm.edpa.availability.interval_start",
    "xmm.edpa.availability.interval_end",
  )

private val VID_CORRELATION_FIELDS =
  setOf(
    "xmm.model_line.name",
    "xmm.edpa.raw_impression_upload.name",
    "xmm.edpa.raw_impression_upload_model_line.name",
    "xmm.edpa.pool_assignment_job.name",
    "xmm.edpa.ranker_job.name",
    "xmm.edpa.vid_labeling_job.name",
    "xmm.edpa.rank_index_blob.name",
    "xmm.edpa.impression_metadata.name",
    "xmm.edpa.recovery_work_item.name",
  )

private fun vidTraceAttributesFor(value: String): Collection<String> {
  return when {
    "/rawImpressionUploadModelLines/" in value ->
      listOf("xmm.edpa.raw_impression_upload_model_line.name")
    "/poolAssignmentJobs/" in value -> listOf("xmm.edpa.pool_assignment_job.name")
    "/rankerJobs/" in value -> listOf("xmm.edpa.ranker_job.name")
    "/vidLabelingJobs/" in value -> listOf("xmm.edpa.vid_labeling_job.name")
    "/rankIndexBlobs/" in value -> listOf("xmm.edpa.rank_index_blob.name")
    "/rawImpressionUploads/" in value -> listOf("xmm.edpa.raw_impression_upload.name")
    "/impressionMetadata/" in value -> listOf("xmm.edpa.impression_metadata.name")
    value.startsWith("workItems/") -> listOf("xmm.edpa.recovery_work_item.name")
    else -> emptyList()
  }
}

fun main(args: Array<String>) {
  val exitCode = CommandLine(VidLabelingTrace()).execute(*args)
  if (exitCode != 0) kotlin.system.exitProcess(exitCode)
}
