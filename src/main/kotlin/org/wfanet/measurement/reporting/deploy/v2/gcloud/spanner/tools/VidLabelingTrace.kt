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

import com.google.cloud.storage.BlobId
import com.google.cloud.storage.StorageOptions
import java.io.File
import java.nio.file.Files
import java.nio.file.Path
import java.security.MessageDigest
import java.time.Clock
import java.time.Duration
import java.time.Instant
import java.time.format.DateTimeParseException
import java.util.concurrent.Callable
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeoutException
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.TimeoutCancellationException
import kotlinx.coroutines.async
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.runInterruptible
import kotlinx.coroutines.withTimeoutOrNull
import org.wfanet.measurement.api.v2alpha.DataProvidersGrpcKt.DataProvidersCoroutineStub
import org.wfanet.measurement.api.v2alpha.ModelLinesGrpcKt.ModelLinesCoroutineStub
import org.wfanet.measurement.api.v2alpha.ModelRolloutsGrpcKt.ModelRolloutsCoroutineStub
import org.wfanet.measurement.api.v2alpha.ModelShardsGrpcKt.ModelShardsCoroutineStub
import org.wfanet.measurement.common.crypto.SigningCerts
import org.wfanet.measurement.common.grpc.TlsFlags
import org.wfanet.measurement.common.grpc.buildMutualTlsChannel
import org.wfanet.measurement.common.telemetry.CloudLogEntry
import org.wfanet.measurement.common.telemetry.CloudLogReader
import org.wfanet.measurement.common.telemetry.CloudTraceReader
import org.wfanet.measurement.common.telemetry.CloudTraceSpan
import org.wfanet.measurement.common.telemetry.GoogleCloudLogReader
import org.wfanet.measurement.common.telemetry.GoogleCloudTraceReader
import org.wfanet.measurement.common.telemetry.SafeTelemetryText
import org.wfanet.measurement.common.telemetry.buildCloudTelemetryLoggingOptions
import org.wfanet.measurement.common.throttler.MaximumRateThrottler
import org.wfanet.measurement.edpaggregator.telemetry.VidLabelingTraceAttributes
import org.wfanet.measurement.edpaggregator.v1alpha.ImpressionMetadataServiceGrpcKt.ImpressionMetadataServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.v1alpha.PoolAssignmentJobServiceGrpcKt.PoolAssignmentJobServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.v1alpha.RankIndexBlobServiceGrpcKt.RankIndexBlobServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.v1alpha.RankerJobServiceGrpcKt.RankerJobServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadFileServiceGrpcKt.RawImpressionUploadFileServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadModelLineServiceGrpcKt.RawImpressionUploadModelLineServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadServiceGrpcKt.RawImpressionUploadServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.v1alpha.VidLabelingJobServiceGrpcKt.VidLabelingJobServiceCoroutineStub
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemAttemptsGrpcKt.WorkItemAttemptsCoroutineStub
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemsGrpcKt.WorkItemsCoroutineStub
import org.wfanet.measurement.storage.SelectedStorageClient
import picocli.CommandLine
import picocli.CommandLine.Mixin
import picocli.CommandLine.Option

private val DEFAULT_AUTHORITATIVE_SOURCE_TIMEOUT = Duration.ofMinutes(2)
private val DEFAULT_LOGGING_SOURCE_TIMEOUT = Duration.ofMinutes(2)
private val DEFAULT_TRACE_SOURCE_TIMEOUT = Duration.ofMinutes(2)
private val DEFAULT_UPLOAD_TIMEOUT = Duration.ofMinutes(10)
private const val DEFAULT_WORK_ITEM_PAGE_LIMIT = 100

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
  val resource: String = "",
  val authoritativeState: String = "",
  val notApplicable: Boolean = false,
  val authoritativeOnly: Boolean = false,
  val unresolved: Boolean = false,
)

internal data class VidLabelingModelLineGraph(
  val modelLine: String,
  val route: VidLabelingRoute,
  val stages: List<VidLabelingStageCoverage>,
) {
  val missingStages: List<String>
    get() =
      stages
        .filter { !it.present && !it.notApplicable && !it.unresolved }
        .map { it.resource.ifEmpty { it.name } }
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
  val sourceStatuses: List<VidLabelingSourceStatus>,
  val warnings: List<String>,
)

internal data class VidLabelingSourceStatus(
  val source: String,
  val status: String,
  val durationMillis: Long,
  val counts: Map<String, Long> = emptyMap(),
  val note: String = "",
  val project: String = "collector",
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
  val authoritativeSourceTimeout: Duration = DEFAULT_AUTHORITATIVE_SOURCE_TIMEOUT,
  val loggingSourceTimeout: Duration = DEFAULT_LOGGING_SOURCE_TIMEOUT,
  val traceSourceTimeout: Duration = DEFAULT_TRACE_SOURCE_TIMEOUT,
  val workItemPageLimit: Int = DEFAULT_WORK_ITEM_PAGE_LIMIT,
)

private data class SourceAttempt<T>(
  val value: T?,
  val status: String,
  val durationNanos: Long,
  val note: String = "",
)

private data class TraceReference(val project: String, val traceId: String)

private fun parseTraceReference(value: String?, fallbackProject: String): TraceReference? {
  if (value.isNullOrEmpty()) return null
  val match = Regex("^projects/([^/]+)/traces/([^/]+)$").matchEntire(value)
  return if (match == null) {
    TraceReference(fallbackProject, value.substringAfterLast('/'))
  } else {
    TraceReference(match.groupValues[1], match.groupValues[2])
  }
}

private class SourceStatusRecorder {
  private data class SourceKey(val project: String, val source: String)

  private data class MutableStatus(
    var status: String = "complete",
    var durationNanos: Long = 0L,
    val counts: MutableMap<String, Long> = linkedMapOf(),
    val notes: LinkedHashSet<String> = linkedSetOf(),
  )

  private val statuses = linkedMapOf<SourceKey, MutableStatus>()

  fun record(
    source: String,
    status: String,
    durationNanos: Long = 0L,
    counts: Map<String, Long> = emptyMap(),
    note: String = "",
    project: String = "collector",
  ) {
    val current = statuses.getOrPut(SourceKey(project, source)) { MutableStatus() }
    if (STATUS_PRIORITY.getValue(status) > STATUS_PRIORITY.getValue(current.status)) {
      current.status = status
    }
    current.durationNanos += durationNanos.coerceAtLeast(0L)
    for ((name, value) in counts) {
      current.counts[name] = current.counts.getOrDefault(name, 0L) + value
    }
    if (note.isNotEmpty()) current.notes += note
  }

  fun snapshot(): List<VidLabelingSourceStatus> {
    return statuses.entries
      .sortedWith(
        compareBy<Map.Entry<SourceKey, MutableStatus>> {
            SOURCE_ORDER.indexOf(it.key.source).let { index ->
              if (index < 0) Int.MAX_VALUE else index
            }
          }
          .thenBy { it.key.project }
          .thenBy { it.key.source }
      )
      .map { (key, status) ->
        VidLabelingSourceStatus(
          key.source,
          status.status,
          (status.durationNanos + NANOS_PER_MILLISECOND - 1L) / NANOS_PER_MILLISECOND,
          status.counts.toSortedMap(),
          status.notes.joinToString(" "),
          key.project,
        )
      }
  }

  companion object {
    private val STATUS_PRIORITY =
      mapOf("complete" to 0, "truncated" to 1, "failed" to 2, "timed_out" to 3)
    private val SOURCE_ORDER =
      listOf(
        "edpa_pipeline_state",
        "secure_computation_work_items",
        "kingdom_route_state",
        "gcs_and_availability_state",
        "cloud_logging",
        "cloud_trace",
        "correlation_discovery",
      )
    private const val NANOS_PER_MILLISECOND = 1_000_000L
  }
}

private class SourceBudget(timeout: Duration, private val nanoTime: () -> Long) {
  private val timeoutNanos = timeout.toNanos()
  private var consumedNanos = 0L

  fun remainingMillis(): Long {
    val remainingNanos = (timeoutNanos - consumedNanos).coerceAtLeast(0L)
    if (remainingNanos == 0L) return 0L
    return ((remainingNanos + NANOS_PER_MILLISECOND - 1L) / NANOS_PER_MILLISECOND).coerceAtLeast(1L)
  }

  fun start(): Long = nanoTime()

  fun finish(startNanos: Long, timedOutAfterMillis: Long? = null): Long {
    val remainingNanos = (timeoutNanos - consumedNanos).coerceAtLeast(0L)
    val measured = (nanoTime() - startNanos).coerceAtLeast(0L)
    val observed =
      if (timedOutAfterMillis == null) {
        measured
      } else {
        maxOf(measured, timedOutAfterMillis * NANOS_PER_MILLISECOND)
      }
    val charged = observed.coerceAtMost(remainingNanos)
    consumedNanos += charged
    return charged
  }

  companion object {
    private const val NANOS_PER_MILLISECOND = 1_000_000L
  }
}

private val EMPTY_WORK_ITEM_RESOLVER = VidLabelingWorkItemResolver { _, _, _ ->
  VidLabelingWorkItemResolution(emptyList(), emptyList(), false, emptyMap())
}
private val EMPTY_ROUTE_RESOLVER = VidLabelingRouteResolver { _, _ ->
  VidLabelingRouteResolution(emptyMap(), emptyMap())
}

/** Collects a bounded, metadata-only view of one VID-labeling workflow. */
internal class VidLabelingTraceCollector(
  private val logReaderFactory: (String) -> CloudLogReader,
  private val spanReader: CloudTraceReader,
  private val stateResolver: VidLabelingStateResolver,
  private val finalStateResolver: VidLabelingFinalStateResolver,
  private val workItemResolver: VidLabelingWorkItemResolver = EMPTY_WORK_ITEM_RESOLVER,
  private val routeResolver: VidLabelingRouteResolver = EMPTY_ROUTE_RESOLVER,
  private val nanoTime: () -> Long = System::nanoTime,
) {
  private val logReaders = mutableMapOf<String, CloudLogReader>()

  private fun logReader(project: String): CloudLogReader =
    logReaders.getOrPut(project) { logReaderFactory(project) }

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
    requirePositive(request.authoritativeSourceTimeout, "authoritativeSourceTimeout")
    requirePositive(request.loggingSourceTimeout, "loggingSourceTimeout")
    requirePositive(request.traceSourceTimeout, "traceSourceTimeout")
    require(request.workItemPageLimit > 0) { "workItemPageLimit must be positive" }

    val sourceStatuses = SourceStatusRecorder()
    val warnings = mutableListOf<String>()
    val baseAttempt =
      readSource(request.authoritativeSourceTimeout) {
        stateResolver.resolve(request.rawImpressionUpload)
      }
    val baseGraph = baseAttempt.value
    sourceStatuses.record(
      "edpa_pipeline_state",
      baseAttempt.status,
      baseAttempt.durationNanos,
      baseGraph?.counts.orEmpty(),
      baseAttempt.note,
    )
    if (baseGraph == null) {
      warnings += "EDPA pipeline state collection ${baseAttempt.status.replace('_', ' ')}."
      return VidLabelingTraceCollection(
        request.rawImpressionUpload,
        VidLabelingTraceStatus.FAILED,
        VidLabelingExecutionStatus.UNKNOWN,
        emptyList(),
        emptyList(),
        sourceStatuses.snapshot(),
        warnings,
      )
    }

    val authoritativeAttempts = coroutineScope {
      val workItems = async {
        readSource(request.authoritativeSourceTimeout) {
          workItemResolver.resolve(
            request.rawImpressionUpload,
            baseGraph.expectedWorkItems,
            request.workItemPageLimit,
          )
        }
      }
      val routes = async {
        readSource(request.authoritativeSourceTimeout) {
          routeResolver.resolve(
            baseGraph.upload.name.substringBefore("/rawImpressionUploads/"),
            baseGraph.modelLines.map { it.cmmsModelLine },
          )
        }
      }
      val finalState = async {
        readSource(request.authoritativeSourceTimeout) {
          finalStateResolver.resolve(baseGraph.upload, baseGraph.modelLines)
        }
      }
      Triple(workItems.await(), routes.await(), finalState.await())
    }
    val workItemAttempt = authoritativeAttempts.first
    val workItemResolution = workItemAttempt.value
    sourceStatuses.record(
      "secure_computation_work_items",
      if (workItemResolution?.truncated == true) "truncated" else workItemAttempt.status,
      workItemAttempt.durationNanos,
      workItemResolution?.counts.orEmpty(),
      workItemAttempt.note,
    )
    if (workItemResolution == null) {
      warnings +=
        "Secure Computation WorkItem collection ${workItemAttempt.status.replace('_', ' ')}."
    } else if (workItemResolution.truncated) {
      warnings += "Secure Computation WorkItem inventory reached the configured page limit."
    }

    val routeAttempt = authoritativeAttempts.second
    val routeResolution = routeAttempt.value
    sourceStatuses.record(
      "kingdom_route_state",
      routeAttempt.status,
      routeAttempt.durationNanos,
      routeResolution?.counts.orEmpty(),
      routeAttempt.note,
    )
    if (routeResolution == null) {
      warnings += "Kingdom route collection ${routeAttempt.status.replace('_', ' ')}."
    }

    var authoritativeGraph =
      baseGraph.copy(nodes = baseGraph.nodes + workItemResolution?.nodes.orEmpty())
    if (routeResolution != null) authoritativeGraph = authoritativeGraph.withRoutes(routeResolution)
    val finalStateAttempt = authoritativeAttempts.third
    val finalState = finalStateAttempt.value
    sourceStatuses.record(
      "gcs_and_availability_state",
      finalStateAttempt.status,
      finalStateAttempt.durationNanos,
      finalState?.counts.orEmpty(),
      finalStateAttempt.note,
    )
    if (finalState == null) {
      warnings +=
        "GCS and availability state collection ${finalStateAttempt.status.replace('_', ' ')}."
    } else {
      authoritativeGraph =
        authoritativeGraph.copy(nodes = authoritativeGraph.nodes + finalState.nodes)
    }
    val unresolvedExpectedWorkItemNames =
      if (workItemResolution == null || workItemResolution.truncated) {
        baseGraph.expectedWorkItems.flatMap { it.candidateNames }
      } else {
        emptyList()
      }
    val expectedWorkItemNames =
      (unresolvedExpectedWorkItemNames +
          authoritativeGraph.nodes.mapNotNull { it.identifiers[WORK_ITEM] })
        .toSet()
    val retentionSeeds = evidenceRetentionSeeds(authoritativeGraph)
    val evidence = linkedSetOf<VidLabelingEvidence>()
    val correlationValues = linkedSetOf(request.rawImpressionUpload)
    val traceIds = linkedSetOf<String>()
    val traceIdsByProject = mutableMapOf<String, MutableSet<String>>()
    val loggingBudget = SourceBudget(request.loggingSourceTimeout, nanoTime)
    val traceBudget = SourceBudget(request.traceSourceTimeout, nanoTime)
    var correlationValuesCapped = false
    var traceIdsCapped = false
    var expansionRoundsExhausted = false
    var remainingRounds = request.expansionRounds
    val queriedLogCorrelationValues = mutableMapOf<String, MutableSet<String>>()
    val queriedLogTraceIds = mutableMapOf<String, MutableSet<String>>()
    val queriedTraceIds = mutableMapOf<String, MutableSet<String>>()
    val queriedTraceCorrelationValues = mutableMapOf<String, MutableSet<String>>()
    val rootLogDiscoveryProjects = mutableSetOf<String>()
    val rootTraceDiscoveryProjects = mutableSetOf<String>()
    while (remainingRounds-- > 0) {
      val originalCorrelationCount = correlationValues.size
      val originalTraceCount = traceIds.size
      for (project in request.projects.distinct()) {
        val projectLogCorrelationValues =
          queriedLogCorrelationValues.getOrPut(project) { mutableSetOf() }
        val projectLogTraceIds = queriedLogTraceIds.getOrPut(project) { mutableSetOf() }
        val pendingLogCorrelationValues =
          if (rootLogDiscoveryProjects.add(project)) setOf(request.rawImpressionUpload)
          else correlationValues - projectLogCorrelationValues
        val pendingLogTraceIds = traceIds - projectLogTraceIds
        projectLogCorrelationValues += pendingLogCorrelationValues
        projectLogTraceIds += pendingLogTraceIds
        if (pendingLogCorrelationValues.isNotEmpty() || pendingLogTraceIds.isNotEmpty()) {
          val logResult =
            readLogs(
              project,
              pendingLogCorrelationValues,
              pendingLogTraceIds,
              request,
              loggingBudget,
              sourceStatuses,
              warnings,
            )
          evidence += logResult.map { it.toEvidence() }
          val traceReferences = logResult.mapNotNull { parseTraceReference(it.trace, project) }
          val discoveredLogTraceIds = traceReferences.map { it.traceId }
          traceIdsCapped =
            addBounded(traceIds, discoveredLogTraceIds, request.traceIdLimit) || traceIdsCapped
          for (reference in traceReferences.filter { it.traceId in traceIds }) {
            if (reference.project in request.projects) {
              traceIdsByProject
                .getOrPut(reference.project) { mutableSetOf() }
                .add(reference.traceId)
            } else {
              sourceStatuses.record(
                "cloud_trace",
                "failed",
                note = "Trace project was not configured for collection.",
                project = reference.project,
              )
              warnings += "Cloud Trace project ${reference.project} was not configured."
            }
          }
        }

        val projectTraceIds = queriedTraceIds.getOrPut(project) { mutableSetOf() }
        val projectTraceCorrelationValues =
          queriedTraceCorrelationValues.getOrPut(project) { mutableSetOf() }
        val knownProjectTraceIds = traceIdsByProject.getOrPut(project) { mutableSetOf() }
        val pendingTraceIds = knownProjectTraceIds - projectTraceIds
        projectTraceIds += pendingTraceIds
        val rootTraceDiscovery = rootTraceDiscoveryProjects.add(project)
        val pendingTraceCorrelationValues = buildSet {
          if (rootTraceDiscovery) add(request.rawImpressionUpload)
          addAll(
            (correlationValues - projectTraceCorrelationValues).filter { value ->
              GCS_PATH_HASH_PATTERN.matches(value) || value in unresolvedExpectedWorkItemNames
            }
          )
        }
        projectTraceCorrelationValues += pendingTraceCorrelationValues
        if (pendingTraceCorrelationValues.isNotEmpty() || pendingTraceIds.isNotEmpty()) {
          val spans =
            readSpans(
              project,
              pendingTraceCorrelationValues,
              pendingTraceIds,
              knownProjectTraceIds,
              request,
              traceBudget,
              sourceStatuses,
              warnings,
            )
          evidence += spans.map { it.toEvidence() }
          knownProjectTraceIds += spans.map { it.traceId }
          projectTraceIds += spans.map { it.traceId }
          traceIdsCapped =
            addBounded(traceIds, spans.map { it.traceId }, request.traceIdLimit) || traceIdsCapped
        }
      }
      val relatedEvidence =
        retainRelatedEvidence(
          request.rawImpressionUpload,
          expectedWorkItemNames,
          retentionSeeds,
          evidence,
        )
      evidence.clear()
      evidence += relatedEvidence
      correlationValuesCapped =
        addBounded(
          correlationValues,
          authoritativeGraph.correlationValues +
            expectedWorkItemNames +
            evidence
              .asSequence()
              .flatMap { item ->
                item.identifiers
                  .filterKeys { it in DISCOVERY_CORRELATION_FIELDS }
                  .values
                  .asSequence()
              }
              .filter(::isCorrelationValue)
              .toList(),
          request.correlationValueLimit,
        ) || correlationValuesCapped
      if (
        correlationValues.size == originalCorrelationCount && traceIds.size == originalTraceCount
      ) {
        break
      }
      if (remainingRounds == 0) expansionRoundsExhausted = true
    }

    if (correlationValuesCapped || traceIdsCapped || expansionRoundsExhausted) {
      val capped = buildList {
        if (correlationValuesCapped) add("correlation values")
        if (traceIdsCapped) add("trace IDs")
        if (expansionRoundsExhausted) add("expansion rounds")
      }
      warnings += "Discovery limit reached for " + capped.joinToString(" and ") + "."
      sourceStatuses.record(
        "correlation_discovery",
        "truncated",
        counts =
          mapOf(
            "correlation_values" to correlationValues.size.toLong(),
            "trace_ids" to traceIds.size.toLong(),
          ),
        note = "Configured " + capped.joinToString(" and ") + " cap reached.",
      )
    }

    val orderedEvidence = evidence.sortedBy { it.timestamp }
    val modelLines = buildModelLineGraphs(authoritativeGraph, orderedEvidence)
    val finalSourceStatuses = sourceStatuses.snapshot()
    val sourceFailures = finalSourceStatuses.any { it.status != "complete" }
    val noWork =
      authoritativeGraph.upload.registrationComplete && authoritativeGraph.modelLines.isEmpty()
    val executionStatus = inferExecutionStatus(orderedEvidence, modelLines, authoritativeGraph)
    if (
      executionStatus == VidLabelingExecutionStatus.SUCCEEDED &&
        orderedEvidence.any { it.outcome?.lowercase() in FAILED_OUTCOMES }
    ) {
      warnings += "Earlier failed attempts were followed by successful terminal evidence."
    }
    val traceStatus =
      when {
        sourceFailures ||
          orderedEvidence.isEmpty() ||
          (!noWork && modelLines.isEmpty()) ||
          modelLines.any { it.missingStages.isNotEmpty() } -> VidLabelingTraceStatus.PARTIAL
        else -> VidLabelingTraceStatus.COMPLETE
      }
    if (modelLines.isEmpty() && !noWork) {
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
      finalSourceStatuses,
      warnings.distinct(),
    )
  }

  private suspend fun <T> readSource(timeout: Duration, block: suspend () -> T): SourceAttempt<T> {
    val started = nanoTime()
    return try {
      val value = withTimeoutOrNull(timeout.toMillis().coerceAtLeast(1L)) { block() }
      val elapsed = (nanoTime() - started).coerceAtLeast(0L)
      if (value == null) {
        SourceAttempt(null, "timed_out", maxOf(elapsed, timeout.toNanos()))
      } else {
        SourceAttempt(value, "complete", elapsed)
      }
    } catch (e: CancellationException) {
      throw e
    } catch (e: Exception) {
      SourceAttempt(
        null,
        "failed",
        (nanoTime() - started).coerceAtLeast(0L),
        e::class.simpleName.orEmpty(),
      )
    }
  }

  private fun requirePositive(duration: Duration, name: String) {
    require(!duration.isZero && !duration.isNegative) { "$name must be positive" }
  }

  private suspend fun readLogs(
    project: String,
    correlationValues: Set<String>,
    traceIds: Set<String>,
    request: VidLabelingTraceRequest,
    budget: SourceBudget,
    statuses: SourceStatusRecorder,
    warnings: MutableList<String>,
  ): List<CloudLogEntry> {
    val remainingMillis = budget.remainingMillis()
    if (remainingMillis == 0L) {
      statuses.record(
        "cloud_logging",
        "timed_out",
        note = "Source deadline exhausted.",
        project = project,
      )
      warnings += "Cloud Logging collection timed out before querying project $project."
      return emptyList()
    }
    val started = budget.start()
    return try {
      val result =
        logReader(project)
          .readBatch(
            correlationValues,
            traceIds,
            request.startTime,
            request.endTime,
            request.entryLimit,
            Duration.ofMillis(remainingMillis),
          )
      val entries = result.entries.take(request.entryLimit)
      statuses.record(
        "cloud_logging",
        when {
          result.complete -> "complete"
          result.timedOut -> "timed_out"
          result.failed -> "failed"
          else -> "truncated"
        },
        budget.finish(started, if (result.timedOut) remainingMillis else null),
        result.counts.toMap() + ("rows_retained" to entries.size.toLong()),
        result.note,
        project,
      )
      if (result.timedOut) {
        warnings += "Cloud Logging collection timed out for project $project."
      } else if (!result.complete) {
        warnings += "Cloud Logging collection was incomplete for project $project."
      }
      entries
    } catch (e: CancellationException) {
      throw e
    } catch (e: Exception) {
      statuses.record(
        "cloud_logging",
        "failed",
        budget.finish(started),
        emptyMap(),
        e::class.simpleName.orEmpty() + " in " + project,
        project,
      )
      warnings += "Cloud Logging collection failed for project " + project + "."
      emptyList()
    }
  }

  private suspend fun readSpans(
    project: String,
    correlationValues: Set<String>,
    traceIds: Set<String>,
    requiredTraceIds: Set<String>,
    request: VidLabelingTraceRequest,
    budget: SourceBudget,
    statuses: SourceStatusRecorder,
    warnings: MutableList<String>,
  ): List<CloudTraceSpan> {
    val remainingMillis = budget.remainingMillis()
    if (remainingMillis == 0L) {
      statuses.record(
        "cloud_trace",
        "timed_out",
        note = "Source deadline exhausted.",
        project = project,
      )
      warnings += "Cloud Trace collection timed out before querying project $project."
      return emptyList()
    }
    val started = budget.start()
    return try {
      val result =
        spanReader.readBatch(
          project,
          correlationValues,
          traceIds,
          request.startTime,
          request.endTime,
          request.entryLimit,
          request.traceIdLimit,
          requiredTraceIds,
          Duration.ofMillis(remainingMillis),
        )
      val retained = result.entries.take(request.entryLimit)
      statuses.record(
        "cloud_trace",
        when {
          result.complete -> "complete"
          result.timedOut -> "timed_out"
          result.failed -> "failed"
          else -> "truncated"
        },
        budget.finish(started, if (result.timedOut) remainingMillis else null),
        result.counts.toMap() + ("rows_retained" to retained.size.toLong()),
        result.note,
        project,
      )
      if (result.timedOut) {
        warnings += "Cloud Trace collection timed out for project $project."
      } else if (!result.complete) {
        warnings += "Cloud Trace collection was incomplete for project $project."
      }
      retained
    } catch (e: CancellationException) {
      throw e
    } catch (e: Exception) {
      statuses.record(
        "cloud_trace",
        "failed",
        budget.finish(started),
        emptyMap(),
        e::class.simpleName.orEmpty() + " in " + project,
        project,
      )
      warnings += "Cloud Trace collection failed for project " + project + "."
      emptyList()
    }
  }

  private fun buildModelLineGraphs(
    graph: VidLabelingAuthoritativeGraph,
    evidence: List<VidLabelingEvidence>,
  ): List<VidLabelingModelLineGraph> {
    val modelLines = graph.modelLines.map { it.cmmsModelLine }.distinct().sorted()
    return modelLines.map { modelLine ->
      val matching =
        evidence.filter { item ->
          modelLine in item.identifiers.values ||
            item.identifiers.values.any { value -> value.startsWith(modelLine + "/") } ||
            (item.stage == "upload_registration" && item.identifiers.containsKey(RAW_UPLOAD))
        }
      val expectedNodes = graph.nodes.filter { it.modelLine == modelLine || it.modelLine == null }
      val route =
        when (expectedNodes.firstNotNullOfOrNull { it.identifiers[ROUTE] }) {
          "memoized" -> VidLabelingRoute.MEMOIZED
          "non_memoized" -> VidLabelingRoute.NON_MEMOIZED
          else -> inferRoute(matching)
        }
      VidLabelingModelLineGraph(
        modelLine,
        route,
        expectedNodes.map { node ->
          val nodeEvidence = evidence.filter { item -> nodeMatches(node, item) }
          VidLabelingStageCoverage(
            node.stage,
            (nodeEvidence.isNotEmpty() ||
              node.disposition == ExpectedNodeDisposition.AUTHORITATIVE_ONLY) &&
              node.authoritativeState != "MISSING",
            nodeEvidence.size,
            node.id,
            node.authoritativeState,
            node.disposition == ExpectedNodeDisposition.NOT_APPLICABLE,
            node.disposition == ExpectedNodeDisposition.AUTHORITATIVE_ONLY,
            node.disposition == ExpectedNodeDisposition.UNRESOLVED,
          )
        },
      )
    }
  }

  private fun nodeMatches(node: ExpectedTraceNode, evidence: VidLabelingEvidence): Boolean {
    val workItemName = node.identifiers[WORK_ITEM]
    if (workItemName != null) {
      if (evidence.stage != node.stage && evidence.stage != WORK_ITEM_PROCESSING_STAGE) {
        return false
      }
      if (evidence.identifiers[WORK_ITEM] != workItemName) return false

      val generation = node.identifiers[WORK_ITEM_GENERATION]
      if (generation != null && evidence.identifiers[WORK_ITEM_GENERATION] != generation) {
        return false
      }

      val attempt = node.identifiers[WORK_ITEM_ATTEMPT]
      return attempt == null || evidence.identifiers[WORK_ITEM_ATTEMPT] == attempt
    }

    if (evidence.stage != node.stage) return false

    val primaryKey = NODE_MATCH_KEY_PRIORITY.firstOrNull(node.identifiers::containsKey)
    if (primaryKey != null) {
      if (evidence.identifiers[primaryKey] != node.identifiers[primaryKey]) return false
      if (primaryKey == GCS_PATH_HASH && node.identifiers.containsKey(GCS_GENERATION)) {
        val expectedIdentity = node.identifiers.gcsObjectIdentity() ?: return false
        return evidence.identifiers.gcsObjectIdentity() == expectedIdentity
      }
      if (primaryKey == IMPRESSION_METADATA) {
        return evidence.identifiers[MODEL_LINE] == node.identifiers[MODEL_LINE] &&
          evidence.identifiers[GCS_PATH_HASH] == node.identifiers[GCS_PATH_HASH]
      }
      if (
        primaryKey == AVAILABILITY_INTERVAL_START &&
          node.identifiers.containsKey(AVAILABILITY_INTERVAL_END)
      ) {
        return evidence.identifiers[MODEL_LINE] == node.identifiers[MODEL_LINE] &&
          evidence.identifiers[AVAILABILITY_INTERVAL_END] ==
            node.identifiers[AVAILABILITY_INTERVAL_END]
      }
      return true
    }
    return node.identifiers.any { (key, value) -> evidence.identifiers[key] == value }
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
    graph: VidLabelingAuthoritativeGraph,
  ): VidLabelingExecutionStatus {
    if (graph.upload.registrationComplete && graph.modelLines.isEmpty()) {
      return VidLabelingExecutionStatus.NO_WORK
    }
    if (graph.modelLines.any { it.state.name == "FAILED" }) {
      return VidLabelingExecutionStatus.FAILED
    }
    val availabilityNodes = graph.nodes.filter { it.stage == "data_availability_publish" }
    if (
      graph.modelLines.isNotEmpty() &&
        graph.modelLines.all { it.state.name == "COMPLETED" } &&
        availabilityNodes.isNotEmpty() &&
        availabilityNodes.all { it.authoritativeState == "PUBLISHED" }
    ) {
      return VidLabelingExecutionStatus.SUCCEEDED
    }
    if (graph.modelLines.any { it.state.name in ACTIVE_MODEL_LINE_STATES }) {
      return VidLabelingExecutionStatus.IN_PROGRESS
    }
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
        "xmm.edpa.replaces_raw_impression_upload.name",
        "xmm.edpa.upload_healing_operation.name",
        "xmm.edpa.recovery_predecessor_raw_impression_upload.name",
        "xmm.work_item.name",
        "xmm.work_item_attempt.name",
        "xmm.work_item.generation",
        "xmm.error.type",
        "xmm.error.code",
        MODEL_LINE,
        RAW_UPLOAD,
      )
      .firstNotNullOfOrNull { item.identifiers[it] } ?: item.service
  }

  private data class AvailabilityIdentity(
    val modelLine: String,
    val intervalStart: String,
    val intervalEnd: String,
  )

  private data class EvidenceRetentionSeeds(
    val uniqueIdentifierValues: Set<String>,
    val gcsObjectIdentities: Set<VidLabelingTraceAttributes.GcsObjectIdentity>,
    val availabilityIdentities: Set<AvailabilityIdentity>,
  )

  private fun retainRelatedEvidence(
    rawImpressionUpload: String,
    expectedWorkItemNames: Set<String>,
    retentionSeeds: EvidenceRetentionSeeds,
    candidates: Collection<VidLabelingEvidence>,
  ): List<VidLabelingEvidence> {
    val retained = linkedSetOf<VidLabelingEvidence>()
    val identifiers =
      linkedSetOf(rawImpressionUpload).apply { addAll(retentionSeeds.uniqueIdentifierValues) }
    val traceIds = linkedSetOf<String>()
    val objectIdentities = retentionSeeds.gcsObjectIdentities.toMutableSet()
    val availabilityIdentities = retentionSeeds.availabilityIdentities.toMutableSet()
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
          candidate.identifiers[WORK_ITEM] in expectedWorkItemNames ||
            uniqueValues.any {
              it == rawImpressionUpload || it.startsWith(rawImpressionUpload + "/")
            }
        val objectIdentity = candidate.gcsObjectIdentity()
        val availabilityIdentity = candidate.availabilityIdentity()
        if (
          directlyRelated ||
            uniqueValues.any { it in identifiers } ||
            (candidate.traceId != null && candidate.traceId in traceIds) ||
            (objectIdentity != null && objectIdentity in objectIdentities) ||
            (availabilityIdentity != null && availabilityIdentity in availabilityIdentities)
        ) {
          retained += candidate
          identifiers += uniqueValues
          candidate.traceId?.let { traceIds += it }
          objectIdentity?.let { objectIdentities += it }
          availabilityIdentity?.let { availabilityIdentities += it }
          changed = true
        }
      }
    } while (changed)
    return retained.toList()
  }

  private fun VidLabelingEvidence.gcsObjectIdentity():
    VidLabelingTraceAttributes.GcsObjectIdentity? = identifiers.gcsObjectIdentity()

  private fun Map<String, String>.gcsObjectIdentity():
    VidLabelingTraceAttributes.GcsObjectIdentity? {
    val pathHash = this[GCS_PATH_HASH] ?: return null
    val generation = this[GCS_GENERATION]?.toLongOrNull()?.takeIf { it > 0L } ?: return null
    return VidLabelingTraceAttributes.GcsObjectIdentity(pathHash, generation)
  }

  private fun VidLabelingEvidence.availabilityIdentity(): AvailabilityIdentity? =
    identifiers.availabilityIdentity()

  private fun Map<String, String>.availabilityIdentity(): AvailabilityIdentity? {
    return AvailabilityIdentity(
      this[MODEL_LINE] ?: return null,
      this[AVAILABILITY_INTERVAL_START] ?: return null,
      this[AVAILABILITY_INTERVAL_END] ?: return null,
    )
  }

  private fun evidenceRetentionSeeds(graph: VidLabelingAuthoritativeGraph): EvidenceRetentionSeeds {
    val identifiers = graph.nodes.flatMap { it.identifiers.entries }
    return EvidenceRetentionSeeds(
      identifiers.filter { it.key in UNIQUE_CORRELATION_FIELDS }.mapTo(mutableSetOf()) { it.value },
      graph.nodes.mapNotNullTo(mutableSetOf()) { it.identifiers.gcsObjectIdentity() },
      graph.nodes.mapNotNullTo(mutableSetOf()) { it.identifiers.availabilityIdentity() },
    )
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
    private const val WORK_ITEM = "xmm.work_item.name"
    private const val WORK_ITEM_ATTEMPT = "xmm.work_item_attempt.name"
    private const val WORK_ITEM_GENERATION = "xmm.work_item.generation"
    private const val WORK_ITEM_PROCESSING_STAGE = "work_item_processing"
    private const val RAW_UPLOAD = "xmm.edpa.raw_impression_upload.name"
    private const val RAW_UPLOAD_MODEL_LINE = "xmm.edpa.raw_impression_upload_model_line.name"
    private const val IMPRESSION_METADATA = "xmm.edpa.impression_metadata.name"
    private const val GCS_PATH_HASH = "xmm.gcs.object.path_hash"
    private const val GCS_GENERATION = "xmm.gcs.object.generation"

    private val SAFE_IDENTIFIER_FIELDS =
      setOf(
        "xmm.data_provider.name",
        MODEL_LINE,
        "xmm.model_line.names",
        RAW_UPLOAD,
        "xmm.edpa.raw_impression_upload_file.name",
        RAW_UPLOAD_MODEL_LINE,
        "xmm.edpa.pool_assignment_job.name",
        "xmm.edpa.ranker_job.name",
        "xmm.edpa.vid_labeling_job.name",
        "xmm.edpa.rank_index_blob.name",
        "xmm.edpa.rank_index_blob.type",
        IMPRESSION_METADATA,
        "xmm.edpa.recovery_work_item.name",
        "xmm.edpa.replaces_raw_impression_upload.name",
        "xmm.edpa.upload_healing_operation.name",
        "xmm.edpa.recovery_predecessor_raw_impression_upload.name",
        "xmm.work_item.name",
        "xmm.work_item_attempt.name",
        "xmm.work_item.generation",
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
        "xmm.error.type",
        "xmm.error.code",
      )
    private val UNIQUE_CORRELATION_FIELDS =
      setOf(
        RAW_UPLOAD,
        "xmm.edpa.raw_impression_upload_file.name",
        RAW_UPLOAD_MODEL_LINE,
        "xmm.edpa.pool_assignment_job.name",
        "xmm.edpa.ranker_job.name",
        "xmm.edpa.vid_labeling_job.name",
        "xmm.edpa.rank_index_blob.name",
        IMPRESSION_METADATA,
        "xmm.edpa.recovery_work_item.name",
      )
    private val DISCOVERY_CORRELATION_FIELDS = UNIQUE_CORRELATION_FIELDS + GCS_PATH_HASH
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
    private val ACTIVE_MODEL_LINE_STATES = setOf("CREATED", "POOL_ASSIGNING", "RANKING", "LABELING")
    private val NODE_MATCH_KEY_PRIORITY =
      listOf(
        "xmm.edpa.impression_metadata.name",
        "xmm.edpa.pool_assignment_job.name",
        "xmm.edpa.ranker_job.name",
        "xmm.edpa.vid_labeling_job.name",
        "xmm.edpa.rank_index_blob.name",
        "xmm.edpa.raw_impression_upload_file.name",
        "xmm.gcs.object.path_hash",
        AVAILABILITY_INTERVAL_START,
        RAW_UPLOAD_MODEL_LINE,
      )
    private const val AVAILABILITY_INTERVAL_START = "xmm.edpa.availability.interval_start"
    private const val AVAILABILITY_INTERVAL_END = "xmm.edpa.availability.interval_end"
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
    appendLine("| Project | Source | Status | Duration | Counts | Note |")
    appendLine("|---|---|---:|---:|---|---|")
    for (status in collection.sourceStatuses) {
      val counts =
        status.counts.entries.joinToString("<br>") { (name, value) -> safe(name) + "=" + value }
      appendLine(
        "| " +
          safe(status.project) +
          " | " +
          safe(status.source) +
          " | " +
          safe(status.status) +
          " | " +
          status.durationMillis +
          " ms | " +
          counts +
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
            (if (stage.unresolved) "?"
            else if (stage.present || stage.notApplicable) "x" else " ") +
            "] " +
            safe(stage.name) +
            (if (stage.resource.isEmpty()) "" else " `" + safe(stage.resource) + "`") +
            (if (stage.authoritativeState.isEmpty()) ""
            else " state=" + safe(stage.authoritativeState)) +
            (if (stage.notApplicable) " (not applicable)" else "") +
            (if (stage.authoritativeOnly) " (authoritative state)" else "") +
            (if (stage.unresolved) " (unresolved)" else "") +
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
    val failures =
      collection.evidence.filter {
        it.outcome?.lowercase() in setOf("failed", "failure", "error", "aborted") ||
          it.identifiers.containsKey("xmm.error.type") ||
          it.identifiers.containsKey("xmm.error.code")
      }
    if (failures.isNotEmpty()) {
      appendLine()
      appendLine("## Errors")
      appendLine()
      appendLine("| Time | Stage | Error type | Error code |")
      appendLine("|---|---|---|---|")
      for (failure in failures) {
        appendLine(
          "| " +
            failure.timestamp +
            " | " +
            safe(failure.stage.orEmpty()) +
            " | " +
            safe(failure.identifiers["xmm.error.type"].orEmpty()) +
            " | " +
            safe(failure.identifiers["xmm.error.code"].orEmpty()) +
            " |"
        )
      }
    }
    if (collection.warnings.isNotEmpty()) {
      appendLine()
      appendLine("## Warnings")
      appendLine()
      for (warning in collection.warnings) appendLine("- " + safe(warning))
    }
  }

  fun renderFailure(
    rawImpressionUpload: String,
    source: String,
    status: String,
    throwable: Throwable,
  ): String = buildString {
    appendLine("# VID labeling trace")
    appendLine()
    appendLine("- Raw impression upload: `" + safe(rawImpressionUpload) + "`")
    appendLine("- Evidence status: **failed**")
    appendLine("- Failed source: `" + safe(source) + "`")
    appendLine("- Source status: `" + safe(status) + "`")
    appendLine("- Failure type: `" + safe(throwable::class.simpleName.orEmpty()) + "`")
  }

  fun artifactFileName(rawImpressionUpload: String): String {
    val match = RAW_UPLOAD_PATTERN.matchEntire(rawImpressionUpload)
    val digest =
      MessageDigest.getInstance("SHA-256")
        .digest(rawImpressionUpload.toByteArray(Charsets.UTF_8))
        .take(8)
        .joinToString("") { byte -> (byte.toInt() and 0xff).toString(16).padStart(2, '0') }
    if (match != null) {
      return match.groupValues[1] + "__" + match.groupValues[2] + "__" + digest + ".md"
    }
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
  uploadTimeout: Duration = DEFAULT_UPLOAD_TIMEOUT,
  collect: suspend (String) -> VidLabelingTraceCollection,
  write: (String, String) -> Unit,
): Boolean {
  require(!uploadTimeout.isZero && !uploadTimeout.isNegative) { "uploadTimeout must be positive" }
  var failed = false
  for (rawImpressionUpload in rawImpressionUploads.distinct()) {
    try {
      val collection = withTimeoutOrNull(uploadTimeout.toMillis()) { collect(rawImpressionUpload) }
      if (collection == null) {
        failed = true
        write(
          VidLabelingTraceOutput.artifactFileName(rawImpressionUpload),
          VidLabelingTraceOutput.renderFailure(
            rawImpressionUpload,
            "overall_upload",
            "timed_out",
            TimeoutException("Upload collection deadline exceeded"),
          ),
        )
        continue
      }
      write(
        VidLabelingTraceOutput.artifactFileName(rawImpressionUpload),
        VidLabelingTraceOutput.render(collection),
      )
      if (
        collection.traceStatus == VidLabelingTraceStatus.FAILED ||
          collection.traceStatus == VidLabelingTraceStatus.PARTIAL && !allowPartial
      ) {
        failed = true
      }
    } catch (e: TimeoutCancellationException) {
      throw e
    } catch (e: CancellationException) {
      throw e
    } catch (e: Exception) {
      failed = true
      write(
        VidLabelingTraceOutput.artifactFileName(rawImpressionUpload),
        VidLabelingTraceOutput.renderFailure(
          rawImpressionUpload,
          "input_or_collection",
          "failed",
          e,
        ),
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
internal data class VidLabelingTraceDependencies(
  val collector: VidLabelingTraceCollector,
  val clock: Clock = Clock.systemUTC(),
  val output: java.io.PrintWriter = java.io.PrintWriter(System.out, true),
)

internal class VidLabelingTraceServiceTlsFlags {
  @Option(names = ["--control-plane-tls-cert-file"], required = true)
  lateinit var controlPlaneCertFile: File
    private set

  @Option(names = ["--control-plane-tls-key-file"], required = true)
  lateinit var controlPlanePrivateKeyFile: File
    private set

  @Option(names = ["--control-plane-cert-collection-file"], required = true)
  lateinit var controlPlaneCertCollectionFile: File
    private set

  @Option(names = ["--kingdom-tls-cert-file"], required = true)
  lateinit var kingdomCertFile: File
    private set

  @Option(names = ["--kingdom-tls-key-file"], required = true)
  lateinit var kingdomPrivateKeyFile: File
    private set

  @Option(names = ["--kingdom-cert-collection-file"], required = true)
  lateinit var kingdomCertCollectionFile: File
    private set

  val controlPlaneSigningCerts: SigningCerts by lazy {
    SigningCerts.fromPemFiles(
      controlPlaneCertFile,
      controlPlanePrivateKeyFile,
      controlPlaneCertCollectionFile,
    )
  }

  val kingdomSigningCerts: SigningCerts by lazy {
    SigningCerts.fromPemFiles(kingdomCertFile, kingdomPrivateKeyFile, kingdomCertCollectionFile)
  }
}

internal class VidLabelingTrace(private val dependencies: VidLabelingTraceDependencies? = null) :
  Callable<Int> {
  @Mixin private lateinit var tlsFlags: TlsFlags
  @Mixin private lateinit var serviceTlsFlags: VidLabelingTraceServiceTlsFlags

  @Option(
    names = ["--edpa-public-api-target"],
    required = true,
    description = ["gRPC target of the EDP Aggregator public API."],
  )
  private lateinit var edpaPublicApiTarget: String

  @Option(
    names = ["--edpa-public-api-cert-host"],
    description = ["Expected EDP Aggregator TLS certificate hostname."],
  )
  private var edpaPublicApiCertHost: String? = null

  @Option(
    names = ["--control-plane-api-target"],
    required = true,
    description = ["gRPC target of the Secure Computation control-plane API."],
  )
  private lateinit var controlPlaneApiTarget: String

  @Option(
    names = ["--control-plane-api-cert-host"],
    description = ["Expected control-plane TLS certificate hostname."],
  )
  private var controlPlaneApiCertHost: String? = null

  @Option(
    names = ["--kingdom-public-api-target"],
    required = true,
    description = ["gRPC target of the Kingdom public API."],
  )
  private lateinit var kingdomPublicApiTarget: String

  @Option(
    names = ["--kingdom-public-api-cert-host"],
    description = ["Expected Kingdom TLS certificate hostname."],
  )
  private var kingdomPublicApiCertHost: String? = null

  @Option(
    names = ["--gcs-project"],
    description = ["Google Cloud project used for object-metadata reads."],
  )
  private var gcsProject: String? = null
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

  @Option(names = ["--trace-quota-units-per-second"], defaultValue = "4.0")
  private var traceQuotaUnitsPerSecond: Double = 4.0

  @Option(names = ["--logging-requests-per-second"], defaultValue = "0.5")
  private var loggingRequestsPerSecond: Double = 0.5

  @Option(names = ["--include-grpc-payloads"], defaultValue = "false")
  private var includeGrpcPayloads: Boolean = false

  @Option(names = ["--allow-partial"], defaultValue = "false")
  private var allowPartial: Boolean = false

  @Option(
    names = ["--authoritative-source-timeout"],
    defaultValue = "PT2M",
    description = ["Total ISO-8601 deadline for each authoritative source."],
  )
  private lateinit var authoritativeSourceTimeout: Duration

  @Option(
    names = ["--logging-source-timeout"],
    defaultValue = "PT2M",
    description = ["Total ISO-8601 deadline for Cloud Logging collection."],
  )
  private lateinit var loggingSourceTimeout: Duration

  @Option(
    names = ["--trace-source-timeout"],
    defaultValue = "PT2M",
    description = ["Total ISO-8601 deadline for Cloud Trace collection."],
  )
  private lateinit var traceSourceTimeout: Duration

  @Option(
    names = ["--upload-timeout"],
    defaultValue = "PT10M",
    description = ["Overall ISO-8601 deadline for one upload."],
  )
  private lateinit var uploadTimeout: Duration

  @Option(
    names = ["--work-item-page-limit"],
    defaultValue = "100",
    description = ["Maximum global Secure Computation WorkItem pages per upload."],
  )
  private var workItemPageLimit: Int = DEFAULT_WORK_ITEM_PAGE_LIMIT

  override fun call(): Int = runBlocking {
    require(rawImpressionUploads.size == 1 || outputDirectory != null) {
      "--output-dir is required when collecting more than one upload"
    }
    requirePositive(authoritativeSourceTimeout, "--authoritative-source-timeout")
    requirePositive(loggingSourceTimeout, "--logging-source-timeout")
    requirePositive(traceSourceTimeout, "--trace-source-timeout")
    requirePositive(uploadTimeout, "--upload-timeout")
    require(workItemPageLimit > 0) { "--work-item-page-limit must be positive" }
    require(traceConcurrency > 0) { "--trace-concurrency must be positive" }
    require(traceQuotaUnitsPerSecond > 0.0 && traceQuotaUnitsPerSecond <= 5.0) {
      "--trace-quota-units-per-second must be in (0, 5]"
    }
    require(loggingRequestsPerSecond > 0.0 && loggingRequestsPerSecond <= 1.0) {
      "--logging-requests-per-second must be in (0, 1]"
    }
    val now = dependencies?.clock?.instant() ?: Clock.systemUTC().instant()
    val resolvedEndTime = parseInstant(endTime) ?: now
    val resolvedStartTime =
      parseInstant(startTime) ?: resolvedEndTime.minus(Duration.parse(lookback))
    val outputRoot = outputDirectory?.toAbsolutePath()?.normalize()
    outputRoot?.let { Files.createDirectories(it) }
    val injectedCollector = dependencies?.collector
    if (injectedCollector != null) {
      val failed =
        collectVidLabelingTraceBatch(
          rawImpressionUploads,
          allowPartial,
          uploadTimeout,
          collect = { rawImpressionUpload ->
            injectedCollector.collect(
              VidLabelingTraceRequest(
                rawImpressionUpload,
                observabilityProjects,
                resolvedStartTime,
                resolvedEndTime,
                entryLimit,
                expansionRounds,
                correlationValueLimit,
                traceIdLimit,
                authoritativeSourceTimeout,
                loggingSourceTimeout,
                traceSourceTimeout,
                workItemPageLimit,
              )
            )
          },
          write = { fileName, contents -> writeArtifact(outputRoot, fileName, contents) },
        )
      return@runBlocking if (failed) 1 else 0
    }
    val spanReader =
      GoogleCloudTraceReader(::vidTraceAttributesFor)
        .withMaxConcurrency(traceConcurrency)
        .withRequestThrottlerFactory { MaximumRateThrottler(traceQuotaUnitsPerSecond) }
    val edpaChannel =
      buildMutualTlsChannel(edpaPublicApiTarget, tlsFlags.signingCerts, edpaPublicApiCertHost)
    val controlPlaneChannel =
      buildMutualTlsChannel(
        controlPlaneApiTarget,
        serviceTlsFlags.controlPlaneSigningCerts,
        controlPlaneApiCertHost,
      )
    val kingdomChannel =
      buildMutualTlsChannel(
        kingdomPublicApiTarget,
        serviceTlsFlags.kingdomSigningCerts,
        kingdomPublicApiCertHost,
      )
    val storage =
      if (gcsProject == null) StorageOptions.getDefaultInstance().service
      else StorageOptions.newBuilder().setProjectId(gcsProject).build().service
    val stateResolver =
      GrpcVidLabelingStateResolver(
        RawImpressionUploadServiceCoroutineStub(edpaChannel),
        RawImpressionUploadFileServiceCoroutineStub(edpaChannel),
        RawImpressionUploadModelLineServiceCoroutineStub(edpaChannel),
        PoolAssignmentJobServiceCoroutineStub(edpaChannel),
        RankerJobServiceCoroutineStub(edpaChannel),
        VidLabelingJobServiceCoroutineStub(edpaChannel),
        RankIndexBlobServiceCoroutineStub(edpaChannel),
      )
    val workItemResolver =
      GrpcVidLabelingWorkItemResolver(
        WorkItemsCoroutineStub(controlPlaneChannel),
        WorkItemAttemptsCoroutineStub(controlPlaneChannel),
      )
    val routeResolver =
      KingdomVidLabelingRouteResolver(
        ModelLinesCoroutineStub(kingdomChannel),
        ModelRolloutsCoroutineStub(kingdomChannel),
        ModelShardsCoroutineStub(kingdomChannel),
      )
    val finalStateResolver =
      GcsKingdomFinalStateResolver(
        ImpressionMetadataServiceCoroutineStub(edpaChannel),
        DataProvidersCoroutineStub(kingdomChannel),
        readObjectMetadata = { uri, generation ->
          val parsed = SelectedStorageClient.parseBlobUri(uri)
          val blobId =
            if (generation == null) {
              BlobId.of(parsed.bucket, parsed.key)
            } else {
              BlobId.of(parsed.bucket, parsed.key, generation)
            }
          runInterruptible(Dispatchers.IO) {
            storage.get(blobId)?.let { blob -> StoredObjectMetadata(blob.generation) }
          }
        },
      )
    val collector =
      VidLabelingTraceCollector(
        logReaderFactory = { project ->
          GoogleCloudLogReader(
              project,
              buildCloudTelemetryLoggingOptions(project).service,
              VID_SAFE_LOG_FIELDS,
              VID_CORRELATION_FIELDS,
              includeGrpcPayloads,
              correlationFieldsFor = ::vidLogCorrelationFieldsFor,
            )
            .withRequestThrottler(MaximumRateThrottler(loggingRequestsPerSecond))
        },
        spanReader = spanReader,
        stateResolver = stateResolver,
        finalStateResolver = finalStateResolver,
        workItemResolver = workItemResolver,
        routeResolver = routeResolver,
      )
    try {
      val failed =
        collectVidLabelingTraceBatch(
          rawImpressionUploads,
          allowPartial,
          uploadTimeout,
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
                authoritativeSourceTimeout,
                loggingSourceTimeout,
                traceSourceTimeout,
                workItemPageLimit,
              )
            )
          },
          write = { fileName, contents -> writeArtifact(outputRoot, fileName, contents) },
        )
      if (failed) 1 else 0
    } finally {
      edpaChannel.shutdown()
      controlPlaneChannel.shutdown()
      kingdomChannel.shutdown()
      edpaChannel.awaitTermination(30, TimeUnit.SECONDS)
      controlPlaneChannel.awaitTermination(30, TimeUnit.SECONDS)
      kingdomChannel.awaitTermination(30, TimeUnit.SECONDS)
    }
  }

  private fun writeArtifact(outputRoot: Path?, fileName: String, contents: String) {
    if (outputRoot == null) {
      dependencies?.output?.print(contents) ?: print(contents)
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

  private fun requirePositive(duration: Duration, optionName: String) {
    require(!duration.isZero && !duration.isNegative) { "$optionName must be positive" }
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
    "xmm.edpa.raw_impression_upload_file.name",
    "xmm.edpa.raw_impression_upload_model_line.name",
    "xmm.edpa.pool_assignment_job.name",
    "xmm.edpa.ranker_job.name",
    "xmm.edpa.vid_labeling_job.name",
    "xmm.edpa.rank_index_blob.name",
    "xmm.edpa.rank_index_blob.type",
    "xmm.edpa.impression_metadata.name",
    "xmm.edpa.recovery_work_item.name",
    "xmm.edpa.replaces_raw_impression_upload.name",
    "xmm.edpa.upload_healing_operation.name",
    "xmm.edpa.recovery_predecessor_raw_impression_upload.name",
    "xmm.work_item.name",
    "xmm.work_item_attempt.name",
    "xmm.work_item.generation",
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
    "xmm.edpa.raw_impression_upload_file.name",
    "xmm.edpa.raw_impression_upload_model_line.name",
    "xmm.edpa.pool_assignment_job.name",
    "xmm.edpa.ranker_job.name",
    "xmm.edpa.vid_labeling_job.name",
    "xmm.edpa.rank_index_blob.name",
    "xmm.edpa.impression_metadata.name",
    "xmm.edpa.recovery_work_item.name",
    "xmm.edpa.replaces_raw_impression_upload.name",
    "xmm.edpa.upload_healing_operation.name",
    "xmm.edpa.recovery_predecessor_raw_impression_upload.name",
    "xmm.work_item.name",
    "xmm.work_item_attempt.name",
  )

internal fun vidTraceAttributesFor(value: String): Collection<String> {
  return when {
    GCS_PATH_HASH_PATTERN.matches(value) -> listOf("xmm.gcs.object.path_hash")
    "/rawImpressionUploadModelLines/" in value ->
      listOf("xmm.edpa.raw_impression_upload_model_line.name")
    "/files/" in value -> listOf("xmm.edpa.raw_impression_upload_file.name")
    "/poolAssignmentJobs/" in value -> listOf("xmm.edpa.pool_assignment_job.name")
    "/rankerJobs/" in value -> listOf("xmm.edpa.ranker_job.name")
    "/vidLabelingJobs/" in value -> listOf("xmm.edpa.vid_labeling_job.name")
    "/rankIndexBlobs/" in value -> listOf("xmm.edpa.rank_index_blob.name")
    "/rawImpressionUploads/" in value -> listOf("xmm.edpa.raw_impression_upload.name")
    "/impressionMetadata/" in value -> listOf("xmm.edpa.impression_metadata.name")
    "/uploadHealingOperations/" in value -> listOf("xmm.edpa.upload_healing_operation.name")
    "/workItemAttempts/" in value -> listOf("xmm.work_item_attempt.name")
    value.startsWith("workItems/") ->
      listOf("xmm.work_item.name", "xmm.edpa.recovery_work_item.name")
    else -> emptyList()
  }
}

private fun vidLogCorrelationFieldsFor(value: String): Collection<String> {
  return when {
    "/rawImpressionUploads/" in value ->
      listOf(
        "xmm.edpa.raw_impression_upload.name",
        "xmm.edpa.replaces_raw_impression_upload.name",
        "xmm.edpa.recovery_predecessor_raw_impression_upload.name",
      )
    "/modelLines/" in value -> listOf("xmm.model_line.name")
    else -> vidTraceAttributesFor(value).ifEmpty { VID_CORRELATION_FIELDS }
  }
}

private val GCS_PATH_HASH_PATTERN = Regex("(?i)^[0-9a-f]{64}$")

internal fun runVidLabelingTrace(
  args: Array<String>,
  dependencies: VidLabelingTraceDependencies,
): Int {
  return CommandLine(VidLabelingTrace(dependencies)).execute(*args)
}

fun main(args: Array<String>) {
  val exitCode = CommandLine(VidLabelingTrace()).execute(*args)
  if (exitCode != 0) kotlin.system.exitProcess(exitCode)
}
