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

import com.google.common.truth.Truth.assertThat
import java.io.PrintWriter
import java.io.StringWriter
import java.time.Clock
import java.time.Duration
import java.time.Instant
import java.time.ZoneOffset
import java.util.logging.Handler
import java.util.logging.Level
import java.util.logging.LogRecord
import java.util.logging.Logger
import kotlin.test.assertFailsWith
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.TimeoutCancellationException
import kotlinx.coroutines.awaitCancellation
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.withTimeout
import org.junit.Test
import org.wfanet.measurement.common.telemetry.CloudLogEntry
import org.wfanet.measurement.common.telemetry.CloudLogReader
import org.wfanet.measurement.common.telemetry.CloudTelemetryReadCounts
import org.wfanet.measurement.common.telemetry.CloudTelemetryReadResult
import org.wfanet.measurement.common.telemetry.CloudTraceReader
import org.wfanet.measurement.common.telemetry.CloudTraceSpan
import org.wfanet.measurement.edpaggregator.telemetry.VidLabelingTraceAttributes
import org.wfanet.measurement.edpaggregator.telemetry.VidLabelingTraceLogging
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUpload
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadModelLine

private fun main(args: Array<String>, dependencies: VidLabelingTraceDependencies): Int =
  runVidLabelingTrace(args, dependencies)

class VidLabelingTraceTest {
  @Test
  fun `collect joins separate traces by exact GCS object identity`() = runBlocking {
    val uri = "gs://output-bucket/model-line/direct/2026-09-01/done"
    val identity = VidLabelingTraceAttributes.gcsObjectIdentity(uri, 77L)
    val job = RAW_UPLOAD + "/vidLabelingJobs/direct"
    val graph =
      testGraph(listOf(NON_MEMOIZED_MODEL_LINE))
        .copy(
          nodes =
            listOf(
              ExpectedTraceNode(
                job,
                NON_MEMOIZED_MODEL_LINE,
                "label_finalize",
                "SUCCEEDED",
                mapOf(
                  "xmm.model_line.name" to NON_MEMOIZED_MODEL_LINE,
                  "xmm.edpa.vid_labeling_job.name" to job,
                ),
              )
            )
        )
    val entries =
      listOf(
        entry(
          "label_finalize",
          objectIdentityFields(identity) +
            " xmm.model_line.name=" +
            NON_MEMOIZED_MODEL_LINE +
            " xmm.edpa.vid_labeling_job.name=" +
            job,
          rawImpressionUpload = RAW_UPLOAD,
          traceId = ROOT_TRACE,
        ),
        entry(
          "data_watcher",
          objectIdentityFields(identity),
          rawImpressionUpload = RAW_UPLOAD,
          traceId = MEMO_TRACE,
        ),
        entry(
          "data_availability_publish",
          objectIdentityFields(identity) + " xmm.model_line.name=" + NON_MEMOIZED_MODEL_LINE,
          traceId = DIRECT_TRACE,
        ),
      )
    val logReader = FakeCloudLogReader(entries)
    val finalStateResolver = VidLabelingFinalStateResolver { _, _, _ ->
      VidLabelingFinalStateResolution(
        listOf(
          ExpectedTraceNode(
            "watcher",
            NON_MEMOIZED_MODEL_LINE,
            "data_watcher",
            "SUCCEEDED",
            mapOf(
              "xmm.gcs.object.path_hash" to identity.pathHash,
              "xmm.gcs.object.generation" to identity.generation.toString(),
            ),
          ),
          ExpectedTraceNode(
            "availability",
            NON_MEMOIZED_MODEL_LINE,
            "data_availability_publish",
            "PUBLISHED",
            mapOf(
              "xmm.gcs.object.path_hash" to identity.pathHash,
              "xmm.gcs.object.generation" to identity.generation.toString(),
            ),
          ),
        ),
        emptyMap(),
      )
    }
    val collector =
      VidLabelingTraceCollector(
        logReaderFactory = { logReader },
        spanReader = completeTraceReader(),
        stateResolver = VidLabelingStateResolver { graph },
        finalStateResolver = finalStateResolver,
      )

    val collection = collector.collect(request())

    assertThat(collection.traceStatus).isEqualTo(VidLabelingTraceStatus.COMPLETE)
    assertThat(collection.evidence.mapNotNull { it.traceId })
      .containsAtLeast(ROOT_TRACE, MEMO_TRACE, DIRECT_TRACE)
    Unit
  }

  @Test
  fun `collect discovers a new trace by exact GCS object identity`() = runBlocking {
    val identity =
      VidLabelingTraceAttributes.gcsObjectIdentity(
        "gs://output-bucket/model-line/direct/2026-09-01/done",
        77L,
      )
    val producerEntry =
      entry(
        "label_finalize",
        objectIdentityFields(identity),
        rawImpressionUpload = RAW_UPLOAD,
        traceId = ROOT_TRACE,
      )
    val downstreamEntry =
      entry(
        "data_watcher",
        objectIdentityFields(identity),
        rawImpressionUpload = RAW_UPLOAD,
        traceId = MEMO_AVAILABILITY_TRACE,
      )
    val traceQueries = mutableListOf<Pair<Set<String>, Set<String>>>()
    val spanReader = CloudTraceReader { project, correlationValues, traceIds, _, _, _ ->
      traceQueries += correlationValues.toSet() to traceIds.toSet()
      if (MEMO_AVAILABILITY_TRACE !in traceIds) {
        emptyList()
      } else {
        listOf(
          span(project, MEMO_AVAILABILITY_TRACE, "data_watcher", identity, generation = 77L),
          span(project, OTHER_TRACE, "data_watcher", identity, generation = 78L),
        )
      }
    }
    val collector =
      VidLabelingTraceCollector(
        logReaderFactory = { FakeCloudLogReader(listOf(producerEntry, downstreamEntry)) },
        spanReader = spanReader,
        stateResolver = VidLabelingStateResolver { testGraph(emptyList()) },
        finalStateResolver = NOOP_FINAL_STATE_RESOLVER,
      )

    val collection = collector.collect(request())

    assertThat(vidTraceAttributesFor(identity.pathHash)).containsExactly("xmm.gcs.object.path_hash")
    assertThat(traceQueries.any { identity.pathHash in it.first }).isTrue()
    assertThat(traceQueries.any { MEMO_AVAILABILITY_TRACE in it.second }).isTrue()
    assertThat(collection.evidence.mapNotNull { it.traceId }).contains(MEMO_AVAILABILITY_TRACE)
    assertThat(collection.evidence.mapNotNull { it.traceId }).doesNotContain(OTHER_TRACE)
    Unit
  }

  @Test
  fun `collect matches the metadata sidecar lifecycle record emitted by the producer`() =
    runBlocking {
      val sidecarUri = "gs://output-bucket/model-line/direct/output.metadata.binpb"
      val pathHash = VidLabelingTraceAttributes.gcsObjectPathHash(sidecarUri)
      val records = mutableListOf<LogRecord>()
      val logger =
        Logger.getAnonymousLogger().apply {
          useParentHandlers = false
          addHandler(
            object : Handler() {
              override fun publish(record: LogRecord) {
                records += record
              }

              override fun flush() {}

              override fun close() {}
            }
          )
        }
      VidLabelingTraceLogging.logLabelOutput(
        logger,
        Level.INFO,
        "edpa.vid_labeling.label.metadata_sidecar",
        "dataProviders/123",
        RAW_UPLOAD,
        RAW_UPLOAD + "/vidLabelingJobs/direct",
        "non_memoized",
        NON_MEMOIZED_MODEL_LINE,
        "metadata_sidecar",
        pathHash,
        "written",
      )
      val graph =
        testGraph(listOf(NON_MEMOIZED_MODEL_LINE))
          .copy(
            nodes =
              listOf(
                ExpectedTraceNode(
                  "gcs:$pathHash",
                  NON_MEMOIZED_MODEL_LINE,
                  VidLabelingTraceLogging.LABEL_OUTPUT_STAGE,
                  "PUBLISHED",
                  mapOf(
                    "xmm.model_line.name" to NON_MEMOIZED_MODEL_LINE,
                    "xmm.gcs.object.path_hash" to pathHash,
                  ),
                )
              )
          )
      val emittedEntry =
        CloudLogEntry(
          "project",
          Instant.parse("2026-09-01T00:00:01Z"),
          "vid-labeler",
          "INFO",
          ROOT_TRACE,
          records.single().message,
        )
      val collector =
        VidLabelingTraceCollector(
          logReaderFactory = { FakeCloudLogReader(listOf(emittedEntry)) },
          spanReader = completeTraceReader(),
          stateResolver = VidLabelingStateResolver { graph },
          finalStateResolver = NOOP_FINAL_STATE_RESOLVER,
        )

      val collection = collector.collect(request())

      assertThat(collection.traceStatus).isEqualTo(VidLabelingTraceStatus.COMPLETE)
      assertThat(collection.evidence.map { it.stage })
        .contains(VidLabelingTraceLogging.LABEL_OUTPUT_STAGE)
      Unit
    }

  @Test
  fun `artifact file names cannot collide`() {
    val first = "dataProviders/a/rawImpressionUploads/b__c"
    val second = "dataProviders/a__b/rawImpressionUploads/c"

    assertThat(VidLabelingTraceOutput.artifactFileName(first))
      .isNotEqualTo(VidLabelingTraceOutput.artifactFileName(second))
  }

  @Test
  fun `render retains safe error classification`() {
    val collection =
      VidLabelingTraceCollection(
        RAW_UPLOAD,
        VidLabelingTraceStatus.PARTIAL,
        VidLabelingExecutionStatus.FAILED,
        emptyList(),
        listOf(
          VidLabelingEvidence(
            Instant.parse("2026-09-01T00:00:00Z"),
            "project",
            "log",
            "worker",
            "label",
            "failed",
            mapOf("xmm.error.type" to "IllegalStateException", "xmm.error.code" to "grpc.INTERNAL"),
          )
        ),
        emptyList(),
        emptyList(),
      )

    val output = VidLabelingTraceOutput.render(collection)

    assertThat(output).contains("xmm.error.type=IllegalStateException")
    assertThat(output).contains("xmm.error.code=grpc.INTERNAL")
    assertThat(output).contains("## Errors")
  }

  @Test
  fun `render distinguishes unresolved from not applicable`() {
    val collection =
      VidLabelingTraceCollection(
        RAW_UPLOAD,
        VidLabelingTraceStatus.PARTIAL,
        VidLabelingExecutionStatus.UNKNOWN,
        listOf(
          VidLabelingModelLineGraph(
            NON_MEMOIZED_MODEL_LINE,
            VidLabelingRoute.NON_MEMOIZED,
            listOf(
              VidLabelingStageCoverage(
                "label",
                present = false,
                evidenceCount = 0,
                resource = "workItems/expected",
                authoritativeState = "UNRESOLVED",
                unresolved = true,
              )
            ),
          )
        ),
        emptyList(),
        listOf(
          VidLabelingSourceStatus(
            "secure_computation_work_items",
            "truncated",
            12,
            mapOf("pages_fetched" to 1L),
          )
        ),
        emptyList(),
      )

    val output = VidLabelingTraceOutput.render(collection)

    assertThat(output).contains("[?] label `workItems/expected` state=UNRESOLVED (unresolved)")
    assertThat(output).doesNotContain("state=UNRESOLVED (not applicable)")
  }

  @Test
  fun `collect and render retain safe error classification from logs and spans`() = runBlocking {
    val logEntry =
      entry(
        "label",
        "xmm.error.type=IllegalStateException xmm.error.code=grpc.INTERNAL " +
          "exception.message=sensitive-log-message " +
          "xmm.edpa.processed_column.value=email_address",
        rawImpressionUpload = RAW_UPLOAD,
        outcome = "failed",
        traceId = ROOT_TRACE,
      )
    val traceSpan =
      CloudTraceSpan(
        "observability-project",
        ROOT_TRACE,
        "span-error",
        null,
        "edpa.vid_labeling.label",
        "worker",
        Instant.parse("2026-09-01T00:00:01Z"),
        Instant.parse("2026-09-01T00:00:02Z"),
        mapOf(
          "xmm.lifecycle.stage" to "label",
          "xmm.outcome" to "failed",
          "xmm.edpa.raw_impression_upload.name" to RAW_UPLOAD,
          "xmm.error.type" to "StorageException",
          "xmm.error.code" to "storage.500",
          "exception.message" to "sensitive-span-message",
          "xmm.edpa.processed_column.value" to "email_address",
        ),
      )
    val collector =
      VidLabelingTraceCollector(
        logReaderFactory = { FakeCloudLogReader(listOf(logEntry)) },
        spanReader = CloudTraceReader { _, _, _, _, _, _ -> listOf(traceSpan) },
        stateResolver = VidLabelingStateResolver { testGraph(emptyList()) },
        finalStateResolver = NOOP_FINAL_STATE_RESOLVER,
      )

    val collection = collector.collect(request())

    val collectedLog = collection.evidence.single { it.source == "log" }
    assertThat(collectedLog.identifiers).containsEntry("xmm.error.type", "IllegalStateException")
    assertThat(collectedLog.identifiers).containsEntry("xmm.error.code", "grpc.INTERNAL")
    assertThat(collectedLog.identifiers).doesNotContainKey("exception.message")
    assertThat(collectedLog.identifiers).doesNotContainKey("xmm.edpa.processed_column.value")
    val collectedSpan = collection.evidence.single { it.source == "span" }
    assertThat(collectedSpan.identifiers).containsEntry("xmm.error.type", "StorageException")
    assertThat(collectedSpan.identifiers).containsEntry("xmm.error.code", "storage.500")
    assertThat(collectedSpan.identifiers).doesNotContainKey("exception.message")
    assertThat(collectedSpan.identifiers).doesNotContainKey("xmm.edpa.processed_column.value")
    val output = VidLabelingTraceOutput.render(collection)
    assertThat(output).contains("IllegalStateException")
    assertThat(output).contains("grpc.INTERNAL")
    assertThat(output).contains("StorageException")
    assertThat(output).contains("storage.500")
    assertThat(output).doesNotContain("sensitive-log-message")
    assertThat(output).doesNotContain("sensitive-span-message")
    assertThat(output).doesNotContain("email_address")
  }

  @Test
  fun `collect retains WorkItem generation for exact evidence matching`() = runBlocking {
    val workItemName = "workItems/exact-generation"
    val expectedNode =
      ExpectedTraceNode(
        "$workItemName:generation:2",
        NON_MEMOIZED_MODEL_LINE,
        "label",
        "SUCCEEDED",
        mapOf("xmm.work_item.name" to workItemName, "xmm.work_item.generation" to "2"),
      )
    val graph =
      testGraph(listOf(NON_MEMOIZED_MODEL_LINE))
        .copy(nodes = testGraph(listOf(NON_MEMOIZED_MODEL_LINE)).nodes + expectedNode)
    val span =
      CloudTraceSpan(
        "observability-project",
        WORK_ITEM_TRACE,
        "span-generation",
        null,
        "secure_computation.work_item.process",
        "worker",
        Instant.parse("2026-09-01T00:00:01Z"),
        Instant.parse("2026-09-01T00:00:02Z"),
        mapOf(
          "xmm.lifecycle.stage" to "work_item_processing",
          "xmm.outcome" to "succeeded",
          "xmm.work_item.name" to workItemName,
          "xmm.work_item.generation" to "2",
        ),
      )
    val collector =
      VidLabelingTraceCollector(
        logReaderFactory = {
          FakeCloudLogReader(
            listOf(
              entry("upload_registration", rawImpressionUpload = RAW_UPLOAD, traceId = ROOT_TRACE)
            )
          )
        },
        spanReader = CloudTraceReader { _, _, _, _, _, _ -> listOf(span) },
        stateResolver = VidLabelingStateResolver { graph },
        finalStateResolver = NOOP_FINAL_STATE_RESOLVER,
      )

    val collection = collector.collect(request())

    assertThat(
        collection.evidence.any { evidence ->
          evidence.identifiers["xmm.work_item.generation"] == "2"
        }
      )
      .isTrue()
    assertThat(
        collection.modelLines
          .single()
          .stages
          .single { it.resource == expectedNode.id }
          .evidenceCount
      )
      .isEqualTo(1)
  }

  @Test
  fun `collect shares WorkItem attempt evidence across model lines`() = runBlocking {
    val workItemName = "workItems/shared-labeling"
    val generation = 2L
    val attemptName = "$workItemName/workItemAttempts/attempt-1"
    val modelLines = listOf(MEMOIZED_MODEL_LINE, NON_MEMOIZED_MODEL_LINE)
    val nodes =
      modelLines.map { modelLine ->
        ExpectedTraceNode(
          attemptName,
          modelLine,
          "label",
          "SUCCEEDED",
          mapOf(
            "xmm.work_item.name" to workItemName,
            "xmm.work_item.generation" to generation.toString(),
            "xmm.work_item_attempt.name" to attemptName,
          ),
        )
      }
    val logReader =
      FakeCloudLogReader(
        listOf(
          entry("upload_registration", rawImpressionUpload = RAW_UPLOAD, traceId = ROOT_TRACE),
          entry(
            "work_item_processing",
            "xmm.work_item.name=$workItemName",
            traceId = WORK_ITEM_TRACE,
          ),
        )
      )
    val collector =
      VidLabelingTraceCollector(
        logReaderFactory = { logReader },
        spanReader =
          CloudTraceReader { project, _, traceIds, startTime, endTime, _ ->
            requiredTraceSpans(project, traceIds, startTime, endTime) +
              if (traceIds.any { it.substringAfterLast('/') == WORK_ITEM_TRACE }) {
                listOf(workItemSpan("shared-attempt", workItemName, generation, attemptName))
              } else {
                emptyList()
              }
          },
        stateResolver = VidLabelingStateResolver { testGraph(modelLines).copy(nodes = nodes) },
        finalStateResolver = NOOP_FINAL_STATE_RESOLVER,
      )

    val collection = collector.collect(request())

    assertThat(collection.traceStatus).isEqualTo(VidLabelingTraceStatus.COMPLETE)
    for (modelLine in modelLines) {
      assertThat(
          collection.modelLines
            .single { it.modelLine == modelLine }
            .stages
            .single { it.resource == attemptName }
            .evidenceCount
        )
        .isEqualTo(1)
    }
    assertThat(
        collection.evidence.count {
          it.identifiers["xmm.work_item.name"] == workItemName &&
            it.identifiers["xmm.work_item_attempt.name"] == attemptName
        }
      )
      .isEqualTo(1)
    assertThat(logReader.correlationQueries.flatten()).contains(workItemName)
  }

  @Test
  fun `collect rejects incorrect WorkItem composite identifiers`() = runBlocking {
    val workItemName = "workItems/expected"
    val generation = 2L
    val attemptName = "$workItemName/workItemAttempts/expected"
    val node =
      ExpectedTraceNode(
        attemptName,
        NON_MEMOIZED_MODEL_LINE,
        "label",
        "SUCCEEDED",
        mapOf(
          "xmm.work_item.name" to workItemName,
          "xmm.work_item.generation" to generation.toString(),
          "xmm.work_item_attempt.name" to attemptName,
        ),
      )
    val collector =
      VidLabelingTraceCollector(
        logReaderFactory = {
          FakeCloudLogReader(
            listOf(
              entry("upload_registration", rawImpressionUpload = RAW_UPLOAD, traceId = ROOT_TRACE)
            )
          )
        },
        spanReader =
          CloudTraceReader { _, _, _, _, _, _ ->
            listOf(
              workItemSpan("wrong-name", "workItems/wrong", generation, attemptName),
              workItemSpan("wrong-generation", workItemName, generation + 1L, attemptName),
              workItemSpan(
                "wrong-attempt",
                workItemName,
                generation,
                "$workItemName/workItemAttempts/wrong",
              ),
            )
          },
        stateResolver =
          VidLabelingStateResolver {
            testGraph(listOf(NON_MEMOIZED_MODEL_LINE)).copy(nodes = listOf(node))
          },
        finalStateResolver = NOOP_FINAL_STATE_RESOLVER,
      )

    val collection = collector.collect(request())

    assertThat(collection.traceStatus).isEqualTo(VidLabelingTraceStatus.PARTIAL)
    assertThat(collection.modelLines.single().stages.single().evidenceCount).isEqualTo(0)
  }

  @Test
  fun `collect queries trace-only WorkItem evidence when authoritative sources fail`() =
    runBlocking {
      val workItemName = "workItems/unresolved-labeling"
      val baseGraph =
        testGraph(listOf(NON_MEMOIZED_MODEL_LINE))
          .copy(
            expectedWorkItems =
              listOf(
                ExpectedWorkItemGroup(
                  workItemName,
                  NON_MEMOIZED_MODEL_LINE,
                  "label",
                  listOf(workItemName),
                )
              )
          )
      val collector =
        VidLabelingTraceCollector(
          logReaderFactory = { CloudLogReader { _, _, _, _ -> error("logging unavailable") } },
          spanReader =
            CloudTraceReader { _, correlationValues, _, _, _, _ ->
              if (workItemName in correlationValues) {
                listOf(workItemSpan("unresolved-attempt", workItemName, 1L, null))
              } else {
                emptyList()
              }
            },
          stateResolver = VidLabelingStateResolver { baseGraph },
          finalStateResolver = NOOP_FINAL_STATE_RESOLVER,
          workItemResolver = VidLabelingWorkItemResolver { _, _, _ -> error("inventory failed") },
        )

      val collection = collector.collect(request())

      assertThat(collection.traceStatus).isEqualTo(VidLabelingTraceStatus.PARTIAL)
      assertThat(
          collection.sourceStatuses.single { it.source == "secure_computation_work_items" }.status
        )
        .isEqualTo("failed")
      assertThat(collection.sourceStatuses.single { it.source == "cloud_logging" }.status)
        .isEqualTo("failed")
      assertThat(collection.evidence.any { it.identifiers["xmm.work_item.name"] == workItemName })
        .isTrue()
    }

  @Test
  fun `collect traverses mixed routes and excludes another upload`() = runBlocking {
    val entries = buildList {
      add(entry("upload_registration", rawImpressionUpload = RAW_UPLOAD, traceId = ROOT_TRACE))
      addAll(routeEntries(MEMOIZED_MODEL_LINE, "memoized", MEMOIZED_STAGES, "memo", MEMO_TRACE))
      addAll(
        routeEntries(
          NON_MEMOIZED_MODEL_LINE,
          "non_memoized",
          NON_MEMOIZED_STAGES,
          "direct",
          DIRECT_TRACE,
        )
      )
      add(
        entry(
          "data_availability_publish",
          modelLineFields(MEMOIZED_MODEL_LINE),
          traceId = MEMO_AVAILABILITY_TRACE,
        )
      )
      add(
        entry(
          "data_availability_publish",
          modelLineFields(NON_MEMOIZED_MODEL_LINE),
          traceId = DIRECT_AVAILABILITY_TRACE,
        )
      )
      addAll(
        routeEntries(
          MEMOIZED_MODEL_LINE,
          "memoized",
          MEMOIZED_STAGES,
          "other",
          OTHER_TRACE,
          OTHER_RAW_UPLOAD,
        )
      )
    }
    val logReader = FakeCloudLogReader(entries)
    val workItem = "workItems/memoized-ranker-retry"
    val spanQueries = mutableListOf<Pair<Set<String>, Set<String>>>()
    val spanReader =
      CloudTraceReader { project, correlationValues, traceIds, startTime, endTime, _ ->
        spanQueries += correlationValues.toSet() to traceIds.toSet()
        val requiredSpans = requiredTraceSpans(project, traceIds, startTime, endTime)
        if (RAW_UPLOAD !in correlationValues && ROOT_TRACE !in traceIds) {
          requiredSpans
        } else {
          listOf(
            CloudTraceSpan(
              project,
              ROOT_TRACE,
              "span-1",
              null,
              "edpa.vid_labeling.rank",
              "vid-rank-builder",
              Instant.parse("2026-09-01T00:00:01Z"),
              Instant.parse("2026-09-01T00:00:02Z"),
              mapOf(
                "xmm.lifecycle.stage" to "rank",
                "xmm.outcome" to "succeeded",
                "xmm.edpa.raw_impression_upload.name" to RAW_UPLOAD,
                "xmm.model_line.name" to MEMOIZED_MODEL_LINE,
                "xmm.work_item.name" to workItem,
                "xmm.work_item.generation" to "2",
              ),
            )
          ) + requiredSpans.filterNot { it.traceId == ROOT_TRACE }
        }
      }
    val collector =
      VidLabelingTraceCollector(
        logReaderFactory = { logReader },
        spanReader = spanReader,
        stateResolver = VidLabelingStateResolver { testGraph() },
        finalStateResolver = NOOP_FINAL_STATE_RESOLVER,
      )

    val collection = collector.collect(request())

    assertThat(collection.traceStatus).isEqualTo(VidLabelingTraceStatus.COMPLETE)
    assertThat(collection.executionStatus).isEqualTo(VidLabelingExecutionStatus.SUCCEEDED)
    assertThat(collection.modelLines.map { it.route })
      .containsExactly(VidLabelingRoute.MEMOIZED, VidLabelingRoute.NON_MEMOIZED)
    assertThat(collection.modelLines.flatMap { it.missingStages }).isEmpty()
    assertThat(collection.evidence.flatMap { it.identifiers.values })
      .doesNotContain(OTHER_RAW_UPLOAD)
    assertThat(collection.evidence.flatMap { it.identifiers.values }).contains(workItem)
    assertThat(collection.evidence.any { it.source == "span" }).isTrue()
    assertThat(spanQueries.any { (values, _) -> RAW_UPLOAD in values }).isTrue()
    assertThat(logReader.traceQueries.flatten())
      .containsAtLeast(ROOT_TRACE, MEMO_AVAILABILITY_TRACE, DIRECT_AVAILABILITY_TRACE)
    val artifact = VidLabelingTraceOutput.render(collection)
    assertThat(artifact).contains(MEMOIZED_MODEL_LINE)
    assertThat(artifact).contains(NON_MEMOIZED_MODEL_LINE)
    assertThat(artifact).doesNotContain("gs://private-bucket/raw-input")
  }

  @Test
  fun `one broad availability publication satisfies every covered row node`() = runBlocking {
    val intervalStart = "2026-09-01T00:00:00Z"
    val intervalEnd = "2026-09-02T00:00:00Z"
    val graph =
      testGraph(listOf(MEMOIZED_MODEL_LINE))
        .copy(
          nodes =
            listOf(
              availabilityNode(
                "healed-replacement-availability",
                MEMOIZED_MODEL_LINE,
                intervalStart,
                intervalEnd,
              ),
              availabilityNode(
                "deferred-upload-availability",
                MEMOIZED_MODEL_LINE,
                intervalStart,
                intervalEnd,
              ),
            )
        )
    val entry =
      entry(
        "data_availability_publish",
        modelLineFields(MEMOIZED_MODEL_LINE) +
          " xmm.edpa.availability.interval_start=$intervalStart" +
          " xmm.edpa.availability.interval_end=$intervalEnd",
        traceId = ROOT_TRACE,
      )
    val collector =
      VidLabelingTraceCollector(
        logReaderFactory = { FakeCloudLogReader(listOf(entry)) },
        spanReader = completeTraceReader(),
        stateResolver = VidLabelingStateResolver { graph },
        finalStateResolver = NOOP_FINAL_STATE_RESOLVER,
      )

    val collection = collector.collect(request())

    assertThat(collection.traceStatus).isEqualTo(VidLabelingTraceStatus.COMPLETE)
    assertThat(collection.modelLines.single().stages.map { it.evidenceCount }).containsExactly(1, 1)
    assertThat(collection.evidence.count { it.stage == "data_availability_publish" }).isEqualTo(1)
  }

  @Test
  fun `single-day availability evidence remains isolated by model line`() = runBlocking {
    val intervalStart = "2026-09-01T00:00:00Z"
    val intervalEnd = "2026-09-02T00:00:00Z"
    val graph =
      testGraph()
        .copy(
          nodes =
            listOf(
              availabilityNode(
                "memoized-availability",
                MEMOIZED_MODEL_LINE,
                intervalStart,
                intervalEnd,
              ),
              availabilityNode(
                "non-memoized-availability",
                NON_MEMOIZED_MODEL_LINE,
                intervalStart,
                intervalEnd,
              ),
            )
        )
    val entry =
      entry(
        "data_availability_publish",
        modelLineFields(MEMOIZED_MODEL_LINE) +
          " xmm.edpa.availability.interval_start=$intervalStart" +
          " xmm.edpa.availability.interval_end=$intervalEnd",
        traceId = ROOT_TRACE,
      )
    val collector =
      VidLabelingTraceCollector(
        logReaderFactory = { FakeCloudLogReader(listOf(entry)) },
        spanReader = completeTraceReader(),
        stateResolver = VidLabelingStateResolver { graph },
        finalStateResolver = NOOP_FINAL_STATE_RESOLVER,
      )

    val collection = collector.collect(request())

    assertThat(collection.modelLines.single { it.modelLine == MEMOIZED_MODEL_LINE }.missingStages)
      .isEmpty()
    assertThat(
        collection.modelLines.single { it.modelLine == NON_MEMOIZED_MODEL_LINE }.missingStages
      )
      .containsExactly("non-memoized-availability")
    Unit
  }

  @Test
  fun `ImpressionMetadata evidence matches authoritative identity without raw upload`() =
    runBlocking {
      val metadataName = "dataProviders/123/impressionMetadata/metadata-1"
      val pathHash = "a".repeat(64)
      val node =
        ExpectedTraceNode(
          metadataName,
          NON_MEMOIZED_MODEL_LINE,
          "data_availability_metadata",
          "ACTIVE",
          mapOf(
            "xmm.model_line.name" to NON_MEMOIZED_MODEL_LINE,
            "xmm.edpa.impression_metadata.name" to metadataName,
            "xmm.gcs.object.path_hash" to pathHash,
          ),
        )
      val metadataEntry =
        entry(
          "data_availability_metadata",
          "xmm.model_line.name=$NON_MEMOIZED_MODEL_LINE " +
            "xmm.edpa.impression_metadata.name=$metadataName " +
            "xmm.gcs.object.path_hash=$pathHash",
          traceId = DIRECT_TRACE,
        )
      val collector =
        VidLabelingTraceCollector(
          logReaderFactory = { FakeCloudLogReader(listOf(metadataEntry)) },
          spanReader = completeTraceReader(),
          stateResolver =
            VidLabelingStateResolver {
              testGraph(listOf(NON_MEMOIZED_MODEL_LINE)).copy(nodes = listOf(node))
            },
          finalStateResolver = NOOP_FINAL_STATE_RESOLVER,
        )

      val collection = collector.collect(request())

      assertThat(collection.traceStatus).isEqualTo(VidLabelingTraceStatus.COMPLETE)
      assertThat(collection.modelLines.single().stages.single().evidenceCount).isEqualTo(1)
    }

  @Test
  fun `GCS evidence matches authoritative positive object identity without raw upload`() =
    runBlocking {
      val pathHash = "d".repeat(64)
      val generation = 17L
      val node =
        ExpectedTraceNode(
          "gcs-object",
          NON_MEMOIZED_MODEL_LINE,
          "label",
          "PUBLISHED",
          mapOf(
            "xmm.gcs.object.path_hash" to pathHash,
            "xmm.gcs.object.generation" to generation.toString(),
          ),
        )
      val gcsEntry =
        entry(
          "label",
          "xmm.gcs.object.path_hash=$pathHash xmm.gcs.object.generation=$generation",
          traceId = "77777777777777777777777777777777",
        )
      val collector =
        VidLabelingTraceCollector(
          logReaderFactory = { FakeCloudLogReader(listOf(gcsEntry)) },
          spanReader = completeTraceReader(),
          stateResolver =
            VidLabelingStateResolver {
              testGraph(listOf(NON_MEMOIZED_MODEL_LINE)).copy(nodes = listOf(node))
            },
          finalStateResolver = NOOP_FINAL_STATE_RESOLVER,
        )

      val collection = collector.collect(request())

      assertThat(collection.traceStatus).isEqualTo(VidLabelingTraceStatus.COMPLETE)
      assertThat(collection.modelLines.single().stages.single().evidenceCount).isEqualTo(1)
    }

  @Test
  fun `GCS evidence does not match an authoritative zero generation`() = runBlocking {
    val pathHash = "e".repeat(64)
    val node =
      ExpectedTraceNode(
        "missing-gcs-object",
        NON_MEMOIZED_MODEL_LINE,
        "label",
        "PUBLISHED",
        mapOf("xmm.gcs.object.path_hash" to pathHash, "xmm.gcs.object.generation" to "0"),
      )
    val gcsEntry =
      entry(
        "label",
        "xmm.edpa.raw_impression_upload.name=$RAW_UPLOAD " +
          "xmm.gcs.object.path_hash=$pathHash xmm.gcs.object.generation=0",
        traceId = "77777777777777777777777777777778",
      )
    val collector =
      VidLabelingTraceCollector(
        logReaderFactory = { CloudLogReader { _, _, _, _ -> listOf(gcsEntry) } },
        spanReader = completeTraceReader(),
        stateResolver =
          VidLabelingStateResolver {
            testGraph(listOf(NON_MEMOIZED_MODEL_LINE)).copy(nodes = listOf(node))
          },
        finalStateResolver = NOOP_FINAL_STATE_RESOLVER,
      )

    val collection = collector.collect(request())

    assertThat(collection.traceStatus).isEqualTo(VidLabelingTraceStatus.PARTIAL)
    assertThat(collection.modelLines.single().stages.single().evidenceCount).isEqualTo(0)
  }

  @Test
  fun `final-state evidence rejects unrelated composite identities`() = runBlocking {
    val metadataName = "dataProviders/123/impressionMetadata/metadata-1"
    val pathHash = "a".repeat(64)
    val intervalStart = "2026-09-01T00:00:00Z"
    val intervalEnd = "2026-09-02T00:00:00Z"
    val graph =
      testGraph(listOf(NON_MEMOIZED_MODEL_LINE))
        .copy(
          nodes =
            listOf(
              ExpectedTraceNode(
                metadataName,
                NON_MEMOIZED_MODEL_LINE,
                "data_availability_metadata",
                "ACTIVE",
                mapOf(
                  "xmm.model_line.name" to NON_MEMOIZED_MODEL_LINE,
                  "xmm.edpa.impression_metadata.name" to metadataName,
                  "xmm.gcs.object.path_hash" to pathHash,
                ),
              ),
              availabilityNode("availability", NON_MEMOIZED_MODEL_LINE, intervalStart, intervalEnd),
            )
        )
    val unrelatedEntries =
      listOf(
        entry(
          "data_availability_metadata",
          "xmm.model_line.name=$NON_MEMOIZED_MODEL_LINE " +
            "xmm.edpa.impression_metadata.name=dataProviders/123/impressionMetadata/other " +
            "xmm.gcs.object.path_hash=$pathHash",
          traceId = "77777777777777777777777777777771",
        ),
        entry(
          "data_availability_metadata",
          "xmm.model_line.name=$MEMOIZED_MODEL_LINE " +
            "xmm.edpa.impression_metadata.name=$metadataName " +
            "xmm.gcs.object.path_hash=$pathHash",
          traceId = "77777777777777777777777777777772",
        ),
        entry(
          "data_availability_metadata",
          "xmm.model_line.name=$NON_MEMOIZED_MODEL_LINE " +
            "xmm.edpa.impression_metadata.name=$metadataName " +
            "xmm.gcs.object.path_hash=${"b".repeat(64)}",
          traceId = "77777777777777777777777777777773",
        ),
        entry(
          "data_availability_publish",
          "xmm.model_line.name=$MEMOIZED_MODEL_LINE " +
            "xmm.edpa.availability.interval_start=$intervalStart " +
            "xmm.edpa.availability.interval_end=$intervalEnd",
          traceId = "77777777777777777777777777777774",
        ),
        entry(
          "data_availability_publish",
          "xmm.model_line.name=$NON_MEMOIZED_MODEL_LINE " +
            "xmm.edpa.availability.interval_start=2026-08-31T00:00:00Z " +
            "xmm.edpa.availability.interval_end=$intervalEnd",
          traceId = "77777777777777777777777777777775",
        ),
        entry(
          "data_availability_publish",
          "xmm.model_line.name=$NON_MEMOIZED_MODEL_LINE " +
            "xmm.edpa.availability.interval_start=$intervalStart " +
            "xmm.edpa.availability.interval_end=2026-09-03T00:00:00Z",
          traceId = "77777777777777777777777777777776",
        ),
      )
    val collector =
      VidLabelingTraceCollector(
        logReaderFactory = { CloudLogReader { _, _, _, _ -> unrelatedEntries } },
        spanReader = completeTraceReader(),
        stateResolver = VidLabelingStateResolver { graph },
        finalStateResolver = NOOP_FINAL_STATE_RESOLVER,
      )

    val collection = collector.collect(request())

    assertThat(collection.traceStatus).isEqualTo(VidLabelingTraceStatus.PARTIAL)
    assertThat(collection.modelLines.single().stages.map { it.evidenceCount }).containsExactly(0, 0)
    Unit
  }

  @Test
  fun `collect reports discovery caps as partial`() = runBlocking {
    val entries =
      listOf(
        entry(
          "upload_registration",
          "xmm.edpa.vid_labeling_job.name=" + RAW_UPLOAD + "/vidLabelingJobs/job-1",
          rawImpressionUpload = RAW_UPLOAD,
          traceId = ROOT_TRACE,
        ),
        entry(
          "dispatch",
          "xmm.edpa.vid_labeling_job.name=" + RAW_UPLOAD + "/vidLabelingJobs/job-2",
          rawImpressionUpload = RAW_UPLOAD,
          traceId = MEMO_TRACE,
        ),
      )
    val collector =
      VidLabelingTraceCollector(
        logReaderFactory = { FakeCloudLogReader(entries) },
        spanReader = completeTraceReader(),
        stateResolver = VidLabelingStateResolver { testGraph() },
        finalStateResolver = NOOP_FINAL_STATE_RESOLVER,
      )

    val collection = collector.collect(request(correlationValueLimit = 1, traceIdLimit = 1))

    assertThat(collection.traceStatus).isEqualTo(VidLabelingTraceStatus.PARTIAL)
    assertThat(collection.warnings.single { "Discovery limit" in it })
      .contains("correlation values and trace IDs")
    assertThat(collection.sourceStatuses.any { it.status == "truncated" }).isTrue()
  }

  @Test
  fun `collect reports exhausted expansion rounds as partial`() = runBlocking {
    val entries =
      listOf(
        entry(
          "upload_registration",
          "xmm.edpa.vid_labeling_job.name=" + RAW_UPLOAD + "/vidLabelingJobs/new-job",
          rawImpressionUpload = RAW_UPLOAD,
          traceId = ROOT_TRACE,
        )
      )
    val rootOnlyGraph = testGraph(emptyList())
    val collector =
      VidLabelingTraceCollector(
        logReaderFactory = { FakeCloudLogReader(entries) },
        spanReader = CloudTraceReader { _, _, _, _, _, _ -> emptyList() },
        stateResolver = VidLabelingStateResolver { rootOnlyGraph },
        finalStateResolver = NOOP_FINAL_STATE_RESOLVER,
      )

    val collection = collector.collect(request(expansionRounds = 1))

    assertThat(collection.traceStatus).isEqualTo(VidLabelingTraceStatus.PARTIAL)
    assertThat(collection.warnings).contains("Discovery limit reached for expansion rounds.")
  }

  @Test
  fun `collect reports a registered upload with no model lines as no work`() = runBlocking {
    val graph =
      testGraph(emptyList()).let {
        it.copy(upload = it.upload.toBuilder().setRegistrationComplete(true).build())
      }
    val entries =
      listOf(entry("upload_registration", rawImpressionUpload = RAW_UPLOAD, traceId = ROOT_TRACE))
    val collector =
      VidLabelingTraceCollector(
        logReaderFactory = { FakeCloudLogReader(entries) },
        spanReader = completeTraceReader(),
        stateResolver = VidLabelingStateResolver { graph },
        finalStateResolver = NOOP_FINAL_STATE_RESOLVER,
      )

    val collection = collector.collect(request())

    assertThat(collection.traceStatus).isEqualTo(VidLabelingTraceStatus.COMPLETE)
    assertThat(collection.executionStatus).isEqualTo(VidLabelingExecutionStatus.NO_WORK)
  }

  @Test
  fun `collect requires evidence for every concrete child`() = runBlocking {
    val modelLine = NON_MEMOIZED_MODEL_LINE
    val childOne = RAW_UPLOAD + "/vidLabelingJobs/one"
    val childTwo = RAW_UPLOAD + "/vidLabelingJobs/two"
    val graph =
      testGraph(listOf(modelLine))
        .copy(
          nodes =
            listOf(
              ExpectedTraceNode(
                childOne,
                modelLine,
                "label",
                "SUCCEEDED",
                mapOf(
                  "xmm.edpa.vid_labeling_job.name" to childOne,
                  "xmm.model_line.name" to modelLine,
                ),
              ),
              ExpectedTraceNode(
                childTwo,
                modelLine,
                "label",
                "SUCCEEDED",
                mapOf(
                  "xmm.edpa.vid_labeling_job.name" to childTwo,
                  "xmm.model_line.name" to modelLine,
                ),
              ),
            )
        )
    val entries =
      listOf(
        entry(
          "label",
          "xmm.model_line.name=" + modelLine + " xmm.edpa.vid_labeling_job.name=" + childOne,
          rawImpressionUpload = RAW_UPLOAD,
          traceId = ROOT_TRACE,
        )
      )
    val collector =
      VidLabelingTraceCollector(
        logReaderFactory = { FakeCloudLogReader(entries) },
        spanReader = CloudTraceReader { _, _, _, _, _, _ -> emptyList() },
        stateResolver = VidLabelingStateResolver { graph },
        finalStateResolver = NOOP_FINAL_STATE_RESOLVER,
      )

    val collection = collector.collect(request())

    assertThat(collection.traceStatus).isEqualTo(VidLabelingTraceStatus.PARTIAL)
    assertThat(collection.modelLines.single().missingStages).contains(childTwo)
  }

  @Test
  fun `collect treats downstream stages after terminal failure as not applicable`() = runBlocking {
    val modelLine = NON_MEMOIZED_MODEL_LINE
    val job = RAW_UPLOAD + "/vidLabelingJobs/failed"
    val graph =
      testGraph(listOf(modelLine))
        .copy(
          nodes =
            listOf(
              ExpectedTraceNode(
                job,
                modelLine,
                "label",
                "FAILED",
                mapOf("xmm.edpa.vid_labeling_job.name" to job, "xmm.model_line.name" to modelLine),
              ),
              ExpectedTraceNode(
                modelLine + ":availability",
                modelLine,
                "data_availability_publish",
                "NOT_REACHED",
                mapOf("xmm.model_line.name" to modelLine),
                ExpectedNodeDisposition.NOT_APPLICABLE,
              ),
            )
        )
    val entries =
      listOf(
        entry(
          "label",
          "xmm.model_line.name=" + modelLine + " xmm.edpa.vid_labeling_job.name=" + job,
          rawImpressionUpload = RAW_UPLOAD,
          outcome = "failed",
          traceId = ROOT_TRACE,
        )
      )
    val collector =
      VidLabelingTraceCollector(
        logReaderFactory = { FakeCloudLogReader(entries) },
        spanReader = completeTraceReader(),
        stateResolver = VidLabelingStateResolver { graph },
        finalStateResolver = NOOP_FINAL_STATE_RESOLVER,
      )

    val collection = collector.collect(request())

    assertThat(collection.traceStatus).isEqualTo(VidLabelingTraceStatus.COMPLETE)
    assertThat(collection.executionStatus).isEqualTo(VidLabelingExecutionStatus.FAILED)
  }

  @Test
  fun `runVidLabelingTrace exercises CLI contract`() {
    val workItemName = "workItems/cli-labeling"
    val generation = 3L
    val attemptName = "$workItemName/workItemAttempts/attempt-1"
    val metadataName = "dataProviders/123/impressionMetadata/cli-metadata"
    val pathHash = "c".repeat(64)
    val intervalStart = "2026-09-01T00:00:00Z"
    val intervalEnd = "2026-09-02T00:00:00Z"
    val workItemNodes =
      listOf(
        ExpectedTraceNode(
          "$workItemName:generation:$generation",
          NON_MEMOIZED_MODEL_LINE,
          "label",
          "SUCCEEDED",
          mapOf(
            "xmm.work_item.name" to workItemName,
            "xmm.work_item.generation" to generation.toString(),
          ),
        ),
        ExpectedTraceNode(
          attemptName,
          NON_MEMOIZED_MODEL_LINE,
          "label",
          "SUCCEEDED",
          mapOf(
            "xmm.work_item.name" to workItemName,
            "xmm.work_item.generation" to generation.toString(),
            "xmm.work_item_attempt.name" to attemptName,
          ),
        ),
      )
    val entries = buildList {
      add(entry("upload_registration", rawImpressionUpload = RAW_UPLOAD, traceId = ROOT_TRACE))
      add(
        entry("work_item_processing", "xmm.work_item.name=$workItemName", traceId = WORK_ITEM_TRACE)
      )
      addAll(
        routeEntries(
          NON_MEMOIZED_MODEL_LINE,
          "non_memoized",
          NON_MEMOIZED_STAGES,
          "direct",
          DIRECT_TRACE,
        )
      )
      add(
        entry(
          "data_availability_publish",
          modelLineFields(NON_MEMOIZED_MODEL_LINE) +
            " xmm.edpa.availability.interval_start=$intervalStart" +
            " xmm.edpa.availability.interval_end=$intervalEnd",
          traceId = DIRECT_AVAILABILITY_TRACE,
        )
      )
      add(
        entry(
          "data_availability_metadata",
          "xmm.model_line.name=$NON_MEMOIZED_MODEL_LINE " +
            "xmm.edpa.impression_metadata.name=$metadataName " +
            "xmm.gcs.object.path_hash=$pathHash",
          traceId = DIRECT_AVAILABILITY_TRACE,
        )
      )
    }
    var workItemPageLimit = 0
    val collector =
      VidLabelingTraceCollector(
        logReaderFactory = { FakeCloudLogReader(entries) },
        spanReader =
          CloudTraceReader { project, _, traceIds, startTime, endTime, _ ->
            requiredTraceSpans(project, traceIds, startTime, endTime) +
              if (traceIds.any { it.substringAfterLast('/') == WORK_ITEM_TRACE }) {
                listOf(workItemSpan("cli-attempt", workItemName, generation, attemptName))
              } else {
                emptyList()
              }
          },
        stateResolver = VidLabelingStateResolver { testGraph(listOf(NON_MEMOIZED_MODEL_LINE)) },
        finalStateResolver =
          VidLabelingFinalStateResolver { _, _ ->
            VidLabelingFinalStateResolution(
              listOf(
                ExpectedTraceNode(
                  metadataName,
                  NON_MEMOIZED_MODEL_LINE,
                  "data_availability_metadata",
                  "ACTIVE",
                  mapOf(
                    "xmm.model_line.name" to NON_MEMOIZED_MODEL_LINE,
                    "xmm.edpa.impression_metadata.name" to metadataName,
                    "xmm.gcs.object.path_hash" to pathHash,
                  ),
                ),
                availabilityNode(
                  "cli-availability",
                  NON_MEMOIZED_MODEL_LINE,
                  intervalStart,
                  intervalEnd,
                ),
              ),
              emptyMap(),
            )
          },
        workItemResolver =
          VidLabelingWorkItemResolver { _, _, pageLimit ->
            workItemPageLimit = pageLimit
            VidLabelingWorkItemResolution(workItemNodes, emptyList(), false, emptyMap())
          },
      )
    val output = StringWriter()

    val exitCode =
      main(
        arrayOf(
          "--raw-impression-upload=$RAW_UPLOAD",
          "--observability-project=observability-project",
          "--start-time=2026-08-31T00:00:00Z",
          "--end-time=2026-09-02T00:00:00Z",
          "--authoritative-source-timeout=PT3M",
          "--logging-source-timeout=PT4M",
          "--trace-source-timeout=PT5M",
          "--upload-timeout=PT12M",
          "--work-item-page-limit=7",
          "--edpa-public-api-target=unused",
          "--control-plane-api-target=unused",
          "--kingdom-public-api-target=unused",
          "--tls-cert-file=unused",
          "--tls-key-file=unused",
          "--cert-collection-file=unused",
          "--control-plane-tls-cert-file=unused-control-plane",
          "--control-plane-tls-key-file=unused-control-plane",
          "--control-plane-cert-collection-file=unused-control-plane",
          "--kingdom-tls-cert-file=unused-kingdom",
          "--kingdom-tls-key-file=unused-kingdom",
          "--kingdom-cert-collection-file=unused-kingdom",
        ),
        VidLabelingTraceDependencies(
          collector,
          Clock.fixed(Instant.parse("2026-09-02T00:00:00Z"), ZoneOffset.UTC),
          PrintWriter(output, true),
        ),
      )

    assertThat(exitCode).isEqualTo(0)
    assertThat(workItemPageLimit).isEqualTo(7)
    assertThat(output.toString()).contains("Evidence status: **complete**")
    assertThat(output.toString()).contains("# VID labeling trace")
  }

  @Test
  fun `runVidLabelingTrace rejects rates above project quotas`() {
    val collector =
      VidLabelingTraceCollector(
        logReaderFactory = { FakeCloudLogReader(emptyList()) },
        spanReader = CloudTraceReader { _, _, _, _, _, _ -> emptyList() },
        stateResolver = VidLabelingStateResolver { testGraph(emptyList()) },
        finalStateResolver = NOOP_FINAL_STATE_RESOLVER,
      )

    val exitCode =
      main(
        arrayOf(
          "--raw-impression-upload=$RAW_UPLOAD",
          "--observability-project=project",
          "--trace-quota-units-per-second=5.1",
          "--edpa-public-api-target=unused",
          "--control-plane-api-target=unused",
          "--kingdom-public-api-target=unused",
          "--tls-cert-file=unused",
          "--tls-key-file=unused",
          "--cert-collection-file=unused",
          "--control-plane-tls-cert-file=unused-control-plane",
          "--control-plane-tls-key-file=unused-control-plane",
          "--control-plane-cert-collection-file=unused-control-plane",
          "--kingdom-tls-cert-file=unused-kingdom",
          "--kingdom-tls-key-file=unused-kingdom",
          "--kingdom-cert-collection-file=unused-kingdom",
        ),
        VidLabelingTraceDependencies(collector),
      )

    assertThat(exitCode).isNotEqualTo(0)
  }

  @Test
  fun `collect queries a 32 shard graph without per-resource trace discovery`() = runBlocking {
    val shardJobs = (0 until 32).map { index -> "$RAW_UPLOAD/poolAssignmentJobs/shard-$index" }
    val shardTraceIds = (0 until 32).map { index -> index.toString(16).padStart(32, '0') }
    val graph =
      testGraph(listOf(MEMOIZED_MODEL_LINE))
        .copy(
          nodes =
            testGraph(listOf(MEMOIZED_MODEL_LINE)).nodes +
              shardJobs.mapIndexed { index, name ->
                ExpectedTraceNode(
                  name,
                  MEMOIZED_MODEL_LINE,
                  "pool_assignment",
                  "SUCCEEDED",
                  mapOf(
                    "xmm.edpa.raw_impression_upload.name" to RAW_UPLOAD,
                    "xmm.model_line.name" to MEMOIZED_MODEL_LINE,
                    "xmm.edpa.pool_assignment_job.name" to name,
                    "xmm.edpa.shard_index" to index.toString(),
                  ),
                )
              }
        )
    val logReader = FakeCloudLogReader(emptyList())
    val traceQueries = mutableListOf<Pair<Set<String>, Set<String>>>()
    var logReaderCreations = 0
    val collector =
      VidLabelingTraceCollector(
        logReaderFactory = {
          logReaderCreations++
          logReader
        },
        spanReader =
          CloudTraceReader { _, correlationValues, traceIds, _, _, _ ->
            traceQueries += correlationValues.toSet() to traceIds.toSet()
            if (RAW_UPLOAD !in correlationValues) {
              emptyList()
            } else {
              shardTraceIds.mapIndexed { index, traceId ->
                CloudTraceSpan(
                  "observability-project",
                  traceId,
                  "span-$index",
                  null,
                  "edpa.vid_labeling.pool_assignment",
                  "subpool-assigner",
                  Instant.parse("2026-09-01T00:00:01Z"),
                  Instant.parse("2026-09-01T00:00:02Z"),
                  mapOf(
                    "xmm.lifecycle.stage" to "pool_assignment",
                    "xmm.outcome" to "succeeded",
                    "xmm.edpa.raw_impression_upload.name" to RAW_UPLOAD,
                    "xmm.edpa.pool_assignment_job.name" to shardJobs[index],
                  ),
                )
              }
            }
          },
        stateResolver = VidLabelingStateResolver { graph },
        finalStateResolver = NOOP_FINAL_STATE_RESOLVER,
      )

    collector.collect(request(expansionRounds = 2))
    collector.collect(request(expansionRounds = 2))

    assertThat(logReaderCreations).isEqualTo(1)
    assertThat(logReader.correlationQueries).hasSize(4)
    assertThat(logReader.correlationQueries[0]).containsExactly(RAW_UPLOAD)
    assertThat(logReader.correlationQueries[1]).containsAtLeastElementsIn(shardJobs)
    assertThat(logReader.correlationQueries[2]).containsExactly(RAW_UPLOAD)
    assertThat(logReader.correlationQueries[3]).containsAtLeastElementsIn(shardJobs)
    assertThat(traceQueries).hasSize(2)
    assertThat(traceQueries.map { it.first }).containsExactly(setOf(RAW_UPLOAD), setOf(RAW_UPLOAD))
    assertThat(traceQueries.all { it.second.isEmpty() }).isTrue()
  }

  @Test
  fun `collect expands a stable identifier discovered only in a span`() = runBlocking {
    val recoveryWorkItem = "workItems/span-discovered-recovery"
    val entries =
      listOf(
        entry("upload_registration", rawImpressionUpload = RAW_UPLOAD, traceId = ROOT_TRACE),
        entry(
          "monitor_recover",
          "xmm.edpa.recovery_work_item.name=$recoveryWorkItem",
          traceId = MEMO_TRACE,
        ),
      )
    val collector =
      VidLabelingTraceCollector(
        logReaderFactory = { FakeCloudLogReader(entries) },
        spanReader =
          CloudTraceReader { project, correlationValues, _, _, _, _ ->
            if (RAW_UPLOAD !in correlationValues) {
              emptyList()
            } else {
              listOf(
                CloudTraceSpan(
                  project,
                  ROOT_TRACE,
                  "span-root",
                  null,
                  "edpa.vid_labeling.dispatch",
                  "dispatcher",
                  Instant.parse("2026-09-01T00:00:01Z"),
                  Instant.parse("2026-09-01T00:00:02Z"),
                  mapOf(
                    "xmm.lifecycle.stage" to "dispatch",
                    "xmm.outcome" to "succeeded",
                    "xmm.edpa.raw_impression_upload.name" to RAW_UPLOAD,
                    "xmm.edpa.recovery_work_item.name" to recoveryWorkItem,
                  ),
                )
              )
            }
          },
        stateResolver = VidLabelingStateResolver { testGraph(emptyList()) },
        finalStateResolver = NOOP_FINAL_STATE_RESOLVER,
      )

    val collection = collector.collect(request(expansionRounds = 3))

    assertThat(collection.evidence.mapNotNull { it.traceId }).contains(MEMO_TRACE)
    assertThat(collection.evidence.flatMap { it.identifiers.values }).contains(recoveryWorkItem)
  }

  @Test
  fun `collect keeps per-project source failure separate and strict`() = runBlocking {
    val entry = entry("upload_registration", rawImpressionUpload = RAW_UPLOAD)
    val collector =
      VidLabelingTraceCollector(
        logReaderFactory = { project ->
          object : CloudLogReader {
            override suspend fun read(
              correlationValues: Collection<String>,
              startTime: Instant,
              endTime: Instant,
              limit: Int,
            ): List<CloudLogEntry> = listOf(entry)

            override suspend fun readBatch(
              correlationValues: Collection<String>,
              traceIds: Collection<String>,
              startTime: Instant,
              endTime: Instant,
              limit: Int,
            ): CloudTelemetryReadResult<CloudLogEntry> =
              CloudTelemetryReadResult(
                listOf(entry),
                complete = project == "project-1",
                CloudTelemetryReadCounts(physicalRequests = 1, rowsExamined = 1),
                if (project == "project-1") "" else "retry exhausted",
                failed = project != "project-1",
              )
          }
        },
        spanReader = CloudTraceReader { _, _, _, _, _, _ -> emptyList() },
        stateResolver = VidLabelingStateResolver { testGraph(emptyList()) },
        finalStateResolver = NOOP_FINAL_STATE_RESOLVER,
      )

    val collection = collector.collect(request(projects = listOf("project-1", "project-2")))

    assertThat(collection.traceStatus).isEqualTo(VidLabelingTraceStatus.PARTIAL)
    assertThat(
        collection.sourceStatuses
          .single { it.project == "project-1" && it.source == "cloud_logging" }
          .status
      )
      .isEqualTo("complete")
    assertThat(
        collection.sourceStatuses
          .single { it.project == "project-2" && it.source == "cloud_logging" }
          .status
      )
      .isEqualTo("failed")
  }

  @Test
  fun `collect fetches log-linked trace only from its owning project`() = runBlocking {
    val queries = mutableListOf<Triple<String, Set<String>, Set<String>>>()
    val collector =
      VidLabelingTraceCollector(
        logReaderFactory = { project ->
          FakeCloudLogReader(
            if (project == "project-1") {
              listOf(
                CloudLogEntry(
                  "project-1",
                  Instant.parse("2026-09-01T00:00:00Z"),
                  "edpa",
                  "INFO",
                  "projects/project-2/traces/$ROOT_TRACE",
                  "event=registered xmm.lifecycle.stage=upload_registration " +
                    "xmm.edpa.raw_impression_upload.name=$RAW_UPLOAD",
                )
              )
            } else {
              emptyList()
            }
          )
        },
        spanReader =
          CloudTraceReader { project, correlationValues, traceIds, _, _, _ ->
            queries += Triple(project, correlationValues.toSet(), traceIds.toSet())
            emptyList()
          },
        stateResolver = VidLabelingStateResolver { testGraph(emptyList()) },
        finalStateResolver = NOOP_FINAL_STATE_RESOLVER,
      )

    collector.collect(request(projects = listOf("project-1", "project-2")))

    assertThat(queries.none { (project, _, ids) -> project == "project-1" && ROOT_TRACE in ids })
      .isTrue()
    assertThat(queries.any { (project, _, ids) -> project == "project-2" && ROOT_TRACE in ids })
      .isTrue()
  }

  @Test
  fun `latest successful terminal evidence wins over earlier failed attempt`() = runBlocking {
    val entries = buildList {
      add(entry("upload_registration", rawImpressionUpload = RAW_UPLOAD, traceId = ROOT_TRACE))
      addAll(
        routeEntries(
          NON_MEMOIZED_MODEL_LINE,
          "non_memoized",
          NON_MEMOIZED_STAGES,
          "direct",
          DIRECT_TRACE,
        )
      )
      add(
        entry(
          "data_availability_publish",
          modelLineFields(NON_MEMOIZED_MODEL_LINE),
          seconds = -1,
          outcome = "failed",
          traceId = DIRECT_AVAILABILITY_TRACE,
        )
      )
      add(
        entry(
          "data_availability_publish",
          modelLineFields(NON_MEMOIZED_MODEL_LINE),
          seconds = 100,
          traceId = DIRECT_AVAILABILITY_TRACE,
        )
      )
    }
    val collector =
      VidLabelingTraceCollector(
        logReaderFactory = { FakeCloudLogReader(entries) },
        spanReader = CloudTraceReader { _, _, _, _, _, _ -> emptyList() },
        stateResolver = VidLabelingStateResolver { testGraph(listOf(NON_MEMOIZED_MODEL_LINE)) },
        finalStateResolver = NOOP_FINAL_STATE_RESOLVER,
      )

    val collection = collector.collect(request())

    assertThat(collection.executionStatus).isEqualTo(VidLabelingExecutionStatus.SUCCEEDED)
    assertThat(collection.warnings)
      .contains("Earlier failed attempts were followed by successful terminal evidence.")
  }

  @Test
  fun `collect reports source failure independently from execution status`() = runBlocking {
    val collector =
      VidLabelingTraceCollector(
        logReaderFactory = { CloudLogReader { _, _, _, _ -> throw IllegalStateException() } },
        spanReader = CloudTraceReader { _, _, _, _, _, _ -> emptyList() },
        stateResolver = VidLabelingStateResolver { testGraph() },
        finalStateResolver = NOOP_FINAL_STATE_RESOLVER,
      )

    val collection = collector.collect(request())

    assertThat(collection.traceStatus).isEqualTo(VidLabelingTraceStatus.PARTIAL)
    assertThat(collection.executionStatus).isEqualTo(VidLabelingExecutionStatus.UNKNOWN)
    assertThat(collection.sourceStatuses.map { it.status }).contains("failed")
  }

  @Test
  fun `collect returns partial evidence when Cloud Logging times out`() = runTest {
    val traceEvidence =
      CloudTraceSpan(
        "observability-project",
        ROOT_TRACE,
        "span-$ROOT_TRACE",
        null,
        "edpa.vid_labeling.upload_registration",
        "worker",
        Instant.parse("2026-09-01T00:00:01Z"),
        Instant.parse("2026-09-01T00:00:02Z"),
        mapOf(
          "xmm.lifecycle.stage" to "upload_registration",
          "xmm.outcome" to "succeeded",
          "xmm.edpa.raw_impression_upload.name" to RAW_UPLOAD,
        ),
      )
    val collector =
      VidLabelingTraceCollector(
        logReaderFactory = { CloudLogReader { _, _, _, _ -> awaitCancellation() } },
        spanReader = CloudTraceReader { _, _, _, _, _, _ -> listOf(traceEvidence) },
        stateResolver = VidLabelingStateResolver { testGraph(emptyList()) },
        finalStateResolver = NOOP_FINAL_STATE_RESOLVER,
      )

    val collection =
      collector.collect(request(expansionRounds = 1, loggingSourceTimeout = Duration.ofSeconds(1)))

    assertThat(
        collection.sourceStatuses.any { it.source == "cloud_logging" && it.status == "timed_out" }
      )
      .isTrue()
    assertThat(collection.traceStatus).isEqualTo(VidLabelingTraceStatus.PARTIAL)
    assertThat(collection.evidence).hasSize(1)
  }

  @Test
  fun `collect returns partial evidence when Cloud Trace times out`() = runTest {
    val collector =
      VidLabelingTraceCollector(
        logReaderFactory = {
          FakeCloudLogReader(
            listOf(
              entry("upload_registration", rawImpressionUpload = RAW_UPLOAD, traceId = ROOT_TRACE)
            )
          )
        },
        spanReader = CloudTraceReader { _, _, _, _, _, _ -> awaitCancellation() },
        stateResolver = VidLabelingStateResolver { testGraph(emptyList()) },
        finalStateResolver = NOOP_FINAL_STATE_RESOLVER,
      )

    val collection =
      collector.collect(request(expansionRounds = 1, traceSourceTimeout = Duration.ofSeconds(1)))

    assertThat(
        collection.sourceStatuses.any { it.source == "cloud_trace" && it.status == "timed_out" }
      )
      .isTrue()
    assertThat(collection.traceStatus).isEqualTo(VidLabelingTraceStatus.PARTIAL)
    assertThat(collection.evidence).isNotEmpty()
  }

  @OptIn(ExperimentalCoroutinesApi::class)
  @Test
  fun `collect names each later authoritative source timeout`() = runTest {
    val collectors =
      listOf(
        "secure_computation_work_items" to
          VidLabelingTraceCollector(
            logReaderFactory = { FakeCloudLogReader(emptyList()) },
            spanReader = CloudTraceReader { _, _, _, _, _, _ -> emptyList() },
            stateResolver = VidLabelingStateResolver { testGraph(emptyList()) },
            finalStateResolver = NOOP_FINAL_STATE_RESOLVER,
            workItemResolver = VidLabelingWorkItemResolver { _, _, _ -> awaitCancellation() },
          ),
        "kingdom_route_state" to
          VidLabelingTraceCollector(
            logReaderFactory = { FakeCloudLogReader(emptyList()) },
            spanReader = CloudTraceReader { _, _, _, _, _, _ -> emptyList() },
            stateResolver = VidLabelingStateResolver { testGraph(emptyList()) },
            finalStateResolver = NOOP_FINAL_STATE_RESOLVER,
            routeResolver = VidLabelingRouteResolver { _, _ -> awaitCancellation() },
          ),
        "gcs_and_availability_state" to
          VidLabelingTraceCollector(
            logReaderFactory = { FakeCloudLogReader(emptyList()) },
            spanReader = CloudTraceReader { _, _, _, _, _, _ -> emptyList() },
            stateResolver = VidLabelingStateResolver { testGraph(emptyList()) },
            finalStateResolver = VidLabelingFinalStateResolver { _, _, _ -> awaitCancellation() },
          ),
      )

    for ((source, collector) in collectors) {
      val collection =
        collector.collect(request(authoritativeSourceTimeout = Duration.ofSeconds(1)))

      assertThat(collection.traceStatus).isEqualTo(VidLabelingTraceStatus.PARTIAL)
      assertThat(collection.sourceStatuses.single { it.source == source }.status)
        .isEqualTo("timed_out")
    }
  }

  @Test
  fun `collect resolves final state once`() = runBlocking {
    var invocations = 0
    val collector =
      VidLabelingTraceCollector(
        logReaderFactory = {
          FakeCloudLogReader(
            listOf(entry("upload_registration", rawImpressionUpload = RAW_UPLOAD, traceId = null))
          )
        },
        spanReader = CloudTraceReader { _, _, _, _, _, _ -> emptyList() },
        stateResolver = VidLabelingStateResolver { testGraph(emptyList()) },
        finalStateResolver =
          VidLabelingFinalStateResolver { _, _, _ ->
            invocations++
            VidLabelingFinalStateResolution(emptyList(), mapOf("gcs_reads" to 0L))
          },
      )

    collector.collect(request())

    assertThat(invocations).isEqualTo(1)
  }

  @Test
  fun `Logging deadline is shared across projects and expansion rounds`() = runBlocking {
    var nowNanos = 0L
    var queries = 0
    val reader =
      object : CloudLogReader {
        override suspend fun read(
          correlationValues: Collection<String>,
          startTime: Instant,
          endTime: Instant,
          limit: Int,
        ): List<CloudLogEntry> {
          queries++
          nowNanos += Duration.ofMillis(600).toNanos()
          return listOf(
            entry("upload_registration", rawImpressionUpload = RAW_UPLOAD, traceId = null)
          )
        }
      }
    val collector =
      VidLabelingTraceCollector(
        logReaderFactory = { reader },
        spanReader = CloudTraceReader { _, _, _, _, _, _ -> emptyList() },
        stateResolver = VidLabelingStateResolver { testGraph(emptyList()) },
        finalStateResolver = NOOP_FINAL_STATE_RESOLVER,
        nanoTime = { nowNanos },
      )

    val collection =
      collector.collect(
        request(
          projects = listOf("project-1", "project-2", "project-3"),
          loggingSourceTimeout = Duration.ofSeconds(1),
        )
      )

    val statuses = collection.sourceStatuses.filter { it.source == "cloud_logging" }
    assertThat(statuses).hasSize(3)
    assertThat(statuses.single { it.project == "project-1" }.status).isEqualTo("complete")
    assertThat(statuses.single { it.project == "project-1" }.durationMillis).isEqualTo(600)
    assertThat(statuses.single { it.project == "project-2" }.status).isEqualTo("complete")
    assertThat(statuses.single { it.project == "project-2" }.durationMillis).isEqualTo(400)
    assertThat(statuses.single { it.project == "project-3" }.status).isEqualTo("timed_out")
    assertThat(queries).isEqualTo(2)
  }

  @Test
  fun `batch accepts only partial status when partial is allowed`() = runBlocking {
    val partial =
      VidLabelingTraceCollection(
        RAW_UPLOAD,
        VidLabelingTraceStatus.PARTIAL,
        VidLabelingExecutionStatus.UNKNOWN,
        emptyList(),
        emptyList(),
        emptyList(),
        emptyList(),
      )

    val strictFailed =
      collectVidLabelingTraceBatch(
        listOf(RAW_UPLOAD),
        allowPartial = false,
        collect = { partial },
        write = { _, _ -> },
      )
    val partialFailed =
      collectVidLabelingTraceBatch(
        listOf(RAW_UPLOAD),
        allowPartial = true,
        collect = { partial },
        write = { _, _ -> },
      )

    assertThat(strictFailed).isTrue()
    assertThat(partialFailed).isFalse()
  }

  @OptIn(ExperimentalCoroutinesApi::class)
  @Test
  fun `batch rejects failed EDPA state even when partial is allowed`() = runTest {
    val collector =
      VidLabelingTraceCollector(
        logReaderFactory = { FakeCloudLogReader(emptyList()) },
        spanReader = CloudTraceReader { _, _, _, _, _, _ -> emptyList() },
        stateResolver = VidLabelingStateResolver { awaitCancellation() },
        finalStateResolver = NOOP_FINAL_STATE_RESOLVER,
      )
    val artifacts = mutableListOf<String>()

    val failed =
      collectVidLabelingTraceBatch(
        listOf(RAW_UPLOAD),
        allowPartial = true,
        uploadTimeout = Duration.ofSeconds(10),
        collect = {
          collector.collect(request(authoritativeSourceTimeout = Duration.ofSeconds(1)))
        },
        write = { _, contents -> artifacts += contents },
      )

    assertThat(failed).isTrue()
    assertThat(artifacts.single()).contains("edpa_pipeline_state")
    assertThat(artifacts.single()).contains("timed_out")
  }

  @Test
  fun `batch times out one upload and continues with the next`() = runTest {
    val writtenFiles = mutableListOf<String>()

    val failed =
      collectVidLabelingTraceBatch(
        listOf(RAW_UPLOAD, OTHER_RAW_UPLOAD),
        allowPartial = true,
        uploadTimeout = Duration.ofSeconds(1),
        collect = { rawImpressionUpload ->
          if (rawImpressionUpload == RAW_UPLOAD) awaitCancellation()
          VidLabelingTraceCollection(
            rawImpressionUpload,
            VidLabelingTraceStatus.COMPLETE,
            VidLabelingExecutionStatus.SUCCEEDED,
            emptyList(),
            emptyList(),
            emptyList(),
            emptyList(),
          )
        },
        write = { fileName, _ -> writtenFiles += fileName },
      )

    assertThat(failed).isTrue()
    assertThat(writtenFiles).hasSize(2)
    assertThat(writtenFiles[1]).isEqualTo(VidLabelingTraceOutput.artifactFileName(OTHER_RAW_UPLOAD))
  }

  @Test
  fun `overall upload timeout is not swallowed by a longer source timeout`() = runTest {
    val collector =
      VidLabelingTraceCollector(
        logReaderFactory = { FakeCloudLogReader(emptyList()) },
        spanReader = CloudTraceReader { _, _, _, _, _, _ -> emptyList() },
        stateResolver = VidLabelingStateResolver { awaitCancellation() },
        finalStateResolver = NOOP_FINAL_STATE_RESOLVER,
      )
    val artifacts = mutableListOf<String>()

    val failed =
      collectVidLabelingTraceBatch(
        listOf(RAW_UPLOAD),
        allowPartial = true,
        uploadTimeout = Duration.ofSeconds(1),
        collect = {
          collector.collect(request(authoritativeSourceTimeout = Duration.ofSeconds(10)))
        },
        write = { _, contents -> artifacts += contents },
      )

    assertThat(failed).isTrue()
    assertThat(artifacts.single()).contains("Failed source: `overall_upload`")
    assertThat(artifacts.single()).contains("Source status: `timed_out`")
  }

  @Test
  fun `batch preserves an external timeout cancellation`() = runTest {
    var attemptedUploads = 0
    var writtenArtifacts = 0
    assertFailsWith<TimeoutCancellationException> {
      withTimeout(Duration.ofSeconds(1).toMillis()) {
        collectVidLabelingTraceBatch(
          listOf(RAW_UPLOAD, OTHER_RAW_UPLOAD),
          allowPartial = true,
          uploadTimeout = Duration.ofSeconds(10),
          collect = {
            attemptedUploads++
            awaitCancellation()
          },
          write = { _, _ -> writtenArtifacts++ },
        )
      }
    }
    assertThat(attemptedUploads).isEqualTo(1)
    assertThat(writtenArtifacts).isEqualTo(0)
  }

  @Test
  fun `batch preserves external cancellation`() = runTest {
    assertFailsWith<CancellationException> {
      collectVidLabelingTraceBatch(
        listOf(RAW_UPLOAD),
        allowPartial = true,
        uploadTimeout = Duration.ofSeconds(1),
        collect = { throw CancellationException("cancelled externally") },
        write = { _, _ -> },
      )
    }
  }

  @Test
  fun `batch isolates malformed upload and continues with valid upload`() = runBlocking {
    val writtenFiles = mutableListOf<String>()

    val failed =
      collectVidLabelingTraceBatch(
        listOf("malformed", RAW_UPLOAD),
        allowPartial = true,
        collect = { rawImpressionUpload ->
          require(rawImpressionUpload == RAW_UPLOAD)
          VidLabelingTraceCollection(
            rawImpressionUpload,
            VidLabelingTraceStatus.COMPLETE,
            VidLabelingExecutionStatus.SUCCEEDED,
            emptyList(),
            emptyList(),
            emptyList(),
            emptyList(),
          )
        },
        write = { fileName, _ -> writtenFiles += fileName },
      )

    assertThat(failed).isTrue()
    assertThat(writtenFiles).hasSize(2)
    assertThat(writtenFiles[0]).startsWith("invalid__")
    assertThat(writtenFiles[1]).isEqualTo(VidLabelingTraceOutput.artifactFileName(RAW_UPLOAD))
  }

  private fun request(
    projects: List<String> = listOf("observability-project"),
    correlationValueLimit: Int = 500,
    traceIdLimit: Int = 500,
    expansionRounds: Int = 4,
    authoritativeSourceTimeout: Duration = Duration.ofMinutes(2),
    loggingSourceTimeout: Duration = Duration.ofMinutes(2),
    traceSourceTimeout: Duration = Duration.ofMinutes(2),
  ) =
    VidLabelingTraceRequest(
      RAW_UPLOAD,
      projects,
      Instant.parse("2026-08-31T00:00:00Z"),
      Instant.parse("2026-09-02T00:00:00Z"),
      correlationValueLimit = correlationValueLimit,
      traceIdLimit = traceIdLimit,
      expansionRounds = expansionRounds,
      authoritativeSourceTimeout = authoritativeSourceTimeout,
      loggingSourceTimeout = loggingSourceTimeout,
      traceSourceTimeout = traceSourceTimeout,
    )

  private fun testGraph(
    modelLineNames: List<String> = listOf(MEMOIZED_MODEL_LINE, NON_MEMOIZED_MODEL_LINE)
  ): VidLabelingAuthoritativeGraph {
    val upload =
      RawImpressionUpload.newBuilder()
        .setName(RAW_UPLOAD)
        .setState(RawImpressionUpload.State.COMPLETED)
        .setDoneBlobGeneration(1)
        .build()
    val modelLines =
      modelLineNames.mapIndexed { index, modelLine ->
        val id = if (modelLine == MEMOIZED_MODEL_LINE) "memo" else "direct"
        RawImpressionUploadModelLine.newBuilder()
          .setName(RAW_UPLOAD + "/rawImpressionUploadModelLines/" + id)
          .setCmmsModelLine(modelLine)
          .setState(RawImpressionUploadModelLine.State.COMPLETED)
          .build()
      }
    val nodes = buildList {
      add(
        ExpectedTraceNode(
          RAW_UPLOAD,
          null,
          "upload_registration",
          "COMPLETED",
          mapOf("xmm.edpa.raw_impression_upload.name" to RAW_UPLOAD),
        )
      )
      for ((index, modelLine) in modelLineNames.withIndex()) {
        val memoized = modelLine == MEMOIZED_MODEL_LINE
        val stages = if (memoized) MEMOIZED_STAGES else NON_MEMOIZED_STAGES
        val child =
          RAW_UPLOAD + "/rawImpressionUploadModelLines/" + if (memoized) "memo" else "direct"
        for (stage in stages) {
          val identifiers =
            if (stage in setOf("data_watcher", "data_availability_publish")) {
              mapOf(
                "xmm.model_line.name" to modelLine,
                "xmm.edpa.label.route" to if (memoized) "memoized" else "non_memoized",
              )
            } else {
              mapOf(
                "xmm.model_line.name" to modelLine,
                "xmm.edpa.raw_impression_upload_model_line.name" to child,
                "xmm.edpa.label.route" to if (memoized) "memoized" else "non_memoized",
              )
            }
          add(ExpectedTraceNode(child + ":" + stage, modelLine, stage, "SUCCEEDED", identifiers))
        }
      }
    }
    return VidLabelingAuthoritativeGraph(upload, modelLines, nodes)
  }

  private fun routeEntries(
    modelLine: String,
    route: String,
    stages: List<String>,
    id: String,
    routeTraceId: String,
    rawImpressionUpload: String = RAW_UPLOAD,
  ): List<CloudLogEntry> {
    val child = rawImpressionUpload + "/rawImpressionUploadModelLines/" + id
    val availabilityTrace =
      if (route == "memoized") MEMO_AVAILABILITY_TRACE else DIRECT_AVAILABILITY_TRACE
    return stages
      .filterNot { it == "data_availability_publish" }
      .mapIndexed { index, stage ->
        val traceId =
          when (stage) {
            "dispatch" -> if (rawImpressionUpload == RAW_UPLOAD) ROOT_TRACE else OTHER_TRACE
            "data_availability_metadata" -> availabilityTrace
            else -> routeTraceId
          }
        entry(
          stage,
          modelLineFields(modelLine) +
            " xmm.edpa.raw_impression_upload_model_line.name=" +
            child +
            " xmm.edpa.label.route=" +
            route,
          index + 1L,
          rawImpressionUpload = rawImpressionUpload,
          traceId = traceId,
        )
      }
  }

  private fun modelLineFields(modelLine: String): String = "xmm.model_line.name=" + modelLine

  private fun availabilityNode(
    id: String,
    modelLine: String,
    intervalStart: String,
    intervalEnd: String,
  ) =
    ExpectedTraceNode(
      id,
      modelLine,
      "data_availability_publish",
      "PUBLISHED",
      mapOf(
        "xmm.model_line.name" to modelLine,
        "xmm.edpa.availability.interval_start" to intervalStart,
        "xmm.edpa.availability.interval_end" to intervalEnd,
      ),
    )

  private fun span(
    project: String,
    traceId: String,
    stage: String,
    identity: VidLabelingTraceAttributes.GcsObjectIdentity,
    generation: Long,
  ) =
    CloudTraceSpan(
      project,
      traceId,
      "span-$traceId",
      null,
      "edpa.vid_labeling.$stage",
      "worker",
      Instant.parse("2026-09-01T00:00:01Z"),
      Instant.parse("2026-09-01T00:00:02Z"),
      mapOf(
        "xmm.lifecycle.stage" to stage,
        "xmm.outcome" to "failed",
        "xmm.gcs.object.path_hash" to identity.pathHash,
        "xmm.gcs.object.generation" to generation.toString(),
      ),
    )

  private fun workItemSpan(
    spanId: String,
    workItemName: String,
    generation: Long,
    attemptName: String?,
    traceId: String = WORK_ITEM_TRACE,
  ) =
    CloudTraceSpan(
      "observability-project",
      traceId,
      spanId,
      null,
      "secure_computation.work_item.process",
      "worker",
      Instant.parse("2026-09-01T00:00:01Z"),
      Instant.parse("2026-09-01T00:00:02Z"),
      buildMap {
        put("xmm.lifecycle.stage", "work_item_processing")
        put("xmm.outcome", "succeeded")
        put("xmm.work_item.name", workItemName)
        put("xmm.work_item.generation", generation.toString())
        if (attemptName != null) put("xmm.work_item_attempt.name", attemptName)
      },
    )

  private fun objectIdentityFields(identity: VidLabelingTraceAttributes.GcsObjectIdentity): String =
    "xmm.gcs.object.path_hash=" +
      identity.pathHash +
      " xmm.gcs.object.generation=" +
      identity.generation

  private fun entry(
    stage: String,
    additionalFields: String = "",
    seconds: Long = 0,
    rawImpressionUpload: String? = null,
    outcome: String = "succeeded",
    traceId: String? = null,
  ): CloudLogEntry {
    val uploadField =
      rawImpressionUpload?.let { " xmm.edpa.raw_impression_upload.name=" + it }.orEmpty()
    return CloudLogEntry(
      "observability-project",
      Instant.parse("2026-09-01T00:00:00Z").plusSeconds(seconds),
      "edpa",
      "INFO",
      traceId?.let { "projects/observability-project/traces/" + it },
      "event=edpa.vid_labeling." +
        stage +
        " xmm.lifecycle.stage=" +
        stage +
        " xmm.outcome=" +
        outcome +
        uploadField +
        " " +
        additionalFields +
        " unsafe_uri=gs://private-bucket/raw-input",
    )
  }

  private fun completeTraceReader(): CloudTraceReader =
    CloudTraceReader { project, _, traceIds, startTime, endTime, _ ->
      requiredTraceSpans(project, traceIds, startTime, endTime)
    }

  private fun requiredTraceSpans(
    project: String,
    traceIds: Collection<String>,
    startTime: Instant,
    endTime: Instant,
  ): List<CloudTraceSpan> =
    traceIds
      .map { it.substringAfterLast('/') }
      .distinct()
      .mapIndexed { index, traceId ->
        CloudTraceSpan(
          project,
          traceId,
          "required-$index",
          null,
          "required-trace",
          "test",
          startTime,
          endTime,
          emptyMap(),
        )
      }

  private class FakeCloudLogReader(private val entries: List<CloudLogEntry>) : CloudLogReader {
    val correlationQueries = mutableListOf<Set<String>>()
    val traceQueries = mutableListOf<Set<String>>()

    override suspend fun read(
      correlationValues: Collection<String>,
      startTime: Instant,
      endTime: Instant,
      limit: Int,
    ): List<CloudLogEntry> {
      correlationQueries += correlationValues.toSet()
      return entries.filter { entry ->
        val fieldValues =
          entry.message.split(' ').mapNotNull { token ->
            token.substringAfter('=', "").takeIf { it.isNotEmpty() }
          }
        correlationValues.any { it in fieldValues }
      }
    }

    override suspend fun readTraceIds(
      traceIds: Collection<String>,
      startTime: Instant,
      endTime: Instant,
      limit: Int,
    ): List<CloudLogEntry> {
      val normalized = traceIds.map { it.substringAfterLast('/') }.toSet()
      traceQueries += normalized
      return entries.filter { it.trace?.substringAfterLast('/') in normalized }
    }
  }

  companion object {
    private val NOOP_FINAL_STATE_RESOLVER = VidLabelingFinalStateResolver { _, _, _ ->
      VidLabelingFinalStateResolution(emptyList(), emptyMap())
    }
    private const val RAW_UPLOAD = "dataProviders/123/rawImpressionUploads/upload-1"
    private const val OTHER_RAW_UPLOAD = "dataProviders/123/rawImpressionUploads/upload-2"
    private const val MEMOIZED_MODEL_LINE =
      "modelProviders/456/modelSuites/suite/modelLines/memoized"
    private const val NON_MEMOIZED_MODEL_LINE =
      "modelProviders/456/modelSuites/suite/modelLines/non-memoized"
    private const val ROOT_TRACE = "11111111111111111111111111111111"
    private const val MEMO_TRACE = "22222222222222222222222222222222"
    private const val DIRECT_TRACE = "33333333333333333333333333333333"
    private const val MEMO_AVAILABILITY_TRACE = "44444444444444444444444444444444"
    private const val DIRECT_AVAILABILITY_TRACE = "55555555555555555555555555555555"
    private const val WORK_ITEM_TRACE = "66666666666666666666666666666666"
    private const val OTHER_TRACE = "99999999999999999999999999999999"
    private val MEMOIZED_STAGES =
      listOf(
        "dispatch",
        "pool_assignment",
        "pool_assignment_finalize",
        "rank",
        "rank_finalize",
        "label",
        "label_finalize",
        "data_watcher",
        "data_availability_metadata",
        "data_availability_publish",
      )
    private val NON_MEMOIZED_STAGES =
      listOf(
        "dispatch",
        "label",
        "label_finalize",
        "data_watcher",
        "data_availability_metadata",
        "data_availability_publish",
      )
  }
}
