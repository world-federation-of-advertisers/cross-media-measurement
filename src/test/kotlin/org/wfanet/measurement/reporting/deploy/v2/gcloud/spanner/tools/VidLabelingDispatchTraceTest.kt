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
import io.grpc.Status
import java.io.PrintWriter
import java.io.StringWriter
import java.time.Clock
import java.time.Instant
import java.time.ZoneOffset
import kotlin.test.assertFailsWith
import kotlinx.coroutines.runBlocking
import org.junit.Test
import org.wfanet.measurement.common.pack
import org.wfanet.measurement.common.telemetry.CloudLogEntry
import org.wfanet.measurement.common.telemetry.CloudLogReader
import org.wfanet.measurement.common.telemetry.CloudTraceReader
import org.wfanet.measurement.common.toProtoTime
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUpload
import org.wfanet.measurement.edpaggregator.v1alpha.VidLabelingDispatchWorkItemParamsKt.recoveryParams
import org.wfanet.measurement.edpaggregator.v1alpha.vidLabelingDispatchWorkItemParams
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItem
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemAttempt
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemKt.WorkItemParamsKt.dataPathParams
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemKt.workItemParams
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.copy
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.listWorkItemAttemptsResponse
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.listWorkItemsResponse
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.workItem
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.workItemAttempt

class VidLabelingDispatchTraceTest {
  private val provider = "dataProviders/provider"
  private val uri = "gs://private/raw/day/done"
  private val time = Instant.parse("2026-11-01T00:00:00Z")
  private val source = VidLabelingDispatchSource.fromUri(provider, uri, 123L, time)

  private fun task(
    id: String = "dispatch",
    generation: Long = 123L,
    dataProvider: String = provider,
    path: String = uri,
    recovery: Boolean = false,
    outer: Boolean = true,
    state: WorkItem.State = WorkItem.State.FAILED,
  ) = workItem {
    name = "workItems/" + id
    queue = "vid-labeling-dispatch-queue"
    this.generation = 7L
    this.state = state
    createTime = time.toProtoTime()
    workItemParams =
      workItemParams {
          val source = dataPathParams {
            dataPath = path
            this.generation = generation
            createTime = time.toProtoTime()
          }
          appParams =
            vidLabelingDispatchWorkItemParams {
                this.dataProvider = dataProvider
                if (recovery)
                  this.recovery = recoveryParams {
                    this.source = source
                    sourceUpload = provider + "/rawImpressionUploads/source"
                    uploadHealingOperation = provider + "/uploadHealingOperations/heal"
                  }
              }
              .pack()
          if (outer) dataPathParams = source
        }
        .pack()
  }

  private fun namedTask(recovery: Boolean = false): WorkItem {
    val item = task(recovery = recovery, outer = !recovery)
    return item.copy { name = item.dispatchCandidateName("vid-labeling-dispatch") }
  }

  private fun resolver(inventory: List<WorkItem>) =
    GrpcVidLabelingWorkItemResolver.fromFunctions(
      listWorkItems = { error("Dispatch lookup must never list the global WorkItems collection") },
      getWorkItem = { request ->
        inventory.firstOrNull { it.name == request.name } ?: throw Status.NOT_FOUND.asException()
      },
      listWorkItemAttempts = { request ->
        listWorkItemAttemptsResponse {
          workItemAttempts += workItemAttempt {
            name = request.parent + "/workItemAttempts/attempt"
            workItemGeneration = 7L
            state = WorkItemAttempt.State.FAILED
            errorMessage = "secret gs://private/payload"
          }
        }
      },
    )

  private fun collector(
    resolver: GrpcVidLabelingWorkItemResolver,
    upload: RawImpressionUpload? = null,
  ): VidLabelingTraceCollector {
    val message =
      "event=edpa.vid_labeling.dispatch.work_item xmm.lifecycle.stage=dispatch xmm.outcome=failed xmm.work_item.name=" +
        namedTask().name +
        " xmm.work_item_attempt.name=" +
        namedTask().name +
        "/workItemAttempts/attempt xmm.gcs.object.path_hash=" +
        source.pathHash +
        " xmm.gcs.object.generation=123 unsafe_uri=" +
        uri
    return VidLabelingTraceCollector(
      logReaderFactory = {
        object : CloudLogReader {
          override suspend fun read(
            correlationValues: Collection<String>,
            startTime: Instant,
            endTime: Instant,
            limit: Int,
          ) = listOf(CloudLogEntry("project", time, "worker", "INFO", null, message))
        }
      },
      spanReader = CloudTraceReader { _, _, _, _, _, _ -> emptyList() },
      stateResolver =
        VidLabelingStateResolver {
          if (upload == null) error("No upload exists; EDPA must not be queried")
          VidLabelingAuthoritativeGraph(upload, emptyList(), emptyList())
        },
      finalStateResolver =
        VidLabelingFinalStateResolver { _, _, _ ->
          if (upload == null) error("No upload exists")
          VidLabelingFinalStateResolution(emptyList(), emptyMap())
        },
      dispatchWorkItemResolver = VidLabelingDispatchWorkItemResolver(resolver::resolveDispatch),
    )
  }

  @Test
  fun `large unrelated inventory cannot truncate exact ordinary and recovery lookup`() =
    runBlocking {
      val ordinary = namedTask()
      val recovery = namedTask(recovery = true)
      val inventory = (1..10_000).map { task("unrelated-$it") } + ordinary + recovery
      var listCalls = 0
      val gets = mutableListOf<String>()
      val attempts = mutableListOf<String>()
      val resolver =
        GrpcVidLabelingWorkItemResolver.fromFunctions(
          listWorkItems = {
            listCalls++
            listWorkItemsResponse {
              workItems += inventory.take(100)
              nextPageToken = "more"
            }
          },
          getWorkItem = { request ->
            gets += request.name
            inventory.singleOrNull { it.name == request.name }
              ?: throw Status.NOT_FOUND.asException()
          },
          listWorkItemAttempts = { request ->
            attempts += request.parent
            listWorkItemAttemptsResponse {
              workItemAttempts += workItemAttempt {
                name = request.parent + "/workItemAttempts/old-failed-attempt"
                state = WorkItemAttempt.State.FAILED
              }
            }
          },
        )
      val intent =
        source.copy(
          sourceUpload = provider + "/rawImpressionUploads/source",
          uploadHealingOperation = provider + "/uploadHealingOperations/heal",
        )
      val result = resolver.resolveDispatch(intent, pageLimit = 1)
      assertThat(result.workItems.map { it.workItem.name })
        .containsExactly(ordinary.name, recovery.name)
      assertThat(
          result.workItems.all { it.attempts.single().state == WorkItemAttempt.State.FAILED }
        )
        .isTrue()
      assertThat(result.truncated).isFalse()
      assertThat(listCalls).isEqualTo(0)
      assertThat(gets).containsExactly(ordinary.name, recovery.name)
      assertThat(attempts).containsExactly(ordinary.name, recovery.name)
      assertThat(result.counts["work_item_gets"]).isEqualTo(2L)
    }

  @Test
  fun `identity and immutable intent changes cannot return another dispatch`(): Unit = runBlocking {
    val ordinary = namedTask()
    val recovery = namedTask(recovery = true)
    val intent =
      source.copy(
        sourceUpload = provider + "/rawImpressionUploads/source",
        uploadHealingOperation = provider + "/uploadHealingOperations/heal",
      )
    val resolver = resolver(listOf(ordinary, recovery))
    for (changed in
      listOf(
        intent.copy(
          dataProvider = "dataProviders/other",
          sourceUpload = "dataProviders/other/rawImpressionUploads/source",
          uploadHealingOperation = "dataProviders/other/uploadHealingOperations/heal",
        ),
        VidLabelingDispatchSource.fromUri(provider, "gs://private/other/done", 123L, time),
        intent.copy(generation = 124L),
        intent.copy(createTime = time.plusNanos(1)),
        intent.copy(dataWatcherIdentifier = "custom-dispatch"),
      )) assertThat(resolver.resolveDispatch(changed, 1).workItems).isEmpty()
    for (changed in
      listOf(
        intent.copy(sourceUpload = provider + "/rawImpressionUploads/other"),
        intent.copy(uploadHealingOperation = provider + "/uploadHealingOperations/other"),
      )) assertThat(resolver.resolveDispatch(changed, 1).workItems.map { it.workItem.name })
      .containsExactly(ordinary.name)
    assertThat(resolver.resolveDispatch(source, 1).workItems.map { it.workItem.name })
      .containsExactly(ordinary.name)
  }

  @Test
  fun `exact name with inconsistent persisted source is rejected`(): Unit = runBlocking {
    val correct = namedTask()
    val mismatched = task(generation = 124L).copy { name = correct.name }
    assertFailsWith<IllegalStateException> {
      resolver(listOf(mismatched)).resolveDispatch(source, 1)
    }
  }

  @Test
  fun `custom watched path identifier resolves its exact task`() = runBlocking {
    val item = task()
    val named = item.copy { name = item.dispatchCandidateName("custom-dispatch") }
    assertThat(
        resolver(listOf(named))
          .resolveDispatch(source.copy(dataWatcherIdentifier = "custom-dispatch"), 1)
          .workItems
          .single()
          .workItem
          .name
      )
      .isEqualTo(named.name)
  }

  @Test
  fun `done object lookup finds failed task without upload and renders safe lifecycle evidence`() =
    runBlocking {
      val collection =
        collector(resolver(listOf(namedTask())))
          .collect(
            VidLabelingTraceRequest(
              "",
              listOf("project"),
              time.minusSeconds(1),
              time.plusSeconds(1),
              dispatchSource = source,
            )
          )
      assertThat(collection.executionStatus).isEqualTo(VidLabelingExecutionStatus.FAILED)
      assertThat(collection.dispatchWorkItems.single().attempts.single().name)
        .isEqualTo(namedTask().name + "/workItemAttempts/attempt")
      assertThat(collection.evidence.single().stage).isEqualTo("dispatch")
      val output = VidLabelingTraceOutput.render(collection)
      assertThat(output).contains(namedTask().name)
      assertThat(output).contains("FAILED")
      assertThat(output).contains(source.pathHash)
      assertThat(output).doesNotContain(uri)
      assertThat(output).doesNotContain("secret")
    }

  @Test
  fun `NOT_FOUND is distinct from unavailable permission denied and timeout`() = runBlocking {
    for (status in
      listOf(
        Status.NOT_FOUND,
        Status.UNAVAILABLE,
        Status.PERMISSION_DENIED,
        Status.DEADLINE_EXCEEDED,
      )) {
      var attempts = 0
      val resolver =
        GrpcVidLabelingWorkItemResolver.fromFunctions(
          listWorkItems = { error("No global listing") },
          getWorkItem = { throw status.asException() },
          listWorkItemAttempts = {
            attempts++
            error("No attempts for missing or failed GET")
          },
        )
      val collection =
        collector(resolver)
          .collect(
            VidLabelingTraceRequest(
              "",
              listOf("project"),
              time.minusSeconds(1),
              time.plusSeconds(1),
              dispatchSource = source,
            )
          )
      assertThat(collection.executionStatus).isEqualTo(VidLabelingExecutionStatus.UNKNOWN)
      assertThat(collection.traceStatus).isEqualTo(VidLabelingTraceStatus.PARTIAL)
      assertThat(attempts).isEqualTo(0)
      if (status.code == Status.Code.NOT_FOUND) {
        assertThat(
            collection.sourceStatuses.first { it.source == "secure_computation_dispatch" }.status
          )
          .isEqualTo("complete")
        assertThat(collection.warnings)
          .contains("No dispatch WorkItem exists for the supplied source and intent.")
      } else {
        assertThat(
            collection.sourceStatuses.first { it.source == "secure_computation_dispatch" }.status
          )
          .isEqualTo("failed")
        assertThat(collection.warnings)
          .contains("Dispatch WorkItem lookup was incomplete; absence could not be determined.")
        assertThat(collection.warnings)
          .doesNotContain("No dispatch WorkItem exists for the supplied source and intent.")
      }
    }
  }

  @Test
  fun `incomplete recovery intent is rejected before lookup`(): Unit = runBlocking {
    assertFailsWith<IllegalArgumentException> {
      resolver(emptyList())
        .resolveDispatch(source.copy(sourceUpload = provider + "/rawImpressionUploads/source"), 1)
    }
  }

  @Test
  fun `missing creation time is incomplete without any control plane lookup`() = runBlocking {
    val collection =
      collector(resolver(emptyList()))
        .collect(
          VidLabelingTraceRequest(
            "",
            listOf("project"),
            time.minusSeconds(1),
            time.plusSeconds(1),
            dispatchSource = source.copy(createTime = null),
          )
        )
    assertThat(
        collection.sourceStatuses.first { it.source == "secure_computation_dispatch" }.status
      )
      .isEqualTo("failed")
    assertThat(collection.warnings)
      .contains("Dispatch WorkItem lookup was incomplete; absence could not be determined.")
  }

  @Test
  fun `CLI accepts complete done object identity without upload name`() {
    val output = StringWriter()
    val code =
      runVidLabelingTrace(
        arrayOf(
          "--data-provider=" + provider,
          "--done-object-uri=" + uri,
          "--done-object-generation=123",
          "--done-object-create-time=" + time,
          "--observability-project=project",
          "--edpa-public-api-target=unused",
          "--control-plane-api-target=unused",
          "--kingdom-public-api-target=unused",
          "--control-plane-tls-cert-file=unused",
          "--control-plane-tls-key-file=unused",
          "--control-plane-cert-collection-file=unused",
          "--kingdom-tls-cert-file=unused",
          "--kingdom-tls-key-file=unused",
          "--kingdom-cert-collection-file=unused",
          "--allow-partial",
        ),
        VidLabelingTraceDependencies(
          collector(resolver(listOf(namedTask()))),
          Clock.fixed(time, ZoneOffset.UTC),
          PrintWriter(output),
        ),
      )
    assertThat(code).isEqualTo(0)
    assertThat(output.toString()).contains(namedTask().name)
    assertThat(output.toString()).doesNotContain(uri)
  }

  @Test
  fun `upload rooted lookup includes dispatch tasks and lifecycle evidence`() = runBlocking {
    val upload =
      RawImpressionUpload.newBuilder()
        .setName(provider + "/rawImpressionUploads/upload")
        .setDoneBlobUri(uri)
        .setDoneBlobGeneration(123L)
        .setDoneBlobCreateTime(time.toProtoTime())
        .setRegistrationComplete(true)
        .setState(RawImpressionUpload.State.COMPLETED)
        .build()
    val result =
      collector(resolver(listOf(namedTask())), upload)
        .collect(
          VidLabelingTraceRequest(
            upload.name,
            listOf("project"),
            time.minusSeconds(1),
            time.plusSeconds(1),
          )
        )
    assertThat(result.dispatchWorkItems.single().workItem.name).isEqualTo(namedTask().name)
    assertThat(result.evidence.any { it.identifiers["xmm.work_item.name"] == namedTask().name })
      .isTrue()
    assertThat(VidLabelingTraceOutput.render(result)).contains("Dispatch WorkItems")
  }
}
