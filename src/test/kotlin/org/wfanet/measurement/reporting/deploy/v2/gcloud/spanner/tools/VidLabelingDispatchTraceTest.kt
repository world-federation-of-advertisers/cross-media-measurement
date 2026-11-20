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
import java.time.Instant
import java.time.ZoneOffset
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
  private val source = VidLabelingDispatchSource.fromUri(provider, uri, 123L)

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
            eventType = WorkItem.WorkItemParams.DataPathParams.StorageEventType.FINALIZED
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

  private fun resolver(inventory: List<WorkItem>, truncated: Boolean = false) =
    GrpcVidLabelingWorkItemResolver.fromFunctions(
      listWorkItems = {
        listWorkItemsResponse {
          workItems += inventory
          if (truncated) nextPageToken = "more"
        }
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
      "event=edpa.vid_labeling.dispatch.work_item xmm.lifecycle.stage=dispatch xmm.outcome=failed xmm.work_item.name=workItems/dispatch xmm.work_item_attempt.name=workItems/dispatch/workItemAttempts/attempt xmm.gcs.object.path_hash=" +
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
  fun `lookup retains valid source modes and excludes missing or other identities`() = runBlocking {
    val inventory =
      listOf(
        task(),
        task("missing-source", outer = false),
        task("recovery", recovery = true, outer = false),
        task("wrong-generation", generation = 124L),
        task("wrong-provider", dataProvider = "dataProviders/other"),
        task("wrong-path", path = "gs://private/raw/other/done"),
        task("wrong-queue").copy { queue = "other" },
      )
    val result = resolver(inventory).resolveDispatch(source, 1)
    assertThat(result.workItems.map { it.workItem.name })
      .containsExactly("workItems/dispatch", "workItems/recovery")
    assertThat(result.workItems.all { it.attempts.size == 1 }).isTrue()
  }

  @Test
  fun `done object lookup finds failed task without upload and renders safe lifecycle evidence`() =
    runBlocking {
      val collection =
        collector(resolver(listOf(task())))
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
        .isEqualTo("workItems/dispatch/workItemAttempts/attempt")
      assertThat(collection.evidence.single().stage).isEqualTo("dispatch")
      val output = VidLabelingTraceOutput.render(collection)
      assertThat(output).contains("workItems/dispatch")
      assertThat(output).contains("FAILED")
      assertThat(output).contains(source.pathHash)
      assertThat(output).doesNotContain(uri)
      assertThat(output).doesNotContain("secret")
    }

  @Test
  fun `truncated dispatch lookup does not certify absence or execution outcome`() = runBlocking {
    val collection =
      collector(resolver(listOf(task()), truncated = true))
        .collect(
          VidLabelingTraceRequest(
            "",
            listOf("project"),
            time.minusSeconds(1),
            time.plusSeconds(1),
            workItemPageLimit = 1,
            dispatchSource = source,
          )
        )
    assertThat(collection.traceStatus).isEqualTo(VidLabelingTraceStatus.PARTIAL)
    assertThat(collection.executionStatus).isEqualTo(VidLabelingExecutionStatus.UNKNOWN)
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
          collector(resolver(listOf(task()))),
          Clock.fixed(time, ZoneOffset.UTC),
          PrintWriter(output),
        ),
      )
    assertThat(code).isEqualTo(0)
    assertThat(output.toString()).contains("workItems/dispatch")
    assertThat(output.toString()).doesNotContain(uri)
  }

  @Test
  fun `upload rooted lookup includes dispatch tasks and lifecycle evidence`() = runBlocking {
    val upload =
      RawImpressionUpload.newBuilder()
        .setName(provider + "/rawImpressionUploads/upload")
        .setDoneBlobUri(uri)
        .setDoneBlobGeneration(123L)
        .setRegistrationComplete(true)
        .setState(RawImpressionUpload.State.COMPLETED)
        .build()
    val result =
      collector(resolver(listOf(task())), upload)
        .collect(
          VidLabelingTraceRequest(
            upload.name,
            listOf("project"),
            time.minusSeconds(1),
            time.plusSeconds(1),
          )
        )
    assertThat(result.dispatchWorkItems.single().workItem.name).isEqualTo("workItems/dispatch")
    assertThat(result.evidence.any { it.identifiers["xmm.work_item.name"] == "workItems/dispatch" })
      .isTrue()
    assertThat(VidLabelingTraceOutput.render(result)).contains("Dispatch WorkItems")
  }
}
