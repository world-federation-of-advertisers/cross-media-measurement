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

import java.time.Instant
import org.wfanet.measurement.common.toInstant
import org.wfanet.measurement.common.toProtoTime
import org.wfanet.measurement.edpaggregator.telemetry.VidLabelingTraceLogging
import org.wfanet.measurement.edpaggregator.v1alpha.VidLabelingDispatchWorkItemParams
import org.wfanet.measurement.edpaggregator.v1alpha.VidLabelingDispatchWorkItemParamsKt.recoveryParams
import org.wfanet.measurement.edpaggregator.v1alpha.vidLabelingDispatchWorkItemParams
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItem
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemKt.WorkItemParamsKt.dataPathParams
import org.wfanet.measurement.securecomputation.datawatcher.DataWatcherWorkItemIds
import org.wfanet.measurement.storage.SelectedStorageClient

/** Source identity used for lookup without printing the raw object URI. */
internal data class VidLabelingDispatchSource(
  val dataProvider: String,
  val pathHash: String,
  val generation: Long,
  val uri: String,
  val createTime: Instant?,
  val dataWatcherIdentifier: String = "vid-labeling-dispatch",
  val sourceUpload: String = "",
  val uploadHealingOperation: String = "",
) {
  init {
    require(Regex("^dataProviders/[^/]+$").matches(dataProvider)) { "Invalid DataProvider name" }
    require(Regex("^[0-9a-f]{64}$").matches(pathHash)) { "Invalid source path hash" }
    require(generation > 0L) { "Source generation must be positive" }
  }

  fun candidateNames(): List<String> {
    val time =
      requireNotNull(createTime) {
        "Exact done-object creation time is required for dispatch lookup"
      }
    require(dataWatcherIdentifier.isNotEmpty()) { "DataWatcher identifier is required" }
    require(sourceUpload.isEmpty() == uploadHealingOperation.isEmpty()) {
      "Recovery source upload and healing operation must be supplied together"
    }
    val source = dataPathParams {
      dataPath = uri
      generation = this@VidLabelingDispatchSource.generation
      createTime = time.toProtoTime()
    }
    val ordinary = vidLabelingDispatchWorkItemParams {
      dataProvider = this@VidLabelingDispatchSource.dataProvider
    }
    val params = buildList {
      add(ordinary)
      if (sourceUpload.isNotEmpty())
        add(
          vidLabelingDispatchWorkItemParams {
            dataProvider = this@VidLabelingDispatchSource.dataProvider
            recovery = recoveryParams {
              this.source = source
              sourceUpload = this@VidLabelingDispatchSource.sourceUpload
              uploadHealingOperation = this@VidLabelingDispatchSource.uploadHealingOperation
            }
          }
        )
    }
    return params.map {
      "workItems/" +
        DataWatcherWorkItemIds.workItemId(
          DataWatcherWorkItemIds.idempotencyKey(dataWatcherIdentifier, source, it)
        )
    }
  }

  override fun toString(): String =
    "VidLabelingDispatchSource($dataProvider, $pathHash, $generation)"

  companion object {
    fun fromUri(
      provider: String,
      uri: String,
      generation: Long,
      createTime: Instant? = null,
      dataWatcherIdentifier: String = "vid-labeling-dispatch",
      sourceUpload: String = "",
      uploadHealingOperation: String = "",
    ): VidLabelingDispatchSource {
      val parsed = SelectedStorageClient.parseBlobUri(uri)
      require(parsed.scheme == "gs" && parsed.key.substringAfterLast('/') == "done") {
        "Source must identify a GCS done object"
      }
      return VidLabelingDispatchSource(
        provider,
        VidLabelingTraceLogging.sha256(uri),
        generation,
        uri,
        createTime,
        dataWatcherIdentifier,
        sourceUpload,
        uploadHealingOperation,
      )
    }
  }
}

internal fun interface VidLabelingDispatchWorkItemResolver {
  suspend fun resolve(
    source: VidLabelingDispatchSource,
    pageLimit: Int,
  ): VidLabelingWorkItemResolution
}

/** Reads stored identity even when execution rejected another part of the payload. */
internal fun WorkItem.dispatchSource(): VidLabelingDispatchSource? {
  if (queue != "vid-labeling-dispatch-queue") return null
  return runCatching {
      val frame = workItemParams.unpack(WorkItem.WorkItemParams::class.java)
      val app = frame.appParams.unpack(VidLabelingDispatchWorkItemParams::class.java)
      val source =
        if (app.hasRecovery()) {
          require(!frame.hasDataPathParams() && app.recovery.hasSource())
          app.recovery.source
        } else {
          require(frame.hasDataPathParams())
          frame.dataPathParams
        }
      require(source.hasGeneration())
      VidLabelingDispatchSource.fromUri(
        app.dataProvider,
        source.dataPath,
        source.generation,
        source.createTime.takeIf { source.hasCreateTime() }?.toInstant(),
        sourceUpload = app.recovery.sourceUpload,
        uploadHealingOperation = app.recovery.uploadHealingOperation,
      )
    }
    .getOrNull()
}

internal fun WorkItem.dispatchCandidateName(identifier: String): String {
  val frame = workItemParams.unpack(WorkItem.WorkItemParams::class.java)
  val app = frame.appParams.unpack(VidLabelingDispatchWorkItemParams::class.java)
  val source = if (app.hasRecovery()) app.recovery.source else frame.dataPathParams
  return "workItems/" +
    DataWatcherWorkItemIds.workItemId(
      DataWatcherWorkItemIds.idempotencyKey(identifier, source, app)
    )
}

internal fun ResolvedWorkItem.dispatchNodes(): List<ExpectedTraceNode> {
  val source = workItem.dispatchSource() ?: return emptyList()
  val identifiers =
    mapOf(
      "xmm.work_item.name" to workItem.name,
      "xmm.work_item.generation" to workItem.generation.toString(),
      "xmm.data_provider.name" to source.dataProvider,
      "xmm.gcs.object.path_hash" to source.pathHash,
      "xmm.gcs.object.generation" to source.generation.toString(),
    )
  return listOf(
    ExpectedTraceNode(
      workItem.name + ":generation:" + workItem.generation,
      "",
      "dispatch",
      workItem.state.name,
      identifiers,
    )
  ) +
    attempts.map { attempt ->
      ExpectedTraceNode(
        attempt.name,
        "",
        "dispatch",
        attempt.state.name,
        identifiers +
          mapOf(
            "xmm.work_item_attempt.name" to attempt.name,
            "xmm.work_item.generation" to attempt.workItemGeneration.toString(),
          ),
      )
    }
}
