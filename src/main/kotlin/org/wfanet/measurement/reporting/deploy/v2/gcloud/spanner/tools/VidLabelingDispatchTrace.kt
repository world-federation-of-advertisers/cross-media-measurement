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

import org.wfanet.measurement.edpaggregator.telemetry.VidLabelingTraceLogging
import org.wfanet.measurement.edpaggregator.v1alpha.VidLabelingDispatchWorkItemParams
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItem
import org.wfanet.measurement.storage.SelectedStorageClient

/** Source identity used for lookup without printing the raw object URI. */
internal data class VidLabelingDispatchSource(
  val dataProvider: String,
  val pathHash: String,
  val generation: Long,
) {
  init {
    require(Regex("^dataProviders/[^/]+$").matches(dataProvider)) { "Invalid DataProvider name" }
    require(Regex("^[0-9a-f]{64}$").matches(pathHash)) { "Invalid source path hash" }
    require(generation > 0L) { "Source generation must be positive" }
  }

  companion object {
    fun fromUri(provider: String, uri: String, generation: Long): VidLabelingDispatchSource {
      val parsed = SelectedStorageClient.parseBlobUri(uri)
      require(parsed.scheme == "gs" && parsed.key.substringAfterLast('/') == "done") {
        "Source must identify a GCS done object"
      }
      return VidLabelingDispatchSource(provider, VidLabelingTraceLogging.sha256(uri), generation)
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
      VidLabelingDispatchSource.fromUri(app.dataProvider, source.dataPath, source.generation)
    }
    .getOrNull()
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
