// Copyright 2026 The Cross-Media Measurement Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.wfanet.measurement.loadtest.edpaggregator.tools

import kotlinx.coroutines.flow.collect
import org.measurement.integration.k8s.testing.HighOverlapDatasetCleanupState
import org.measurement.integration.k8s.testing.HighOverlapDatasetManifest
import org.measurement.integration.k8s.testing.highOverlapDatasetCleanupState
import org.wfanet.measurement.common.flatten
import org.wfanet.measurement.integration.common.HighOverlapDataset
import org.wfanet.measurement.storage.StorageClient

/** Removes previously seeded high overlap data for one ModelLine. */
class HighOverlapDatasetCleanup(
  private val storageClient: StorageClient,
  private val modelLineId: String,
) {
  private val cleanupStateKey = "high-overlap/$modelLineId/cleanup-state.binpb"

  /** Invalidates the manifest and removes data under both previous and new impression paths. */
  suspend fun prepareForReseeding(impressionPaths: Set<String>) {
    val manifestBlob = storageClient.getBlob(HighOverlapDataset.manifestBlobKey(modelLineId))
    val cleanupStateBlob = storageClient.getBlob(cleanupStateKey)
    val previousPaths: List<String> =
      if (cleanupStateBlob != null) {
        HighOverlapDatasetCleanupState.parseFrom(cleanupStateBlob.read().flatten())
          .impressionPathsList
      } else if (manifestBlob != null) {
        // Seeds written before cleanup state used the fixed edp/<name> layout.
        HighOverlapDatasetManifest.parseFrom(manifestBlob.read().flatten()).seededEdpsList.map {
          "edp/$it"
        }
      } else {
        emptyList()
      }
    val pathsToClean: Set<String> = previousPaths.toSet() + impressionPaths
    writeCleanupState(pathsToClean)
    manifestBlob?.delete()
    for (impressionPath in pathsToClean) {
      val prefix = "$impressionPath/model-line/$modelLineId/"
      storageClient.listBlobs(prefix).collect { blob -> blob.delete() }
    }
    writeCleanupState(impressionPaths)
  }

  private suspend fun writeCleanupState(paths: Set<String>) {
    storageClient.writeBlob(
      cleanupStateKey,
      highOverlapDatasetCleanupState { impressionPaths += paths.sorted() }.toByteString(),
    )
  }
}
