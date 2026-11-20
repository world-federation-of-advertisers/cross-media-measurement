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

  /** Cleans the dataset when its manifest differs; returns false when it can be reused. */
  suspend fun prepareIfNeeded(
    expectedManifest: HighOverlapDatasetManifest,
    impressionPaths: Set<String>,
  ): Boolean {
    val manifestBlob = storageClient.getBlob(HighOverlapDataset.manifestBlobKey(modelLineId))
    if (manifestBlob != null) {
      val manifest = HighOverlapDatasetManifest.parseFrom(manifestBlob.read().flatten())
      if (
        manifest.modelLine == expectedManifest.modelLine &&
          manifest.specFingerprint == expectedManifest.specFingerprint &&
          manifest.seededEdpsList.toSet() == expectedManifest.seededEdpsList.toSet()
      ) {
        return false
      }
    }
    prepareForReseeding(impressionPaths)
    return true
  }

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
