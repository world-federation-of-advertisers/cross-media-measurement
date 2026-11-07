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
import org.wfanet.measurement.integration.common.HighOverlapDataset
import org.wfanet.measurement.storage.StorageClient

/** Removes previously seeded high overlap data for one ModelLine. */
class HighOverlapDatasetCleanup(
  private val storageClient: StorageClient,
  private val modelLineId: String,
) {
  /** Invalidates the manifest before removing data under the selected impression paths. */
  suspend fun prepareForReseeding(impressionPaths: Set<String>) {
    storageClient.getBlob(HighOverlapDataset.manifestBlobKey(modelLineId))?.delete()
    for (impressionPath in impressionPaths) {
      val prefix = "$impressionPath/model-line/$modelLineId/"
      storageClient.listBlobs(prefix).collect { blob -> blob.delete() }
    }
  }
}
