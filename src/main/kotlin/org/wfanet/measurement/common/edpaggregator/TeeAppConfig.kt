// Copyright 2025 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.common.edpaggregator

import java.net.URI
import java.nio.file.Paths
import org.wfanet.measurement.common.flatten
import org.wfanet.measurement.storage.SelectedStorageClient

/** Function to get Tee Application' configurations from Storage */
object TeeAppConfig {

  /**
   * Fetches and returns the raw bytes of a UTF-8–encoded configuration blob from storage.
   *
   * @param projectId    GCP project ID (used for GCS access; ignored for `file:` URIs).
   * @param blobUri      Full URI of the config blob to load.
   * @return             Raw bytes of the loaded config.
   * @throws IllegalArgumentException if [blobUri] is empty or malformed.
   * @throws IllegalStateException    if the blob isn’t found at the given URI.
   */
  suspend fun getConfig(
    projectId: String,
    blobUri: String,
  ): ByteArray {

    val uri = URI(blobUri)
    val rootDirectory = if (uri.scheme == "file") {
      val path = Paths.get(uri)
      path.parent?.toFile()
    } else {
      null
    }

    val storageClient = SelectedStorageClient(url = blobUri, rootDirectory = rootDirectory, projectId = projectId)
    val blobKey = uri.path.removePrefix("/")

    val blob = checkNotNull(storageClient.getBlob(blobKey)) {
      "Blob '$blobKey' not found at '$blobUri'"
    }

    return blob.read().flatten().toByteArray()
  }
}
