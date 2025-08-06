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

import com.google.protobuf.ByteString
import java.io.File
import java.net.URI
import java.nio.file.Paths
import org.wfanet.measurement.common.flatten
import org.wfanet.measurement.storage.SelectedStorageClient

class BlobLoader {
  suspend fun getBytes(
    blobUriPrefix: String, blobKey: String, projectId: String?
  ): ByteString {
    val (fullUri, rootDir) = resolve(blobUriPrefix, blobKey)
    val client = SelectedStorageClient(url = fullUri, rootDirectory = rootDir, projectId = projectId)
    val blob = requireNotNull(client.getBlob(blobKey)) {
      "Blob '$blobKey' not found at '$fullUri'"
    }
    return blob.read().flatten()
  }

  fun resolve(storageUriPrefix: String, blobKey: String): Pair<String, File?> {
    val prefix = storageUriPrefix.removeSuffix("/")

    val uri = URI("$storageUriPrefix/$blobKey")
    return if (uri.scheme == "file") {
      val path = Paths.get(URI(prefix))
      val parent = path.parent!!.toFile()
      val bucket = path.fileName.toString()
      "file:///$bucket/$blobKey" to parent
    } else {
      "$prefix/$blobKey" to null
    }
  }

}
