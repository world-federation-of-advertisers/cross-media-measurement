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

package org.wfanet.measurement.loadtest.edpaggregator.testing

import com.google.protobuf.ByteString
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flowOf
import org.wfanet.measurement.storage.ConditionalOperationStorageClient
import org.wfanet.measurement.storage.StorageClient

/**
 * Writes fixtures against their observed generation so storage retries cannot overwrite a conflict.
 */
class FixtureStorageClient(private val delegate: ConditionalOperationStorageClient) :
  ConditionalOperationStorageClient by delegate {
  override suspend fun writeBlob(blobKey: String, content: Flow<ByteString>): StorageClient.Blob {
    val freshnessToken = delegate.getFreshnessToken(blobKey)
    return if (freshnessToken == null) {
      delegate.writeBlobIfNotFound(blobKey, content)
    } else {
      delegate.writeBlobIfUnchanged(blobKey, freshnessToken, content)
    }
  }

  override suspend fun writeBlob(blobKey: String, content: ByteString): StorageClient.Blob =
    writeBlob(blobKey, flowOf(content))
}
