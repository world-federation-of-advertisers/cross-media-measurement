// Copyright 2020 The Measurement System Authors
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

package org.wfanet.measurement.storage.forwarded

import com.google.protobuf.ByteString
import io.grpc.Status
import io.grpc.StatusException
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.onStart
import kotlinx.coroutines.runBlocking
import org.wfanet.measurement.internal.testing.CreateBlobRequest
import org.wfanet.measurement.internal.testing.DeleteBlobRequest
import org.wfanet.measurement.internal.testing.ForwardedStorageGrpcKt.ForwardedStorageCoroutineStub
import org.wfanet.measurement.internal.testing.GetBlobMetadataRequest
import org.wfanet.measurement.internal.testing.ReadBlobRequest
import org.wfanet.measurement.storage.StorageClient

private const val DEFAULT_BUFFER_SIZE_BYTES = 1024 * 32 // 32 KiB

/** [StorageClient] for ForwardedStorage service. */
class ForwardedStorageClient(
  private val storageStub: ForwardedStorageCoroutineStub
) : StorageClient {

  override val defaultBufferSizeBytes: Int
    get() = DEFAULT_BUFFER_SIZE_BYTES

  @OptIn(ExperimentalCoroutinesApi::class) // For `onStart`.
  override suspend fun createBlob(blobKey: String, content: Flow<ByteString>): StorageClient.Blob {
    val requests = content.map {
      CreateBlobRequest.newBuilder().apply { bodyChunkBuilder.content = it }.build()
    }.onStart {
      emit(CreateBlobRequest.newBuilder().apply { headerBuilder.blobKey = blobKey }.build())
    }
    val metadata = storageStub.createBlob(requests)

    return Blob(blobKey, metadata.size)
  }

  override fun getBlob(blobKey: String): StorageClient.Blob? {
    // Check if the blob exists
    val blobSize = try {
      runBlocking {
        storageStub.getBlobMetadata(
          GetBlobMetadataRequest.newBuilder().setBlobKey(blobKey).build()
        ).size
      }
    } catch (e: StatusException) {
      if (e.status.code == Status.NOT_FOUND.code) {
        return null
      } else {
        throw e
      }
    }

    return Blob(blobKey, blobSize)
  }

  private inner class Blob(
    private val blobKey: String,
    override val size: Long
  ) : StorageClient.Blob {
    override val storageClient: StorageClient
      get() = this@ForwardedStorageClient

    override fun read(bufferSizeBytes: Int): Flow<ByteString> =
      storageStub.readBlob(
        ReadBlobRequest.newBuilder().setBlobKey(blobKey).setChunkSize(bufferSizeBytes).build()
      ).map {
        it.chunk
      }

    override fun delete() = runBlocking<Unit> {
      storageStub.deleteBlob(DeleteBlobRequest.newBuilder().setBlobKey(blobKey).build())
    }
  }
}
