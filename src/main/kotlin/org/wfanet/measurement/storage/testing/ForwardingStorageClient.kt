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

package org.wfanet.measurement.storage.testing

import com.google.protobuf.ByteString
import io.grpc.Status
import io.grpc.StatusException
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.runBlocking
import org.wfanet.measurement.internal.testing.CreateBlobRequest
import org.wfanet.measurement.internal.testing.DeleteBlobRequest
import org.wfanet.measurement.internal.testing.ForwardingStorageServiceGrpcKt
import org.wfanet.measurement.internal.testing.GetBlobMetadataRequest
import org.wfanet.measurement.internal.testing.ReadBlobRequest
import org.wfanet.measurement.storage.StorageClient

class ForwardingStorageClient(
  private val storageStub: ForwardingStorageServiceGrpcKt.ForwardingStorageServiceCoroutineStub
) : StorageClient {

  override suspend fun createBlob(blobKey: String, content: Flow<ByteString>): StorageClient.Blob {
    val blobSize = storageStub.createBlob(
      content.map {
        CreateBlobRequest.newBuilder().setBlobKey(blobKey).setContent(it)
          .build()
      }
    ).size
    return Blob(storageStub, blobKey, blobSize)
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

    return Blob(storageStub, blobKey, blobSize)
  }

  private class Blob(
    private val storageStub: ForwardingStorageServiceGrpcKt.ForwardingStorageServiceCoroutineStub,
    private val blobKey: String,
    override val size: Long
  ) : StorageClient.Blob {
    override fun read(flowBufferSize: Int): Flow<ByteString> =
      storageStub.readBlob(
        ReadBlobRequest.newBuilder().setBlobKey(blobKey).setChunkSize(flowBufferSize).build()
      ).map {
        it.chunk
      }

    override fun delete() = runBlocking<Unit> {
      storageStub.deleteBlob(DeleteBlobRequest.newBuilder().setBlobKey(blobKey).build())
    }
  }
}
