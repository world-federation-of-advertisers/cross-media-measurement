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
import java.nio.ByteBuffer

class ForwardingStorageClient(
  private val storageStub: ForwardingStorageServiceGrpcKt.ForwardingStorageServiceCoroutineStub
) : StorageClient {

  override suspend fun createBlob(blobKey: String, content: Flow<ByteBuffer>): StorageClient.Blob {
    val blobSize = storageStub.createBlob(
      content.map {
        CreateBlobRequest.newBuilder().setBlobKey(blobKey).setContent(ByteString.copyFrom(it))
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
    override fun read(flowBufferSize: Int): Flow<ByteBuffer> =
      storageStub.readBlob(
        ReadBlobRequest.newBuilder().setBlobKey(blobKey).setChunkSize(flowBufferSize).build()
      ).map {
        it.chunk.asReadOnlyByteBuffer()
      }

    override fun delete() = runBlocking<Unit> {
      storageStub.deleteBlob(DeleteBlobRequest.newBuilder().setBlobKey(blobKey).build())
    }
  }
}
