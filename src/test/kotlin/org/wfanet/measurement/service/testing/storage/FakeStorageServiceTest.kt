package org.wfanet.measurement.service.testing.storage

import com.google.common.truth.Truth.assertThat
import com.google.protobuf.ByteString
import io.grpc.Status
import io.grpc.StatusException
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import org.junit.Assert.assertThrows
import org.junit.Rule
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.internal.testing.DeleteBlobRequest
import org.wfanet.measurement.internal.testing.ForwardingStorageServiceGrpcKt
import org.wfanet.measurement.internal.testing.GetBlobMetadataRequest
import org.wfanet.measurement.internal.testing.ReadBlobRequest
import org.wfanet.measurement.service.testing.GrpcTestServerRule
import org.wfanet.measurement.storage.testing.ForwardingStorageClient

@RunWith(JUnit4::class)
class FakeStorageServiceTest {
  @get:Rule
  val grpcTestServerRule = GrpcTestServerRule {
    addService(FakeStorageService())
  }

  val storageStub =
    ForwardingStorageServiceGrpcKt.ForwardingStorageServiceCoroutineStub(grpcTestServerRule.channel)
  val storageClient = ForwardingStorageClient(storageStub)

  private val letters = listOf("abcde", "fghij", "klmno")
  private val content = letters.map {
    ByteString.copyFromUtf8(it).asReadOnlyByteBuffer()
  }

  @Test
  fun `getBlobMetadata returns NOT_FOUND when blobKey does not exist`() = runBlocking<Unit> {
    val e = assertThrows(StatusException::class.java) {
      runBlocking {
        storageStub.getBlobMetadata(
          GetBlobMetadataRequest.newBuilder().setBlobKey("get/blob/size/returns/not/found").build()
        )
      }
    }
    assertThat(e.status.code).isEqualTo(Status.NOT_FOUND.code)
  }

  @Test
  fun `readBlob returns NOT_FOUND when blobKey does not exist`() = runBlocking<Unit> {
    val e = assertThrows(StatusException::class.java) {
      runBlocking {
        storageStub.readBlob(
          ReadBlobRequest.newBuilder().setBlobKey("read/blob/returns/not/found").setChunkSize(4096)
            .build()
        ).toList()
      }
    }
    assertThat(e.status.code).isEqualTo(Status.NOT_FOUND.code)
  }

  @Test
  fun `deleteBlob returns NOT_FOUND when blobKey does not exist`() = runBlocking<Unit> {
    val e = assertThrows(StatusException::class.java) {
      runBlocking {
        storageStub.deleteBlob(
          DeleteBlobRequest.newBuilder().setBlobKey("delete/blob/returns/not/found").build()
        )
      }
    }
    assertThat(e.status.code).isEqualTo(Status.NOT_FOUND.code)
  }

  @Test
  fun `getBlob returns null when blobKey does not exist`() = runBlocking<Unit> {
    assertThat(storageClient.getBlob("this/blob/does/not/exist")).isNull()
  }

  @Test
  fun `size returns the number of bytes written by create`() = runBlocking<Unit> {
    val blobKey = "blob/for/get/size"

    val blob = storageClient.createBlob(blobKey, content.asFlow())
    val blobSize = blob.size

    assertThat(blobSize).isEqualTo(15)
  }

  @Test
  fun `size returns the number of bytes after getBlob`() = runBlocking<Unit> {
    val blobKey = "blob/for/get/size/after/get"

    storageClient.createBlob(blobKey, content.asFlow())
    val blob = storageClient.getBlob(blobKey)
    val blobSize = blob!!.size

    assertThat(blobSize).isEqualTo(15)
  }

  @Test
  fun `read bytes match create bytes`() = runBlocking<Unit> {
    val blobKey = "blob/to/read"

    val blob = storageClient.createBlob(blobKey, content.asFlow())
    val result = blob.read(5).toList().map {
      ByteString.copyFrom(it).toStringUtf8()
    }

    assertThat(result).containsExactlyElementsIn(letters).inOrder()
  }

  @Test
  fun `read bytes match create bytes after getBlob`() = runBlocking<Unit> {
    val blobKey = "blob/to/read/after/get"

    storageClient.createBlob(blobKey, content.asFlow())
    val blob = storageClient.getBlob(blobKey)
    val result = blob!!.read(5).toList().map {
      ByteString.copyFrom(it).toStringUtf8()
    }

    assertThat(result).containsExactlyElementsIn(letters).inOrder()
  }

  @Test
  fun `get returns null after delete`() = runBlocking<Unit> {
    val blobKey = "blob/to/delete"

    val blob = storageClient.createBlob(blobKey, content.asFlow())
    blob.delete()
    val blobCheck = storageClient.getBlob(blobKey)

    assertThat(blobCheck).isNull()
  }
}
