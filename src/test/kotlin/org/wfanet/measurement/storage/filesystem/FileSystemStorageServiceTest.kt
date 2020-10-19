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

package org.wfanet.measurement.storage.filesystem

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
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.internal.testing.DeleteBlobRequest
import org.wfanet.measurement.internal.testing.ForwardedStorageGrpcKt.ForwardedStorageCoroutineStub
import org.wfanet.measurement.internal.testing.GetBlobMetadataRequest
import org.wfanet.measurement.internal.testing.ReadBlobRequest
import org.wfanet.measurement.storage.forwarded.ForwardedStorageClient

@RunWith(JUnit4::class)
class FileSystemStorageServiceTest {
  @get:Rule
  val grpcTestServerRule = GrpcTestServerRule {
    addService(FileSystemStorageService())
  }

  val storageStub = ForwardedStorageCoroutineStub(grpcTestServerRule.channel)
  val storageClient = ForwardedStorageClient(storageStub)

  private val letters = listOf("abcde", "fghij", "klmno")
  private val content: List<ByteString> = letters.map {
    ByteString.copyFromUtf8(it)
  }

  @Test
  fun `getBlobMetadata returns NOT_FOUND when blobKey does not exist`() {
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
  fun `readBlob returns NOT_FOUND when blobKey does not exist`() {
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
  fun `deleteBlob returns NOT_FOUND when blobKey does not exist`() {
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
  fun `getBlob returns null when blobKey does not exist`() {
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
  fun `read bytes match create bytes`() = runBlocking {
    val blobKey = "blob/to/read"

    val blob = storageClient.createBlob(blobKey, content.asFlow())
    val result = blob.read(5).toList().map {
      it.toStringUtf8()
    }

    assertThat(result).containsExactlyElementsIn(letters).inOrder()
  }

  @Test
  fun `read bytes match create bytes after getBlob`() = runBlocking {
    val blobKey = "blob/to/read/after/get"

    storageClient.createBlob(blobKey, content.asFlow())
    val blob = storageClient.getBlob(blobKey)
    val result = blob!!.read(5).toList().map {
      it.toStringUtf8()
    }

    assertThat(result).containsExactlyElementsIn(letters).inOrder()
  }

  @Test
  fun `get returns null after delete`() = runBlocking {
    val blobKey = "blob/to/delete"

    val blob = storageClient.createBlob(blobKey, content.asFlow())
    blob.delete()
    val blobCheck = storageClient.getBlob(blobKey)

    assertThat(blobCheck).isNull()
  }
}
