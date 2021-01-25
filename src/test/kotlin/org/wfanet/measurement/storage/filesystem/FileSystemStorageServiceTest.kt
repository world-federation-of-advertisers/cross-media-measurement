// Copyright 2020 The Cross-Media Measurement Authors
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
import io.grpc.Status
import io.grpc.StatusException
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import org.junit.Assert.assertThrows
import org.junit.Before
import org.junit.Rule
import org.junit.Test
import org.junit.rules.TemporaryFolder
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.common.testing.chainRulesSequentially
import org.wfanet.measurement.internal.testing.DeleteBlobRequest
import org.wfanet.measurement.internal.testing.ForwardedStorageGrpcKt.ForwardedStorageCoroutineStub
import org.wfanet.measurement.internal.testing.GetBlobMetadataRequest
import org.wfanet.measurement.internal.testing.ReadBlobRequest
import org.wfanet.measurement.storage.forwarded.ForwardedStorageClient
import org.wfanet.measurement.storage.testing.AbstractStorageClientTest

@RunWith(JUnit4::class)
class FileSystemStorageServiceTest : AbstractStorageClientTest<ForwardedStorageClient>() {
  private val tempDirectory = TemporaryFolder()
  private val grpcTestServerRule = GrpcTestServerRule {
    addService(FileSystemStorageService(tempDirectory.root))
  }

  @get:Rule
  val ruleChain = chainRulesSequentially(tempDirectory, grpcTestServerRule)

  private val storageStub = ForwardedStorageCoroutineStub(grpcTestServerRule.channel)

  @Before
  fun initClient() {
    storageClient = ForwardedStorageClient(storageStub)
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
    assertThat(e.status.code).isEqualTo(Status.Code.NOT_FOUND)
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
    assertThat(e.status.code).isEqualTo(Status.Code.NOT_FOUND)
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
    assertThat(e.status.code).isEqualTo(Status.Code.NOT_FOUND)
  }
}
