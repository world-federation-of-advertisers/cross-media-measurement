// Copyright 2024 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.securecomputation.teesdk.cloudstorage.v1alpha

import org.mockito.kotlin.any
import org.mockito.kotlin.doNothing
import org.mockito.kotlin.doReturn
import org.mockito.kotlin.eq
import org.mockito.kotlin.mock
import org.mockito.kotlin.verify
import org.mockito.kotlin.whenever
import org.mockito.kotlin.spy
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import com.google.crypto.tink.Aead
import com.google.protobuf.ByteString
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertTrue
import kotlin.test.assertFailsWith
import org.wfanet.measurement.storage.StorageClient
import java.lang.IllegalArgumentException
import java.util.Base64
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.securecomputation.teesdk.cloudstorage.v1alpha.RecordioStorageClient

@RunWith(JUnit4::class)
class RecordioStorageClientTest {

  private lateinit var storageClient: StorageClient
  private lateinit var aead: Aead
  private lateinit var recordioStorageClient: RecordioStorageClient

  @Before
  fun setUp() {
    storageClient = mock()
    aead = mock()
    val validKey = ByteArray(16) { 0 }
    val encodedBlobKey = Base64.getEncoder().encodeToString(validKey)
    recordioStorageClient = spy(RecordioStorageClient(storageClient, encodedBlobKey))
  }

  @Test
  fun `test getBlob returns wrapped RecordioBlob`() = runBlocking {
    val blobKey = "test-blob"
    val mockBlob = mock<StorageClient.Blob>()
    whenever(storageClient.getBlob(blobKey)).thenReturn(mockBlob)
    val resultBlob = recordioStorageClient.getBlob(blobKey)
    verify(storageClient).getBlob(blobKey)
    assertNotNull(resultBlob)
    assertEquals(mockBlob.size, resultBlob.size)
  }

  @Test
  fun `test readBlob decrypts data correctly`() {
    runBlocking {
      val blobKey = "test-blob"
      val encryptedRow = ByteString.copyFromUtf8("encrypted-row")
      val decryptedRow = "decrypted-row".toByteArray()
      val mockBlob = mock<StorageClient.Blob>()
      whenever(mockBlob.read()).thenReturn(flowOf(encryptedRow))
      val encodedBlobKey = blobKey.encodeToByteArray() // Matches the encoding used in RecordioBlob
      whenever(aead.decrypt(any(), eq(encodedBlobKey))).thenReturn(decryptedRow)
      whenever(storageClient.getBlob(blobKey)).thenReturn(mockBlob)
      val resultBlob = recordioStorageClient.getBlob(blobKey)
      assertNotNull(resultBlob)
    }

  }


  @Test
  fun `test readBlob throws IllegalArgumentException on invalid length`() {
    runBlocking {
      val blobKey = "test-blob"
      val invalidRecord = ByteString.copyFromUtf8("invalidLength\ninvalid-length-row")
      val mockBlob = mock<StorageClient.Blob>()
      whenever(mockBlob.read()).thenReturn(flowOf(invalidRecord))
      whenever(storageClient.getBlob(blobKey)).thenReturn(mockBlob)
      val resultBlob = recordioStorageClient.getBlob(blobKey)
      val exception = assertFailsWith<IllegalArgumentException> {
        runBlocking {
          resultBlob!!.read().toList()
        }
      }
      assertTrue(exception.message!!.contains("Invalid length"))
    }
  }
}
