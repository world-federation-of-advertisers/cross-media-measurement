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

package org.wfanet.measurement.secure_computation.tee_sdk.cloud_storage.v1alpha

import org.wfanet.measurement.secure_computation.tee_sdk.cloud_storage.v1alpha.GcsStorageClient
import org.junit.Test
import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertTrue
import org.mockito.Mockito
import org.mockito.Mockito.any
import org.mockito.Mockito.mock
import org.mockito.Mockito.mockStatic
import org.mockito.Mockito.`when`
import org.mockito.Mockito.verify
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import com.google.cloud.storage.Blob
import com.google.cloud.storage.BlobInfo
import com.google.cloud.storage.Storage
import com.google.cloud.ReadChannel
import com.google.cloud.storage.StorageOptions
import java.nio.channels.Channels
import java.nio.channels.WritableByteChannel
import java.io.InputStream
import java.nio.ByteBuffer
import java.util.UUID

@RunWith(JUnit4::class)
class GcsStorageClientTest {

    @Test
    fun test_getData_successfulRetrieval() {
        val mockStorage = Mockito.mock(Storage::class.java)
        val mockBlob = Mockito.mock(Blob::class.java)
        val mockReadChannel = Mockito.mock(ReadChannel::class.java)
        val mockInputStream = Mockito.mock(InputStream::class.java)
        `when`(mockStorage.get("valid-bucket", "valid-object")).thenReturn(mockBlob)
        `when`(mockBlob.reader()).thenReturn(mockReadChannel)
        mockStatic(Channels::class.java).use { mockedChannels ->
            mockedChannels.`when`<InputStream> { Channels.newInputStream(mockReadChannel) }.thenReturn(mockInputStream)
            mockStatic(StorageOptions::class.java).use { _ ->
                val mockStorageOptions = Mockito.mock(StorageOptions::class.java)
                `when`(mockStorageOptions.service).thenReturn(mockStorage)
                `when`(StorageOptions.getDefaultInstance()).thenReturn(mockStorageOptions)
                val storageClient = GcsStorageClient()
                val inputStream = storageClient.getData("valid-bucket", "valid-object")
                assertEquals(mockInputStream, inputStream)
            }
        }
    }

    @Test(expected = com.google.cloud.storage.StorageException::class)
    fun test_getData_nonExistentBucket() {
        val mockStorage = mock(Storage::class.java)
        `when`(mockStorage.get("non-existent-bucket", "any-object")).thenReturn(null)
        val storageClient = GcsStorageClient()
        storageClient.getData("non-existent-bucket", "any-object")
    }

    @Test(expected = RuntimeException::class)
    fun test_getData_errorDuringRetrieval() {
        val mockStorage = mock(Storage::class.java)
        `when`(mockStorage.get("error-bucket", "error-object")).thenThrow(RuntimeException())
        val storageClient = GcsStorageClient()
        storageClient.getData("error-bucket", "error-object")
    }

    @Test
    fun test_startMultipartUpload_success() {
        val mockStorage = mock<Storage>()
        val mockWriteChannel = mock(com.google.cloud.WriteChannel::class.java)
        mockStatic(StorageOptions::class.java).use { _ ->
            val mockStorageOptions = mock<StorageOptions>()
            `when`(StorageOptions.getDefaultInstance()).thenReturn(mockStorageOptions)
            `when`(mockStorageOptions.service).thenReturn(mockStorage)
            `when`(mockStorage.writer(any(BlobInfo::class.java))).thenReturn(mockWriteChannel)
            val storageClient = GcsStorageClient()
            val uploadId = storageClient.startMultipartUpload("valid-bucket", "valid-object")
            assertTrue(GcsStorageClient.multipartUploads.containsKey(uploadId))
            assertEquals(mockWriteChannel, GcsStorageClient.multipartUploads[uploadId])
        }
    }

    @Test(expected = com.google.cloud.storage.StorageException::class)
    fun test_startMultipartUpload_invalidBucketName() {
        val mockStorage = mock(Storage::class.java)
        `when`(mockStorage.writer(any(BlobInfo::class.java))).thenThrow(com.google.cloud.storage.StorageException::class.java)
        val storageClient = GcsStorageClient()
        storageClient.startMultipartUpload("invalid-bucket", "valid-object")
    }

    @Test(expected = com.google.cloud.storage.StorageException::class)
    fun test_startMultipartUpload_invalidObjectName() {
        val mockStorage = mock(Storage::class.java)
        `when`(mockStorage.writer(any(BlobInfo::class.java))).thenThrow(com.google.cloud.storage.StorageException::class.java)
        val storageClient = GcsStorageClient()
        storageClient.startMultipartUpload("valid-bucket", "invalid-object")
    }

    @Test(expected = com.google.cloud.storage.StorageException::class)
    fun test_startMultipartUpload_emptyBucketName() {
        val mockStorage = mock(Storage::class.java)
        `when`(mockStorage.writer(any(BlobInfo::class.java))).thenThrow(com.google.cloud.storage.StorageException::class.java)
        val storageClient = GcsStorageClient()
        storageClient.startMultipartUpload("", "valid-object")
    }

    @Test(expected = com.google.cloud.storage.StorageException::class)
    fun test_startMultipartUpload_emptyObjectName() {
        val mockStorage = mock(Storage::class.java)
        `when`(mockStorage.writer(any(BlobInfo::class.java))).thenThrow(com.google.cloud.storage.StorageException::class.java)
        val storageClient = GcsStorageClient()
        storageClient.startMultipartUpload("valid-bucket", "")
    }

    @Test
    fun test_uploadPart_success() {
        val mockWritableByteChannel = mock(WritableByteChannel::class.java)
        val byteArray = "Hello World".toByteArray()
        GcsStorageClient.multipartUploads["validUploadId"] = mockWritableByteChannel
        val storageClient = GcsStorageClient()
        storageClient.uploadPart(byteArray, "validUploadId")
        verify(mockWritableByteChannel).write(ByteBuffer.wrap(byteArray))
    }

    @Test
    fun test_uploadPart_invalidUploadId() {
        val storageClient = GcsStorageClient()
        storageClient.uploadPart("Hello World".toByteArray(), "invalidUploadId")
    }

    @Test
    fun test_uploadPart_emptyByteArray() {
        val mockWritableByteChannel = mock(WritableByteChannel::class.java)
        val byteArray = ByteArray(0)
        GcsStorageClient.multipartUploads["validUploadId"] = mockWritableByteChannel
        val storageClient = GcsStorageClient()
        storageClient.uploadPart(byteArray, "validUploadId")
        verify(mockWritableByteChannel).write(ByteBuffer.wrap(byteArray))
    }

    @Test
    fun test_completeMultipart_success() {
        val mockWritableByteChannel = mock(WritableByteChannel::class.java)
        GcsStorageClient.multipartUploads["abc123"] = mockWritableByteChannel
        val storageClient = GcsStorageClient()
        storageClient.completeMultipart("abc123")
        verify(mockWritableByteChannel).close()
        assertFalse(GcsStorageClient.multipartUploads.containsKey("abc123"))
    }

    @Test
    fun test_completeMultipart_nonExistentUploadId() {
        val storageClient = GcsStorageClient()
        storageClient.completeMultipart("nonExistentId")
        assertFalse(GcsStorageClient.multipartUploads.containsKey("nonExistentId"))
    }

    @Test
    fun test_completeMultipart_alreadyClosedId() {
        val mockWritableByteChannel = mock(WritableByteChannel::class.java)
        GcsStorageClient.multipartUploads["alreadyClosedId"] = mockWritableByteChannel
        val storageClient = GcsStorageClient()
        storageClient.completeMultipart("alreadyClosedId")
        verify(mockWritableByteChannel).close()
        assertFalse(GcsStorageClient.multipartUploads.containsKey("alreadyClosedId"))
    }
}
