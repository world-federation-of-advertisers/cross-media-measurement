/*
 * Copyright 2026 The Cross-Media Measurement Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.wfanet.measurement.edpaggregator.rawimpressions

import com.google.cloud.WriteChannel
import com.google.cloud.storage.Blob
import com.google.cloud.storage.BlobInfo
import com.google.cloud.storage.Storage
import com.google.cloud.storage.StorageException as GcsStorageException
import com.google.common.truth.Truth.assertThat
import com.google.protobuf.ByteString
import java.io.IOException
import java.nio.ByteBuffer
import java.time.OffsetDateTime
import kotlin.test.assertFailsWith
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.runBlocking
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.mockito.kotlin.any
import org.mockito.kotlin.argumentCaptor
import org.mockito.kotlin.doAnswer
import org.mockito.kotlin.mock
import org.mockito.kotlin.never
import org.mockito.kotlin.verify
import org.mockito.kotlin.whenever
import org.wfanet.measurement.gcloud.gcs.GcsStorageClient
import org.wfanet.measurement.storage.BlobChangedException

@RunWith(JUnit4::class)
class CompleteGcsStorageClientTest {
  private val channel =
    mock<WriteChannel> {
      on { write(any()) } doAnswer
        { invocation ->
          val buffer = invocation.getArgument<ByteBuffer>(0)
          val size = buffer.remaining()
          buffer.position(buffer.limit())
          size
        }
    }
  private val blob =
    mock<Blob> {
      on { getGeneration() } doAnswer { 7L }
      on { getName() } doAnswer { "merged" }
      on { getSize() } doAnswer { 12L }
      on { getCreateTimeOffsetDateTime() } doAnswer { OffsetDateTime.now() }
      on { getUpdateTimeOffsetDateTime() } doAnswer { OffsetDateTime.now() }
    }
  private val storage =
    mock<Storage> {
      on { writer(any<BlobInfo>(), any<Storage.BlobWriteOption>()) } doAnswer { channel }
      on { get("bucket", "merged") } doAnswer { blob }
    }
  private val client = CompleteGcsStorageClient(storage, "bucket")

  @Test
  fun `failed input does not finalize a replacement`() =
    runBlocking<Unit> {
      assertFailsWith<IOException> {
        client.writeBlobIfUnchanged(
          "merged",
          "7",
          flow {
            emit(ByteString.copyFromUtf8("prefix"))
            throw IOException("input failed")
          },
        )
      }
      verify(channel).write(any())
      verify(channel, never()).close()
    }

  @Test
  fun `cancelled input does not finalize the first publication`() =
    runBlocking<Unit> {
      assertFailsWith<CancellationException> {
        client.writeBlobIfNotFound(
          "merged",
          flow {
            emit(ByteString.copyFromUtf8("prefix"))
            throw CancellationException("cancelled input")
          },
        )
      }
      verify(channel).write(any())
      verify(channel, never()).close()
    }

  @Test
  fun `successful replacement uses observed generation and finalizes`() =
    runBlocking<Unit> {
      val written =
        client.writeBlobIfUnchanged("merged", "7", flowOf(ByteString.copyFromUtf8("complete")))
      verify(channel).close()
      val options = argumentCaptor<Storage.BlobWriteOption>()
      verify(storage).writer(any<BlobInfo>(), options.capture())
      assertThat(options.firstValue).isEqualTo(Storage.BlobWriteOption.generationMatch(7L))
      assertThat(written.blobKey).isEqualTo("merged")
    }

  @Test
  fun `first publication requires object absence`() =
    runBlocking<Unit> {
      client.writeBlobIfNotFound("merged", flowOf(ByteString.copyFromUtf8("complete")))
      val options = argumentCaptor<Storage.BlobWriteOption>()
      verify(storage).writer(any<BlobInfo>(), options.capture())
      assertThat(options.firstValue).isEqualTo(Storage.BlobWriteOption.generationMatch(0L))
      verify(channel).close()
    }

  @Test
  fun `generation conflict remains a retryable blob conflict`() =
    runBlocking<Unit> {
      whenever(channel.close()).thenThrow(GcsStorageException(412, "generation changed"))
      assertFailsWith<BlobChangedException> {
        client.writeBlobIfUnchanged("merged", "7", flowOf(ByteString.copyFromUtf8("complete")))
      }
    }

  @Test
  fun `released common writer finalizes on input failure`() =
    runBlocking<Unit> {
      // Demonstrate why CAS alone is insufficient with common-jvm v0.162.0.
      assertFailsWith<IOException> {
        GcsStorageClient(storage, "bucket")
          .writeBlobIfUnchanged(
            "merged",
            "7",
            flow {
              emit(ByteString.copyFromUtf8("prefix"))
              throw IOException("input failed")
            },
          )
      }
      verify(channel).close()
    }
}
