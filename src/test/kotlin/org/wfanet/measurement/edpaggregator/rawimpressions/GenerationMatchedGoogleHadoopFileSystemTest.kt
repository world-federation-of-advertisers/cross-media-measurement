/*
 * Copyright 2026 The Cross-Media Measurement Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.wfanet.measurement.edpaggregator.rawimpressions

import com.google.cloud.ReadChannel
import com.google.cloud.storage.Blob
import com.google.cloud.storage.BlobId
import com.google.cloud.storage.Storage
import com.google.cloud.storage.StorageException
import com.google.cloud.storage.StorageOptions
import com.google.common.truth.Truth.assertThat
import java.io.IOException
import java.net.URI
import java.nio.ByteBuffer
import kotlin.test.assertFailsWith
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.mockito.Mockito.mockStatic
import org.mockito.kotlin.any
import org.mockito.kotlin.eq
import org.mockito.kotlin.mock
import org.mockito.kotlin.verify
import org.mockito.kotlin.whenever

@RunWith(JUnit4::class)
class GenerationMatchedGoogleHadoopFileSystemTest {
  @Test
  fun `generation qualifier identifies original object and expected generation`() {
    val path =
      checkNotNull(
        parseGenerationMatchedPath(
          Path(generationMatchedBlobUri("gs://bucket/folder/file.parquet", 123L))
        )
      )

    assertThat(path.cleanPath.toString()).isEqualTo("gs://bucket/folder/file.parquet")
    assertThat(path.blobId).isEqualTo(BlobId.of("bucket", "folder/file.parquet", 123L))
    assertThat(path.generation).isEqualTo(123L)
  }

  @Test
  fun `generation qualifiers keep the Hadoop filesystem authority stable`() {
    val first = Path(generationMatchedBlobUri("gs://bucket/folder/first.parquet", 123L)).toUri()
    val second = Path(generationMatchedBlobUri("gs://bucket/folder/second.parquet", 456L)).toUri()

    assertThat(first.authority).isEqualTo("bucket")
    assertThat(second.authority).isEqualTo(first.authority)
  }

  @Test
  fun `metadata lookup requests the captured object version`() {
    val storage = mock<Storage>()
    val blob = mock<Blob>()
    val path =
      checkNotNull(
        parseGenerationMatchedPath(
          Path(generationMatchedBlobUri("gs://bucket/folder/file.parquet", 123L))
        )
      )
    whenever(blob.generation).thenReturn(123L)
    whenever(storage.get(eq(path.blobId), any<Storage.BlobGetOption>())).thenReturn(blob)

    assertThat(getGenerationMatchedBlob(storage, path)).isSameInstanceAs(blob)

    verify(storage)
      .get(eq(BlobId.of("bucket", "folder/file.parquet", 123L)), any<Storage.BlobGetOption>())
  }

  @Test
  fun `content reads and seeks request the captured version after overwrite`() {
    val storage = mock<Storage>()
    val blob = mock<Blob>()
    val captured = mock<ReadChannel>()
    val latest = mock<ReadChannel>()
    val bytes = byteArrayOf(11, 22, 33)
    var position = 0
    whenever(blob.generation).thenReturn(123L)
    whenever(blob.size).thenReturn(bytes.size.toLong())
    // Metadata is available independently; the content assertion must detect an unqualified reader.
    whenever(storage.get(any<BlobId>(), any<Storage.BlobGetOption>())).thenReturn(blob)
    whenever(storage.reader(any<BlobId>(), any<Storage.BlobSourceOption>())).thenAnswer { invocation
      ->
      if (invocation.getArgument<BlobId>(0).generation == 123L) captured else latest
    }
    whenever(captured.read(any<ByteBuffer>())).thenAnswer { invocation ->
      val buffer = invocation.getArgument<ByteBuffer>(0)
      if (position == bytes.size) -1
      else {
        val count = minOf(buffer.remaining(), bytes.size - position)
        buffer.put(bytes, position, count)
        position += count
        count
      }
    }
    whenever(captured.seek(any())).thenAnswer { invocation ->
      position = invocation.getArgument<Long>(0).toInt()
      null
    }
    whenever(latest.read(any<ByteBuffer>())).thenAnswer { invocation ->
      invocation.getArgument<ByteBuffer>(0).put(99.toByte())
      1
    }
    withFileSystem(storage) { fileSystem ->
      fileSystem
        .open(Path(generationMatchedBlobUri("gs://bucket/folder/file.parquet", 123L)), 1024)
        .use { input ->
          assertThat(input.read()).isEqualTo(11)
          input.seek(1L)
          val remainder = ByteArray(2)
          input.readFully(remainder)
          assertThat(remainder).isEqualTo(byteArrayOf(22, 33))
        }
    }
    verify(storage)
      .reader(
        eq(BlobId.of("bucket", "folder/file.parquet", 123L)),
        eq(Storage.BlobSourceOption.generationMatch(123L)),
      )
    verify(captured).close()
  }

  @Test
  fun `content precondition failure gives the recovery instruction`() {
    val storage = mock<Storage>()
    val blob = mock<Blob>()
    val channel = mock<ReadChannel>()
    whenever(blob.generation).thenReturn(123L)
    whenever(blob.size).thenReturn(1L)
    whenever(storage.get(any<BlobId>(), any<Storage.BlobGetOption>())).thenReturn(blob)
    whenever(storage.reader(any<BlobId>(), any<Storage.BlobSourceOption>())).thenReturn(channel)
    whenever(channel.read(any<ByteBuffer>())).thenThrow(StorageException(412, "conditionNotMet"))
    withFileSystem(storage) { fileSystem ->
      fileSystem.open(Path(generationMatchedBlobUri("gs://bucket/file.parquet", 123L)), 1024).use {
        input ->
        val failure = assertFailsWith<IOException> { input.read() }
        assertThat(failure).hasMessageThat().contains("registered generation 123")
        assertThat(failure).hasMessageThat().contains("write a new done blob")
      }
    }
    verify(channel).close()
  }

  private fun withFileSystem(
    storage: Storage,
    block: (GenerationMatchedGoogleHadoopFileSystem) -> Unit,
  ) {
    val builder = mock<StorageOptions.Builder>()
    val options = mock<StorageOptions>()
    whenever(builder.setProjectId(any())).thenReturn(builder)
    whenever(builder.build()).thenReturn(options)
    whenever(options.service).thenReturn(storage)
    mockStatic(StorageOptions::class.java).use { factory ->
      factory.`when`<StorageOptions.Builder> { StorageOptions.newBuilder() }.thenReturn(builder)
      val fileSystem = GenerationMatchedGoogleHadoopFileSystem()
      val configuration = Configuration(false)
      configuration.set("fs.gs.project.id", "test-project")
      configuration.set("fs.gs.auth.type", "UNAUTHENTICATED")
      fileSystem.initialize(URI.create("gs://bucket"), configuration)
      try {
        block(fileSystem)
      } finally {
        fileSystem.close()
      }
    }
  }

  @Test
  fun `generation mismatch gives EDP actionable recovery instruction`() {
    val storage = mock<Storage>()
    val path =
      GenerationMatchedPath(
        Path("gs://bucket/file.parquet"),
        BlobId.of("bucket", "file.parquet", 123L),
        123L,
      )
    whenever(storage.get(any<BlobId>(), any<Storage.BlobGetOption>()))
      .thenThrow(StorageException(412, "conditionNotMet"))

    val exception = assertFailsWith<IOException> { getGenerationMatchedBlob(storage, path) }

    assertThat(exception)
      .hasMessageThat()
      .contains("no longer matches its registered generation 123")
    assertThat(exception).hasMessageThat().contains("write a new done blob")
  }
}
