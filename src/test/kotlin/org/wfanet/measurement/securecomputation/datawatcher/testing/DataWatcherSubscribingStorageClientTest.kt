/*
 * Copyright 2024 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.securecomputation.datawatcher.testing

import com.google.common.truth.Truth.assertThat
import com.google.protobuf.kotlin.toByteStringUtf8
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.runBlocking
import org.junit.Before
import org.junit.Rule
import org.junit.Test
import org.junit.rules.TemporaryFolder
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.mockito.kotlin.argumentCaptor
import org.mockito.kotlin.eq
import org.mockito.kotlin.mock
import org.mockito.kotlin.times
import org.mockito.kotlin.verify
import org.wfanet.measurement.securecomputation.datawatcher.DataWatcher
import org.wfanet.measurement.storage.ConditionalOperationStorageClient
import org.wfanet.measurement.storage.filesystem.FileSystemStorageClient
import org.wfanet.measurement.storage.testing.AbstractStorageClientTest

@RunWith(JUnit4::class)
class DataWatcherSubscribingStorageClientTest :
  AbstractStorageClientTest<FileSystemStorageClient>() {

  @Rule @JvmField val tempDirectory = TemporaryFolder()

  @Before
  fun initStorageClient() {
    storageClient = FileSystemStorageClient(tempDirectory.root)
  }

  @Test
  fun `writeBlob publishes to subscribing DataWatcher`() = runBlocking {
    val subscribingStorageClient =
      DataWatcherSubscribingStorageClient(storageClient, "file:///some-bucket/")
    val dataWatcher: DataWatcher = mock {}
    subscribingStorageClient.subscribe(dataWatcher)
    subscribingStorageClient.writeBlob("some-blob-key", flowOf("some-contents".toByteStringUtf8()))
    val metadata = argumentCaptor<Map<String, String>>()
    verify(dataWatcher, times(1))
      .receivePath(eq("file:///some-bucket/some-blob-key"), metadata.capture())
    assertThat(metadata.firstValue)
      .containsEntry(
        DataWatcher.GENERATION_METADATA_KEY,
        checkNotNull(storageClient.getFreshnessToken("some-blob-key")),
      )
  }

  @Test
  fun `writeBlob publishes custom metadata with generation`() =
    runBlocking<Unit> {
      val subscribingStorageClient =
        DataWatcherSubscribingStorageClient(storageClient, "file:///some-bucket/")
      val dataWatcher: DataWatcher = mock {}
      subscribingStorageClient.subscribe(dataWatcher)

      subscribingStorageClient.writeBlob(
        "some-blob-key",
        flowOf("some-contents".toByteStringUtf8()),
        mapOf("recovery-source" to "upload-1"),
      )

      val metadata = argumentCaptor<Map<String, String>>()
      verify(dataWatcher).receivePath(eq("file:///some-bucket/some-blob-key"), metadata.capture())
      assertThat(metadata.firstValue)
        .containsAtLeast(
          "recovery-source",
          "upload-1",
          DataWatcher.GENERATION_METADATA_KEY,
          checkNotNull(storageClient.getFreshnessToken("some-blob-key")),
        )
    }

  @Test
  fun `writeBlobIfUnchanged atomically rewrites and publishes new generation`() =
    runBlocking<Unit> {
      val original =
        storageClient.writeBlob("some-blob-key", flowOf("old-contents".toByteStringUtf8()))
          as ConditionalOperationStorageClient.Blob
      val subscribingStorageClient =
        DataWatcherSubscribingStorageClient(storageClient, "file:///some-bucket/")
      val dataWatcher: DataWatcher = mock {}
      subscribingStorageClient.subscribe(dataWatcher)

      val rewritten =
        subscribingStorageClient.writeBlobIfUnchanged(
          "some-blob-key",
          original.freshnessToken,
          flowOf("new-contents".toByteStringUtf8()),
          mapOf("recovery-source" to "upload-1"),
        ) as ConditionalOperationStorageClient.Blob

      assertThat(rewritten.freshnessToken).isNotEqualTo(original.freshnessToken)
      val metadata = argumentCaptor<Map<String, String>>()
      verify(dataWatcher).receivePath(eq("file:///some-bucket/some-blob-key"), metadata.capture())
      assertThat(metadata.firstValue)
        .containsAtLeast(
          "recovery-source",
          "upload-1",
          DataWatcher.GENERATION_METADATA_KEY,
          rewritten.freshnessToken,
        )
    }
}
