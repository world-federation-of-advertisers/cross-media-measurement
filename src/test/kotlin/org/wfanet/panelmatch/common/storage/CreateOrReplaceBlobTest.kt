// Copyright 2021 The Cross-Media Measurement Authors
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

package org.wfanet.panelmatch.common.storage

import com.google.common.truth.Truth.assertThat
import com.google.protobuf.kotlin.toByteStringUtf8
import org.junit.Rule
import org.junit.Test
import org.junit.rules.TemporaryFolder
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.storage.filesystem.FileSystemStorageClient
import org.wfanet.panelmatch.common.testing.runBlockingTest

private const val BLOB_KEY = "some-blob-key"
private val BLOB = "some-blob-contents".toByteStringUtf8()

@RunWith(JUnit4::class)
class CreateOrReplaceBlobTest {
  @get:Rule val temporaryFolder = TemporaryFolder()

  private val storage by lazy { FileSystemStorageClient(temporaryFolder.root) }

  @Test
  fun create() = runBlockingTest {
    val blob = storage.createOrReplaceBlob(BLOB_KEY, BLOB)
    assertThat(blob.toByteString()).isEqualTo(BLOB)
    assertThat(storage.getBlob(BLOB_KEY)?.toByteString()).isEqualTo(BLOB)
  }

  @Test
  fun replace() = runBlockingTest {
    storage.createBlob(BLOB_KEY, BLOB.concat(BLOB))

    val blob = storage.createOrReplaceBlob(BLOB_KEY, BLOB)
    assertThat(blob.toByteString()).isEqualTo(BLOB)
    assertThat(storage.getBlob(BLOB_KEY)?.toByteString()).isEqualTo(BLOB)
  }

  @Test
  fun replaceManyTimes() = runBlockingTest {
    storage.createBlob(BLOB_KEY, BLOB.concat(BLOB))

    // This is to detect bugs around filesystem flushing.
    repeat(10) { storage.createOrReplaceBlob(BLOB_KEY, BLOB) }

    assertThat(storage.getBlob(BLOB_KEY)?.toByteString()).isEqualTo(BLOB)
  }
}
