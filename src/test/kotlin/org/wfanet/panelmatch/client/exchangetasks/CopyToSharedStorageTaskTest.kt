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

package org.wfanet.panelmatch.client.exchangetasks

import com.google.common.truth.Truth.assertThat
import com.google.protobuf.ByteString
import com.google.protobuf.kotlin.toByteStringUtf8
import kotlin.test.assertFails
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.storage.testing.InMemoryStorageClient
import org.wfanet.panelmatch.client.internal.ExchangeWorkflow.Step.CopyOptions.LabelType
import org.wfanet.panelmatch.client.internal.ExchangeWorkflowKt.StepKt.copyOptions
import org.wfanet.panelmatch.client.storage.signatureBlobKeyFor
import org.wfanet.panelmatch.client.storage.testing.makeTestSigningStorageClient
import org.wfanet.panelmatch.client.storage.testing.makeTestVerifyingStorageClient
import org.wfanet.panelmatch.common.testing.runBlockingTest

private const val SOURCE_BLOB_KEY = "source-blob-key"
private const val DESTINATION_BLOB_KEY = "destination-blob-key"
private val BLOB_CONTENTS = "some-blob-contents".toByteStringUtf8()
private val MANIFEST_CONTENTS = "foo-?-of-2".toByteStringUtf8()
private const val SHARD_BLOB_KEY1 = "foo-0-of-2"
private const val SHARD_BLOB_KEY2 = "foo-1-of-2"
private val SHARD_CONTENTS1 = "shard-1-contents".toByteStringUtf8()
private val SHARD_CONTENTS2 = "shard-2-contents".toByteStringUtf8()

@RunWith(JUnit4::class)
class CopyToSharedStorageTaskTest {
  private val source = InMemoryStorageClient()
  private val destinationClient = InMemoryStorageClient()
  private val destination = makeTestSigningStorageClient(destinationClient)
  private val verifyingStorage = makeTestVerifyingStorageClient(destinationClient)

  private suspend fun executeTask(labelType: LabelType) {
    CopyToSharedStorageTask(
        source,
        destination,
        copyOptions { this.labelType = labelType },
        SOURCE_BLOB_KEY,
        DESTINATION_BLOB_KEY,
      )
      .execute()
  }

  private suspend fun addSourceBlob(blobKey: String, contents: ByteString = BLOB_CONTENTS) {
    source.writeBlob(blobKey, contents)
  }

  @Test
  fun singleFile() = runBlockingTest {
    addSourceBlob(SOURCE_BLOB_KEY, BLOB_CONTENTS)
    executeTask(LabelType.BLOB)

    // Does not throw; verifies signature:
    assertThat(verifyingStorage.getBlob(DESTINATION_BLOB_KEY).toByteString())
      .isEqualTo(BLOB_CONTENTS)

    assertThat(destinationClient.contents.keys)
      .containsExactly(DESTINATION_BLOB_KEY, signatureBlobKeyFor(DESTINATION_BLOB_KEY))
  }

  @Test
  fun manifest() = runBlockingTest {
    addSourceBlob(SOURCE_BLOB_KEY, MANIFEST_CONTENTS)
    addSourceBlob(SHARD_BLOB_KEY1, SHARD_CONTENTS1)
    addSourceBlob(SHARD_BLOB_KEY2, SHARD_CONTENTS2)

    executeTask(LabelType.MANIFEST)

    assertThat(verifyingStorage.getBlob(DESTINATION_BLOB_KEY).toByteString())
      .isEqualTo(MANIFEST_CONTENTS)
    assertThat(verifyingStorage.getBlob(SHARD_BLOB_KEY1).toByteString()).isEqualTo(SHARD_CONTENTS1)
    assertThat(verifyingStorage.getBlob(SHARD_BLOB_KEY2).toByteString()).isEqualTo(SHARD_CONTENTS2)

    assertThat(destinationClient.contents.keys)
      .containsExactly(
        DESTINATION_BLOB_KEY,
        signatureBlobKeyFor(DESTINATION_BLOB_KEY),
        SHARD_BLOB_KEY1,
        signatureBlobKeyFor(SHARD_BLOB_KEY1),
        SHARD_BLOB_KEY2,
        signatureBlobKeyFor(SHARD_BLOB_KEY2),
      )
  }

  @Test
  fun missingFiles() = runBlockingTest {
    assertFails { executeTask(LabelType.BLOB) }
    assertFails { executeTask(LabelType.MANIFEST) }
  }

  @Test
  fun missingManifestFile() = runBlockingTest {
    addSourceBlob(SOURCE_BLOB_KEY, MANIFEST_CONTENTS)
    addSourceBlob(SHARD_BLOB_KEY1)

    assertFails { executeTask(LabelType.MANIFEST) }
  }

  @Test
  fun nonManifestContents() = runBlockingTest {
    addSourceBlob(SOURCE_BLOB_KEY, BLOB_CONTENTS)

    assertFails { executeTask(LabelType.MANIFEST) }
  }
}
