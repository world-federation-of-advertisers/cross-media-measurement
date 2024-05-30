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
import kotlinx.coroutines.runBlocking
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.storage.testing.InMemoryStorageClient
import org.wfanet.panelmatch.client.internal.ExchangeWorkflow.Step.CopyOptions.LabelType
import org.wfanet.panelmatch.client.internal.ExchangeWorkflow.Step.CopyOptions.LabelType.BLOB
import org.wfanet.panelmatch.client.internal.ExchangeWorkflow.Step.CopyOptions.LabelType.MANIFEST
import org.wfanet.panelmatch.client.internal.ExchangeWorkflowKt.StepKt.copyOptions
import org.wfanet.panelmatch.client.storage.signatureBlobKeyFor
import org.wfanet.panelmatch.client.storage.testing.makeTestSigningStorageClient
import org.wfanet.panelmatch.client.storage.testing.makeTestVerifyingStorageClient
import org.wfanet.panelmatch.common.storage.toByteString
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
class CopyFromSharedStorageTaskTest {
  private val underlyingSource = InMemoryStorageClient()
  private val signingStorage = makeTestSigningStorageClient(underlyingSource)
  private val source = makeTestVerifyingStorageClient(underlyingSource)
  private val destination = InMemoryStorageClient()

  private suspend fun executeTask(labelType: LabelType) {
    CopyFromSharedStorageTask(
        source,
        destination,
        copyOptions { this.labelType = labelType },
        SOURCE_BLOB_KEY,
        DESTINATION_BLOB_KEY,
      )
      .execute()
  }

  private suspend fun addUnsignedSourceBlob(blobKey: String, contents: ByteString = BLOB_CONTENTS) {
    underlyingSource.writeBlob(blobKey, contents)
  }

  private suspend fun addSignedSourceBlob(blobKey: String, contents: ByteString = BLOB_CONTENTS) {
    signingStorage.writeBlob(blobKey, contents)
  }

  private val destinationByteStrings: List<Pair<String, ByteString>>
    get() = runBlocking { destination.contents.mapValues { it.value.toByteString() }.toList() }

  @Test
  fun singleFile() = runBlockingTest {
    addSignedSourceBlob(SOURCE_BLOB_KEY)
    executeTask(BLOB)

    val sourceBlob = source.getBlob(SOURCE_BLOB_KEY)
    assertThat(destinationByteStrings)
      .containsExactly(
        DESTINATION_BLOB_KEY to sourceBlob.toByteString(),
        signatureBlobKeyFor(DESTINATION_BLOB_KEY) to sourceBlob.signature,
      )
  }

  @Test
  fun manifest() = runBlockingTest {
    addSignedSourceBlob(SOURCE_BLOB_KEY, MANIFEST_CONTENTS)
    addSignedSourceBlob(SHARD_BLOB_KEY1, SHARD_CONTENTS1)
    addSignedSourceBlob(SHARD_BLOB_KEY2, SHARD_CONTENTS2)

    executeTask(MANIFEST)

    assertThat(destinationByteStrings)
      .containsExactly(
        DESTINATION_BLOB_KEY to MANIFEST_CONTENTS,
        signatureBlobKeyFor(DESTINATION_BLOB_KEY) to source.getBlob(SOURCE_BLOB_KEY).signature,
        SHARD_BLOB_KEY1 to SHARD_CONTENTS1,
        signatureBlobKeyFor(SHARD_BLOB_KEY1) to source.getBlob(SHARD_BLOB_KEY1).signature,
        SHARD_BLOB_KEY2 to SHARD_CONTENTS2,
        signatureBlobKeyFor(SHARD_BLOB_KEY2) to source.getBlob(SHARD_BLOB_KEY2).signature,
      )
  }

  @Test
  fun missingFiles() = runBlockingTest {
    assertFails { executeTask(BLOB) }
    assertFails { executeTask(MANIFEST) }
  }

  @Test
  fun missingSignature() = runBlockingTest {
    addUnsignedSourceBlob(SOURCE_BLOB_KEY)
    assertFails { executeTask(BLOB) }
  }

  @Test
  fun missingManifestSignature() = runBlockingTest {
    addUnsignedSourceBlob(SOURCE_BLOB_KEY, MANIFEST_CONTENTS)
    addSignedSourceBlob(SHARD_BLOB_KEY1)
    addSignedSourceBlob(SHARD_BLOB_KEY2)

    assertFails { executeTask(MANIFEST) }
  }

  @Test
  fun missingManifestFileSignature() = runBlockingTest {
    addSignedSourceBlob(SOURCE_BLOB_KEY, MANIFEST_CONTENTS)
    addSignedSourceBlob(SHARD_BLOB_KEY1)
    addUnsignedSourceBlob(SHARD_BLOB_KEY2)

    assertFails { executeTask(MANIFEST) }
  }

  @Test
  fun missingManifestFile() = runBlockingTest {
    addSignedSourceBlob(SOURCE_BLOB_KEY, MANIFEST_CONTENTS)
    addSignedSourceBlob(SHARD_BLOB_KEY1)

    assertFails { executeTask(MANIFEST) }
  }

  @Test
  fun nonManifestContents() = runBlockingTest {
    addSignedSourceBlob(SOURCE_BLOB_KEY, BLOB_CONTENTS)

    assertFails { executeTask(MANIFEST) }
  }
}
