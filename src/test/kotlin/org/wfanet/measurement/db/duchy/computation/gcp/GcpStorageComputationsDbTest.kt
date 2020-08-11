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

package org.wfanet.measurement.db.duchy.computation.gcp

import com.google.cloud.storage.BlobId
import com.google.cloud.storage.BlobInfo
import com.google.cloud.storage.Storage
import com.google.cloud.storage.contrib.nio.testing.LocalStorageHelper
import com.google.common.truth.Truth.assertThat
import kotlin.test.assertFailsWith
import kotlinx.coroutines.runBlocking
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.db.duchy.computation.BlobRef
import org.wfanet.measurement.db.duchy.computation.ComputationStorageEditToken
import org.wfanet.measurement.internal.LiquidLegionsSketchAggregationStage

@RunWith(JUnit4::class)
class GcpStorageComputationsDbTest {
  private lateinit var blobsDb: GcpStorageComputationsDb<LiquidLegionsSketchAggregationStage>

  companion object {
    const val TEST_BUCKET = "testing-bucket"
    private val storage: Storage = LocalStorageHelper.getOptions().service
    private val token = ComputationStorageEditToken(
      localId = 5432L, attempt = 1, editVersion = 1234567891011L,
      stage = LiquidLegionsSketchAggregationStage.TO_DECRYPT_FLAG_COUNTS
    )
  }

  @Before
  fun setUp() {
    blobsDb = GcpStorageComputationsDb<LiquidLegionsSketchAggregationStage>(storage, TEST_BUCKET)
  }

  @Test
  fun readBlob() = runBlocking<Unit> {
    val blobData = "Somedata".toByteArray()
    storage.create(BlobInfo.newBuilder(BlobId.of(TEST_BUCKET, "path")).build(), blobData)
    val data = blobsDb.read(BlobRef(1234L, "path"))
    assertThat(data).isEqualTo(blobData)
  }

  @Test
  fun `readBlob fails when blob is missing`() = runBlocking<Unit> {
    assertFailsWith<IllegalStateException> {
      blobsDb.read(BlobRef(1234L, "path/to/unwritten/blob"))
    }
  }

  @Test
  fun writeBlobPath() = runBlocking<Unit> {
    val pathToBlob = "path/to/a/blob"
    val blobData = "data-to-write-to-storage".toByteArray()
    blobsDb.blockingWrite(pathToBlob, blobData)
    assertThat(storage[BlobId.of(TEST_BUCKET, pathToBlob)].getContent()).isEqualTo(blobData)
  }

  @Test
  fun `blob lifecycle`() = runBlocking {
    val blob1 = "abcdefghijklmnopqrstuvwxyz".toByteArray()
    val blob2 = "123456789011121314151617181920".toByteArray()
    val blob1Path = newBlobPath(token, "my-new-blob")
    val blob1Ref = BlobRef(0L, blob1Path)
    val blob2Path = newBlobPath(token, "some-other-blob")
    val blob2Ref = BlobRef(1L, blob2Path)
    // Write both blobs using the implementation.
    blobsDb.blockingWrite(blob1Ref, blob1)
    blobsDb.blockingWrite(blob2Ref, blob2)
    // Read both blobs making sure they are as expected.
    assertThat(blobsDb.read(blob1Ref)).isEqualTo(blob1)
    assertThat(blobsDb.read(blob2Ref)).isEqualTo(blob2)
    // Delete one of the blobs
    blobsDb.delete(blob1Ref)
    // Make sure the delted blob is no longer present, and the other one is still there.
    assertFailsWith<IllegalStateException> { blobsDb.read(blob1Ref) }
    assertThat(blobsDb.read(blob2Ref)).isEqualTo(blob2)
  }
}

/** A deterministic name for a blob useful for testing. */
private fun <StageT> newBlobPath(
  token: ComputationStorageEditToken<StageT>,
  name: String
): String = "${token.localId}/${token.stage}/$name"
