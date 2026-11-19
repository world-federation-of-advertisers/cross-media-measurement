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

import com.google.common.truth.Truth.assertThat
import kotlin.test.assertFailsWith
import kotlinx.coroutines.flow.emptyFlow
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.edpaggregator.v1alpha.OutputManifestRecord
import org.wfanet.measurement.edpaggregator.v1alpha.OutputManifestRecordKt
import org.wfanet.measurement.edpaggregator.v1alpha.outputBlobReference
import org.wfanet.measurement.edpaggregator.v1alpha.outputManifestRecord
import org.wfanet.measurement.storage.BlobChangedException
import org.wfanet.measurement.storage.MesosRecordIoStorageClient
import org.wfanet.measurement.storage.testing.InMemoryStorageClient

@RunWith(JUnit4::class)
class OutputManifestStoreTest {
  private val attempt = "00000000-0000-4000-8000-000000000001"
  private val upload = "dataProviders/provider/rawImpressionUploads/upload"
  private val owner = "$upload/vidLabelingJobs/job"
  private val model = "modelProviders/provider/modelSuites/suite/modelLines/line"
  private val uri = "gs://bucket/output/$attempt/manifest"
  private val header =
    OutputManifestRecordKt.header {
      phase = OutputManifestRecord.Header.Phase.VID_LABELING
      rawImpressionUpload = upload
      this.owner = this@OutputManifestStoreTest.owner
      modelLines += model
      attemptId = attempt
    }
  private val completed = outputManifestRecord {
    fileCompleted =
      OutputManifestRecordKt.fileCompleted {
        rawImpressionUploadFile = "$upload/rawImpressionUploadFiles/file"
        modelLine = model
      }
  }

  @Test
  fun `write and read preserve complete empty output without an encryption key`() =
    runBlocking<Unit> {
      val storage = InMemoryStorageClient()
      val store = OutputManifestStore(storage, "gs://bucket/output")
      val reference = store.write(uri, header, emptyFlow())
      val records = store.read(reference).toList()
      assertThat(records).hasSize(1)
      assertThat(records.single().header).isEqualTo(header)
      assertThat(records.single().header.hasEncryptedDek()).isFalse()
      assertThat(reference.blobGeneration).isGreaterThan(0L)
    }

  @Test
  fun `write and read preserve file completion records`() =
    runBlocking<Unit> {
      val store = OutputManifestStore(InMemoryStorageClient(), "gs://bucket/output")
      val reference = store.write(uri, header, flowOf(completed))
      assertThat(store.read(reference).toList())
        .containsExactly(
          outputManifestRecord { this.header = this@OutputManifestStoreTest.header },
          completed,
        )
        .inOrder()
    }

  @Test
  fun `write collision leaves the winning manifest unchanged`() =
    runBlocking<Unit> {
      val store = OutputManifestStore(InMemoryStorageClient(), "gs://bucket/output")
      val reference = store.write(uri, header, flowOf(completed))
      assertFailsWith<BlobChangedException> { store.write(uri, header, emptyFlow()) }
      assertThat(store.read(reference).toList().last()).isEqualTo(completed)
    }

  @Test
  fun `read rejects a replaced manifest instead of substituting its latest generation`() =
    runBlocking<Unit> {
      val storage = InMemoryStorageClient()
      val store = OutputManifestStore(storage, "gs://bucket/output")
      val original = store.write(uri, header, flowOf(completed))
      MesosRecordIoStorageClient(storage)
        .writeBlob("output/$attempt/manifest", flowOf(completed.toByteString()))
      assertFailsWith<IllegalStateException> { store.read(original).toList() }
    }

  @Test
  fun `read rejects a manifest whose footer is missing`() =
    runBlocking<Unit> {
      val storage = InMemoryStorageClient()
      val store = OutputManifestStore(storage, "gs://bucket/output")
      store.write(uri, header, flowOf(completed))
      val recordIo = MesosRecordIoStorageClient(storage)
      val all = checkNotNull(recordIo.getBlob("output/$attempt/manifest")).read().toList()
      val truncated =
        recordIo.writeBlob("output/$attempt/manifest", flowOf(*all.dropLast(1).toTypedArray()))
      assertFailsWith<IllegalStateException> {
        store.read(store.reference(uri, truncated)).toList()
      }
    }

  @Test
  fun `read rejects reordered records even when its object generation matches`() =
    runBlocking<Unit> {
      val storage = InMemoryStorageClient()
      val store = OutputManifestStore(storage, "gs://bucket/output")
      store.write(uri, header, flowOf(completed))
      val recordIo = MesosRecordIoStorageClient(storage)
      val all = checkNotNull(recordIo.getBlob("output/$attempt/manifest")).read().toList()
      val malformed = recordIo.writeBlob("output/$attempt/manifest", flowOf(all[1], all[0], all[2]))
      assertFailsWith<IllegalStateException> {
        store.read(store.reference(uri, malformed)).toList()
      }
    }

  @Test
  fun `readBlob rejects output in a different configured root`() =
    runBlocking<Unit> {
      val store = OutputManifestStore(InMemoryStorageClient(), "gs://bucket/output")
      assertFailsWith<IllegalStateException> {
        store.readBlob(
          outputBlobReference {
            blobUri = "gs://other/output/data"
            blobGeneration = 1
          }
        )
      }
    }
}
