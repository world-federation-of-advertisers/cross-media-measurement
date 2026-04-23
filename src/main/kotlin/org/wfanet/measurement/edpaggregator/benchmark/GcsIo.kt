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

package org.wfanet.measurement.edpaggregator.benchmark

import com.google.cloud.storage.BlobId
import com.google.cloud.storage.BlobInfo
import com.google.cloud.storage.Storage
import com.google.cloud.storage.StorageOptions
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.nio.ByteBuffer
import org.wfanet.virtualpeople.common.LabelerInput
import org.wfanet.virtualpeople.common.LabelerOutput

/** Number of impressions per GCS file. */
const val IMPRESSIONS_PER_FILE = 100_000

class GcsIo(private val bucket: String) {
  private val storage: Storage = StorageOptions.getDefaultInstance().service

  fun writeImpressions(path: String, impressions: List<LabelerInput>) {
    val baos = ByteArrayOutputStream()
    for (impression in impressions) {
      impression.writeDelimitedTo(baos)
    }
    val blobInfo = BlobInfo.newBuilder(BlobId.of(bucket, path)).build()
    storage.create(blobInfo, baos.toByteArray())
  }

  fun readImpressions(path: String): List<LabelerInput> {
    val blobId = BlobId.of(bucket, path)
    val content = storage.readAllBytes(blobId)
    val inputStream = ByteArrayInputStream(content)
    val result = ArrayList<LabelerInput>(IMPRESSIONS_PER_FILE)
    while (inputStream.available() > 0) {
      val input = LabelerInput.parseDelimitedFrom(inputStream) ?: break
      result.add(input)
    }
    return result
  }

  fun readRawBytes(path: String): ByteArray {
    return storage.readAllBytes(BlobId.of(bucket, path))
  }

  fun parseImpressionsFromBytes(rawBytes: ByteArray): List<LabelerInput> {
    val inputStream = ByteArrayInputStream(rawBytes)
    val result = ArrayList<LabelerInput>(IMPRESSIONS_PER_FILE)
    while (inputStream.available() > 0) {
      val input = LabelerInput.parseDelimitedFrom(inputStream) ?: break
      result.add(input)
    }
    return result
  }

  fun writeLabeledEvents(path: String, outputs: List<LabelerOutput>) {
    val baos = ByteArrayOutputStream()
    for (output in outputs) {
      output.writeDelimitedTo(baos)
    }
    val blobInfo = BlobInfo.newBuilder(BlobId.of(bucket, path)).build()
    storage.create(blobInfo, baos.toByteArray())
  }

  fun writeLabeledEventsRaw(path: String, delimitedOutputs: List<ByteArray>) {
    val blobInfo = BlobInfo.newBuilder(BlobId.of(bucket, path)).build()
    storage.writer(blobInfo).use { writer ->
      for (bytes in delimitedOutputs) {
        writer.write(ByteBuffer.wrap(bytes))
      }
    }
  }

  fun writeBytes(path: String, data: ByteArray) {
    val blobInfo = BlobInfo.newBuilder(BlobId.of(bucket, path)).build()
    storage.create(blobInfo, data)
  }

  /**
   * Opens a streaming writer to a GCS blob. Caller must close the writer.
   * Writes are buffered internally (~15MB chunks), so small writes are efficient.
   */
  fun withWriter(path: String, block: (java.nio.channels.WritableByteChannel) -> Unit) {
    val blobInfo = BlobInfo.newBuilder(BlobId.of(bucket, path)).build()
    storage.writer(blobInfo).use { writer -> block(writer) }
  }

  fun listBlobs(prefix: String): List<String> {
    val blobs = storage.list(bucket, Storage.BlobListOption.prefix(prefix))
    return blobs.iterateAll().map { it.name }
  }

  fun listBlobsWithSize(prefix: String): List<Pair<String, Long>> {
    val blobs = storage.list(bucket, Storage.BlobListOption.prefix(prefix))
    return blobs.iterateAll().map { it.name to it.size }
  }

  fun deletePrefix(prefix: String) {
    val blobs = storage.list(bucket, Storage.BlobListOption.prefix(prefix))
    for (blob in blobs.iterateAll()) {
      blob.delete()
    }
  }
}
