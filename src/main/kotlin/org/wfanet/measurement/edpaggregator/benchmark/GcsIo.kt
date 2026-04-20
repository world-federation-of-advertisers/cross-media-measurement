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
import org.wfanet.virtualpeople.common.LabelerInput
import org.wfanet.virtualpeople.common.LabelerOutput

/** Number of impressions per GCS file. */
const val IMPRESSIONS_PER_FILE = 100_000

class GcsIo(private val bucket: String) {
  private val storage: Storage = StorageOptions.getDefaultInstance().service

  /**
   * Writes a list of [LabelerInput] protos to a GCS blob as length-delimited protobuf. Overwrites
   * if the blob already exists.
   */
  fun writeImpressions(path: String, impressions: List<LabelerInput>) {
    val baos = ByteArrayOutputStream()
    for (impression in impressions) {
      impression.writeDelimitedTo(baos)
    }
    val blobInfo = BlobInfo.newBuilder(BlobId.of(bucket, path)).build()
    storage.create(blobInfo, baos.toByteArray())
  }

  /** Reads [LabelerInput] protos from a GCS blob containing length-delimited protobuf. */
  fun readImpressions(path: String): List<LabelerInput> {
    val blobId = BlobId.of(bucket, path)
    val content = storage.readAllBytes(blobId)
    val inputStream = ByteArrayInputStream(content)
    val result = mutableListOf<LabelerInput>()
    while (inputStream.available() > 0) {
      val input = LabelerInput.parseDelimitedFrom(inputStream) ?: break
      result.add(input)
    }
    return result
  }

  /** Writes a list of [LabelerOutput] protos to a GCS blob as length-delimited protobuf. */
  fun writeLabeledEvents(path: String, outputs: List<LabelerOutput>) {
    val baos = ByteArrayOutputStream()
    for (output in outputs) {
      output.writeDelimitedTo(baos)
    }
    val blobInfo = BlobInfo.newBuilder(BlobId.of(bucket, path)).build()
    storage.create(blobInfo, baos.toByteArray())
  }

  /**
   * Writes pre-serialized length-delimited proto bytes to a GCS blob. Each entry in
   * [delimitedOutputs] is already in writeDelimitedTo format (varint length + proto bytes).
   */
  fun writeLabeledEventsRaw(path: String, delimitedOutputs: List<ByteArray>) {
    val blobInfo = BlobInfo.newBuilder(BlobId.of(bucket, path)).build()
    storage.writer(blobInfo).use { writer ->
      for (bytes in delimitedOutputs) {
        writer.write(java.nio.ByteBuffer.wrap(bytes))
      }
    }
  }

  /** Lists all blobs under a given prefix. */
  fun listBlobs(prefix: String): List<String> {
    val blobs = storage.list(bucket, Storage.BlobListOption.prefix(prefix))
    return blobs.iterateAll().map { it.name }
  }

  /** Deletes all blobs under a given prefix (for cleanup/overwrite). */
  fun deletePrefix(prefix: String) {
    val blobs = storage.list(bucket, Storage.BlobListOption.prefix(prefix))
    for (blob in blobs.iterateAll()) {
      blob.delete()
    }
  }
}
