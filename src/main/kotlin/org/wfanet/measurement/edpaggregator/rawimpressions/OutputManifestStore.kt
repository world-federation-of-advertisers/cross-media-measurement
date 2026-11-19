// Copyright 2025 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.edpaggregator.rawimpressions

import com.google.protobuf.ByteString
import java.security.MessageDigest
import java.util.UUID
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.flow
import org.wfanet.measurement.edpaggregator.v1alpha.OutputBlobReference
import org.wfanet.measurement.edpaggregator.v1alpha.OutputManifestRecord
import org.wfanet.measurement.edpaggregator.v1alpha.OutputManifestRecord.Header
import org.wfanet.measurement.edpaggregator.v1alpha.OutputManifestRecordKt
import org.wfanet.measurement.edpaggregator.v1alpha.outputBlobReference
import org.wfanet.measurement.edpaggregator.v1alpha.outputManifestRecord
import org.wfanet.measurement.storage.ConditionalOperationStorageClient
import org.wfanet.measurement.storage.MesosRecordIoStorageClient
import org.wfanet.measurement.storage.SelectedStorageClient
import org.wfanet.measurement.storage.StorageClient

/** Streams complete immutable manifests and reads only their recorded object versions. */
class OutputManifestStore(
  private val storageClient: ConditionalOperationStorageClient,
  storageRoot: String,
) {
  private val root = SelectedStorageClient.parseBlobUri(storageRoot)

  /** Creates one immutable manifest after all referenced output has become durable. */
  suspend fun write(
    blobUri: String,
    header: Header,
    records: Flow<OutputManifestRecord>,
  ): OutputBlobReference {
    validateHeader(header)
    val key = keyFor(blobUri)
    check(key.contains(header.attemptId)) { "Manifest path must contain its attempt identity" }
    val content = flow {
      val digest = MessageDigest.getInstance("SHA-256")
      var count = 0L
      suspend fun append(record: OutputManifestRecord) {
        val bytes = record.toByteString()
        digest.update(bytes.toByteArray())
        count++
        emit(bytes)
      }
      append(outputManifestRecord { this.header = header })
      records.collect { record ->
        check(!record.hasHeader() && !record.hasFooter()) { "Unexpected manifest boundary record" }
        validateRecord(record, header)
        append(record)
      }
      emit(
        outputManifestRecord {
            footer =
              OutputManifestRecordKt.footer {
                recordCount = count
                checksum = ByteString.copyFrom(digest.digest())
              }
          }
          .toByteString()
      )
    }
    val written = MesosRecordIoStorageClient(storageClient).writeBlobIfNotFound(key, content)
    return reference(blobUri, written)
  }

  /** Reads a manifest through its footer before the caller may select or publish its output. */
  fun read(reference: OutputBlobReference): Flow<OutputManifestRecord> = flow {
    val blob = readBlob(reference)
    val records =
      MesosRecordIoStorageClient(storageClient).getBlob(blob.blobKey)
        ?: error("Committed manifest is missing")
    check(
      (records as ConditionalOperationStorageClient.Blob).freshnessToken ==
        reference.blobGeneration.toString()
    ) {
      "Committed manifest generation changed"
    }
    val digest = MessageDigest.getInstance("SHA-256")
    var count = 0L
    var header: Header? = null
    var finished = false
    records.read().collect { bytes ->
      check(!finished) { "Manifest contains records after its footer" }
      val record = OutputManifestRecord.parseFrom(bytes)
      when {
        record.hasFooter() -> {
          check(
            header != null &&
              record.footer.recordCount == count &&
              record.footer.checksum == ByteString.copyFrom(digest.digest())
          ) {
            "Incomplete or corrupt manifest"
          }
          finished = true
        }
        record.hasHeader() -> {
          check(count == 0L) { "Manifest header is not first" }
          validateHeader(record.header)
          header = record.header
          digest.update(bytes.toByteArray())
          count++
          emit(record)
        }
        else -> {
          validateRecord(record, checkNotNull(header) { "Manifest header is missing" })
          digest.update(bytes.toByteArray())
          count++
          emit(record)
        }
      }
    }
    check(finished) { "Manifest completion proof is missing" }
  }

  /** Returns the exact recorded version, failing rather than substituting a different object. */
  suspend fun readBlob(reference: OutputBlobReference): StorageClient.Blob {
    check(reference.blobGeneration > 0L) { "Object generation must be positive" }
    val blob =
      storageClient.getBlob(keyFor(reference.blobUri))
        ?: error("Committed output object is missing")
    check(
      (blob as ConditionalOperationStorageClient.Blob).freshnessToken ==
        reference.blobGeneration.toString()
    ) {
      "Committed output object generation changed"
    }
    return blob
  }

  /** Captures the immutable version returned by a successful create. */
  fun reference(blobUri: String, blob: StorageClient.Blob): OutputBlobReference {
    check(blob.blobKey == keyFor(blobUri)) { "Object key does not match its URI" }
    val generation = (blob as ConditionalOperationStorageClient.Blob).freshnessToken.toLongOrNull()
    check(generation != null && generation > 0L) { "Object generation is not positive" }
    return outputBlobReference {
      this.blobUri = blobUri
      blobGeneration = generation
    }
  }

  private fun keyFor(blobUri: String): String {
    val parsed = SelectedStorageClient.parseBlobUri(blobUri)
    check(
      parsed.scheme == root.scheme &&
        parsed.bucket == root.bucket &&
        parsed.key.isNotEmpty() &&
        (root.key.isEmpty() || parsed.key.startsWith(root.key.trimEnd('/') + "/"))
    ) {
      "Output reference is outside its configured storage root"
    }
    return parsed.key
  }

  private fun validateHeader(header: Header) {
    check(
      header.phase != Header.Phase.PHASE_UNSPECIFIED && header.phase != Header.Phase.UNRECOGNIZED
    )
    check(
      header.rawImpressionUpload.isNotEmpty() &&
        header.owner.startsWith(header.rawImpressionUpload + "/")
    ) {
      "Manifest owner does not belong to its upload"
    }
    check(
      header.modelLinesList.isNotEmpty() &&
        header.modelLinesList.distinct().size == header.modelLinesCount
    )
    UUID.fromString(header.attemptId)
  }

  private fun validateRecord(record: OutputManifestRecord, header: Header) {
    when (record.bodyCase) {
      OutputManifestRecord.BodyCase.SUBPOOL_BLOB -> {
        check(
          header.phase == Header.Phase.POOL_ASSIGNMENT || header.phase == Header.Phase.POOL_MERGE
        )
        check(
          header.hasEncryptedDek() &&
            !header.encryptedDek.ciphertext.isEmpty &&
            header.encryptedDek.kekUri.isNotEmpty()
        ) {
          "Encrypted subpool output has no wrapped key"
        }
        check(record.subpoolBlob.poolOffset >= 0L)
        validateReference(record.subpoolBlob.blob, header)
      }
      OutputManifestRecord.BodyCase.LABELED_BLOB -> {
        check(header.phase == Header.Phase.VID_LABELING)
        check(
          record.labeledBlob.rawImpressionUploadFile.startsWith(
            header.rawImpressionUpload + "/rawImpressionUploadFiles/"
          )
        )
        check(record.labeledBlob.modelLine in header.modelLinesList)
        check(record.labeledBlob.hasEventDate())
        validateReference(record.labeledBlob.impressions, header)
        validateReference(record.labeledBlob.metadata, header)
        check(
          record.labeledBlob.metadata.blobUri ==
            record.labeledBlob.impressions.blobUri + ".metadata.binpb"
        )
      }
      OutputManifestRecord.BodyCase.FILE_COMPLETED -> {
        check(header.phase == Header.Phase.VID_LABELING)
        check(
          record.fileCompleted.rawImpressionUploadFile.startsWith(
            header.rawImpressionUpload + "/rawImpressionUploadFiles/"
          )
        )
        check(record.fileCompleted.modelLine in header.modelLinesList)
      }
      OutputManifestRecord.BodyCase.HEADER,
      OutputManifestRecord.BodyCase.FOOTER,
      OutputManifestRecord.BodyCase.BODY_NOT_SET -> error("Unexpected manifest record")
    }
  }

  private fun validateReference(reference: OutputBlobReference, header: Header) {
    check(reference.blobUri.contains(header.attemptId)) { "Output belongs to another attempt" }
    check(reference.blobGeneration > 0L)
    keyFor(reference.blobUri)
  }
}
