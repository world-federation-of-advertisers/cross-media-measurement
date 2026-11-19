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

package org.wfanet.measurement.edpaggregator.vidlabeler

import java.io.Closeable
import java.nio.file.Files
import java.time.LocalDate
import java.util.UUID
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.flowOn
import org.wfanet.measurement.edpaggregator.rawimpressions.OutputManifestStore
import org.wfanet.measurement.edpaggregator.rawimpressions.ParquetRawEvent
import org.wfanet.measurement.edpaggregator.rawimpressions.RawImpressionSource
import org.wfanet.measurement.edpaggregator.v1alpha.OutputBlobReference
import org.wfanet.measurement.edpaggregator.v1alpha.OutputManifestRecord
import org.wfanet.measurement.edpaggregator.v1alpha.OutputManifestRecordKt
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadFile
import org.wfanet.measurement.edpaggregator.v1alpha.outputManifestRecord
import org.wfanet.measurement.storage.ConditionalOperationStorageClient
import org.wfanet.measurement.storage.SelectedStorageClient

/** One private output namespace and a bounded-memory spool of its completed file scopes. */
class LabelingOutputAttempt(
  private val storage: ConditionalOperationStorageClient,
  private val rootUri: String,
  private val job: String,
  private val models: List<String>,
  val attemptId: String = UUID.randomUUID().toString(),
) : Closeable {
  private val upload = job.substringBeforeLast("/vidLabelingJobs/")
  private val store = OutputManifestStore(storage, rootUri)
  private val spool = Files.createTempFile("vid-labeling-output-", ".records")
  private val output = Files.newOutputStream(spool).buffered()
  private var sealed = false

  init {
    UUID.fromString(attemptId)
    require(job.startsWith(upload + "/vidLabelingJobs/"))
  }

  /** Includes file resource identity so two registered inputs sharing a URI never collide. */
  fun path(file: String): String {
    require(file.startsWith(upload + "/files/"))
    return upload.substringAfterLast('/') +
      "/" +
      job.substringAfterLast('/') +
      "/" +
      file.substringAfterLast('/') +
      "/" +
      attemptId
  }

  /** Appends output references only after this real input sink has successfully committed. */
  fun <E : ParquetRawEvent> wrap(
    delegate: RawImpressionSource.BlobSink<E>,
    file: RawImpressionUploadFile,
    eventDate: LocalDate,
  ): RawImpressionSource.BlobSink<E> =
    object : RawImpressionSource.BlobSink<E> {
      override suspend fun processBatch(events: List<E>) = delegate.processBatch(events)

      override suspend fun close() = delegate.close()

      override suspend fun commit() {
        delegate.commit()
        check(
          file.hasEventDate() &&
            file.eventDate.year == eventDate.year &&
            file.eventDate.month == eventDate.monthValue &&
            file.eventDate.day == eventDate.dayOfMonth
        ) {
          "Raw-impression footer date differs from its registered event date"
        }
        for (model in models) {
          val uri =
            LabeledImpressionsBlobKeys.forAttemptInputUri(
              rootUri,
              file.blobUri,
              model,
              eventDate,
              path(file.name),
            )
          val key = SelectedStorageClient.parseBlobUri(uri).key
          val data = storage.getBlob(key)
          val metadata = storage.getBlob(key + ".metadata.binpb")
          check((data == null) == (metadata == null)) { "Incomplete labeled output pair" }
          if (data != null && metadata != null) {
            append(
              outputManifestRecord {
                labeledBlob =
                  OutputManifestRecordKt.labeledBlob {
                    rawImpressionUploadFile = file.name
                    modelLine = model
                    this.eventDate = file.eventDate
                    impressions = store.reference(uri, data)
                    this.metadata = store.reference(uri + ".metadata.binpb", metadata)
                  }
              }
            )
          }
          append(
            outputManifestRecord {
              fileCompleted =
                OutputManifestRecordKt.fileCompleted {
                  rawImpressionUploadFile = file.name
                  modelLine = model
                }
            }
          )
        }
      }
    }

  private fun append(record: OutputManifestRecord) =
    synchronized(output) {
      check(!sealed) { "Output attempt is sealed" }
      record.writeDelimitedTo(output)
    }

  /** Uploads the complete manifest once labeling has returned successfully for every input. */
  suspend fun publish(): OutputBlobReference {
    synchronized(output) {
      check(!sealed)
      sealed = true
      output.close()
    }
    val uri =
      rootUri.trimEnd('/') +
        "/.attempts/" +
        upload.substringAfterLast('/') +
        "/" +
        job.substringAfterLast('/') +
        "/" +
        attemptId +
        "/manifest"
    return store.write(
      uri,
      OutputManifestRecordKt.header {
        phase = OutputManifestRecord.Header.Phase.VID_LABELING
        rawImpressionUpload = upload
        owner = job
        modelLines += models
        this.attemptId = this@LabelingOutputAttempt.attemptId
      },
      flow {
          Files.newInputStream(spool).buffered().use { input ->
            while (true) {
              val record = OutputManifestRecord.parseDelimitedFrom(input) ?: break
              emit(record)
            }
          }
        }
        .flowOn(Dispatchers.IO),
    )
  }

  override fun close() {
    output.close()
    Files.deleteIfExists(spool)
  }
}
