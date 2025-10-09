/*
 * Copyright 2025 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.edpaggregator.resultsfulfiller

import com.google.crypto.tink.KmsClient
import com.google.protobuf.Descriptors
import com.google.protobuf.DynamicMessage
import com.google.protobuf.Message
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.buffer
import kotlinx.coroutines.flow.flow
import org.wfanet.measurement.common.toInstant
import org.wfanet.measurement.edpaggregator.EncryptedStorage
import org.wfanet.measurement.edpaggregator.StorageConfig
import org.wfanet.measurement.edpaggregator.v1alpha.BlobDetails
import org.wfanet.measurement.edpaggregator.v1alpha.LabeledImpression
import org.wfanet.measurement.storage.MesosRecordIoStorageClient
import org.wfanet.measurement.storage.SelectedStorageClient

/**
 * Reads labeled events from impression blobs in storage.
 *
 * Streams [LabeledImpression] records from a storage blob and parses them into [LabeledEvent]
 * messages by decoding the embedded event with the provided dynamic [descriptor]. When [kmsClient]
 * is present, data is decrypted using the DEK in [blobDetails].
 *
 * @property blobDetails metadata describing how to access and decrypt the blob.
 * @property kmsClient KMS client for DEK decryption. If null, reads unencrypted data.
 * @property impressionsStorageConfig configuration for accessing impression blobs.
 * @property descriptor protobuf descriptor for dynamic parsing of the embedded event.
 * @property batchSize maximum number of events per emitted batch.
 * @property bufferCapacity number of upstream records to prefetch and buffer between I/O and
 *   parsing.
 * @throws IllegalArgumentException if [batchSize] is not positive.
 */
class StorageEventReader(
  private val blobDetails: BlobDetails,
  private val kmsClient: KmsClient?,
  private val impressionsStorageConfig: StorageConfig,
  private val descriptor: Descriptors.Descriptor,
  private val batchSize: Int = DEFAULT_BATCH_SIZE,
  private val bufferCapacity: Int = DEFAULT_BUFFER_CAPACITY,
) : EventReader<Message> {

  /** Returns the underlying blob details. */
  fun getBlobDetails(): BlobDetails {
    return blobDetails
  }

  /**
   * Reads events from the configured blob and emits batches.
   *
   * The returned flow is cold. Each collection performs a fresh read of the underlying blob.
   *
   * @return flow of batches with up to [batchSize] events per batch.
   * @throws ImpressionReadException if the blob cannot be read or parsed.
   */
  override suspend fun readEvents(): Flow<List<LabeledEvent<Message>>> =
    readEventsFromBlob(blobDetails)

  /**
   * Streams and parses events from the data blob.
   *
   * This internal method implements the core event reading logic:
   *
   * ## Processing Pipeline
   * 1. **Storage Setup**: Configures encrypted or plain storage based on [kmsClient]
   * 2. **Streaming**: Reads [LabeledImpression] messages as a stream
   * 3. **Parsing**: Converts impressions to [LabeledEvent] using [descriptor]
   * 4. **Batching**: Accumulates events until [batchSize] is reached
   * 5. **Emission**: Emits complete batches through the flow
   *
   * @param blobDetails metadata containing encryption keys and blob configuration
   * @return cold [Flow] emitting batched lists of [LabeledEvent] instances
   * @throws ImpressionReadException with [ImpressionReadException.Code.BLOB_NOT_FOUND] if the data
   *   blob doesn't exist at [blobPath]
   * @throws ImpressionReadException with [ImpressionReadException.Code.INVALID_FORMAT] if an event
   *   type URL is not found in [typeRegistry]
   */
  private fun readEventsFromBlob(blobDetails: BlobDetails): Flow<List<LabeledEvent<Message>>> =
    flow {
      val storageClientUri = SelectedStorageClient.parseBlobUri(blobDetails.blobUri)

      // Create and configure storage client with encryption
      val encryptedDek = blobDetails.encryptedDek
      val selectedStorageClient =
        SelectedStorageClient(
          storageClientUri,
          impressionsStorageConfig.rootDirectory,
          impressionsStorageConfig.projectId,
        )

      val impressionsStorage =
        if (kmsClient == null) {
          MesosRecordIoStorageClient(selectedStorageClient)
        } else {
          EncryptedStorage.buildEncryptedMesosStorageClient(
            selectedStorageClient,
            kekUri = encryptedDek.kekUri,
            kmsClient = kmsClient,
            encryptedDek = encryptedDek,
          )
        }

      val impressionBlob =
        impressionsStorage.getBlob(storageClientUri.key)
          ?: throw ImpressionReadException(
            storageClientUri.key,
            ImpressionReadException.Code.BLOB_NOT_FOUND,
          )

      val currentBatch = mutableListOf<LabeledEvent<Message>>()

      impressionBlob.read().buffer(bufferCapacity).collect { impressionByteString ->
        val impression: LabeledImpression = LabeledImpression.parseFrom(impressionByteString)
        val eventMessage: Message = DynamicMessage.parseFrom(descriptor, impression.event.value)
        val labeledEvent =
          LabeledEvent(
            timestamp = impression.eventTime.toInstant(),
            vid = impression.vid,
            message = eventMessage,
          )

        currentBatch.add(labeledEvent)

        // Emit batch when it reaches the desired size
        if (currentBatch.size >= batchSize) {
          emit(currentBatch.toList())
          currentBatch.clear()
        }
      }

      // Emit any remaining events in the last batch
      if (currentBatch.isNotEmpty()) {
        emit(currentBatch.toList())
      }
    }

  companion object {
    /**
     * Default number of events per batch.
     *
     * This value (256) is chosen to balance:
     * - **Memory Usage**: Each batch holds at most 256 events in memory
     * - **Processing Overhead**: Fewer emissions reduce flow processing overhead
     */
    const val DEFAULT_BATCH_SIZE = 256

    /**
     * Default prefetch depth for the upstream read flow, in **records**.
     *
     * Used as the `capacity` argument to `Flow.buffer(capacity)`. This controls how far storage
     * reads may run ahead of parsing and batching, smoothing producer/consumer jitter.
     *
     * Assumptions:
     * - Same-region GCS, ~100 Gb/s effective link, RTT ≈ 2–5 ms.
     * - Average record size ≈ 1 KiB (payload; `buffer(capacity)` counts records, not bytes).
     *
     * Rationale: For small (~1 KiB) records over low RTT, a capacity around **16k records (~16
     * MiB)** keeps the pipe busy without excessive heap/GC churn. Throughput is driven mainly by
     * storage-layer concurrency; this constant only sets prefetch depth between I/O and parsing.
     *
     * Tuning guide (same region):
     *
     * |Scenario (RTT)  |Capacity (records)|~Buffered bytes|When to use                         |
     * |----------------|-----------------:|--------------:|------------------------------------|
     * |1–2 ms          |             8,192|         ~8 MiB|Very fast path, light parsing       |
     * |2–5 ms (default)|            16,384|        ~16 MiB|Typical same-region, balanced choice|
     * |5–10 ms         |            32,768|        ~32 MiB|Heavier parsing or bursty consumer  |
     */
    const val DEFAULT_BUFFER_CAPACITY = 16_384
  }
}
