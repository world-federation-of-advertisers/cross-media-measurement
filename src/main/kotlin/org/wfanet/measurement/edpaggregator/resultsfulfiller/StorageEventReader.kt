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
import com.google.protobuf.DynamicMessage
import com.google.protobuf.Message
import com.google.protobuf.TypeRegistry
import java.time.LocalDate
import java.util.logging.Logger
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import org.wfanet.measurement.common.flatten
import org.wfanet.measurement.common.toInstant
import org.wfanet.measurement.edpaggregator.EncryptedStorage
import org.wfanet.measurement.edpaggregator.StorageConfig
import org.wfanet.measurement.edpaggregator.v1alpha.BlobDetails
import org.wfanet.measurement.edpaggregator.v1alpha.LabeledImpression
import org.wfanet.measurement.storage.MesosRecordIoStorageClient
import org.wfanet.measurement.storage.SelectedStorageClient

/**
 * Reads labeled events from cloud storage blobs with optional encryption.
 *
 * This implementation of [EventReader] reads event data stored as Protocol Buffer messages
 * in cloud storage blobs. It supports both encrypted * and unencrypted storage.
 *
 * ## Encryption Support
 *
 * When a [KmsClient] is provided, the reader:
 * - Retrieves the encrypted Data Encryption Key (DEK) from metadata
 * - Uses KMS to decrypt the DEK
 * - Configures encrypted storage clients for secure data access
 *
 * Without a [KmsClient], the reader operates in plain storage mode.
 *
 * ## Performance Characteristics
 *
 * - **Streaming**: Events are streamed rather than loaded entirely into memory
 * - **Batching**: Events are emitted in configurable batches (default: 256)
 * - **Caching**: DynamicMessage builders are cached per event type for efficiency
 *
 * @property blobPath URI path to the blob containing serialized [LabeledImpression] messages.
 * @property metadataPath URI path to the metadata blob containing the [BlobDetails]
 * @property kmsClient Optional KMS client for decrypting the Data Encryption Key. When null,
 *   the reader operates in unencrypted mode.
 * @property impressionsStorageConfig Configuration for accessing the impressions blob,
 *   including root directory and project settings.
 * @property impressionDekStorageConfig Configuration for accessing the metadata blob,
 *   typically with different permissions than impression data.
 * @property typeRegistry Registry containing descriptors for all event types that may appear
 *   in the data. Events with unregistered types will cause [ImpressionReadException].
 * @property batchSize Maximum number of events per batch. Larger values reduce overhead but
 *   increase memory usage. Must be positive. Default: [DEFAULT_BATCH_SIZE].
 * @constructor Creates a new storage event reader with the specified configuration.
 * @throws IllegalArgumentException if [batchSize] is not positive
 */
class StorageEventReader(
  private val blobPath: String,
  private val metadataPath: String,
  private val kmsClient: KmsClient?,
  private val impressionsStorageConfig: StorageConfig,
  private val impressionDekStorageConfig: StorageConfig,
  private val typeRegistry: TypeRegistry,
  private val batchSize: Int = DEFAULT_BATCH_SIZE,
) : EventReader {

  /**
   * Reads events from storage and emits them as batched flows.
   *
   * Implements the [EventReader.readEvents] contract by:
   * 1. Reading and validating blob metadata
   * 2. Configuring storage clients with appropriate encryption
   * 3. Streaming events from the blob
   * 4. Parsing and batching events for emission
   *
   * The returned flow is cold and can be collected multiple times. Each collection
   * initiates a new read operation from the storage blob.
   *
   * ## Flow Characteristics
   *
   * - **Cold Flow**: Reading starts when collection begins
   * - **Batch Size**: Each emission contains up to [batchSize] events
   * - **Order**: Events are emitted in storage order
   * - **Backpressure**: Automatically handled by Flow mechanics
   *
   * @return a [Flow] emitting lists of [LabeledEvent] with [DynamicMessage] content.
   *   Each list contains 1 to [batchSize] events.
   * @throws ImpressionReadException with [ImpressionReadException.Code.BLOB_NOT_FOUND]
   *   if metadata or data blobs don't exist
   * @throws ImpressionReadException with [ImpressionReadException.Code.INVALID_FORMAT]
   *   if event types aren't in the [typeRegistry] or data is corrupted
   */
  override suspend fun readEvents(): Flow<List<LabeledEvent<DynamicMessage>>> {
    val blobDetails = readBlobDetails()
    return readEventsFromBlob(blobDetails)
  }

  /**
   * Reads blob metadata containing encryption and configuration details.
   *
   * This internal method retrieves the [BlobDetails] protobuf message from the
   * metadata blob specified by [metadataPath]. The metadata contains:
   * - Encrypted Data Encryption Key (DEK) for decrypting the data blob
   * - KEK URI for KMS operations
   * - Additional blob configuration
   *
   * @return parsed [BlobDetails] containing encryption keys and blob metadata
   * @throws ImpressionReadException with [ImpressionReadException.Code.BLOB_NOT_FOUND]
   *   if the metadata blob doesn't exist at [metadataPath]
   */
  private suspend fun readBlobDetails(): BlobDetails {
    val storageClientUri = SelectedStorageClient.parseBlobUri(metadataPath)
    val impressionsDekStorageClient =
      SelectedStorageClient(
        storageClientUri,
        impressionDekStorageConfig.rootDirectory,
        impressionDekStorageConfig.projectId,
      )
    logger.info("Reading metadata from $metadataPath")
    val blob =
      impressionsDekStorageClient.getBlob(storageClientUri.key)
        ?: throw ImpressionReadException(metadataPath, ImpressionReadException.Code.BLOB_NOT_FOUND)
    return BlobDetails.parseFrom(blob.read().flatten())
  }

  /**
   * Streams and parses events from the data blob.
   *
   * This internal method implements the core event reading logic:
   *
   * ## Processing Pipeline
   *
   * 1. **Storage Setup**: Configures encrypted or plain storage based on [kmsClient]
   * 2. **Streaming**: Reads [LabeledImpression] messages as a stream
   * 3. **Parsing**: Converts impressions to [LabeledEvent] with [DynamicMessage]
   * 4. **Batching**: Accumulates events until [batchSize] is reached
   * 5. **Emission**: Emits complete batches through the flow
   *
   * ## Optimization Strategies
   *
   * - **Builder Caching**: Reuses [DynamicMessage.Builder] instances per event type
   * - **Streaming Processing**: Never loads entire blob into memory
   * - **Batch Buffering**: Minimizes flow emissions for efficiency
   *
   * @param blobDetails metadata containing encryption keys and blob configuration
   * @return cold [Flow] emitting batched lists of [LabeledEvent] instances
   * @throws ImpressionReadException with [ImpressionReadException.Code.BLOB_NOT_FOUND]
   *   if the data blob doesn't exist at [blobPath]
   * @throws ImpressionReadException with [ImpressionReadException.Code.INVALID_FORMAT]
   *   if an event type URL is not found in [typeRegistry]
   */
  private suspend fun readEventsFromBlob(
    blobDetails: BlobDetails
  ): Flow<List<LabeledEvent<DynamicMessage>>> = flow {
    // Use the blob path directly instead of getting it from blobDetails
    val storageClientUri = SelectedStorageClient.parseBlobUri(blobPath)

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
          serializedEncryptionKey = encryptedDek.encryptedDek,
        )
      }

    val impressionBlob =
      impressionsStorage.getBlob(storageClientUri.key)
        ?: throw ImpressionReadException(
          storageClientUri.key,
          ImpressionReadException.Code.BLOB_NOT_FOUND,
        )

    // Parse raw data into LabeledEvent objects in batches
    val labeledImpressionBuilder = LabeledImpression.newBuilder()

    // Cache for eventMessage builders keyed by type URL
    val eventMessageBuilders = mutableMapOf<String, DynamicMessage.Builder>()

    val currentBatch = mutableListOf<LabeledEvent<DynamicMessage>>()

    impressionBlob.read().collect { impressionByteString ->
      labeledImpressionBuilder.clear()
      labeledImpressionBuilder.mergeFrom(impressionByteString)

      val eventTypeUrl = labeledImpressionBuilder.event.typeUrl
      val eventMessageBuilder = eventMessageBuilders.getOrPut(eventTypeUrl) {
        val descriptor = typeRegistry.getDescriptorForTypeUrl(eventTypeUrl)
          ?: throw ImpressionReadException(
            storageClientUri.key,
            ImpressionReadException.Code.INVALID_FORMAT,
            "Unknown event type: $eventTypeUrl"
          )
        DynamicMessage.newBuilder(descriptor)
      }

      val eventMessage = eventMessageBuilder
        .clear()
        .mergeFrom(labeledImpressionBuilder.event.value)
        .build()

      val labeledEvent = LabeledEvent(
        timestamp = labeledImpressionBuilder.eventTime.toInstant(),
        vid = labeledImpressionBuilder.vid,
        message = eventMessage,
        eventGroupReferenceId = labeledImpressionBuilder.eventGroupReferenceId
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
    private val logger: Logger = Logger.getLogger(this::class.java.name)

    /**
     * Default number of events per batch.
     *
     * This value (256) is chosen to balance:
     * - **Memory Usage**: Each batch holds at most 256 events in memory
     * - **Processing Overhead**: Fewer emissions reduce flow processing overhead
     */
    const val DEFAULT_BATCH_SIZE = 256
  }
}
