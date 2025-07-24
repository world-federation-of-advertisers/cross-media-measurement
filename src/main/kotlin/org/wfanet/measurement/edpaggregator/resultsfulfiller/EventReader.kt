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
import kotlinx.coroutines.flow.map
import org.wfanet.measurement.common.flatten
import org.wfanet.measurement.common.toInstant
import org.wfanet.measurement.edpaggregator.EncryptedStorage
import org.wfanet.measurement.edpaggregator.StorageConfig
import org.wfanet.measurement.edpaggregator.v1alpha.BlobDetails
import org.wfanet.measurement.edpaggregator.v1alpha.LabeledImpression
import org.wfanet.measurement.storage.MesosRecordIoStorageClient
import org.wfanet.measurement.storage.SelectedStorageClient

/**
 * Reads labeled impressions from storage.
 *
 * @param kmsClient The KMS client for encryption operations
 * @param impressionsStorageConfig Configuration for impressions storage
 * @param impressionDekStorageConfig Configuration for impression DEK storage
 * @param labeledImpressionsDekPrefix Prefix for labeled impressions DEK
 * @param typeRegistry Type registry for parsing protobuf messages
 */
class EventReader(
  private val kmsClient: KmsClient?,
  private val impressionsStorageConfig: StorageConfig,
  private val impressionDekStorageConfig: StorageConfig,
  private val labeledImpressionsDekPrefix: String,
  private val typeRegistry: TypeRegistry,
  private val defaultBatchSize: Int = DEFAULT_BATCH_SIZE,
) {
  /**
   * Retrieves a flow of labeled events for a given ds and event group ID.
   *
   * @param ds The ds for the labeled events
   * @param eventGroupReferenceId The event group reference ID of the event group
   * @return A flow of labeled events
   */
  suspend fun getLabeledEvents(
    ds: LocalDate,
    eventGroupReferenceId: String,
  ): Flow<LabeledEvent<Message>> {
    val blobDetails = getBlobDetails(ds, eventGroupReferenceId)
    return getLabeledEvents(blobDetails)
  }

  /**
   * Retrieves a flow of batched labeled events for a given ds and event group ID.
   *
   * @param ds The ds for the labeled events
   * @param eventGroupReferenceId The event group reference ID of the event group
   * @param batchSize The size of each batch (defaults to defaultBatchSize)
   * @return A flow of lists of labeled events
   */
  suspend fun getLabeledEventsBatched(
    ds: LocalDate,
    eventGroupReferenceId: String,
    batchSize: Int = defaultBatchSize,
  ): Flow<List<LabeledEvent<out Message>>> {
    val blobDetails = getBlobDetails(ds, eventGroupReferenceId)
    return getLabeledEventsBatched(blobDetails, batchSize)
  }

  /**
   * Retrieves blob details for a given ds and event group ID.
   *
   * @param ds The ds of the encrypted dek
   * @param eventGroupId The ID of the event group
   * @return The blob details with the DEK
   */
  private suspend fun getBlobDetails(ds: LocalDate, eventGroupReferenceId: String): BlobDetails {
    val dekBlobKey = "ds/$ds/event-group-reference-id/$eventGroupReferenceId/metadata"
    val dekBlobUri = "$labeledImpressionsDekPrefix/$dekBlobKey"
    val storageClientUri = SelectedStorageClient.parseBlobUri(dekBlobUri)
    val impressionsDekStorageClient =
      SelectedStorageClient(
        storageClientUri,
        impressionDekStorageConfig.rootDirectory,
        impressionDekStorageConfig.projectId,
      )
    // Get EncryptedDek message from storage using the blobKey made up of the ds and eventGroupId
    logger.info("Reading blob $dekBlobKey")
    val blob =
      impressionsDekStorageClient.getBlob(dekBlobKey)
        ?: throw ImpressionReadException(dekBlobKey, ImpressionReadException.Code.BLOB_NOT_FOUND)
    return BlobDetails.parseFrom(blob.read().flatten())
  }

  /**
   * Retrieves labeled events from blob details in batches.
   *
   * @param blobDetails The blob details with the DEK
   * @param batchSize The size of each batch
   * @return A flow of lists of labeled events
   */
  private suspend fun getLabeledEventsBatched(
    blobDetails: BlobDetails,
    batchSize: Int
  ): Flow<List<LabeledEvent<out Message>>> = flow {
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
    
    // Reusable DynamicMessage builder and descriptor - assuming all events have the same type
    var eventMessageBuilder: DynamicMessage.Builder? = null
    var descriptor: com.google.protobuf.Descriptors.Descriptor? = null
    
    val currentBatch = mutableListOf<LabeledEvent<out Message>>()
    
    impressionBlob.read().collect { impressionByteString ->
      labeledImpressionBuilder.clear()
      labeledImpressionBuilder.mergeFrom(impressionByteString)

      // Initialize the reusable builder and cache descriptor on first use
      if (eventMessageBuilder == null) {
        val eventTypeUrl = labeledImpressionBuilder.event.typeUrl
        descriptor = typeRegistry.getDescriptorForTypeUrl(eventTypeUrl)
          ?: throw ImpressionReadException(
            storageClientUri.key,
            ImpressionReadException.Code.INVALID_FORMAT,
            "Unknown event type: $eventTypeUrl"
          )
        eventMessageBuilder = DynamicMessage.newBuilder(descriptor)
      }

      val eventMessage = eventMessageBuilder!!
        .clear()
        .mergeFrom(labeledImpressionBuilder.event.value)
        .build()

      val labeledEvent = LabeledEvent(
        timestamp = labeledImpressionBuilder.eventTime.toInstant(),
        vid = labeledImpressionBuilder.vid,
        message = eventMessage
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

  /**
   * Retrieves labeled events from blob details.
   *
   * @param blobDetails The blob details with the DEK
   * @return A flow of labeled events
   */
  private suspend fun getLabeledEvents(blobDetails: BlobDetails): Flow<LabeledEvent<Message>> {
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
          serializedEncryptionKey = encryptedDek.encryptedDek,
        )
      }

    val impressionBlob =
      impressionsStorage.getBlob(storageClientUri.key)
        ?: throw ImpressionReadException(
          storageClientUri.key,
          ImpressionReadException.Code.BLOB_NOT_FOUND,
        )

    // Parse raw data into LabeledEvent objects
    val labeledImpressionBuilder = LabeledImpression.newBuilder()
    
    // Reusable DynamicMessage builder and descriptor - assuming all events have the same type
    var eventMessageBuilder: DynamicMessage.Builder? = null
    var descriptor: com.google.protobuf.Descriptors.Descriptor? = null
    
    return impressionBlob.read().map { impressionByteString ->
      labeledImpressionBuilder.clear()
      labeledImpressionBuilder.mergeFrom(impressionByteString)

      // Initialize the reusable builder and cache descriptor on first use
      if (eventMessageBuilder == null) {
        val eventTypeUrl = labeledImpressionBuilder.event.typeUrl
        descriptor = typeRegistry.getDescriptorForTypeUrl(eventTypeUrl)
          ?: throw ImpressionReadException(
            storageClientUri.key,
            ImpressionReadException.Code.INVALID_FORMAT,
            "Unknown event type: $eventTypeUrl"
          )
        eventMessageBuilder = DynamicMessage.newBuilder(descriptor)
      }

      val eventMessage = eventMessageBuilder!!
        .clear()
        .mergeFrom(labeledImpressionBuilder.event.value)
        .build()

      LabeledEvent(
        timestamp = labeledImpressionBuilder.eventTime.toInstant(),
        vid = labeledImpressionBuilder.vid,
        message = eventMessage
      )
    }
  }

  companion object {
    private val logger: Logger = Logger.getLogger(this::class.java.name)
    const val DEFAULT_BATCH_SIZE = 256
  }
}
