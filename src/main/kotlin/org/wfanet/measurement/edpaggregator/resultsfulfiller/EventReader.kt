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
import kotlinx.coroutines.flow.map
import org.wfanet.measurement.common.flatten
import org.wfanet.measurement.common.toInstant
import org.wfanet.measurement.edpaggregator.EncryptedStorage
import org.wfanet.measurement.edpaggregator.StorageConfig
import org.wfanet.measurement.edpaggregator.v1alpha.BlobDetails
import org.wfanet.measurement.edpaggregator.v1alpha.LabeledImpression
import org.wfanet.measurement.loadtest.dataprovider.LabeledEvent
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
   * Retrieves labeled events from blob details.
   *
   * @param blobDetails The blob details with the DEK
   * @return A flow of labeled events
   */
  private suspend fun getLabeledEvents(blobDetails: BlobDetails): Flow<LabeledEvent<Message>> {
    var impressionCount = 0
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
    return impressionBlob.read().map { impressionByteString ->
      val labeledImpression = LabeledImpression.parseFrom(impressionByteString)
        ?: throw ImpressionReadException(
          storageClientUri.key,
          ImpressionReadException.Code.INVALID_FORMAT,
        )

      // Convert LabeledImpression to LabeledEvent
      val eventTypeUrl = labeledImpression.event.typeUrl
      val descriptor = typeRegistry.getDescriptorForTypeUrl(eventTypeUrl)
        ?: throw ImpressionReadException(
          storageClientUri.key,
          ImpressionReadException.Code.INVALID_FORMAT,
          "Unknown event type: $eventTypeUrl"
        )

      val eventMessage = DynamicMessage.parseFrom(descriptor, labeledImpression.event.value)

      impressionCount++
      if (impressionCount % 100000 == 0) {
        logger.info("Processed $impressionCount impressions, running GC")
        System.gc()
      }

      LabeledEvent(
        timestamp = labeledImpression.eventTime.toInstant(),
        vid = labeledImpression.vid,
        message = eventMessage
      )
    }
  }

  companion object {
    private val logger: Logger = Logger.getLogger(this::class.java.name)
  }
}
