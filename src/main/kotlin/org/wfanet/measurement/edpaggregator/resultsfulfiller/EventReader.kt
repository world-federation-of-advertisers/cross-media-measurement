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
import java.time.LocalDate
import java.util.logging.Logger
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.map
import org.wfanet.measurement.common.flatten
import org.wfanet.measurement.edpaggregator.EncryptedStorage
import org.wfanet.measurement.edpaggregator.StorageConfig
import org.wfanet.measurement.edpaggregator.v1alpha.BlobDetails
import org.wfanet.measurement.edpaggregator.v1alpha.LabeledImpression
import org.wfanet.measurement.storage.SelectedStorageClient

/**
 * Reads labeled impressions from storage.
 *
 * @param kmsClient The KMS client for encryption operations
 * @param impressionsStorageConfig Configuration for impressions storage
 * @param impressionDekStorageConfig Configuration for impression DEK storage
 * @param labeledImpressionsDekPrefix Prefix for labeled impressions DEK
 */
class EventReader(
  private val kmsClient: KmsClient,
  private val impressionsStorageConfig: StorageConfig,
  private val impressionDekStorageConfig: StorageConfig,
  private val labeledImpressionsDekPrefix: String,
) {
  /**
   * Retrieves a flow of labeled impressions for a given ds and event group ID.
   *
   * @param ds The ds for the labeled impressions
   * @param eventGroupReferenceId The event group reference ID of the event group
   * @return A flow of labeled impressions
   */
  suspend fun getLabeledImpressions(
    ds: LocalDate,
    eventGroupReferenceId: String,
  ): Flow<LabeledImpression> {
    val blobDetails = getBlobDetails(ds, eventGroupReferenceId)
    return getLabeledImpressions(blobDetails)
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
   * Retrieves labeled impressions from blob details.
   *
   * @param blobDetails The blob details with the DEK
   * @return A flow of labeled impressions
   */
  private suspend fun getLabeledImpressions(blobDetails: BlobDetails): Flow<LabeledImpression> {
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
      EncryptedStorage.buildEncryptedMesosStorageClient(
        selectedStorageClient,
        kekUri = encryptedDek.kekUri,
        kmsClient = kmsClient,
        serializedEncryptionKey = encryptedDek.encryptedDek,
      )
    val impressionBlob =
      impressionsStorage.getBlob(storageClientUri.key)
        ?: throw ImpressionReadException(
          storageClientUri.key,
          ImpressionReadException.Code.BLOB_NOT_FOUND,
        )

    // Parse raw data into LabeledImpression objects
    return impressionBlob.read().map { impressionByteString ->
      LabeledImpression.parseFrom(impressionByteString)
        ?: throw ImpressionReadException(
          storageClientUri.key,
          ImpressionReadException.Code.INVALID_FORMAT,
        )
    }
  }

  companion object {
    private val logger: Logger = Logger.getLogger(this::class.java.name)
  }
}
