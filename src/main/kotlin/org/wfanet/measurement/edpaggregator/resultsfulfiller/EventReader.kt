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
import com.google.type.Interval
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.map
import org.wfanet.measurement.common.flatten
import org.wfanet.measurement.common.toInstant
import org.wfanet.measurement.edpaggregator.v1alpha.BlobDetails
import org.wfanet.measurement.edpaggregator.v1alpha.LabeledImpression
import org.wfanet.measurement.storage.BlobUri
import org.wfanet.measurement.storage.MesosRecordIoStorageClient
import org.wfanet.measurement.storage.SelectedStorageClient
import org.wfanet.measurement.storage.StorageClient
import org.wfanet.measurement.common.crypto.tink.withEnvelopeEncryption

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
  private val labeledImpressionsDekPrefix: String
) {
  /**
   * Retrieves a flow of labeled impressions for a given collection interval and event group ID.
   *
   * @param collectionInterval The time interval for collection
   * @param eventGroupId The ID of the event group
   * @return A flow of labeled impressions
   */
  suspend fun getLabeledImpressionsFlow(
    collectionInterval: Interval,
    eventGroupId: String
  ): Flow<LabeledImpression> {
    val blobDetails = getBlobDetails(collectionInterval, eventGroupId)
    return getLabeledImpressions(blobDetails)
  }

  /**
   * Retrieves blob details for a given collection interval and event group ID.
   *
   * @param collectionInterval The time interval for collection
   * @param eventGroupId The ID of the event group
   * @return The blob details with the DEK
   */
  private suspend fun getBlobDetails(
    collectionInterval: Interval,
    eventGroupId: String
  ): BlobDetails {
    val ds = collectionInterval.startTime.toInstant().toString()
    val dekBlobKey = "ds/$ds/event-group-id/$eventGroupId/metadata"
    val dekBlobUri = "$labeledImpressionsDekPrefix/$dekBlobKey"
    val dekStorageClientUri = SelectedStorageClient.parseBlobUri(dekBlobUri)
    val impressionsDekStorageClient = createStorageClient(dekStorageClientUri, impressionDekStorageConfig)
    // Get EncryptedDek message from storage using the blobKey made up of the ds and eventGroupId
    return BlobDetails.parseFrom(impressionsDekStorageClient.getBlob(dekBlobKey)!!.read().flatten())
  }

  /**
   * Retrieves labeled impressions from blob details.
   *
   * @param blobDetails The blob details
   * @return A flow of labeled impressions
   */
  private suspend fun getLabeledImpressions(blobDetails: BlobDetails): Flow<LabeledImpression> {
    // Get blob URI from encrypted DEK
    val storageClientUri = SelectedStorageClient.parseBlobUri(blobDetails.blobUri)

    // Create and configure storage client with encryption
    val encryptedDek = blobDetails.encryptedDek
    val encryptedImpressionsClient = createStorageClient(storageClientUri, impressionsStorageConfig)
    val impressionsAeadStorageClient = encryptedImpressionsClient.withEnvelopeEncryption(
      kmsClient,
      encryptedDek.kekUri,
      encryptedDek.encryptedDek
    )

    // Access blob storage
    val impressionsMesosStorage = MesosRecordIoStorageClient(impressionsAeadStorageClient)
    val impressionBlob = impressionsMesosStorage.getBlob(storageClientUri.key)
      ?: throw IllegalStateException("Could not retrieve impression blob from ${storageClientUri.key}")

    // Parse raw data into LabeledImpression objects
    return impressionBlob.read().map { impressionByteString ->
      LabeledImpression.parseFrom(impressionByteString)
        ?: throw IllegalStateException("Failed to parse LabeledImpression from bytes")
    }
  }

  /**
   * Creates a storage client based on the provided URI and configuration.
   */
  private fun createStorageClient(blobUri: BlobUri, storageConfig: StorageConfig): StorageClient {
    return SelectedStorageClient(blobUri, storageConfig.rootDirectory, storageConfig.projectId)
  }
}
