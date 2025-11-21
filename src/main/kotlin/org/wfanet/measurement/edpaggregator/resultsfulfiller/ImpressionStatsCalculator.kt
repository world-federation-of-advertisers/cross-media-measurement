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
import com.google.protobuf.InvalidProtocolBufferException
import it.unimi.dsi.fastutil.longs.LongOpenHashSet
import java.util.logging.Logger
import org.wfanet.measurement.edpaggregator.EncryptedStorage
import org.wfanet.measurement.edpaggregator.StorageConfig
import org.wfanet.measurement.edpaggregator.v1alpha.BlobDetails
import org.wfanet.measurement.edpaggregator.v1alpha.LabeledImpression
import org.wfanet.measurement.storage.MesosRecordIoStorageClient
import org.wfanet.measurement.storage.SelectedStorageClient

/** Per-blob statistics when scanning impression data. */
data class BlobImpressionStats(
  val metadataUri: String,
  val blobUri: String,
  val recordCount: Long,
  /** Number of distinct VIDs contributed by this blob relative to earlier blobs. */
  val newDistinctVids: Long,
)

/** Aggregated statistics across all processed impression blobs. */
data class ImpressionStats(
  val totalRecords: Long,
  val distinctVids: Long,
  val blobStats: List<BlobImpressionStats>,
)

/**
 * Scans impression blobs described by [BlobDetails] metadata and computes record/VID counts.
 *
 * The calculator deliberately skips filtering or batching logic; it reads each impression record
 * and tallies:
 * - Total records across all blobs
 * - Distinct VIDs across all blobs
 *
 * @param storageConfig storage configuration for accessing metadata and impression blobs.
 * @param kmsClient KMS client used to decrypt impression blobs. If null, blobs are read as
 *   plaintext.
 */
class ImpressionStatsCalculator(
  private val storageConfig: StorageConfig,
  private val kmsClient: KmsClient?,
) {

  /**
   * Computes statistics for the provided metadata URIs.
   *
   * @param metadataUris list of BlobDetails metadata URIs
   * @throws IllegalArgumentException if no metadata URIs are provided
   */
  suspend fun compute(metadataUris: Collection<String>): ImpressionStats {
    require(metadataUris.isNotEmpty()) { "At least one metadata URI is required" }

    logger.info("Scanning ${metadataUris.size} metadata blobs for impression stats")

    val globalVids = LongOpenHashSet()
    val perBlobStats = mutableListOf<BlobImpressionStats>()

    for (metadataUri in metadataUris) {
      val blobDetails = BlobDetailsLoader.load(metadataUri, storageConfig)
      perBlobStats += scanBlob(metadataUri, blobDetails, globalVids)
    }

    val totalRecords = perBlobStats.sumOf { it.recordCount }
    return ImpressionStats(
      totalRecords = totalRecords,
      distinctVids = globalVids.size.toLong(),
      blobStats = perBlobStats,
    )
  }

  private suspend fun scanBlob(
    metadataUri: String,
    blobDetails: BlobDetails,
    globalVids: LongOpenHashSet,
  ): BlobImpressionStats {
    require(blobDetails.blobUri.isNotBlank()) {
      "BlobDetails at $metadataUri missing blob_uri"
    }

    val storageClientUri = SelectedStorageClient.parseBlobUri(blobDetails.blobUri)
    val impressionsStorage = buildImpressionsStorage(blobDetails)
    val impressionBlob =
      impressionsStorage.getBlob(storageClientUri.key)
        ?: throw ImpressionReadException(
          blobDetails.blobUri,
          ImpressionReadException.Code.BLOB_NOT_FOUND,
        )

    logger.info("Counting impressions in ${blobDetails.blobUri}")

    var recordCount = 0L
    val startingDistinct = globalVids.size.toLong()

    try {
      impressionBlob.read().collect { bytes ->
        val impression = LabeledImpression.parseFrom(bytes)
        recordCount++
        globalVids.add(impression.vid)
      }
    } catch (e: InvalidProtocolBufferException) {
      throw ImpressionReadException(
        blobDetails.blobUri,
        ImpressionReadException.Code.INVALID_FORMAT,
        e.message,
      )
    }

    val newDistinct = globalVids.size.toLong() - startingDistinct
    logger.info(
      "Finished ${blobDetails.blobUri}: records=$recordCount, new distinct VIDs=$newDistinct"
    )
    return BlobImpressionStats(
      metadataUri = metadataUri,
      blobUri = blobDetails.blobUri,
      recordCount = recordCount,
      newDistinctVids = newDistinct,
    )
  }

  private fun buildImpressionsStorage(blobDetails: BlobDetails): MesosRecordIoStorageClient {
    val selectedStorageClient =
      SelectedStorageClient(
        blobDetails.blobUri,
        storageConfig.rootDirectory,
        storageConfig.projectId,
      )

    val encryptedDek = blobDetails.encryptedDek
    return if (kmsClient == null || encryptedDek.ciphertext.isEmpty) {
      check(encryptedDek.ciphertext.isEmpty) {
        "KMS client is required to decrypt encrypted blob ${blobDetails.blobUri}"
      }
      MesosRecordIoStorageClient(selectedStorageClient)
    } else {
      EncryptedStorage.buildEncryptedMesosStorageClient(
        selectedStorageClient,
        kmsClient,
        kekUri = encryptedDek.kekUri,
        encryptedDek = encryptedDek,
      )
    }
  }

  companion object {
    private val logger = Logger.getLogger(ImpressionStatsCalculator::class.java.name)
  }
}
