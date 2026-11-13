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

package org.wfanet.measurement.edpaggregator.deploy.gcloud.dataavailability

import com.google.cloud.storage.BlobId
import com.google.cloud.storage.BlobInfo
import com.google.cloud.storage.Storage
import com.google.cloud.storage.StorageException as GcsStorageException
import java.time.Instant
import java.time.OffsetDateTime
import java.time.ZoneOffset
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import org.wfanet.measurement.edpaggregator.dataavailability.ExternalDataAvailabilitySnapshotChangedException
import org.wfanet.measurement.edpaggregator.dataavailability.GenerationMatchedBlobMetadataUpdater

/** Merges GCS metadata while the admitted generation and observed metadata version match. */
class GcsGenerationMatchedBlobMetadataUpdater(
  private val storage: Storage,
  private val bucketName: String,
) : GenerationMatchedBlobMetadataUpdater {
  override suspend fun update(
    blobKey: String,
    generation: Long,
    customCreateTime: Instant?,
    metadata: Map<String, String>,
  ) {
    require(customCreateTime != null || metadata.isNotEmpty()) {
      "At least one of customCreateTime or metadata must be specified"
    }
    withContext(Dispatchers.IO) {
      try {
        val blob =
          storage.get(BlobId.of(bucketName, blobKey, generation))
            ?: throw ExternalDataAvailabilitySnapshotChangedException()
        val metageneration =
          blob.metageneration ?: throw ExternalDataAvailabilitySnapshotChangedException()
        val blobInfo =
          // Match the live object rather than updating a retained historical generation.
          BlobInfo.newBuilder(BlobId.of(bucketName, blobKey))
            .apply {
              if (customCreateTime != null) {
                setCustomTimeOffsetDateTime(
                  OffsetDateTime.ofInstant(customCreateTime, ZoneOffset.UTC)
                )
              }
              if (metadata.isNotEmpty()) {
                setMetadata(blob.metadata.orEmpty() + metadata)
              }
            }
            .build()
        storage.update(
          blobInfo,
          Storage.BlobTargetOption.generationMatch(generation),
          Storage.BlobTargetOption.metagenerationMatch(metageneration),
        ) ?: throw ExternalDataAvailabilitySnapshotChangedException()
      } catch (e: GcsStorageException) {
        if (e.code == HTTP_PRECONDITION_FAILED || e.code == HTTP_NOT_FOUND) {
          throw ExternalDataAvailabilitySnapshotChangedException(cause = e)
        }
        throw e
      }
    }
  }

  companion object {
    private const val HTTP_NOT_FOUND = 404
    private const val HTTP_PRECONDITION_FAILED = 412
  }
}
