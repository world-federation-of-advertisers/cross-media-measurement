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

import com.google.protobuf.ByteString
import com.google.protobuf.InvalidProtocolBufferException
import com.google.protobuf.util.JsonFormat
import kotlin.text.Charsets.UTF_8
import org.wfanet.measurement.common.flatten
import org.wfanet.measurement.edpaggregator.StorageConfig
import org.wfanet.measurement.edpaggregator.v1alpha.BlobDetails
import org.wfanet.measurement.storage.SelectedStorageClient

/**
 * Utility for loading [BlobDetails] records from storage.
 *
 * Supports both binary protobuf and JSON formats to match how impression metadata may be written.
 */
object BlobDetailsLoader {

  /**
   * Reads and parses [BlobDetails] from a metadata blob URI.
   *
   * @param metadataUri full URI for the metadata blob (e.g. `gs://bucket/path/metadata.binpb`)
   * @param storageConfig storage configuration used to build the client (project/root directory)
   * @throws ImpressionReadException if the blob is missing or cannot be parsed
   */
  suspend fun load(metadataUri: String, storageConfig: StorageConfig): BlobDetails {
    val storageClientUri = SelectedStorageClient.parseBlobUri(metadataUri)
    val storageClient =
      SelectedStorageClient(
        storageClientUri,
        storageConfig.rootDirectory,
        storageConfig.projectId,
      )

    val blob =
      storageClient.getBlob(storageClientUri.key)
        ?: throw ImpressionReadException(
          metadataUri,
          ImpressionReadException.Code.BLOB_NOT_FOUND,
          "BlobDetails metadata not found",
        )

    val bytes: ByteString = blob.read().flatten()
    return parse(bytes, metadataUri)
  }

  /**
   * Parses [BlobDetails] from raw bytes, accepting either binary proto or JSON encodings.
   *
   * @param bytes contents of the metadata blob
   * @param sourceUri URI used for error reporting
   * @throws ImpressionReadException if neither binary nor JSON parsing succeeds
   */
  fun parse(bytes: ByteString, sourceUri: String): BlobDetails {
    return try {
      BlobDetails.parseFrom(bytes)
    } catch (@Suppress("TooGenericExceptionCaught") binaryException: InvalidProtocolBufferException) {
      try {
        val builder = BlobDetails.newBuilder()
        JsonFormat.parser().ignoringUnknownFields().merge(bytes.toString(UTF_8), builder)
        builder.build()
      } catch (@Suppress("TooGenericExceptionCaught") e: Exception) {
        throw ImpressionReadException(
          sourceUri,
          ImpressionReadException.Code.INVALID_FORMAT,
          e.message,
        )
      }
    }
  }
}
