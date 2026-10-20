// Copyright 2026 The Cross-Media Measurement Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.wfanet.measurement.edpaggregator.client

import com.google.crypto.tink.KeyTemplates
import com.google.crypto.tink.KeysetHandle
import com.google.crypto.tink.KmsClient
import com.google.crypto.tink.RegistryConfiguration
import com.google.crypto.tink.StreamingAead
import com.google.crypto.tink.TinkProtoKeysetFormat
import com.google.crypto.tink.streamingaead.StreamingAeadConfig
import com.google.protobuf.ByteString
import com.google.protobuf.util.Timestamps
import com.google.type.Interval
import java.io.File
import java.util.SortedMap
import java.util.SortedSet
import java.util.TreeMap
import java.util.TreeSet
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.withContext
import org.wfanet.measurement.common.crypto.tink.StreamingAeadStorageClient
import org.wfanet.measurement.edpaggregator.v1alpha.BlobDetails
import org.wfanet.measurement.edpaggregator.v1alpha.EncryptedDek
import org.wfanet.measurement.edpaggregator.v1alpha.LabeledImpression
import org.wfanet.measurement.edpaggregator.v1alpha.blobDetails
import org.wfanet.measurement.edpaggregator.v1alpha.encryptedDek
import org.wfanet.measurement.edpaggregator.v1alpha.entityKeyGroup
import org.wfanet.measurement.storage.BlobUri
import org.wfanet.measurement.storage.ConditionalOperationStorageClient
import org.wfanet.measurement.storage.MesosRecordIoStorageClient
import org.wfanet.measurement.storage.SelectedStorageClient
import org.wfanet.measurement.storage.StorageClient

/** Writes VID-labeled impressions in the encrypted format accepted by EDP Aggregator. */
class LabeledImpressionWriter
private constructor(
  private val kmsClient: KmsClient,
  private val storageClientFactory: (BlobUri) -> ConditionalOperationStorageClient,
) {
  /**
   * Writes [impressions] as encrypted Mesos RecordIO and writes its binary [BlobDetails] sidecar.
   *
   * [blobUri] and [metadataUri] must be distinct objects in the same bucket. A fresh streaming AEAD
   * data encryption key is generated for each invocation and encrypted with [kekUri]. Entity keys
   * in the sidecar are derived from the records written to the blob. Both destination objects must
   * be new, and the writer never overwrites an existing blob or sidecar. If the impression object
   * is created but a later step fails, the writer deletes that exact object through the handle
   * returned by the successful create. It never looks up a destination object to clean up an
   * ambiguous write failure.
   *
   * @return the [BlobDetails] written to [metadataUri]
   */
  suspend fun write(
    impressions: Flow<LabeledImpression>,
    blobUri: String,
    metadataUri: String,
    kekUri: String,
    modelLine: String,
    interval: Interval,
  ): BlobDetails {
    val parsedBlobUri = SelectedStorageClient.parseBlobUri(blobUri)
    val parsedMetadataUri = SelectedStorageClient.parseBlobUri(metadataUri)
    require(parsedBlobUri.scheme == parsedMetadataUri.scheme) {
      "blobUri and metadataUri must use the same storage scheme"
    }
    require(parsedBlobUri.bucket == parsedMetadataUri.bucket) {
      "blobUri and metadataUri must be in the same bucket"
    }
    require(parsedBlobUri.key.isNotEmpty()) { "blobUri must include an object key" }
    require(parsedMetadataUri.key.isNotEmpty()) { "metadataUri must include an object key" }
    require(parsedBlobUri.key != parsedMetadataUri.key) {
      "blobUri and metadataUri must identify different objects"
    }
    require(parsedMetadataUri.key.endsWith(BINARY_PROTO_SUFFIX)) {
      "metadataUri must end with $BINARY_PROTO_SUFFIX"
    }
    require(
      parsedMetadataUri.key.substringAfterLast('/').contains(METADATA_FILE_NAME, ignoreCase = true)
    ) {
      "metadataUri filename must contain $METADATA_FILE_NAME"
    }
    require(kekUri.isNotEmpty()) { "kekUri must not be empty" }
    require(MODEL_LINE_RESOURCE_NAME.matches(modelLine)) {
      "modelLine must be a full ModelLine resource name"
    }
    validateInterval(interval)

    val dataEncryptionKey = generateDataEncryptionKey(kekUri)
    val encryptedDek = encryptedDek {
      this.kekUri = kekUri
      typeUrl = TINK_KEYSET_TYPE_URL
      protobufFormat = EncryptedDek.ProtobufFormat.BINARY
      ciphertext = dataEncryptionKey.serializedEncryptedKeyset
    }
    val entityIdsByType: SortedMap<String, SortedSet<String>> = TreeMap()
    var impressionCount = 0L
    val records =
      impressions.map { impression ->
        validateImpression(impression, interval)
        for (entityKey in impression.entityKeysList) {
          entityIdsByType.getOrPut(entityKey.entityType) { TreeSet() }.add(entityKey.entityId)
        }
        impressionCount++
        impression.toByteString()
      }

    val blobStorageClient = storageClientFactory(parsedBlobUri)
    val encryptedStorageClient =
      StreamingAeadStorageClient(blobStorageClient, dataEncryptionKey.streamingAead, Dispatchers.IO)
    val createdBlob =
      MesosRecordIoStorageClient(encryptedStorageClient)
        .writeBlobIfNotFound(parsedBlobUri.key, records)

    try {
      check(impressionCount > 0L) { "impressions must not be empty" }
      val details = blobDetails {
        this.blobUri = blobUri
        this.encryptedDek = encryptedDek
        this.modelLine = modelLine
        this.interval = interval
        entityKeys +=
          entityIdsByType.map { (entityType, entityIds) ->
            entityKeyGroup {
              this.entityType = entityType
              this.entityIds += entityIds
            }
          }
      }
      val metadataStorageClient = storageClientFactory(parsedMetadataUri)
      metadataStorageClient.writeBlobIfNotFound(
        parsedMetadataUri.key,
        flowOf(details.toByteString()),
      )
      return details
    } catch (e: Throwable) {
      cleanupBlob(createdBlob, e)
      throw e
    }
  }

  private suspend fun cleanupBlob(blob: StorageClient.Blob, originalFailure: Throwable) {
    withContext(NonCancellable) {
      try {
        blob.delete()
      } catch (cleanupFailure: Throwable) {
        originalFailure.addSuppressed(cleanupFailure)
      }
    }
  }

  private suspend fun generateDataEncryptionKey(kekUri: String): DataEncryptionKey =
    withContext(Dispatchers.IO) {
      val keysetHandle = KeysetHandle.generateNew(KeyTemplates.get(STREAMING_AEAD_KEY_TEMPLATE))
      DataEncryptionKey(
        keysetHandle.getPrimitive(RegistryConfiguration.get(), StreamingAead::class.java),
        ByteString.copyFrom(
          TinkProtoKeysetFormat.serializeEncryptedKeyset(
            keysetHandle,
            kmsClient.getAead(kekUri),
            EMPTY_ASSOCIATED_DATA,
            RegistryConfiguration.get(),
          )
        ),
      )
    }

  private fun validateImpression(impression: LabeledImpression, interval: Interval) {
    require(impression.vid > 0L) { "Each impression must have a positive VID" }
    require(impression.hasEvent()) { "Each impression must have an event" }
    require(impression.event.typeUrl.isNotEmpty()) { "Each impression event must have a type URL" }
    require(impression.hasEventTime()) { "Each impression must have an event time" }
    Timestamps.checkValid(impression.eventTime)
    require(
      Timestamps.compare(impression.eventTime, interval.startTime) >= 0 &&
        Timestamps.compare(impression.eventTime, interval.endTime) < 0
    ) {
      "Each impression event time must be within interval"
    }
    require(impression.entityKeysCount > 0) { "Each impression must have at least one entity key" }
    for (entityKey in impression.entityKeysList) {
      require(entityKey.entityType.isNotEmpty()) { "Entity key type must not be empty" }
      require(entityKey.entityId.isNotEmpty()) { "Entity key ID must not be empty" }
      require(URL_SAFE_STRING.matches(entityKey.entityType)) { "Entity key type must be URL-safe" }
      require(URL_SAFE_STRING.matches(entityKey.entityId)) { "Entity key ID must be URL-safe" }
    }
  }

  companion object {
    private const val BINARY_PROTO_SUFFIX = ".binpb"
    private const val METADATA_FILE_NAME = "metadata"
    private const val STREAMING_AEAD_KEY_TEMPLATE = "AES128_GCM_HKDF_1MB"
    private const val TINK_KEYSET_TYPE_URL = "type.googleapis.com/google.crypto.tink.Keyset"
    private val EMPTY_ASSOCIATED_DATA = ByteArray(0)
    private val MODEL_LINE_RESOURCE_NAME =
      Regex("modelProviders/[^/]+/modelSuites/[^/]+/modelLines/[^/]+")
    private val URL_SAFE_STRING = Regex("^[a-zA-Z0-9._~-]+$")

    init {
      StreamingAeadConfig.register()
    }

    /** Returns a writer for Google Cloud Storage. Pass `null` to infer [projectId]. */
    @JvmStatic
    fun forCloudStorage(kmsClient: KmsClient, projectId: String?): LabeledImpressionWriter {
      return LabeledImpressionWriter(kmsClient) { blobUri ->
        SelectedStorageClient(blobUri, rootDirectory = null, projectId = projectId)
      }
    }

    /** Returns a writer for `file:///` URIs rooted at [rootDirectory]. */
    @JvmStatic
    fun forFileSystem(kmsClient: KmsClient, rootDirectory: File): LabeledImpressionWriter {
      return LabeledImpressionWriter(kmsClient) { blobUri ->
        SelectedStorageClient(blobUri, rootDirectory, projectId = null)
      }
    }

    internal fun forStorageClient(
      kmsClient: KmsClient,
      storageClient: ConditionalOperationStorageClient,
    ): LabeledImpressionWriter = LabeledImpressionWriter(kmsClient) { storageClient }

    private fun validateInterval(interval: Interval) {
      require(interval.hasStartTime()) { "interval must have a start time" }
      require(interval.hasEndTime()) { "interval must have an end time" }
      Timestamps.checkValid(interval.startTime)
      Timestamps.checkValid(interval.endTime)
      require(Timestamps.compare(interval.startTime, interval.endTime) < 0) {
        "interval start time must be before end time"
      }
    }
  }

  private data class DataEncryptionKey(
    val streamingAead: StreamingAead,
    val serializedEncryptedKeyset: ByteString,
  )
}
