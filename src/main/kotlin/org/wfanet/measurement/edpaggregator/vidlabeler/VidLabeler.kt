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

package org.wfanet.measurement.edpaggregator.vidlabeler

import com.google.crypto.tink.KmsClient
import com.google.protobuf.ByteString
import com.google.protobuf.util.JsonFormat
import com.google.type.interval
import java.util.UUID
import java.util.logging.Level
import java.util.logging.Logger
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.toList
import org.wfanet.measurement.common.crypto.tink.withEnvelopeEncryption
import org.wfanet.measurement.common.flatten
import org.wfanet.measurement.edpaggregator.EncryptedStorage
import org.wfanet.measurement.edpaggregator.StorageConfig
import org.wfanet.measurement.edpaggregator.v1alpha.BlobDetails
import org.wfanet.measurement.edpaggregator.v1alpha.EncryptedDek
import org.wfanet.measurement.edpaggregator.v1alpha.ImpressionMetadataServiceGrpcKt.ImpressionMetadataServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.v1alpha.LabeledImpression
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionMetadataBatchFileServiceGrpcKt.RawImpressionMetadataBatchFileServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionMetadataBatchServiceGrpcKt.RawImpressionMetadataBatchServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.v1alpha.TransportLayerSecurityParams
import org.wfanet.measurement.edpaggregator.v1alpha.VidLabelerParams
import org.wfanet.measurement.edpaggregator.v1alpha.batchCreateImpressionMetadataRequest
import org.wfanet.measurement.edpaggregator.v1alpha.blobDetails
import org.wfanet.measurement.edpaggregator.v1alpha.createImpressionMetadataRequest
import org.wfanet.measurement.edpaggregator.v1alpha.encryptedDek
import org.wfanet.measurement.edpaggregator.v1alpha.impressionMetadata
import org.wfanet.measurement.edpaggregator.v1alpha.listRawImpressionMetadataBatchFilesRequest
import org.wfanet.measurement.edpaggregator.v1alpha.markRawImpressionMetadataBatchFailedRequest
import org.wfanet.measurement.edpaggregator.v1alpha.markRawImpressionMetadataBatchProcessedRequest
import org.wfanet.measurement.edpaggregator.vidlabeler.VidLabelerMetrics.Companion.measured
import org.wfanet.measurement.storage.MesosRecordIoStorageClient
import org.wfanet.measurement.storage.SelectedStorageClient

/**
 * Core business logic for VID labeling of raw encrypted impression data.
 *
 * Instantiated per batch by [VidLabelerApp.runWork] and invoked to label a single batch of raw
 * impressions with VIDs.
 *
 * @param dataProvider EDP resource name (e.g. "dataProviders/abc").
 * @param rawImpressionMetadataBatch batch resource name to process.
 * @param modelLineConfigs model line resource name to field mapping configuration.
 * @param overrideModelLines if non-empty, only label with these model lines.
 * @param storageParams GCS project and output prefix configuration.
 * @param decryptKmsClient decrypt-only KMS client for reading raw impressions.
 * @param encryptKmsClient encrypt/decrypt KMS client for writing labeled output.
 * @param encryptKekUri KEK URI for generating DEKs when writing labeled output.
 * @param impressionMetadataStub gRPC stub for idempotency checks and writing output metadata.
 * @param rawImpressionBatchStub gRPC stub for updating batch state.
 * @param batchFileStub gRPC stub for listing files in a batch.
 * @param vidRepoConnection TLS params for VID Model Repository.
 * @param storageConfig storage configuration built from [storageParams].
 * @param metrics telemetry for VID labeling operations.
 */
class VidLabeler(
  private val dataProvider: String,
  private val rawImpressionMetadataBatch: String,
  private val modelLineConfigs: Map<String, VidLabelerParams.ModelLineConfig>,
  private val overrideModelLines: List<String>,
  private val storageParams: VidLabelerParams.StorageParams,
  private val decryptKmsClient: KmsClient,
  private val encryptKmsClient: KmsClient,
  private val encryptKekUri: String,
  private val impressionMetadataStub: ImpressionMetadataServiceCoroutineStub,
  private val rawImpressionBatchStub: RawImpressionMetadataBatchServiceCoroutineStub,
  private val batchFileStub: RawImpressionMetadataBatchFileServiceCoroutineStub,
  private val vidRepoConnection: TransportLayerSecurityParams,
  private val storageConfig: StorageConfig,
  private val metrics: VidLabelerMetrics,
) {

  /** Labels raw impressions in the batch with VIDs. */
  suspend fun labelBatch() {
    logger.info("Starting VID labeling for batch $rawImpressionMetadataBatch")
    try {
      metrics.batchLabelingLatency.measured {
        val blobUris = listBatchFileUris()
        logger.info("Found ${blobUris.size} files in batch")

        val rawImpressions =
          metrics.readDecryptDuration.measured { readAndDecryptRawImpressions(blobUris) }
        logger.info("Read ${rawImpressions.size} raw impressions")

        val activeModelLines = resolveModelLines()
        logger.info("Resolved ${activeModelLines.size} model lines")

        val labeledImpressions =
          metrics.inferenceDuration.measured { labelImpressions(rawImpressions, activeModelLines) }
        logger.info("Labeled ${labeledImpressions.size} impressions")

        val blobDetailsList =
          metrics.encryptWriteDuration.measured {
            encryptAndWriteLabeledImpressions(labeledImpressions)
          }

        metrics.metadataWriteDuration.measured { writeOutputMetadata(blobDetailsList) }

        markBatchProcessed()
        metrics.batchesProcessed.add(1, VidLabelerMetrics.statusSuccess)
        metrics.impressionsLabeled.add(labeledImpressions.size.toLong())
      }
    } catch (e: Exception) {
      logger.log(Level.SEVERE, "Failed to label batch $rawImpressionMetadataBatch", e)
      metrics.batchesProcessed.add(1, VidLabelerMetrics.statusFailure)
      markBatchFailed()
      throw e
    }
  }

  /** Lists blob URIs of all files in the batch. */
  private suspend fun listBatchFileUris(): List<String> {
    val blobUris = mutableListOf<String>()
    var pageToken = ""
    do {
      val response =
        batchFileStub.listRawImpressionMetadataBatchFiles(
          listRawImpressionMetadataBatchFilesRequest {
            parent = rawImpressionMetadataBatch
            if (pageToken.isNotEmpty()) {
              this.pageToken = pageToken
            }
          }
        )
      blobUris.addAll(response.rawImpressionMetadataBatchFilesList.map { it.blobUri })
      pageToken = response.nextPageToken
    } while (pageToken.isNotEmpty())
    return blobUris
  }

  /**
   * Reads raw impression blobs from GCS and decrypts them.
   *
   * Each blob URI points to a BlobDetails proto which contains the actual encrypted data location
   * and the DEK needed for decryption.
   */
  private suspend fun readAndDecryptRawImpressions(blobUris: List<String>): List<RawImpression> {
    val rawImpressions = mutableListOf<RawImpression>()
    for (blobUri in blobUris) {
      val blobDetails = readBlobDetails(blobUri)

      val storageClientUri = SelectedStorageClient.parseBlobUri(blobDetails.blobUri)
      val selectedStorageClient =
        SelectedStorageClient(
          storageClientUri,
          storageConfig.rootDirectory,
          storageConfig.projectId,
        )

      val encryptedDek = blobDetails.encryptedDek
      val impressionsStorage =
        EncryptedStorage.buildEncryptedMesosStorageClient(
          selectedStorageClient,
          kekUri = encryptedDek.kekUri,
          kmsClient = decryptKmsClient,
          encryptedDek = encryptedDek,
        )

      val blob =
        impressionsStorage.getBlob(storageClientUri.key)
          ?: throw IllegalStateException("Raw impression blob not found: ${blobDetails.blobUri}")

      blob.read().toList().forEach { recordBytes ->
        rawImpressions.add(
          RawImpression(
            eventGroupReferenceId = blobDetails.eventGroupReferenceId,
            blobUri = blobUri,
            blobDetails = blobDetails,
            rawRecord = recordBytes,
          )
        )
      }
    }
    return rawImpressions
  }

  /** Reads a [BlobDetails] proto from a metadata blob URI. */
  private suspend fun readBlobDetails(metadataPath: String): BlobDetails {
    val storageClientUri = SelectedStorageClient.parseBlobUri(metadataPath)
    val storageClient =
      SelectedStorageClient(storageClientUri, storageConfig.rootDirectory, storageConfig.projectId)
    val blob =
      storageClient.getBlob(storageClientUri.key)
        ?: throw IllegalStateException("BlobDetails metadata not found: $metadataPath")
    val bytes: ByteString = blob.read().flatten()
    return try {
      BlobDetails.parseFrom(bytes)
    } catch (e: com.google.protobuf.InvalidProtocolBufferException) {
      val builder = BlobDetails.newBuilder()
      JsonFormat.parser().ignoringUnknownFields().merge(bytes.toString(Charsets.UTF_8), builder)
      builder.build()
    }
  }

  /**
   * Determines which model lines to use for labeling.
   *
   * If [overrideModelLines] is non-empty, only those model lines are used (validated against
   * [modelLineConfigs]). Otherwise all model lines from [modelLineConfigs] are used.
   */
  private fun resolveModelLines(): Map<String, VidLabelerParams.ModelLineConfig> {
    return metrics.modelResolutionDuration.measured {
      if (overrideModelLines.isNotEmpty()) {
        val resolved = mutableMapOf<String, VidLabelerParams.ModelLineConfig>()
        for (modelLine in overrideModelLines) {
          val config =
            requireNotNull(modelLineConfigs[modelLine]) {
              "Override model line $modelLine not found in model_line_configs"
            }
          resolved[modelLine] = config
        }
        resolved
      } else {
        modelLineConfigs
      }
    }
  }

  /**
   * Runs VID model inference on raw impressions for each model line.
   *
   * For each model line, applies the field mapping from [VidLabelerParams.ModelLineConfig] to
   * extract labeler inputs, then runs the VID model to produce labeled impressions.
   */
  private suspend fun labelImpressions(
    rawImpressions: List<RawImpression>,
    modelLines: Map<String, VidLabelerParams.ModelLineConfig>,
  ): List<LabeledImpressionOutput> {
    // TODO(world-federation-of-advertisers/cross-media-measurement#T263656471): Implement VID
    //  model inference using VidModelCache and VidModelResolver.
    //  1. For each model line, use vidRepoConnection to connect to the VID Model Repository.
    //  2. Download/cache the model.
    //  3. For each raw impression, apply labeler_input_field_mapping to extract model inputs.
    //  4. Run inference to get VID.
    //  5. Build LabeledImpression with VID, event_time, event, and event_group_reference_id.
    throw NotImplementedError("VID model inference not yet implemented")
  }

  /**
   * Encrypts labeled impressions and writes them to GCS.
   *
   * Groups impressions by (model line, event group), generates a new DEK per group, writes the
   * encrypted data blob, then writes a BlobDetails metadata blob. Returns the BlobDetails for each
   * group for downstream metadata registration.
   */
  private suspend fun encryptAndWriteLabeledImpressions(
    labeledImpressions: List<LabeledImpressionOutput>
  ): List<BlobDetails> {
    if (labeledImpressions.isEmpty()) return emptyList()

    val groups = labeledImpressions.groupBy { it.modelLine to it.eventGroupReferenceId }
    val blobDetailsList = mutableListOf<BlobDetails>()

    for ((key, impressions) in groups) {
      val (modelLine, eventGroupReferenceId) = key

      val serializedEncryptionKey =
        EncryptedStorage.generateSerializedEncryptionKey(
          encryptKmsClient,
          encryptKekUri,
          TINK_KEY_TEMPLATE,
        )

      val outputEncryptedDek = encryptedDek {
        kekUri = encryptKekUri
        ciphertext = serializedEncryptionKey
        protobufFormat = EncryptedDek.ProtobufFormat.BINARY
        typeUrl = "type.googleapis.com/google.crypto.tink.Keyset"
      }

      val blobKey = "${UUID.randomUUID()}/labeled-impressions"
      val outputBlobUri = "${storageParams.impressionsBlobPrefix}/$blobKey"

      val outputStorageClient =
        SelectedStorageClient(
          SelectedStorageClient.parseBlobUri(outputBlobUri),
          storageConfig.rootDirectory,
          storageConfig.projectId,
        )

      val aeadStorageClient =
        outputStorageClient.withEnvelopeEncryption(
          encryptKmsClient,
          encryptKekUri,
          serializedEncryptionKey,
        )
      val encryptedStorage = MesosRecordIoStorageClient(aeadStorageClient)

      encryptedStorage.writeBlob(
        blobKey,
        impressions.map { it.labeledImpression.toByteString() }.asFlow(),
      )

      val timestamps = impressions.map { it.labeledImpression.eventTime }
      val startTime = timestamps.minBy { it.seconds * 1_000_000_000L + it.nanos }
      val endTime = timestamps.maxBy { it.seconds * 1_000_000_000L + it.nanos }

      val details = blobDetails {
        this.blobUri = outputBlobUri
        this.encryptedDek = outputEncryptedDek
        this.eventGroupReferenceId = eventGroupReferenceId
        this.modelLine = modelLine
        this.interval = interval {
          this.startTime = startTime
          this.endTime = endTime
        }
      }

      val metadataBlobKey = "${UUID.randomUUID()}/metadata.binpb"
      val metadataUri = "${storageParams.impressionsBlobPrefix}/$metadataBlobKey"
      val metadataStorageClient =
        SelectedStorageClient(
          SelectedStorageClient.parseBlobUri(metadataUri),
          storageConfig.rootDirectory,
          storageConfig.projectId,
        )
      metadataStorageClient.writeBlob(metadataBlobKey, details.toByteString())

      logger.info(
        "Wrote labeled impressions for $modelLine/$eventGroupReferenceId to $outputBlobUri"
      )
      blobDetailsList.add(details)
    }

    return blobDetailsList
  }

  /**
   * Creates ImpressionMetadata entries for each output blob via gRPC.
   *
   * Uses [ImpressionMetadataServiceCoroutineStub.batchCreateImpressionMetadata] for efficiency.
   */
  private suspend fun writeOutputMetadata(blobDetailsList: List<BlobDetails>) {
    if (blobDetailsList.isEmpty()) return

    val requests =
      blobDetailsList.map { blobDetails ->
        createImpressionMetadataRequest {
          parent = dataProvider
          impressionMetadata = impressionMetadata {
            blobUri = blobDetails.blobUri
            blobTypeUrl = LABELED_IMPRESSION_TYPE_URL
            eventGroupReferenceId = blobDetails.eventGroupReferenceId
            modelLine = blobDetails.modelLine
            interval = blobDetails.interval
          }
          requestId = UUID.randomUUID().toString()
        }
      }

    impressionMetadataStub.batchCreateImpressionMetadata(
      batchCreateImpressionMetadataRequest {
        parent = dataProvider
        this.requests.addAll(requests)
      }
    )
    logger.info("Created ${requests.size} ImpressionMetadata entries")
  }

  /** Marks the batch as PROCESSED after successful labeling. */
  private suspend fun markBatchProcessed() {
    rawImpressionBatchStub.markRawImpressionMetadataBatchProcessed(
      markRawImpressionMetadataBatchProcessedRequest { name = rawImpressionMetadataBatch }
    )
    logger.info("Marked batch $rawImpressionMetadataBatch as PROCESSED")
  }

  /** Marks the batch as FAILED after an error. */
  private suspend fun markBatchFailed() {
    try {
      rawImpressionBatchStub.markRawImpressionMetadataBatchFailed(
        markRawImpressionMetadataBatchFailedRequest { name = rawImpressionMetadataBatch }
      )
      logger.info("Marked batch $rawImpressionMetadataBatch as FAILED")
    } catch (e: Exception) {
      logger.log(Level.SEVERE, "Failed to mark batch $rawImpressionMetadataBatch as FAILED", e)
    }
  }

  companion object {
    private val logger = Logger.getLogger(VidLabeler::class.java.name)

    private const val LABELED_IMPRESSION_TYPE_URL =
      "type.googleapis.com/wfa.measurement.edpaggregator.LabeledImpression"

    private const val TINK_KEY_TEMPLATE = "AES128_GCM_HKDF_1MB"
  }
}

/**
 * Intermediate representation of a raw impression before VID labeling.
 *
 * Populated from decrypted raw impression data read from GCS.
 */
data class RawImpression(
  val eventGroupReferenceId: String,
  val blobUri: String,
  val blobDetails: BlobDetails,
  val rawRecord: ByteString,
)

/**
 * Intermediate representation of a labeled impression ready for encryption and storage.
 *
 * Groups the labeled impression proto with its source context for output file organization.
 */
data class LabeledImpressionOutput(
  val labeledImpression: LabeledImpression,
  val modelLine: String,
  val eventGroupReferenceId: String,
)
