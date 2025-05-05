/*
 * Copyright 2023 The Cross-Media Measurement Authors
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

import com.google.common.hash.HashFunction
import com.google.common.hash.Hashing
import com.google.crypto.tink.KmsClient
import com.google.protobuf.Descriptors.Descriptor
import com.google.protobuf.DynamicMessage
import com.google.protobuf.TypeRegistry
import com.google.type.Interval
import java.io.File
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.filter
import kotlinx.coroutines.flow.flatMapConcat
import kotlinx.coroutines.flow.map
import org.projectnessie.cel.Program
import org.projectnessie.cel.common.types.BoolT
import org.wfanet.sampling.VidSampler
import org.wfanet.measurement.api.v2alpha.MeasurementSpec
import org.wfanet.measurement.api.v2alpha.RequisitionSpec
import org.wfanet.measurement.common.flatten
import org.wfanet.measurement.common.toInstant
import org.wfanet.measurement.edpaggregator.v1alpha.BlobDetails
import org.wfanet.measurement.edpaggregator.v1alpha.LabeledImpression
import org.wfanet.measurement.eventdataprovider.eventfiltration.EventFilters
import org.wfanet.measurement.storage.BlobUri
import org.wfanet.measurement.storage.MesosRecordIoStorageClient
import org.wfanet.measurement.storage.SelectedStorageClient
import org.wfanet.measurement.storage.StorageClient
import org.wfanet.measurement.common.crypto.tink.withEnvelopeEncryption

data class StorageConfig(
  val rootDirectory: File? = null,
  val projectId: String? = null,
)

/**
 * Utility functions for working with VIDs (Virtual IDs) in the EDP Aggregator.
 */
object VidUtils {
  private val VID_SAMPLER_HASH_FUNCTION: HashFunction = Hashing.farmHashFingerprint64()
  private val TRUE_EVAL_RESULT = Program.newEvalResult(BoolT.True, null)
  private val sampler = VidSampler(VID_SAMPLER_HASH_FUNCTION)

  /**
   * Creates a storage client based on the provided URI and configuration.
   */
  fun createStorageClient(blobUri: BlobUri, storageConfig: StorageConfig): StorageClient {
    return SelectedStorageClient(blobUri, storageConfig.rootDirectory, storageConfig.projectId)
  }

  /**
   * Retrieves blob details for a given collection interval and event group ID.
   */
  private suspend fun getBlobDetails(
    collectionInterval: Interval,
    eventGroupId: String,
    labeledImpressionMetadataPrefix: String,
    impressionMetadataStorageConfig: StorageConfig
  ): BlobDetails {
    val ds = collectionInterval.startTime.toInstant().toString()
    val metadataBlobKey = "ds/$ds/event-group-id/$eventGroupId/metadata"
    val metadataBlobUri = "$labeledImpressionMetadataPrefix/$metadataBlobKey"
    val metadataStorageClientUri = SelectedStorageClient.parseBlobUri(metadataBlobUri)
    val impressionsMetadataStorageClient = createStorageClient(metadataStorageClientUri, impressionMetadataStorageConfig)
    // Get EncryptedDek message from storage using the blobKey made up of the ds and eventGroupId
    return BlobDetails.parseFrom(impressionsMetadataStorageClient.getBlob(metadataBlobKey)!!.read().flatten())
  }

  /**
   * Retrieves labeled impressions from blob details.
   */
  private suspend fun getLabeledImpressions(
    blobDetails: BlobDetails,
    kmsClient: KmsClient,
    impressionsStorageConfig: StorageConfig
  ): Flow<LabeledImpression> {
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
   * Retrieves sampled VIDs from a requisition specification based on a sampling interval.
   *
   * @param requisitionSpec The requisition specification containing event groups
   * @param vidSamplingInterval The sampling interval to filter VIDs
   * @param typeRegistry The registry for looking up protobuf descriptors
   * @param kmsClient The KMS client for encryption operations
   * @param impressionsStorageConfig Configuration for impressions storage
   * @param impressionMetadataStorageConfig Configuration for impression metadata storage
   * @param labeledImpressionMetadataPrefix Prefix for labeled impression metadata
   * @return A Flow of sampled VIDs (Long values)
   */
  suspend fun getSampledVids(
    requisitionSpec: RequisitionSpec,
    vidSamplingInterval: MeasurementSpec.VidSamplingInterval,
    typeRegistry: TypeRegistry,
    kmsClient: KmsClient,
    impressionsStorageConfig: StorageConfig,
    impressionMetadataStorageConfig: StorageConfig,
    labeledImpressionMetadataPrefix: String
  ): Flow<Long> {
    val vidSamplingIntervalStart = vidSamplingInterval.start
    val vidSamplingIntervalWidth = vidSamplingInterval.width
    require(vidSamplingIntervalWidth > 0 && vidSamplingIntervalWidth <= 1.0) {
      "Invalid vidSamplingIntervalWidth $vidSamplingIntervalWidth"
    }
    require(
      vidSamplingIntervalStart < 1 &&
        vidSamplingIntervalStart >= 0 &&
        vidSamplingIntervalWidth > 0 &&
        vidSamplingIntervalStart + vidSamplingIntervalWidth <= 1
    ) {
      "Invalid vidSamplingInterval: start = $vidSamplingIntervalStart, width = " +
        "$vidSamplingIntervalWidth"
    }

    // Return a Flow that processes event groups and extracts valid VIDs
    return requisitionSpec.events.eventGroupsList
      .asFlow()
      .flatMapConcat { eventGroup ->
        val collectionInterval = eventGroup.value.collectionInterval
        val blobDetails = getBlobDetails(
          collectionInterval,
          eventGroup.key,
          labeledImpressionMetadataPrefix,
          impressionMetadataStorageConfig
        )

        getLabeledImpressions(blobDetails, kmsClient, impressionsStorageConfig)
          .filter { labeledImpression ->
            isValidImpression(
              labeledImpression,
              collectionInterval,
              eventGroup.value.filter,
              vidSamplingIntervalStart,
              vidSamplingIntervalWidth,
              typeRegistry
            )
          }
          .map { labeledImpression -> labeledImpression.vid }
      }
  }

  /**
   * Compiles a CEL program from an event filter and event message descriptor.
   *
   * @param eventFilter The event filter containing a CEL expression
   * @param eventMessageDescriptor The descriptor for the event message type
   * @return A compiled Program that can be used to filter events
   */
  fun compileProgram(
    eventFilter: RequisitionSpec.EventFilter,
    eventMessageDescriptor: Descriptor,
  ): Program {
    // EventFilters should take care of this, but checking here is an optimization that can skip
    // creation of a CEL Env.
    if (eventFilter.expression.isEmpty()) {
      return Program { TRUE_EVAL_RESULT }
    }
    return EventFilters.compileProgram(eventMessageDescriptor, eventFilter.expression)
  }

  /**
   * Determines if an impression is valid based on various criteria.
   *
   * @param labeledImpression The impression to validate
   * @param collectionInterval The time interval for collection
   * @param filter The filter information used to validate the impression
   * @param vidSamplingIntervalStart The start of the VID sampling interval
   * @param vidSamplingIntervalWidth The width of the VID sampling interval
   * @param typeRegistry The registry for looking up protobuf descriptors
   * @return True if the impression is valid, false otherwise
   */
  fun isValidImpression(
    labeledImpression: LabeledImpression,
    collectionInterval: Interval,
    filter: RequisitionSpec.EventFilter,
    vidSamplingIntervalStart: Float,
    vidSamplingIntervalWidth: Float,
    typeRegistry: TypeRegistry
  ): Boolean {
    // Check if impression is within collection time interval
    val isInCollectionInterval =
      labeledImpression.eventTime.toInstant() >= collectionInterval.startTime.toInstant() &&
        labeledImpression.eventTime.toInstant() < collectionInterval.endTime.toInstant()

    // Check if VID is in sampling bucket
    val isInSamplingInterval = sampler.vidIsInSamplingBucket(
      labeledImpression.vid,
      vidSamplingIntervalStart,
      vidSamplingIntervalWidth
    )

    // Create filter program
    val eventMessageData = labeledImpression.event!!
    val eventTemplateDescriptor = getDescriptorForTypeUrl(eventMessageData.typeUrl, typeRegistry)
    val eventMessage = DynamicMessage.parseFrom(eventTemplateDescriptor, eventMessageData.value)
    val program = compileProgram(filter, eventTemplateDescriptor)

    // Pass event message through program
    val passesFilter = EventFilters.matches(eventMessage, program)

    // Return true only if all conditions are met
    return isInCollectionInterval && passesFilter && isInSamplingInterval
  }

  /**
   * Gets a descriptor for a given type URL.
   * This is a helper method to extract the descriptor from the TypeRegistry.
   */
  private fun getDescriptorForTypeUrl(typeUrl: String, typeRegistry: TypeRegistry): Descriptor {
    // Extract the message name from the type URL
    val messageName = typeUrl.substringAfter("type.googleapis.com/")

    // Find the descriptor in the registry
    // Note: This is a simplified implementation and may need to be adjusted
    // based on how the TypeRegistry is actually used in the project
    return typeRegistry.find(messageName)
      ?: throw IllegalArgumentException("Unknown type URL: $typeUrl")
  }
}
