// Copyright 2025 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.loadtest.edpaggregator.testing

import com.google.crypto.tink.KmsClient
import com.google.protobuf.Any
import com.google.protobuf.Message
import com.google.type.interval
import java.io.File
import java.time.LocalDate
import java.time.ZoneOffset
import java.util.logging.Logger
import kotlinx.coroutines.flow.asFlow
import org.wfanet.measurement.api.v2alpha.ModelLineKey
import org.wfanet.measurement.common.crypto.tink.withEnvelopeEncryption
import org.wfanet.measurement.common.toProtoTime
import org.wfanet.measurement.edpaggregator.EncryptedStorage
import org.wfanet.measurement.edpaggregator.v1alpha.EncryptedDek
import org.wfanet.measurement.edpaggregator.v1alpha.LabeledImpression
import org.wfanet.measurement.edpaggregator.v1alpha.LabeledImpressionKt
import org.wfanet.measurement.edpaggregator.v1alpha.blobDetails
import org.wfanet.measurement.edpaggregator.v1alpha.labeledImpression
import org.wfanet.measurement.loadtest.dataprovider.EntityKey
import org.wfanet.measurement.loadtest.dataprovider.EntityKeyedLabeledEventDateShard
import org.wfanet.measurement.loadtest.dataprovider.EntityKeysWithLabeledEvents
import org.wfanet.measurement.storage.MesosRecordIoStorageClient
import org.wfanet.measurement.storage.SelectedStorageClient

/**
 * A class responsible for writing labeled impression data to storage with encryption.
 *
 * This class handles the encryption of impression data using a Key Management Service (KMS) and
 * outputs the encrypted data to a specified storage location. It also generates and stores the
 * necessary metadata for the ResultsFulfiller to locate and read the contents.
 *
 * Uses a SelectedStorageClient based on the schema (supports gs:// and file:///).
 *
 * Impressions are written using a Mesos Record IO format using streaming envelope encryption.
 *
 * Each emitted [LabeledImpression] is stamped with the [EntityKey]s from its containing
 * [EntityKeysWithLabeledEvents] group. Different groups within the same [LabeledEventDateShard]
 * therefore land in the same impressions blob with potentially different entity keys, while every
 * impression in a single blob still belongs to the same `event_group_reference_id` recorded on the
 * per-blob `BlobDetails` metadata.
 *
 * @property eventGroupReferenceId The event group reference ID recorded on the per-blob
 *   `BlobDetails` metadata. Each impressions blob is homogeneous with respect to event group, so
 *   the id is recorded once at the metadata level rather than on every `LabeledImpression`.
 * @property eventGroupPath The path to the event group where impressions are stored.
 * @property kekUri The URI of the Key Encryption Key (KEK) used for envelope encryption.
 * @property kmsClient The KMS client used for encryption operations.
 * @property impressionsBucket The storage bucket where encrypted impressions are stored.
 * @property impressionsMetadataBucket The storage bucket where metadata for impressions is stored.
 * @property storagePath An optional file path for local storage, defaulting to null.
 * @property schema The URI schema for storage paths, defaulting to "file:///".
 */
class ImpressionsWriter(
  private val eventGroupReferenceId: String,
  private val eventGroupPath: String,
  private val kekUri: String,
  private val kmsClient: KmsClient,
  private val impressionsBucket: String,
  private val impressionsMetadataBucket: String,
  private val storagePath: File? = null,
  private val schema: String = "file:///",
) {

  /**
   * Takes a sequence of [EntityKeyedLabeledEventDateShard]s, encrypts the data with a KMS, and
   * outputs the data to storage along with the necessary metadata for the ResultsFulfiller to be
   * able to find and read the contents.
   *
   * @param blobModelLine full ModelLine resource name. Must be a valid resource name because
   *   downstream services validate and persist the value as such.
   */
  suspend fun <T : Message> writeLabeledImpressionData(
    events: Sequence<EntityKeyedLabeledEventDateShard<T>>,
    blobModelLine: String,
    impressionsBasePath: String? = null,
    flatOutputBasePath: String? = null,
  ) {
    val modelLineName =
      requireNotNull(ModelLineKey.fromName(blobModelLine)) {
          "blobModelLine must be a full ModelLine resource name: $blobModelLine"
        }
        .toName()
    val serializedEncryptionKey =
      EncryptedStorage.generateSerializedEncryptionKey(kmsClient, kekUri, "AES128_GCM_HKDF_1MB")
    val encryptedDek =
      EncryptedDek.newBuilder()
        .setKekUri(kekUri)
        .setCiphertext(serializedEncryptionKey)
        .setProtobufFormat(EncryptedDek.ProtobufFormat.BINARY)
        .setTypeUrl("type.googleapis.com/google.crypto.tink.Keyset")
        .build()
    events.forEach { (localDate: LocalDate, groups: List<EntityKeysWithLabeledEvents<T>>) ->
      val labeledImpressions: Sequence<LabeledImpression> =
        groups.asSequence().flatMap { group: EntityKeysWithLabeledEvents<T> ->
          val protoEntityKeys: List<LabeledImpression.EntityKey> =
            group.entityKeys.map { it.toProto() }
          group.labeledEvents.map { event ->
            labeledImpression {
              vid = event.vid
              this.event = Any.pack(event.message)
              eventTime = event.timestamp.toProtoTime()
              entityKeys += protoEntityKeys
            }
          }
        }
      val ds = localDate.toString()
      logger.info("Writing Date: $ds")

      val impressionsBlobKey =
        if (flatOutputBasePath != null) {
          "$flatOutputBasePath/$ds/impressions"
        } else if (impressionsBasePath != null) {
          "$impressionsBasePath/ds/$ds/$eventGroupPath/impressions"
        } else {
          "ds/$ds/$eventGroupPath/impressions"
        }
      val impressionsFileUri = "$schema$impressionsBucket/$impressionsBlobKey"
      val encryptedStorage = run {
        val selectedStorageClient = SelectedStorageClient(impressionsFileUri, storagePath)

        val aeadStorageClient =
          selectedStorageClient.withEnvelopeEncryption(kmsClient, kekUri, serializedEncryptionKey)

        MesosRecordIoStorageClient(aeadStorageClient)
      }
      logger.info("Writing impressions to $impressionsFileUri")
      // Write impressions to storage
      encryptedStorage.writeBlob(
        impressionsBlobKey,
        labeledImpressions.map { it.toByteString() }.asFlow(),
      )
      val impressionsMetaDataBlobKey =
        if (flatOutputBasePath != null) {
          "$flatOutputBasePath/$ds/metadata.binpb"
        } else if (impressionsBasePath != null) {
          "$impressionsBasePath/ds/$ds/$eventGroupPath/metadata.binpb"
        } else {
          "ds/$ds/$eventGroupPath/metadata.binpb"
        }

      val impressionsMetadataFileUri =
        "$schema$impressionsMetadataBucket/$impressionsMetaDataBlobKey"

      logger.info("Writing metadata to $impressionsMetadataFileUri")

      // Create the impressions metadata store
      val impressionsMetadataStorageClient =
        SelectedStorageClient(impressionsMetadataFileUri, storagePath)

      val zoneId = ZoneOffset.UTC
      val startOfDay = localDate.atStartOfDay(zoneId).toInstant().toProtoTime()
      val endOfDay = localDate.plusDays(1).atStartOfDay(zoneId).toInstant().toProtoTime()

      val blobDetails = blobDetails {
        this.blobUri = impressionsFileUri
        this.encryptedDek = encryptedDek
        this.eventGroupReferenceId = this@ImpressionsWriter.eventGroupReferenceId
        this.interval = interval {
          startTime = startOfDay
          endTime = endOfDay
        }
        this.modelLine = modelLineName
      }
      impressionsMetadataStorageClient.writeBlob(
        impressionsMetaDataBlobKey,
        blobDetails.toByteString(),
      )
    }
  }

  companion object {
    private val logger: Logger = Logger.getLogger(this::class.java.name)
  }
}

private fun EntityKey.toProto(): LabeledImpression.EntityKey =
  LabeledImpressionKt.entityKey {
    entityType = this@toProto.entityType
    entityId = this@toProto.entityId
  }
