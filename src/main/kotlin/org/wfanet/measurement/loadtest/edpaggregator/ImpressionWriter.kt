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

package org.wfanet.measurement.loadtest.edpaggregator

import com.google.crypto.tink.KeyTemplates
import com.google.crypto.tink.KeysetHandle
import com.google.crypto.tink.KmsClient
import com.google.crypto.tink.TinkProtoKeysetFormat
import com.google.protobuf.ByteString
import com.google.protobuf.kotlin.toByteString
import java.io.File
import java.time.LocalDate
import java.util.logging.Logger
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.runBlocking
import org.wfanet.measurement.common.crypto.tink.withEnvelopeEncryption
import org.wfanet.measurement.edpaggregator.v1alpha.EncryptedDek
import org.wfanet.measurement.edpaggregator.v1alpha.LabeledImpression
import org.wfanet.measurement.edpaggregator.v1alpha.blobDetails
import org.wfanet.measurement.storage.MesosRecordIoStorageClient
import org.wfanet.measurement.storage.SelectedStorageClient

class ImpressionsWriter(
  private val eventGroupReferenceId: String,
  private val kekUri: String,
  private val kmsClient: KmsClient,
  private val bucket: String,
  private val storagePath: File? = null,
  private val schema: String = "file:///",
) {

  /*
 * Takes a Flow<Pair<LocalDate, Flow<LabeledImpression>>>, encrypts that data with a KMS,
 * and outputs the data to storage along with the necessary metadata for a the ResultsFulfiller
 * to be able to find and read the contents.
 */
  suspend fun writeLabeledImpressionData(
    events: Flow<Pair<LocalDate, Flow<LabeledImpression>>>,
  ) {
    // Set up streaming encryption
    val tinkKeyTemplateType = "AES128_GCM_HKDF_1MB"
    val aeadKeyTemplate = KeyTemplates.get(tinkKeyTemplateType)
    val keyEncryptionHandle = KeysetHandle.generateNew(aeadKeyTemplate)
    val serializedEncryptionKey =
      ByteString.copyFrom(
        TinkProtoKeysetFormat.serializeEncryptedKeyset(
          keyEncryptionHandle,
          kmsClient.getAead(kekUri),
          byteArrayOf(),
        )
      )
    val encryptedDek =
      EncryptedDek.newBuilder().setKekUri(kekUri).setEncryptedDek(serializedEncryptionKey).build()

    val storageMap: MutableMap<String, MesosRecordIoStorageClient> = mutableMapOf()

    var currentDsEvents = mutableListOf<LabeledImpression>()
    var currentDs: String? = null
    events.collect { (localDate: LocalDate, labeledImpressions: Flow<LabeledImpression>) ->
      val ds = localDate.toString()
      logger.info("Writing Date: $ds")

      val impressionsBlobKey =
        "ds/$currentDs/event-group-reference-id/$eventGroupReferenceId/impressions"
      val impressionsFileUri = "$schema$bucket/$impressionsBlobKey"
      val mesosRecordIoStorageClient = run {
        val impressionsStorageClient = SelectedStorageClient(impressionsFileUri, storagePath)

        val aeadStorageClient =
          impressionsStorageClient.withEnvelopeEncryption(
            kmsClient,
            kekUri,
            serializedEncryptionKey,
          )

        // Wrap aead client in mesos client
        val mesosRecordIoStorageClient = MesosRecordIoStorageClient(aeadStorageClient)
        storageMap.set(impressionsBlobKey, mesosRecordIoStorageClient)
        mesosRecordIoStorageClient
      }
      logger.info("Writing impressions to $impressionsBlobKey")
      // Write impressions to storage
      runBlocking {
        mesosRecordIoStorageClient.writeBlob(
          impressionsBlobKey,
          labeledImpressions.map { it.toByteString() },
        )
      }
      val impressionsMetaDataBlobKey =
        "ds/$ds/event-group-reference-id/$eventGroupReferenceId/metadata"

      val impressionsMetadataFileUri = "$schema$bucket/$impressionsMetaDataBlobKey"

      logger.info("Writing metadata to $impressionsMetadataFileUri")

      // Create the impressions metadata store
      val impressionsMetadataStorageClient =
        SelectedStorageClient(impressionsMetadataFileUri, storagePath)

      val blobDetails = blobDetails {
        this.blobUri = impressionsFileUri
        this.encryptedDek = encryptedDek
      }
      runBlocking {
        impressionsMetadataStorageClient.writeBlob(
          impressionsMetaDataBlobKey,
          blobDetails.toByteString(),
        )
      }
      currentDs = ds
      currentDsEvents.clear()
    }
  }

  companion object {
    private val logger: Logger = Logger.getLogger(this::class.java.name)
  }
}
