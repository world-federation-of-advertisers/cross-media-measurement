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
import com.google.protobuf.ByteString
import com.google.protobuf.Message
import java.io.File
import java.time.LocalDate
import java.util.concurrent.atomic.AtomicLong
import java.util.logging.Logger
import kotlin.time.TimeSource
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.flow.withIndex
import org.wfanet.measurement.common.crypto.tink.withEnvelopeEncryption
import org.wfanet.measurement.common.toProtoTime
import org.wfanet.measurement.edpaggregator.EncryptedStorage
import org.wfanet.measurement.edpaggregator.v1alpha.EncryptedDek
import org.wfanet.measurement.edpaggregator.v1alpha.LabeledImpression
import org.wfanet.measurement.edpaggregator.v1alpha.blobDetails
import org.wfanet.measurement.edpaggregator.v1alpha.labeledImpression
import org.wfanet.measurement.loadtest.dataprovider.LabeledEvent
import org.wfanet.measurement.loadtest.dataprovider.LabeledEventDateShardFlow
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

  /*
   * Takes a Flow<DateShardedLabeledImpression<T>>, encrypts that data with a KMS,
   * and outputs the data to storage along with the necessary metadata for the ResultsFulfiller
   * to be able to find and read the contents.
   *
   * Date shards are processed in parallel for better performance.
   */
  suspend fun <T : Message> writeLabeledImpressionData(events: Flow<LabeledEventDateShardFlow<T>>) {
    val serializedEncryptionKey =
      EncryptedStorage.generateSerializedEncryptionKey(kmsClient, kekUri, "AES128_GCM_HKDF_1MB")
    val encryptedDek =
      EncryptedDek.newBuilder().setKekUri(kekUri).setEncryptedDek(serializedEncryptionKey).build()

    // Collect all date shards first to enable parallel processing
    val dateShards = events.toList()
    logger.info("Processing ${dateShards.size} date shards in parallel")

    // Process all date shards in parallel
    coroutineScope {
      dateShards
        .map { (localDate: LocalDate, labeledEvents: Flow<LabeledEvent<T>>) ->
          async {
            processSingleDateShard(localDate, labeledEvents, serializedEncryptionKey, encryptedDek)
          }
        }
        .awaitAll()
    }
  }

  private suspend fun <T : Message> processSingleDateShard(
    localDate: LocalDate,
    labeledEvents: Flow<LabeledEvent<T>>,
    serializedEncryptionKey: ByteString,
    encryptedDek: EncryptedDek,
  ) {
    val eventCounter = AtomicLong(0)
    val ds = localDate.toString()
    val startTime = TimeSource.Monotonic.markNow()
    var lastLogTime = startTime

    val labeledImpressions: Flow<LabeledImpression> =
      labeledEvents.withIndex().map { (index, labeledEvent): IndexedValue<LabeledEvent<T>> ->
        val count = eventCounter.incrementAndGet()

        // Log progress every 1000 events
        if (count % 1000 == 0L) {
          val currentTime = TimeSource.Monotonic.markNow()
          val elapsed = currentTime - startTime
          val intervalElapsed = currentTime - lastLogTime
          val eventsPerSecond =
            if (elapsed.inWholeMilliseconds > 0) {
              (count * 1000.0 / elapsed.inWholeMilliseconds).format(2)
            } else "N/A"
          val intervalEventsPerSecond =
            if (intervalElapsed.inWholeMilliseconds > 0) {
              (1000.0 * 1000.0 / intervalElapsed.inWholeMilliseconds).format(2)
            } else "N/A"

          logger.info(
            "Date $ds: Processed $count events (avg: $eventsPerSecond events/sec, current: $intervalEventsPerSecond events/sec)"
          )
          lastLogTime = currentTime
        }

        labeledImpression {
          vid = labeledEvent.vid
          event = Any.pack(labeledEvent.message)
          eventTime = labeledEvent.timestamp.toProtoTime()
        }
      }
    logger.info("Starting to write date: $ds")

    val impressionsBlobKey = "ds/$ds/$eventGroupPath/impressions"
    val impressionsFileUri = "$schema$impressionsBucket/$impressionsBlobKey"
    val encryptedStorage = run {
      val selectedStorageClient = SelectedStorageClient(impressionsFileUri, storagePath)

      val aeadStorageClient =
        selectedStorageClient.withEnvelopeEncryption(kmsClient, kekUri, serializedEncryptionKey)

      MesosRecordIoStorageClient(aeadStorageClient)
    }
    logger.info("Writing impressions to $impressionsFileUri")
    // Write impressions to storage with progress tracking
    val writeStartTime = TimeSource.Monotonic.markNow()
    var lastWriteLogTime = writeStartTime
    val impressionBytesFlow =
      labeledImpressions.withIndex().map { (index, impression) ->
        val count = index + 1
        // Log write progress every 1000 impressions
        if (count % 1000000 == 0) {
          val currentTime = TimeSource.Monotonic.markNow()
          val elapsed = currentTime - writeStartTime
          val intervalElapsed = currentTime - lastWriteLogTime
          val writesPerSecond =
            if (elapsed.inWholeMilliseconds > 0) {
              (count * 1000.0 / elapsed.inWholeMilliseconds).format(2)
            } else "N/A"
          val intervalWritesPerSecond =
            if (intervalElapsed.inWholeMilliseconds > 0) {
              (1000.0 * 1000.0 / intervalElapsed.inWholeMilliseconds).format(2)
            } else "N/A"

          logger.info(
            "Date $ds: Written $count impressions to storage (avg: $writesPerSecond/sec, current: $intervalWritesPerSecond/sec)"
          )
          lastWriteLogTime = currentTime
        }
        impression.toByteString()
      }
    encryptedStorage.writeBlob(impressionsBlobKey, impressionBytesFlow)
    val impressionsMetaDataBlobKey = "ds/$ds/$eventGroupPath/metadata"

    val impressionsMetadataFileUri = "$schema$impressionsMetadataBucket/$impressionsMetaDataBlobKey"

    logger.info("Writing metadata to $impressionsMetadataFileUri")

    // Create the impressions metadata store
    val impressionsMetadataStorageClient =
      SelectedStorageClient(impressionsMetadataFileUri, storagePath)

    val blobDetails = blobDetails {
      this.blobUri = impressionsFileUri
      this.encryptedDek = encryptedDek
      this.eventGroupReferenceId = this@ImpressionsWriter.eventGroupReferenceId
    }
    impressionsMetadataStorageClient.writeBlob(
      impressionsMetaDataBlobKey,
      blobDetails.toByteString(),
    )

    val totalEvents = eventCounter.get()
    val totalElapsed = TimeSource.Monotonic.markNow() - startTime
    val overallEventsPerSecond =
      if (totalElapsed.inWholeMilliseconds > 0) {
        (totalEvents * 1000.0 / totalElapsed.inWholeMilliseconds).format(2)
      } else "N/A"
    logger.info(
      "Completed writing date: $ds - Total: $totalEvents events in ${totalElapsed.inWholeSeconds}s ($overallEventsPerSecond events/sec)"
    )
  }

  companion object {
    private val logger: Logger = Logger.getLogger(this::class.java.name)

    private fun Double.format(decimals: Int): String = "%.${decimals}f".format(this)
  }
}
