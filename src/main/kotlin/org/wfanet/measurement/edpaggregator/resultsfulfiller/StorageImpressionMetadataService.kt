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

import com.google.crypto.tink.CleartextKeysetHandle
import com.google.crypto.tink.KmsClient
import com.google.crypto.tink.TinkProtoKeysetFormat
import com.google.protobuf.ByteString
import com.google.protobuf.util.JsonFormat
import com.google.type.Interval
import com.google.type.interval
import java.time.LocalDate
import java.time.ZoneId
import java.time.ZoneOffset
import java.util.logging.Logger
import kotlin.streams.asSequence
import org.wfanet.measurement.common.flatten
import org.wfanet.measurement.common.toInstant
import org.wfanet.measurement.common.toProtoTime
import org.wfanet.measurement.edpaggregator.StorageConfig
import org.wfanet.measurement.edpaggregator.v1alpha.BlobDetails
import org.wfanet.measurement.storage.SelectedStorageClient

/**
 * Storage-backed [ImpressionMetadataService].
 *
 * Resolves per-day metadata paths, reads [BlobDetails] messages from storage, and emits sources
 * with UTC day intervals. Date expansion uses [zoneIdForDates].
 *
 * @param impressionsMetadataStorageConfig configuration for metadata storage
 * @param impressionsBlobDetailsUriPrefix the URI prefix for the metadata paths
 * @param zoneIdForDates the zone used to determine date boundaries
 */
class StorageImpressionMetadataService(
  private val impressionsMetadataStorageConfig: StorageConfig,
  private val impressionsBlobDetailsUriPrefix: String,
  private val zoneIdForDates: ZoneId = ZoneOffset.UTC,
) : ImpressionMetadataService {

  /**
   * Lists impression data sources for an event group within a period.
   *
   * Expands [period] to per-day boundaries using [zoneIdForDates], reads `BlobDetails` for each day
   * from storage, and returns a source per day with UTC closed-open intervals [start, end).
   *
   * @param eventGroupReferenceId event group reference identifier.
   * @param period closed-open time interval in UTC.
   * @return sources covering the requested period; empty if the period maps to no dates.
   * @throws ImpressionReadException if a required metadata blob does not exist.
   * @throws com.google.protobuf.InvalidProtocolBufferException if a metadata blob is present but
   *   contains invalid `BlobDetails`.
   */
  override suspend fun listImpressionDataSources(
    modelLine: String,
    eventGroupReferenceId: String,
    period: Interval,
  ): List<ImpressionDataSource> {
    val startDate = LocalDate.ofInstant(period.startTime.toInstant(), zoneIdForDates)
    val endDateInclusive =
      LocalDate.ofInstant(period.endTime.toInstant().minusSeconds(1), zoneIdForDates)
    if (startDate.isAfter(endDateInclusive)) return emptyList()

    val dates = startDate.datesUntil(endDateInclusive.plusDays(1)).asSequence().toList()

    return dates.map { date ->
      val path = resolvePath(modelLine, date, eventGroupReferenceId)
      val blobDetails = readBlobDetails(path).normalizeDek(kmsClient)

      val dayStartUtc = date.atStartOfDay(ZoneOffset.UTC).toInstant()
      val dayEndUtc = date.plusDays(1).atStartOfDay(ZoneOffset.UTC).toInstant()

      ImpressionDataSource(
        modelLine = modelLine,
        eventGroupReferenceId = eventGroupReferenceId,
        interval =
          interval {
            startTime = dayStartUtc.toProtoTime()
            endTime = dayEndUtc.toProtoTime()
          },
        blobDetails = blobDetails,
      )
    }
  }

  /**
   * Resolve a path to a blob details protobuf record.
   *
   * @param modelLine the model line
   * @param date the of the event data
   * @param eventGroupReferenceId referenced event group
   */
  fun resolvePath(modelLine: String, date: LocalDate, eventGroupReferenceId: String): String {
    val prefix =
      if (impressionsBlobDetailsUriPrefix.endsWith('/')) impressionsBlobDetailsUriPrefix
      else "$impressionsBlobDetailsUriPrefix/"
    return "${prefix}ds/$date/model-line/${modelLine}/event-group-reference-id/$eventGroupReferenceId/metadata"
  }

  /**
   * Reads `BlobDetails` from a metadata blob.
   *
   * @param metadataPath blob URI for the metadata (e.g., `file:///bucket/key`).
   * @return parsed [BlobDetails].
   * @throws ImpressionReadException with [ImpressionReadException.Code.BLOB_NOT_FOUND] when the
   *   blob is missing.
   * @throws com.google.protobuf.InvalidProtocolBufferException if parsing fails due to invalid
   *   metadata contents.
   */
  private suspend fun readBlobDetails(metadataPath: String): BlobDetails {
    val storageClientUri = SelectedStorageClient.parseBlobUri(metadataPath)
    val storageClient =
      SelectedStorageClient(
        storageClientUri,
        impressionsMetadataStorageConfig.rootDirectory,
        impressionsMetadataStorageConfig.projectId,
      )
    logger.info("Reading impression metadata from $metadataPath")
    val blob =
      storageClient.getBlob(storageClientUri.key)
        ?: throw ImpressionReadException(
          metadataPath,
          ImpressionReadException.Code.BLOB_NOT_FOUND,
          "BlobDetails metadata not found",
        )

    val bytes: ByteString = blob.read().flatten()
    return if (metadataPath.endsWith(JSON_SUFFIX, ignoreCase = true)) {
      val json = bytes.toStringUtf8()
      val builder = BlobDetails.newBuilder()
      JsonFormat.parser().ignoringUnknownFields().merge(json, builder)
      builder.build()
    } else {
      BlobDetails.parseFrom(bytes)
    }

  }

  private fun mapHashTypeCloneToTink(t: wfa.measurement.common.crypto.AesGcmHkdfStreamingParams.HashTypeClone): HashType =
    when (t) {
      wfa.measurement.common.crypto.AesGcmHkdfStreamingParams.HashTypeClone.SHA1 -> HashType.SHA1
      wfa.measurement.common.crypto.AesGcmHkdfStreamingParams.HashTypeClone.SHA256 -> HashType.SHA256
      wfa.measurement.common.crypto.AesGcmHkdfStreamingParams.HashTypeClone.SHA512 -> HashType.SHA512
      else -> throw IllegalArgumentException("Unsupported hkdf_hash_type: $t")
    }

  private fun synthesizeEncryptedKeyset(
    clone: wfa.measurement.common.crypto.AesGcmHkdfStreamingKey,
    kekUri: String,
    kmsClient: KmsClient
  ): ByteString {
    val kmsAead = kmsClient.getAead(kekUri)

    val rawKeyBytes = kmsAead.decrypt(clone.keyValue.toByteArray(), byteArrayOf())

    val params = AesGcmHkdfStreamingParams.newBuilder()
      .setDerivedKeySize(clone.params.derivedKeySize)
      .setHkdfHashType(mapHashTypeCloneToTink(clone.params.hkdfHashType))
      .setCiphertextSegmentSize(clone.params.ciphertextSegmentSize)
      .setFirstSegmentOffset(clone.params.firstSegmentOffset) 
      .build()

    val tinkKey = AesGcmHkdfStreamingKey.newBuilder()
      .setParams(params)
      .setKeyValue(ByteString.copyFrom(rawKeyBytes))
      .setVersion(clone.version)
      .build()

    val keyData = KeyData.newBuilder()
      .setTypeUrl("type.googleapis.com/google.crypto.tink.AesGcmHkdfStreamingKey")
      .setKeyMaterialType(KeyMaterialType.SYMMETRIC)
      .setValue(tinkKey.toByteString())
      .build()

    val keyId = SecureRandom().nextInt().absoluteValue
    val ks = Keyset.newBuilder()
      .setPrimaryKeyId(keyId)
      .addKey(
        Key.newBuilder()
          .setKeyId(keyId)
          .setStatus(KeyStatusType.ENABLED)
          .setOutputPrefixType(OutputPrefixType.RAW)
          .setKeyData(keyData)
      )
      .build()

    val handle = CleartextKeysetHandle.read(
      com.google.crypto.tink.BinaryKeysetReader.withBytes(ks.toByteArray())
    )
    val enc = TinkProtoKeysetFormat.serializeEncryptedKeyset(handle, kmsAead, /* aad = */ byteArrayOf())
    return ByteString.copyFrom(enc)
  }

  private fun BlobDetails.normalizeDek(kmsClient: KmsClient): BlobDetails {
    val ed = this.encryptedDek
    return when {
      ed.hasEncryptedDek() -> this
      ed.hasAesGcmHkdfStreamingKey() -> {
        val encKs = synthesizeEncryptedKeyset(ed.aesGcmHkdfStreamingKey, ed.kekUri, kmsClient)
        this.toBuilder()
          .setEncryptedDek(ed.toBuilder()
            .clearAesGcmHkdfStreamingKey()
            .setEncryptedDek(encKs)
            .build()
          )
          .build()
      }
      else -> throw IllegalArgumentException("EncryptedDek is missing both encrypted_dek and aes_gcm_hkdf_streaming_key")
    }
  }


  companion object {
    private val logger: Logger = Logger.getLogger(this::class.java.name)
    private const val JSON_SUFFIX: String  = ".json"
  }
}
