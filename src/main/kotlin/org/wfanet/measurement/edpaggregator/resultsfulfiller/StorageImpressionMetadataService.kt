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
import com.google.crypto.tink.proto.AesGcmHkdfStreamingParams
import com.google.crypto.tink.proto.HashType
import com.google.crypto.tink.proto.KeyData
import com.google.crypto.tink.proto.KeyStatusType
import com.google.crypto.tink.proto.Keyset
import com.google.crypto.tink.proto.OutputPrefixType
import com.google.crypto.tink.proto.AesGcmHkdfStreamingKey
import com.google.crypto.tink.BinaryKeysetReader
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
import org.wfanet.measurement.edpaggregator.v1alpha.EncryptedDek
import org.wfanet.measurement.storage.SelectedStorageClient
import org.wfanet.measurement.edpaggregator.v1alpha.HashType as EdpAggregatorHashType
import org.wfanet.measurement.edpaggregator.v1alpha.EncryptionKey
import java.security.SecureRandom
import kotlin.math.absoluteValue

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
    kmsClient: KmsClient
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
    // TODO(world-federation-of-advertisers/cross-media-measurement#2948): Determine blob parsing logic based on file extension
    return try {
      BlobDetails.parseFrom(bytes)
    } catch (e: com.google.protobuf.InvalidProtocolBufferException) {
      val builder = BlobDetails.newBuilder()
      JsonFormat.parser().ignoringUnknownFields()
        .merge(bytes.toString(Charsets.UTF_8), builder)
      builder.build()
    }

  }

  private fun EdpAggregatorHashType.mapHashTypeCloneToTink(): HashType =
    when (this) {
      EdpAggregatorHashType.SHA1 -> HashType.SHA1
      EdpAggregatorHashType.SHA256 -> HashType.SHA256
      EdpAggregatorHashType.SHA512 -> HashType.SHA512
      else -> throw IllegalArgumentException("Unsupported hkdf_hash_type: $this")
    }

  private fun synthesizeEncryptedKeyset(
    encryptionKey: EncryptionKey,
    kekUri: String,
    kmsClient: KmsClient
  ): ByteString {
    val kmsAead = kmsClient.getAead(kekUri)

    val aesKey = when (encryptionKey.keyCase) {
      EncryptionKey.KeyCase.AES_GCM_HKDF_STREAMING_KEY ->
        encryptionKey.aesGcmHkdfStreamingKey
      EncryptionKey.KeyCase.KEY_NOT_SET ->
        throw IllegalArgumentException("EncryptionKey has no key_type set")
    }

    val rawKeyBytes = aesKey.keyValue.toByteArray()

    // Map params into a proper Tink AesGcmHkdfStreamingParams
    val params = AesGcmHkdfStreamingParams.newBuilder()
      .setDerivedKeySize(aesKey.params.derivedKeySize)
      .setHkdfHashType(aesKey.params.hkdfHashType.mapHashTypeCloneToTink())
      .setCiphertextSegmentSize(aesKey.params.ciphertextSegmentSize)
      .build()

    val tinkKey = AesGcmHkdfStreamingKey.newBuilder()
      .setParams(params)
      .setKeyValue(ByteString.copyFrom(rawKeyBytes))
      .setVersion(aesKey.version)
      .build()


    val keyData = KeyData.newBuilder()
      .setTypeUrl("type.googleapis.com/google.crypto.tink.AesGcmHkdfStreamingKey")
      .setKeyMaterialType(KeyData.KeyMaterialType.SYMMETRIC)
      .setValue(tinkKey.toByteString())
      .build()

    val keyId = SecureRandom().nextInt().absoluteValue
    val ks = Keyset.newBuilder()
      .setPrimaryKeyId(keyId)
      .addKey(
        Keyset.Key.newBuilder()
          .setKeyId(keyId)
          .setStatus(KeyStatusType.ENABLED)
          .setOutputPrefixType(OutputPrefixType.RAW)
          .setKeyData(keyData)
      )
      .build()

    val handle = CleartextKeysetHandle.read(
      BinaryKeysetReader.withBytes(ks.toByteArray())
    )

    // Encrypt the keyset again with KMS
    val enc = TinkProtoKeysetFormat.serializeEncryptedKeyset(
      handle,
      kmsAead,
      byteArrayOf()
    )

    return ByteString.copyFrom(enc)

  }

  private fun BlobDetails.normalizeDek(kmsClient: KmsClient): BlobDetails {
    val ed = this.encryptedDek
    return when (ed.protobufFormat) {
      EncryptedDek.ProtobufFormat.BINARY -> this
      EncryptedDek.ProtobufFormat.JSON -> {
        // 1. Decrypt ciphertext to get EncryptionKey (JSON)
        val kmsAead = kmsClient.getAead(ed.kekUri)
        val decrypted = kmsAead.decrypt(ed.ciphertext.toByteArray(), byteArrayOf())

        val encryptionKey = EncryptionKey.newBuilder()
        when (ed.protobufFormat) {
          EncryptedDek.ProtobufFormat.JSON -> {
            JsonFormat.parser().ignoringUnknownFields()
              .merge(decrypted.toString(Charsets.UTF_8), encryptionKey)
          }
          else -> throw IllegalArgumentException("Unsupported protobuf_format")
        }

        val encKs = synthesizeEncryptedKeyset(encryptionKey.build(), ed.kekUri, kmsClient)
        this.toBuilder()
          .setEncryptedDek(ed.toBuilder()
            .setCiphertext(encKs)
            .setProtobufFormat(EncryptedDek.ProtobufFormat.BINARY)
            .build()
          )
          .build()
      }
      else -> throw IllegalArgumentException("Unsupported protobuf_format: ${ed.protobufFormat}")
    }
  }


  companion object {
    private val logger: Logger = Logger.getLogger(this::class.java.name)
  }
}
