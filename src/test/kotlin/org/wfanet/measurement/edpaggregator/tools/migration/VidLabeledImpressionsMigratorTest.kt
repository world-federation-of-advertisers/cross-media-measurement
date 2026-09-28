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

package org.wfanet.measurement.edpaggregator.tools.migration

import com.google.cloud.storage.BlobId
import com.google.cloud.storage.BlobInfo
import com.google.cloud.storage.Storage
import com.google.common.truth.Truth.assertThat
import com.google.crypto.tink.Aead
import com.google.crypto.tink.KeyTemplates
import com.google.crypto.tink.KeysetHandle
import com.google.crypto.tink.KmsClient
import com.google.crypto.tink.RegistryConfiguration
import com.google.crypto.tink.aead.AeadConfig
import com.google.crypto.tink.streamingaead.StreamingAeadConfig
import com.google.protobuf.ByteString
import com.google.protobuf.timestamp
import com.google.protobuf.util.JsonFormat
import com.google.type.interval
import java.time.LocalDate
import kotlin.test.assertFailsWith
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.runBlocking
import org.junit.After
import org.junit.Before
import org.junit.ClassRule
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.common.crypto.tink.withEnvelopeEncryption
import org.wfanet.measurement.common.flatten
import org.wfanet.measurement.common.testing.CommandLineTesting
import org.wfanet.measurement.common.testing.ExitInterceptingSecurityManager
import org.wfanet.measurement.edpaggregator.EncryptedStorage
import org.wfanet.measurement.edpaggregator.tools.migration.VidLabeledImpressionsMigrator.DateStatus
import org.wfanet.measurement.edpaggregator.v1alpha.BlobDetails
import org.wfanet.measurement.edpaggregator.v1alpha.EncryptedDek
import org.wfanet.measurement.edpaggregator.v1alpha.blobDetails
import org.wfanet.measurement.edpaggregator.v1alpha.encryptedDek
import org.wfanet.measurement.edpaggregator.vidlabeler.LabeledImpressionsBlobKeys
import org.wfanet.measurement.gcloud.gcs.GcsStorageClient
import org.wfanet.measurement.gcloud.gcs.testing.StorageEmulatorRule

@RunWith(JUnit4::class)
class VidLabeledImpressionsMigratorTest {
  private lateinit var storage: Storage
  private lateinit var logs: MutableList<String>
  private lateinit var migrator: VidLabeledImpressionsMigrator
  private lateinit var kmsClient: KmsClient
  private lateinit var encryptedDek: EncryptedDek
  private lateinit var nonStreamingEncryptedDek: EncryptedDek

  @Before
  fun setUp() {
    AeadConfig.register()
    StreamingAeadConfig.register()
    storageEmulator.createBucket(BUCKET)
    storage = storageEmulator.storage
    val kekKeyset = KeysetHandle.generateNew(KeyTemplates.get("AES128_GCM"))
    val kekAead = kekKeyset.getPrimitive(RegistryConfiguration.get(), Aead::class.java)
    kmsClient =
      object : KmsClient {
        override fun doesSupport(keyUri: String?): Boolean = keyUri == KEK_URI

        override fun withCredentials(credentialPath: String?): KmsClient = this

        override fun withDefaultCredentials(): KmsClient = this

        override fun getAead(keyUri: String?): Aead {
          require(doesSupport(keyUri)) { "Unsupported URI: $keyUri" }
          return kekAead
        }
      }
    val serializedEncryptionKey =
      EncryptedStorage.generateSerializedEncryptionKey(kmsClient, KEK_URI, "AES128_GCM_HKDF_1MB")
    encryptedDek = encryptedDek {
      kekUri = KEK_URI
      ciphertext = serializedEncryptionKey
      typeUrl = "type.googleapis.com/google.crypto.tink.Keyset"
      protobufFormat = EncryptedDek.ProtobufFormat.BINARY
    }
    val serializedAeadKey =
      EncryptedStorage.generateSerializedEncryptionKey(kmsClient, KEK_URI, "AES128_GCM")
    nonStreamingEncryptedDek = encryptedDek {
      kekUri = KEK_URI
      ciphertext = serializedAeadKey
      typeUrl = "type.googleapis.com/google.crypto.tink.Keyset"
      protobufFormat = EncryptedDek.ProtobufFormat.BINARY
    }
    logs = mutableListOf()
    migrator = VidLabeledImpressionsMigrator(storage, { kmsClient }, logs::add)
  }

  @After
  fun tearDown() {
    storageEmulator.deleteBucketRecursive(BUCKET)
  }

  @Test
  fun `migrate copies inclusive range and rewrites binary metadata`() {
    val firstDate = LocalDate.parse("2026-06-01")
    val secondDate = firstDate.plusDays(1)
    val outsideDate = secondDate.plusDays(1)
    val first = writeSourceDate(firstDate, "first", byteArrayOf(1, 2, 3))
    val second = writeSourceDate(secondDate, "second", byteArrayOf(4, 5))
    writeSourceDate(outsideDate, "outside", byteArrayOf(6))

    val summary = migrate(request(firstDate, secondDate))

    assertThat(summary.copiedDates).isEqualTo(2)
    assertThat(summary.copiedDataBlobs).isEqualTo(2)
    assertThat(summary.writtenMetadataBlobs).isEqualTo(2)
    assertThat(summary.writtenDoneMarkers).isEqualTo(2)
    assertThat(summary.copiedBytes).isEqualTo(first.encryptedSize + second.encryptedSize)
    assertCopied(firstDate, first)
    assertCopied(secondDate, second)
    assertThat(listDestination(outsideDate)).isEmpty()
  }

  @Test
  fun `migrate parses JSON metadata and writes binary metadata`() {
    val date = LocalDate.parse("2026-06-01")
    val source = writeSourceDate(date, "json", byteArrayOf(7, 8), jsonMetadata = true)

    val summary = migrate(request(date, date))

    assertThat(summary.copiedDates).isEqualTo(1)
    assertCopied(date, source)
  }

  @Test
  fun `migrate skips missing source date`() {
    val date = LocalDate.parse("2026-06-01")

    val summary = migrate(request(date, date))

    assertThat(summary.results.single().status).isEqualTo(DateStatus.SKIPPED_SOURCE_MISSING)
    assertThat(logs.single()).contains("does not exist")
  }

  @Test
  fun `migrate skips source date without done marker`() {
    val date = LocalDate.parse("2026-06-01")
    writeSourceDate(date, "incomplete", byteArrayOf(1), writeDone = false)

    val summary = migrate(request(date, date))

    assertThat(summary.results.single().status).isEqualTo(DateStatus.SKIPPED_SOURCE_INCOMPLETE)
    assertThat(listDestination(date)).isEmpty()
  }

  @Test
  fun `migrate skips date when destination contains any object`() {
    val date = LocalDate.parse("2026-06-01")
    writeSourceDate(date, "source", byteArrayOf(1))
    val existing = "${destinationDatePrefix(date)}placeholder"
    writeObject(existing, byteArrayOf())

    val summary = migrate(request(date, date))

    assertThat(summary.results.single().status).isEqualTo(DateStatus.SKIPPED_DESTINATION_NOT_EMPTY)
    assertThat(listDestination(date).map { it.name }).containsExactly(existing)
  }

  @Test
  fun `migrate dry run validates source without writing destination`() {
    val date = LocalDate.parse("2026-06-01")
    val source = writeSourceDate(date, "source", byteArrayOf(1, 2, 3))

    val summary = migrate(request(date, date, dryRun = true))

    assertThat(summary.plannedDates).isEqualTo(1)
    assertThat(summary.plannedDataBlobs).isEqualTo(1)
    assertThat(summary.plannedBytes).isEqualTo(source.encryptedSize)
    assertThat(listDestination(date)).isEmpty()
  }

  @Test
  fun `migrate fails date when metadata has another model line`() {
    val date = LocalDate.parse("2026-06-01")
    writeSourceDate(date, "wrong-line", byteArrayOf(1), modelLine = DESTINATION_MODEL_LINE)

    val summary = migrate(request(date, date))

    assertThat(summary.results.single().status).isEqualTo(DateStatus.FAILED)
    assertThat(summary.results.single().message).contains("expected $SOURCE_MODEL_LINE")
    assertThat(listDestination(date)).isEmpty()
  }

  @Test
  fun `migrate fails date when metadata is malformed`() {
    val date = LocalDate.parse("2026-06-01")
    val sourceDatePrefix = sourceDatePrefix(date)
    writeObject("${sourceDatePrefix}invalid.metadata.binpb", byteArrayOf(1, 2, 3))
    writeObject("${sourceDatePrefix}done", byteArrayOf())

    val summary = migrate(request(date, date))

    assertThat(summary.results.single().status).isEqualTo(DateStatus.FAILED)
    assertThat(listDestination(date)).isEmpty()
  }

  @Test
  fun `migrate fails date when referenced impression blob is missing`() {
    val date = LocalDate.parse("2026-06-01")
    val sourceDatePrefix = sourceDatePrefix(date)
    val details = blobDetails("gs://$BUCKET/${sourceDatePrefix}missing")
    writeObject("${sourceDatePrefix}missing.metadata.binpb", details.toByteArray())
    writeObject("${sourceDatePrefix}done", byteArrayOf())

    val summary = migrate(request(date, date))

    assertThat(summary.results.single().status).isEqualTo(DateStatus.FAILED)
    assertThat(summary.results.single().message).contains("does not exist")
    assertThat(listDestination(date)).isEmpty()
  }

  @Test
  fun `migrate fails date when metadata interval is missing`() {
    val date = LocalDate.parse("2026-06-01")
    writeSourceDate(date, "missing-interval", byteArrayOf(1), includeInterval = false)

    val summary = migrate(request(date, date, dryRun = true))

    assertThat(summary.results.single().status).isEqualTo(DateStatus.FAILED)
    assertThat(summary.results.single().message).contains("interval")
    assertThat(listDestination(date)).isEmpty()
  }

  @Test
  fun `migrate fails date when metadata event identity is missing`() {
    val date = LocalDate.parse("2026-06-01")
    writeSourceDate(date, "missing-identity", byteArrayOf(1), includeEventIdentity = false)

    val summary = migrate(request(date, date, dryRun = true))

    assertThat(summary.results.single().status).isEqualTo(DateStatus.FAILED)
    assertThat(summary.results.single().message).contains("event_group_reference_id")
    assertThat(listDestination(date)).isEmpty()
  }

  @Test
  fun `migrate fails date when encrypted DEK is missing`() {
    val date = LocalDate.parse("2026-06-01")
    writeSourceDate(date, "missing-dek", byteArrayOf(1), includeEncryptedDek = false)

    val summary = migrate(request(date, date, dryRun = true))

    assertThat(summary.results.single().status).isEqualTo(DateStatus.FAILED)
    assertThat(summary.results.single().message).contains("encrypted_dek")
    assertThat(listDestination(date)).isEmpty()
  }

  @Test
  fun `migrate rejects non-streaming DEK before writing destination`() {
    val date = LocalDate.parse("2026-06-01")
    writeSourceDate(
      date,
      "non-streaming",
      byteArrayOf(1),
      sourceEncryptedDek = nonStreamingEncryptedDek,
    )

    val summary = migrate(request(date, date, dryRun = true))

    assertThat(summary.results.single().status).isEqualTo(DateStatus.FAILED)
    assertThat(summary.results.single().message).contains("non-streaming DEK")
    assertThat(listDestination(date)).isEmpty()
  }

  @Test
  fun `migrate rejects descending date range`() {
    val date = LocalDate.parse("2026-06-01")

    val exception =
      assertFailsWith<IllegalArgumentException> { migrate(request(date, date.minusDays(1))) }

    assertThat(exception).hasMessageThat().contains("startDate")
  }

  @Test
  fun `migrate requires compatibility acknowledgement`() {
    val date = LocalDate.parse("2026-06-01")

    val exception =
      assertFailsWith<IllegalArgumentException> {
        migrate(request(date, date).copy(modelLinesAreCompatible = false))
      }

    assertThat(exception).hasMessageThat().contains("acknowledgement")
  }

  @Test
  fun `summary reports copied skipped and failed dates`() {
    val firstDate = LocalDate.parse("2026-06-01")
    val secondDate = firstDate.plusDays(1)
    val thirdDate = secondDate.plusDays(1)
    writeSourceDate(firstDate, "copied", byteArrayOf(1))
    writeSourceDate(thirdDate, "wrong", byteArrayOf(2), modelLine = DESTINATION_MODEL_LINE)

    val summary = migrate(request(firstDate, thirdDate))

    assertThat(summary.toDisplayString()).contains("copied dates: 1")
    assertThat(summary.toDisplayString()).contains("skipped missing source dates: 1")
    assertThat(summary.toDisplayString()).contains("failed dates: 1")
  }

  @Test
  fun `main accepts documented flags and reports missing source`() {
    val date = LocalDate.parse("2026-06-01")

    val capturedOutput =
      CommandLineTesting.capturingOutput(
        arrayOf(
          "--start-date=$date",
          "--end-date=$date",
          "--source-model-line=$SOURCE_MODEL_LINE",
          "--destination-model-line=$DESTINATION_MODEL_LINE",
          "--source-date-prefix=gs://$BUCKET/$SOURCE_DATE_ROOT",
          "--destination-blob-prefix=gs://$BUCKET/$DESTINATION_BLOB_PREFIX",
          "--gcs-project=test-project",
          "--kms-wif-audience=//iam.googleapis.com/projects/123/locations/global/" +
            "workloadIdentityPools/pool/providers/provider",
          "--kms-service-account=edp-kms@example.iam.gserviceaccount.com",
          "--model-lines-are-compatible",
          "--dry-run",
          "--storage-api-endpoint=${storage.options.host}",
        ),
        ::main,
      )

    CommandLineTesting.assertThat(capturedOutput).status().isEqualTo(0)
    assertThat(listDestination(date)).isEmpty()
  }

  @Test
  fun `main rejects missing compatibility acknowledgement`() {
    val date = LocalDate.parse("2026-06-01")

    val capturedOutput =
      CommandLineTesting.capturingOutput(
        arrayOf(
          "--start-date=$date",
          "--end-date=$date",
          "--source-model-line=$SOURCE_MODEL_LINE",
          "--destination-model-line=$DESTINATION_MODEL_LINE",
          "--source-date-prefix=gs://$BUCKET/$SOURCE_DATE_ROOT",
          "--destination-blob-prefix=gs://$BUCKET/$DESTINATION_BLOB_PREFIX",
          "--kms-wif-audience=//iam.googleapis.com/projects/123/locations/global/" +
            "workloadIdentityPools/pool/providers/provider",
          "--kms-service-account=edp-kms@example.iam.gserviceaccount.com",
        ),
        ::main,
      )

    CommandLineTesting.assertThat(capturedOutput).status().isNotEqualTo(0)
  }

  private fun writeSourceDate(
    date: LocalDate,
    name: String,
    data: ByteArray,
    modelLine: String = SOURCE_MODEL_LINE,
    jsonMetadata: Boolean = false,
    writeDone: Boolean = true,
    includeInterval: Boolean = true,
    includeEventIdentity: Boolean = true,
    includeEncryptedDek: Boolean = true,
    sourceEncryptedDek: EncryptedDek = encryptedDek,
  ): SourceFixture {
    val datePrefix = sourceDatePrefix(date)
    val dataUri = "gs://$BUCKET/$datePrefix$name.recordio"
    val details =
      blobDetails(
        dataUri,
        modelLine,
        includeInterval,
        includeEventIdentity,
        includeEncryptedDek,
        sourceEncryptedDek,
      )
    val encryptedStorage =
      GcsStorageClient(storage, BUCKET)
        .withEnvelopeEncryption(kmsClient, sourceEncryptedDek.kekUri, sourceEncryptedDek.ciphertext)
    runBlocking {
      encryptedStorage.writeBlob("$datePrefix$name.recordio", flowOf(ByteString.copyFrom(data)))
    }
    val metadataName =
      if (jsonMetadata) "$datePrefix$name.metadata.json" else "$datePrefix$name.metadata.binpb"
    val metadata =
      if (jsonMetadata) JsonFormat.printer().print(details).toByteArray() else details.toByteArray()
    writeObject(metadataName, metadata)
    if (writeDone) {
      writeObject("${datePrefix}done", byteArrayOf())
    }
    val encryptedData = storage.readAllBytes(BlobId.of(BUCKET, "$datePrefix$name.recordio"))
    return SourceFixture(dataUri, data, details, encryptedData)
  }

  private fun blobDetails(
    dataUri: String,
    modelLine: String = SOURCE_MODEL_LINE,
    includeInterval: Boolean = true,
    includeEventIdentity: Boolean = true,
    includeEncryptedDek: Boolean = true,
    sourceEncryptedDek: EncryptedDek = encryptedDek,
  ): BlobDetails = blobDetails {
    blobUri = dataUri
    if (includeEncryptedDek) {
      encryptedDek = sourceEncryptedDek
    }
    if (includeEventIdentity) {
      eventGroupReferenceId = "event-group"
    }
    this.modelLine = modelLine
    if (includeInterval) {
      interval = interval {
        startTime = timestamp { seconds = 1 }
        endTime = timestamp { seconds = 2 }
      }
    }
  }

  private fun request(
    startDate: LocalDate,
    endDate: LocalDate,
    dryRun: Boolean = false,
  ): VidLabeledImpressionsMigrator.Request =
    VidLabeledImpressionsMigrator.Request(
      startDate = startDate,
      endDate = endDate,
      sourceModelLine = SOURCE_MODEL_LINE,
      destinationModelLine = DESTINATION_MODEL_LINE,
      sourceDatePrefix = "gs://$BUCKET/$SOURCE_DATE_ROOT",
      destinationBlobPrefix = "gs://$BUCKET/$DESTINATION_BLOB_PREFIX",
      modelLinesAreCompatible = true,
      dryRun = dryRun,
    )

  private fun migrate(
    request: VidLabeledImpressionsMigrator.Request
  ): VidLabeledImpressionsMigrator.Summary = runBlocking { migrator.migrate(request) }

  private fun assertCopied(date: LocalDate, source: SourceFixture) {
    val targetDataUri =
      LabeledImpressionsBlobKeys.forInputUri(
        "gs://$BUCKET/$DESTINATION_BLOB_PREFIX",
        source.dataUri,
        DESTINATION_MODEL_LINE,
        date,
      )
    val targetDataKey = targetDataUri.substringAfter("gs://$BUCKET/")
    val targetCiphertext = storage.readAllBytes(BlobId.of(BUCKET, targetDataKey))
    assertThat(targetCiphertext).isNotEqualTo(source.encryptedData)
    val encryptedStorage =
      GcsStorageClient(storage, BUCKET)
        .withEnvelopeEncryption(
          kmsClient,
          source.details.encryptedDek.kekUri,
          source.details.encryptedDek.ciphertext,
        )
    val decryptedData = runBlocking {
      requireNotNull(encryptedStorage.getBlob(targetDataKey)).read().flatten().toByteArray()
    }
    assertThat(decryptedData).isEqualTo(source.data)
    val targetDetails =
      BlobDetails.parseFrom(
        storage.readAllBytes(BlobId.of(BUCKET, "$targetDataKey.metadata.binpb"))
      )
    assertThat(targetDetails.blobUri).isEqualTo(targetDataUri)
    assertThat(targetDetails.modelLine).isEqualTo(DESTINATION_MODEL_LINE)
    assertThat(targetDetails.encryptedDek).isEqualTo(source.details.encryptedDek)
    assertThat(targetDetails.interval).isEqualTo(source.details.interval)
    assertThat(targetDetails.eventGroupReferenceId).isEqualTo(source.details.eventGroupReferenceId)
    assertThat(storage.get(BlobId.of(BUCKET, "${destinationDatePrefix(date)}done"))).isNotNull()
  }

  private fun sourceDatePrefix(date: LocalDate): String = "$SOURCE_DATE_ROOT/$date/"

  private fun destinationDatePrefix(date: LocalDate): String =
    "$DESTINATION_BLOB_PREFIX/model-line/$DESTINATION_MODEL_LINE_ID/$date/"

  private fun listDestination(date: LocalDate) =
    storage
      .list(BUCKET, Storage.BlobListOption.prefix(destinationDatePrefix(date)))
      .iterateAll()
      .toList()

  private fun writeObject(name: String, bytes: ByteArray) {
    storage.create(BlobInfo.newBuilder(BUCKET, name).build(), bytes)
  }

  private data class SourceFixture(
    val dataUri: String,
    val data: ByteArray,
    val details: BlobDetails,
    val encryptedData: ByteArray,
  ) {
    val encryptedSize: Long
      get() = encryptedData.size.toLong()
  }

  @Suppress("DEPRECATION") // CommandLineTesting intercepts commandLineMain exit calls this way.
  companion object {
    init {
      System.setSecurityManager(ExitInterceptingSecurityManager)
    }

    @get:JvmStatic @get:ClassRule val storageEmulator = StorageEmulatorRule()

    private const val BUCKET = "vid-labeled-migration-test"
    private const val SOURCE_DATE_ROOT = "existing/model-line/source"
    private const val DESTINATION_BLOB_PREFIX = "generated"
    private const val SOURCE_MODEL_LINE =
      "modelProviders/provider/modelSuites/suite/modelLines/source"
    private const val DESTINATION_MODEL_LINE_ID = "destination"
    private const val DESTINATION_MODEL_LINE =
      "modelProviders/provider/modelSuites/suite/modelLines/$DESTINATION_MODEL_LINE_ID"
    private const val KEK_URI =
      "gcp-kms://projects/test/locations/global/keyRings/ring/cryptoKeys/key"
  }
}
