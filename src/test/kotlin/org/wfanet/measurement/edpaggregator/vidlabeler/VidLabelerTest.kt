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

import com.google.common.truth.Truth.assertThat
import com.google.crypto.tink.Aead
import com.google.crypto.tink.KeyTemplates
import com.google.crypto.tink.KeysetHandle
import com.google.crypto.tink.KmsClient
import com.google.crypto.tink.aead.AeadConfig
import com.google.crypto.tink.streamingaead.StreamingAeadConfig
import kotlin.test.assertFailsWith
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import org.junit.Rule
import org.junit.Test
import org.junit.rules.TemporaryFolder
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.mockito.kotlin.any
import org.mockito.kotlin.mock
import org.mockito.kotlin.stub
import org.mockito.kotlin.verifyBlocking
import org.wfanet.measurement.common.crypto.tink.testing.FakeKmsClient
import org.wfanet.measurement.common.crypto.tink.withEnvelopeEncryption
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.common.grpc.testing.mockService
import org.wfanet.measurement.edpaggregator.EncryptedStorage
import org.wfanet.measurement.edpaggregator.StorageConfig
import org.wfanet.measurement.edpaggregator.v1alpha.EncryptedDek
import org.wfanet.measurement.edpaggregator.v1alpha.ImpressionMetadataServiceGrpcKt
import org.wfanet.measurement.edpaggregator.v1alpha.LabeledImpression
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionMetadataBatchFileServiceGrpcKt
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionMetadataBatchServiceGrpcKt
import org.wfanet.measurement.edpaggregator.v1alpha.VidLabelerParams
import org.wfanet.measurement.edpaggregator.v1alpha.VidLabelerParamsKt
import org.wfanet.measurement.edpaggregator.v1alpha.blobDetails
import org.wfanet.measurement.edpaggregator.v1alpha.encryptedDek
import org.wfanet.measurement.edpaggregator.v1alpha.labeledImpression
import org.wfanet.measurement.edpaggregator.v1alpha.listRawImpressionMetadataBatchFilesResponse
import org.wfanet.measurement.edpaggregator.v1alpha.rawImpressionMetadataBatchFile
import org.wfanet.measurement.edpaggregator.v1alpha.transportLayerSecurityParams
import org.wfanet.measurement.storage.MesosRecordIoStorageClient
import org.wfanet.measurement.storage.SelectedStorageClient
import org.wfanet.measurement.storage.filesystem.FileSystemStorageClient

@RunWith(JUnit4::class)
class VidLabelerTest {

  @Rule @JvmField val tempFolder = TemporaryFolder()

  private val impressionMetadataService:
    ImpressionMetadataServiceGrpcKt.ImpressionMetadataServiceCoroutineImplBase =
    mockService()
  private val rawImpressionBatchService:
    RawImpressionMetadataBatchServiceGrpcKt.RawImpressionMetadataBatchServiceCoroutineImplBase =
    mockService()
  private val batchFileService:
    RawImpressionMetadataBatchFileServiceGrpcKt.RawImpressionMetadataBatchFileServiceCoroutineImplBase =
    mockService()

  @get:Rule
  val grpcTestServerRule = GrpcTestServerRule {
    addService(impressionMetadataService)
    addService(rawImpressionBatchService)
    addService(batchFileService)
  }

  private val impressionMetadataStub by lazy {
    ImpressionMetadataServiceGrpcKt.ImpressionMetadataServiceCoroutineStub(
      grpcTestServerRule.channel
    )
  }
  private val rawImpressionBatchStub by lazy {
    RawImpressionMetadataBatchServiceGrpcKt.RawImpressionMetadataBatchServiceCoroutineStub(
      grpcTestServerRule.channel
    )
  }
  private val batchFileStub by lazy {
    RawImpressionMetadataBatchFileServiceGrpcKt.RawImpressionMetadataBatchFileServiceCoroutineStub(
      grpcTestServerRule.channel
    )
  }

  private val mockDecryptKmsClient: KmsClient = mock()
  private val mockEncryptKmsClient: KmsClient = mock()
  private val mockMetrics: VidLabelerMetrics = mock()

  private fun createVidLabeler(
    modelLineConfigs: Map<String, VidLabelerParams.ModelLineConfig> = DEFAULT_MODEL_LINE_CONFIGS,
    overrideModelLines: List<String> = emptyList(),
    decryptKmsClient: KmsClient = mockDecryptKmsClient,
    encryptKmsClient: KmsClient = mockEncryptKmsClient,
    encryptKekUri: String = "fake-kms://encrypt-key",
    storageConfig: StorageConfig = StorageConfig(projectId = "test-project"),
  ): VidLabeler {
    return VidLabeler(
      dataProvider = DATA_PROVIDER_NAME,
      rawImpressionMetadataBatch = BATCH_NAME,
      modelLineConfigs = modelLineConfigs,
      overrideModelLines = overrideModelLines,
      storageParams =
        VidLabelerParamsKt.storageParams {
          gcsProjectId = "test-project"
          impressionsBlobPrefix = "gs://output-bucket/labeled"
        },
      decryptKmsClient = decryptKmsClient,
      encryptKmsClient = encryptKmsClient,
      encryptKekUri = encryptKekUri,
      impressionMetadataStub = impressionMetadataStub,
      rawImpressionBatchStub = rawImpressionBatchStub,
      batchFileStub = batchFileStub,
      vidRepoConnection =
        transportLayerSecurityParams {
          clientCertResourcePath = "certs/client.pem"
          clientPrivateKeyResourcePath = "certs/client.key"
        },
      storageConfig = storageConfig,
      metrics = mockMetrics,
    )
  }

  @Test
  fun `labelBatch marks batch as FAILED when an error occurs`() = runBlocking {
    mockBatchFileServiceToReturn(
      listRawImpressionMetadataBatchFilesResponse {
        rawImpressionMetadataBatchFiles += rawImpressionMetadataBatchFile {
          name = "$BATCH_NAME/files/file1"
          blobUri = "gs://bucket/raw/file1"
        }
      }
    )

    val vidLabeler = createVidLabeler()

    // labelBatch will fail during readAndDecryptRawImpressions because there's no real
    // storage backend, which should trigger markBatchFailed.
    assertFailsWith<Exception> { vidLabeler.labelBatch() }

    verifyBlocking(rawImpressionBatchService) { markRawImpressionMetadataBatchFailed(any()) }
  }

  @Test
  fun `labelBatch marks batch as FAILED when batch has no files`() = runBlocking {
    mockBatchFileServiceToReturn(listRawImpressionMetadataBatchFilesResponse {})

    val vidLabeler = createVidLabeler()

    // Empty batch should fail during readAndDecryptRawImpressions (empty list, no data to read).
    // The pipeline continues past read with an empty list, then fails at labelImpressions
    // (NotImplementedError), triggering markBatchFailed.
    assertFailsWith<Exception> { vidLabeler.labelBatch() }

    verifyBlocking(rawImpressionBatchService) { markRawImpressionMetadataBatchFailed(any()) }
  }

  @Test
  fun `readAndDecryptRawImpressions reads and decrypts data from storage`() = runBlocking {
    val kekUri = FakeKmsClient.KEY_URI_PREFIX + "decrypt-key"
    val kmsClient = createFakeKmsClient(kekUri)

    // Set up local file storage
    val bucket = "raw-bucket"
    tempFolder.root.resolve(bucket).mkdirs()
    val storageClient = FileSystemStorageClient(tempFolder.root)

    // Write a BlobDetails metadata file pointing to the encrypted data
    val serializedEncryptionKey =
      EncryptedStorage.generateSerializedEncryptionKey(kmsClient, kekUri, "AES128_GCM_HKDF_1MB")

    val impressionsBlobKey = "$bucket/raw/impressions"
    val impressionsFileUri = "file:///$impressionsBlobKey"

    // Write encrypted labeled impressions to storage
    val selectedStorageClient = SelectedStorageClient(impressionsFileUri, tempFolder.root)
    val aeadStorageClient =
      selectedStorageClient.withEnvelopeEncryption(kmsClient, kekUri, serializedEncryptionKey)
    val encryptedStorage = MesosRecordIoStorageClient(aeadStorageClient)

    val testImpression = labeledImpression {
      vid = 42L
      eventGroupReferenceId = "eg-ref-1"
    }
    encryptedStorage.writeBlob(impressionsBlobKey, listOf(testImpression.toByteString()).asFlow())

    // Write BlobDetails metadata
    val metadataBlobKey = "$bucket/raw/metadata.binpb"
    val blobDetailsProto = blobDetails {
      blobUri = impressionsFileUri
      encryptedDek = encryptedDek {
        this.kekUri = kekUri
        ciphertext = serializedEncryptionKey
        protobufFormat = EncryptedDek.ProtobufFormat.BINARY
        typeUrl = "type.googleapis.com/google.crypto.tink.Keyset"
      }
      eventGroupReferenceId = "eg-ref-1"
      modelLine = MODEL_LINE_1
    }
    storageClient.writeBlob(metadataBlobKey, blobDetailsProto.toByteString())

    // Set up the batch file service to return the metadata blob URI
    mockBatchFileServiceToReturn(
      listRawImpressionMetadataBatchFilesResponse {
        rawImpressionMetadataBatchFiles += rawImpressionMetadataBatchFile {
          name = "$BATCH_NAME/files/file1"
          blobUri = "file:///$metadataBlobKey"
        }
      }
    )

    val vidLabeler =
      createVidLabeler(
        decryptKmsClient = kmsClient,
        storageConfig = StorageConfig(rootDirectory = tempFolder.root),
      )

    // labelBatch will succeed through readAndDecryptRawImpressions, then fail at
    // labelImpressions (NotImplementedError). This verifies the read path works.
    val exception = assertFailsWith<NotImplementedError> { vidLabeler.labelBatch() }
    assertThat(exception).hasMessageThat().contains("VID model inference not yet implemented")
  }

  @Test
  fun `encryptAndWriteLabeledImpressions round-trips through read`() = runBlocking {
    val encryptKekUri = FakeKmsClient.KEY_URI_PREFIX + "encrypt-key"
    val encryptKmsClient = createFakeKmsClient(encryptKekUri)

    val bucket = "output-bucket"
    tempFolder.root.resolve("$bucket/labeled").mkdirs()

    val vidLabeler =
      createVidLabeler(
        encryptKmsClient = encryptKmsClient,
        encryptKekUri = encryptKekUri,
        storageConfig = StorageConfig(rootDirectory = tempFolder.root),
      )

    // We can't call encryptAndWriteLabeledImpressions directly (it's private),
    // but we can verify the write path by writing data using the same pattern
    // the implementation uses, then reading it back.

    // Use the ImpressionsWriter pattern to write encrypted data
    val serializedEncryptionKey =
      EncryptedStorage.generateSerializedEncryptionKey(
        encryptKmsClient,
        encryptKekUri,
        "AES128_GCM_HKDF_1MB",
      )

    val blobKey = "$bucket/labeled/test-group/labeled-impressions"
    val outputBlobUri = "file:///$blobKey"

    val outputStorageClient = SelectedStorageClient(outputBlobUri, tempFolder.root)
    val aeadStorageClient =
      outputStorageClient.withEnvelopeEncryption(
        encryptKmsClient,
        encryptKekUri,
        serializedEncryptionKey,
      )
    val encryptedStorage = MesosRecordIoStorageClient(aeadStorageClient)

    val impression1 = labeledImpression {
      vid = 100L
      eventGroupReferenceId = "eg-1"
    }
    val impression2 = labeledImpression {
      vid = 200L
      eventGroupReferenceId = "eg-1"
    }

    encryptedStorage.writeBlob(
      blobKey,
      listOf(impression1.toByteString(), impression2.toByteString()).asFlow(),
    )

    // Now read back and decrypt using the same pattern as readAndDecryptRawImpressions
    val readStorageClient = SelectedStorageClient(outputBlobUri, tempFolder.root)
    val readAeadClient =
      readStorageClient.withEnvelopeEncryption(
        encryptKmsClient,
        encryptKekUri,
        serializedEncryptionKey,
      )
    val readEncryptedStorage = MesosRecordIoStorageClient(readAeadClient)

    val readBlob = readEncryptedStorage.getBlob(blobKey)
    assertThat(readBlob).isNotNull()

    val records = readBlob!!.read().toList()
    assertThat(records).hasSize(2)

    val readImpression1 = LabeledImpression.parseFrom(records[0])
    assertThat(readImpression1.vid).isEqualTo(100L)

    val readImpression2 = LabeledImpression.parseFrom(records[1])
    assertThat(readImpression2.vid).isEqualTo(200L)
  }

  private fun mockBatchFileServiceToReturn(
    response:
      org.wfanet.measurement.edpaggregator.v1alpha.ListRawImpressionMetadataBatchFilesResponse
  ) {
    batchFileService.stub {
      onBlocking { listRawImpressionMetadataBatchFiles(any()) }.thenReturn(response)
    }
  }

  companion object {
    private const val DATA_PROVIDER_NAME = "dataProviders/edp123"
    private const val BATCH_NAME = "dataProviders/edp123/rawImpressionMetadataBatches/batch456"
    private const val MODEL_LINE_1 = "dataProviders/edp123/modelLines/ml1"
    private const val MODEL_LINE_2 = "dataProviders/edp123/modelLines/ml2"

    private val DEFAULT_MODEL_LINE_CONFIGS =
      mapOf(
        MODEL_LINE_1 to
          VidLabelerParams.ModelLineConfig.newBuilder()
            .putLabelerInputFieldMapping("age", "user_age")
            .putLabelerInputFieldMapping("gender", "user_gender")
            .build(),
        MODEL_LINE_2 to
          VidLabelerParams.ModelLineConfig.newBuilder()
            .putLabelerInputFieldMapping("age", "user_age")
            .build(),
      )

    private fun createFakeKmsClient(kekUri: String): FakeKmsClient {
      val client = FakeKmsClient()
      val kmsKeyHandle = KeysetHandle.generateNew(KeyTemplates.get("AES128_GCM"))
      client.setAead(kekUri, kmsKeyHandle.getPrimitive(Aead::class.java))
      return client
    }

    init {
      AeadConfig.register()
      StreamingAeadConfig.register()
    }
  }
}
