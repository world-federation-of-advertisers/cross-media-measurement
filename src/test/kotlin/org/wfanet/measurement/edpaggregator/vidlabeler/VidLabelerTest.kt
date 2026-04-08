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
import kotlin.test.assertFailsWith
import kotlinx.coroutines.runBlocking
import org.junit.Rule
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.mockito.kotlin.any
import org.mockito.kotlin.mock
import org.mockito.kotlin.verifyBlocking
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.common.grpc.testing.mockService
import org.wfanet.measurement.edpaggregator.StorageConfig
import org.wfanet.measurement.edpaggregator.v1alpha.ImpressionMetadataServiceGrpcKt
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionMetadataBatchFileServiceGrpcKt
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionMetadataBatchServiceGrpcKt
import org.wfanet.measurement.edpaggregator.v1alpha.VidLabelerParams
import org.wfanet.measurement.edpaggregator.v1alpha.VidLabelerParamsKt
import org.wfanet.measurement.edpaggregator.v1alpha.listRawImpressionMetadataBatchFilesResponse
import org.wfanet.measurement.edpaggregator.v1alpha.rawImpressionMetadataBatchFile
import org.wfanet.measurement.edpaggregator.v1alpha.transportLayerSecurityParams

@RunWith(JUnit4::class)
class VidLabelerTest {

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
  ): VidLabeler {
    return VidLabeler(
      dataProvider = DATA_PROVIDER_NAME,
      rawImpressionMetadataBatch = BATCH_NAME,
      modelLineConfigs = modelLineConfigs,
      overrideModelLines = overrideModelLines,
      storageParams =
        VidLabelerParamsKt.storageParams {
          gcsProjectId = "test-project"
          labeledImpressionsBlobPrefix = "gs://output-bucket/labeled"
        },
      decryptKmsClient = mockDecryptKmsClient,
      encryptKmsClient = mockEncryptKmsClient,
      impressionMetadataStub = impressionMetadataStub,
      rawImpressionBatchStub = rawImpressionBatchStub,
      batchFileStub = batchFileStub,
      vidRepoConnection = transportLayerSecurityParams {
        clientCertResourcePath = "certs/client.pem"
        clientPrivateKeyResourcePath = "certs/client.key"
      },
      storageConfig = StorageConfig(projectId = "test-project"),
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

    verifyBlocking(rawImpressionBatchService) {
      markRawImpressionMetadataBatchFailed(any())
    }
  }

  @Test
  fun `labelBatch marks batch as FAILED when batch has no files`() = runBlocking {
    mockBatchFileServiceToReturn(listRawImpressionMetadataBatchFilesResponse {})

    val vidLabeler = createVidLabeler()

    // Empty batch should fail during readAndDecryptRawImpressions (empty list, no data to read).
    // The pipeline continues past read with an empty list, then fails at labelImpressions
    // (NotImplementedError), triggering markBatchFailed.
    assertFailsWith<Exception> { vidLabeler.labelBatch() }

    verifyBlocking(rawImpressionBatchService) {
      markRawImpressionMetadataBatchFailed(any())
    }
  }

  // TODO: Add tests for resolveModelLines, labelImpressions, encryptAndWriteLabeledImpressions,
  //  and writeOutputMetadata once those methods are fully implemented.

  private fun mockBatchFileServiceToReturn(
    response: org.wfanet.measurement.edpaggregator.v1alpha.ListRawImpressionMetadataBatchFilesResponse
  ) {
    batchFileService.stub {
      onBlocking { listRawImpressionMetadataBatchFiles(any()) }.thenReturn(response)
    }
  }

  companion object {
    private const val DATA_PROVIDER_NAME = "dataProviders/edp123"
    private const val BATCH_NAME =
      "dataProviders/edp123/rawImpressionMetadataBatches/batch456"
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
  }
}
