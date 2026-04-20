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
import com.google.crypto.tink.KmsClient
import kotlin.test.assertFailsWith
import kotlinx.coroutines.runBlocking
import org.junit.Rule
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.mockito.kotlin.mock
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.common.grpc.testing.mockService
import org.wfanet.measurement.common.pack
import org.wfanet.measurement.edpaggregator.StorageConfig
import org.wfanet.measurement.edpaggregator.v1alpha.VidLabelerParams
import org.wfanet.measurement.edpaggregator.v1alpha.VidLabelerParamsKt
import org.wfanet.measurement.edpaggregator.v1alpha.vidLabelerParams
import org.wfanet.measurement.queue.QueueSubscriber
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItem
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemAttemptsGrpcKt
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemKt.workItemParams
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemsGrpcKt

@RunWith(JUnit4::class)
class VidLabelerAppTest {

  private val workItemsService: WorkItemsGrpcKt.WorkItemsCoroutineImplBase = mockService()
  private val workItemAttemptsService: WorkItemAttemptsGrpcKt.WorkItemAttemptsCoroutineImplBase =
    mockService()

  @get:Rule
  val grpcTestServerRule = GrpcTestServerRule {
    addService(workItemsService)
    addService(workItemAttemptsService)
  }

  private val workItemsStub by lazy {
    WorkItemsGrpcKt.WorkItemsCoroutineStub(grpcTestServerRule.channel)
  }
  private val workItemAttemptsStub by lazy {
    WorkItemAttemptsGrpcKt.WorkItemAttemptsCoroutineStub(grpcTestServerRule.channel)
  }

  private val mockDecryptKmsClient: KmsClient = mock()
  private val mockEncryptKmsClient: KmsClient = mock()
  private val mockQueueSubscriber: QueueSubscriber = mock()

  private fun createApp(
    rawImpressionsKmsClient: Map<String, KmsClient> =
      mapOf(DATA_PROVIDER_NAME to mockDecryptKmsClient),
    vidLabeledImpressionsKmsClient: Map<String, KmsClient> =
      mapOf(DATA_PROVIDER_NAME to mockEncryptKmsClient),
  ): VidLabelerApp {
    return VidLabelerApp(
      subscriptionId = "test-subscription",
      queueSubscriber = mockQueueSubscriber,
      parser = WorkItem.parser(),
      workItemsClient = workItemsStub,
      workItemAttemptsClient = workItemAttemptsStub,
      rawImpressionsKmsClient = rawImpressionsKmsClient,
      vidLabeledImpressionsKmsClient = vidLabeledImpressionsKmsClient,
      getStorageConfig = { storageParams -> StorageConfig(projectId = storageParams.gcsProjectId) },
    )
  }

  private fun buildMessage(params: VidLabelerParams): com.google.protobuf.Any {
    return workItemParams { appParams = params.pack() }.pack()
  }

  @Test
  fun `runWork correctly unpacks VidLabelerParams and completes without error`() = runBlocking {
    val app = createApp()
    val params = vidLabelerParams {
      dataProvider = DATA_PROVIDER_NAME
      rawImpressionsStorageParams =
        VidLabelerParamsKt.storageParams {
          gcsProjectId = "test-project"
          impressionsBlobPrefix = "gs://raw-bucket/impressions"
        }
      vidLabeledImpressionsStorageParams =
        VidLabelerParamsKt.storageParams {
          gcsProjectId = "test-project"
          impressionsBlobPrefix = "gs://output-bucket/labeled"
        }
      rawImpressionMetadataBatch = "$DATA_PROVIDER_NAME/rawImpressionMetadataBatches/batch-1"
    }

    app.runWork(buildMessage(params))
  }

  @Test
  fun `runWork throws when decrypt KMS client not found for data provider`() = runBlocking {
    val app = createApp(rawImpressionsKmsClient = emptyMap())
    val params = vidLabelerParams {
      dataProvider = DATA_PROVIDER_NAME
      rawImpressionsStorageParams =
        VidLabelerParamsKt.storageParams {
          gcsProjectId = "test-project"
          impressionsBlobPrefix = "gs://raw-bucket/impressions"
        }
      vidLabeledImpressionsStorageParams =
        VidLabelerParamsKt.storageParams {
          gcsProjectId = "test-project"
          impressionsBlobPrefix = "gs://output-bucket/labeled"
        }
      rawImpressionMetadataBatch = "$DATA_PROVIDER_NAME/rawImpressionMetadataBatches/batch-1"
    }

    val exception = assertFailsWith<IllegalArgumentException> { app.runWork(buildMessage(params)) }
    assertThat(exception).hasMessageThat().contains("Decrypt KMS client not found")
    assertThat(exception).hasMessageThat().contains(DATA_PROVIDER_NAME)
  }

  @Test
  fun `runWork throws when encrypt KMS client not found for data provider`() = runBlocking {
    val app = createApp(vidLabeledImpressionsKmsClient = emptyMap())
    val params = vidLabelerParams {
      dataProvider = DATA_PROVIDER_NAME
      rawImpressionsStorageParams =
        VidLabelerParamsKt.storageParams {
          gcsProjectId = "test-project"
          impressionsBlobPrefix = "gs://raw-bucket/impressions"
        }
      vidLabeledImpressionsStorageParams =
        VidLabelerParamsKt.storageParams {
          gcsProjectId = "test-project"
          impressionsBlobPrefix = "gs://output-bucket/labeled"
        }
      rawImpressionMetadataBatch = "$DATA_PROVIDER_NAME/rawImpressionMetadataBatches/batch-1"
    }

    val exception = assertFailsWith<IllegalArgumentException> { app.runWork(buildMessage(params)) }
    assertThat(exception).hasMessageThat().contains("Encrypt KMS client not found")
    assertThat(exception).hasMessageThat().contains(DATA_PROVIDER_NAME)
  }

  @Test
  fun `runWork throws when data_provider is empty`() = runBlocking {
    val app = createApp()
    val params = vidLabelerParams {
      rawImpressionsStorageParams =
        VidLabelerParamsKt.storageParams {
          gcsProjectId = "test-project"
          impressionsBlobPrefix = "gs://raw-bucket/impressions"
        }
      vidLabeledImpressionsStorageParams =
        VidLabelerParamsKt.storageParams {
          gcsProjectId = "test-project"
          impressionsBlobPrefix = "gs://output-bucket/labeled"
        }
      rawImpressionMetadataBatch = "$DATA_PROVIDER_NAME/rawImpressionMetadataBatches/batch-1"
    }

    val exception = assertFailsWith<IllegalArgumentException> { app.runWork(buildMessage(params)) }
    assertThat(exception).hasMessageThat().contains("data_provider must not be empty")
  }

  @Test
  fun `runWork throws when raw_impressions_storage_params is not set`() = runBlocking {
    val app = createApp()
    val params = vidLabelerParams {
      dataProvider = DATA_PROVIDER_NAME
      rawImpressionMetadataBatch = "$DATA_PROVIDER_NAME/rawImpressionMetadataBatches/batch-1"
    }

    val exception = assertFailsWith<IllegalArgumentException> { app.runWork(buildMessage(params)) }
    assertThat(exception).hasMessageThat().contains("raw_impressions_storage_params must be set")
  }

  @Test
  fun `runWork throws when raw_impression_metadata_batch is empty`() = runBlocking {
    val app = createApp()
    val params = vidLabelerParams {
      dataProvider = DATA_PROVIDER_NAME
      rawImpressionsStorageParams =
        VidLabelerParamsKt.storageParams {
          gcsProjectId = "test-project"
          impressionsBlobPrefix = "gs://raw-bucket/impressions"
        }
      vidLabeledImpressionsStorageParams =
        VidLabelerParamsKt.storageParams {
          gcsProjectId = "test-project"
          impressionsBlobPrefix = "gs://output-bucket/labeled"
        }
    }

    val exception = assertFailsWith<IllegalArgumentException> { app.runWork(buildMessage(params)) }
    assertThat(exception).hasMessageThat().contains("raw_impression_metadata_batch must not be empty")
  }

  companion object {
    private const val DATA_PROVIDER_NAME = "dataProviders/edp123"
  }
}
