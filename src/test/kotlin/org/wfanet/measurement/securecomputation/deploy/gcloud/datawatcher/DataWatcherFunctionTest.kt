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

package org.wfanet.measurement.securecomputation.deploy.gcloud.datawatcher

import com.google.cloud.storage.contrib.nio.testing.LocalStorageHelper
import com.google.common.truth.Truth.assertThat
import com.google.protobuf.Any
import com.google.protobuf.Int32Value
import com.google.protobuf.kotlin.toByteStringUtf8
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.runBlocking
import org.junit.Before
import org.junit.Rule
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.mockito.kotlin.argumentCaptor
import org.mockito.kotlin.mock
import org.mockito.kotlin.times
import org.mockito.kotlin.verify
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.common.grpc.testing.mockService
import org.wfanet.measurement.gcloud.gcs.GcsStorageClient
import org.wfanet.measurement.gcloud.gcs.testing.GcsSubscribingStorageClient
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.CreateWorkItemRequest
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.GooglePubSubWorkItemsService
import org.wfanet.measurement.securecomputation.datawatcher.v1alpha.DataWatcherConfigKt.controlPlaneConfig
import org.wfanet.measurement.securecomputation.datawatcher.v1alpha.dataWatcherConfig
import org.wfanet.measurement.securecomputation.datawatcher.v1alpha.dataWatcherConfigs
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemsGrpcKt.WorkItemsCoroutineImplBase
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemsGrpcKt.WorkItemsCoroutinesStub
import com.google.common.truth.Truth.assertThat
import com.google.protobuf.Any
import com.google.protobuf.Int32Value
import kotlinx.coroutines.runBlocking
import org.junit.Rule
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.mockito.kotlin.argumentCaptor
import org.mockito.kotlin.times
import org.mockito.kotlin.verifyBlocking
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.common.grpc.testing.mockService
import org.wfanet.measurement.common.pack
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.CreateWorkItemRequest
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemsGrpcKt.WorkItemsCoroutineImplBase
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemsGrpcKt.WorkItemsCoroutineStub
import org.wfanet.measurement.securecomputation.datawatcher.v1alpha.DataWatcherConfigKt.controlPlaneConfig
import org.wfanet.measurement.securecomputation.datawatcher.v1alpha.dataWatcherConfig

@RunWith(JUnit4::class)
class DataWatcherFunctionTest() {

  lateinit var storageClient: GcsStorageClient

  private val workItemsServiceMock: WorkItemsCoroutineImplBase = mockService {}

  @get:Rule
  val grpcTestServerRule = GrpcTestServerRule { addService(workItemsServiceMock) }

  private val workItemsStub: WorkItemsCoroutineStub by lazy {
    WorkItemsCoroutineStub(grpcTestServerRule.channel)
  }

  @Before
  fun initStorageClient() {
    val storage = LocalStorageHelper.getOptions().service
    storageClient = GcsStorageClient(storage, BUCKET)
  }

  @Test
  fun `creates WorkItem when path matches`() {
    runBlocking {
      val topicId = "test-topic-id"
      val subscribingStorageClient = GcsSubscribingStorageClient(storageClient)

      val dataWatcherConfigs = dataWatcherConfigs {
        configs += dataWatcherConfig {
          sourcePathRegex = "gs://$BUCKET/path-to-watch/(.*)"
          this.controlPlaneConfig = controlPlaneConfig {
            queueName = topicId
            appConfig = Any.pack(Int32Value.newBuilder().setValue(5).build())
          }
        }
      }
      System.setProperty("DATA_WATCHER_CONFIGS", dataWatcherConfigs.toString())

      val dataWatcher = DataWatcherFunction(lazy { workItemsStub })
      subscribingStorageClient.subscribe(dataWatcher)

      subscribingStorageClient.writeBlob(
        "path-to-watch/some-data",
        flowOf("some-data".toByteStringUtf8()),
      )
      val createWorkItemRequestCaptor = argumentCaptor<CreateWorkItemRequest>()
      verifyBlocking(mockWorkItemsService, times(1)) { createWorkItem(createWorkItemRequestCaptor.capture()) }
      assertThat(createWorkItemRequestCaptor.allValues.single().workItem.queue).isEqualTo(topicId)
    }
  }

  @Test
  fun `does not create WorkItem with path does not match`() {
    runBlocking {
      val topicId = "test-topic-id"
      val mockWorkItemsService: GooglePubSubWorkItemsService = mock {}
      val subscribingStorageClient = GcsSubscribingStorageClient(storageClient)

      val dataWatcherConfigs = dataWatcherConfigs {
        configs += dataWatcherConfig {
          sourcePathRegex = "gs://$BUCKET/path-to-watch/(.*)"
          this.controlPlaneConfig = controlPlaneConfig {
            queueName = topicId
            appConfig = Any.pack(Int32Value.newBuilder().setValue(5).build())
          }
        }
      }
      System.setProperty("DATA_WATCHER_CONFIGS", dataWatcherConfigs.toString())

      val dataWatcher = DataWatcherFunction(lazy { mockWorkItemsService })
      subscribingStorageClient.subscribe(dataWatcher)

      subscribingStorageClient.writeBlob(
        "some-other-path/some-data",
        flowOf("some-data".toByteStringUtf8()),
      )
      val createWorkItemRequestCaptor = argumentCaptor<CreateWorkItemRequest>()
      verify(mockWorkItemsService, times(0)).createWorkItem(createWorkItemRequestCaptor.capture())
    }
  }

  companion object {
    private const val BUCKET = "test-bucket"
  }
}



