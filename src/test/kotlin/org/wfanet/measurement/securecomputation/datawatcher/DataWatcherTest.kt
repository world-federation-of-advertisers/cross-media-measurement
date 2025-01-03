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

package org.wfanet.measurement.securecomputation.datawatcher

import com.google.common.truth.Truth.assertThat
import com.google.protobuf.Any
import com.google.protobuf.Int32Value
import com.google.protobuf.kotlin.toByteStringUtf8
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.runBlocking
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.mockito.kotlin.argumentCaptor
import org.mockito.kotlin.mock
import org.mockito.kotlin.times
import org.mockito.kotlin.verify
import org.wfanet.measurement.gcloud.gcs.testing.GcsSubscribingStorageClient
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.CreateWorkItemRequest
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.GooglePubSubWorkItemsService
import org.wfanet.measurement.securecomputation.datawatcher.v1alpha.DataWatcherConfigKt.controlPlaneConfig
import org.wfanet.measurement.securecomputation.datawatcher.v1alpha.dataWatcherConfig
import org.wfanet.measurement.storage.testing.InMemoryStorageClient

@RunWith(JUnit4::class)
class DataWatcherTest() {

  @Test
  fun matchedPath() {
    runBlocking {
      val topicId = "test-topic-id"
      val mockWorkItemsService: GooglePubSubWorkItemsService = mock {}
      val inMemoryStorageClient = InMemoryStorageClient()
      val subscribingStorageClient = GcsSubscribingStorageClient(inMemoryStorageClient)

      val dataWatcherConfig = dataWatcherConfig {
        sourcePathRegex = "^path-to-watch"
        this.controlPlaneConfig = controlPlaneConfig {
          queueName = topicId
          appConfig = Any.pack(Int32Value.newBuilder().setValue(5).build())
        }
      }

      val dataWatcher = DataWatcher(mockWorkItemsService, listOf(dataWatcherConfig))
      subscribingStorageClient.subscribe(dataWatcher)

      subscribingStorageClient.writeBlob(
        "path-to-watch/some-data",
        flowOf("some-data".toByteStringUtf8())
      )
      val createWorkItemRequestCaptor = argumentCaptor<CreateWorkItemRequest>()
      verify(mockWorkItemsService, times(1)).createWorkItem(createWorkItemRequestCaptor.capture())
      assertThat(createWorkItemRequestCaptor.allValues.single().workItem.queue).isEqualTo(topicId)
    }
  }

  @Test
  fun noMatch() {
    runBlocking {
      val topicId = "test-topic-id"
      val mockWorkItemsService: GooglePubSubWorkItemsService = mock {}
      val inMemoryStorageClient = InMemoryStorageClient()
      val subscribingStorageClient = GcsSubscribingStorageClient(inMemoryStorageClient)

      val dataWatcherConfig = dataWatcherConfig {
        sourcePathRegex = "^path-to-watch"
        this.controlPlaneConfig = controlPlaneConfig {
          queueName = topicId
          appConfig = Any.pack(Int32Value.newBuilder().setValue(5).build())
        }
      }

      val dataWatcher = DataWatcher(mockWorkItemsService, listOf(dataWatcherConfig))
      subscribingStorageClient.subscribe(dataWatcher)

      subscribingStorageClient.writeBlob(
        "some-other-path/some-data",
        flowOf("some-data".toByteStringUtf8())
      )
      val createWorkItemRequestCaptor = argumentCaptor<CreateWorkItemRequest>()
      verify(mockWorkItemsService, times(0)).createWorkItem(createWorkItemRequestCaptor.capture())
    }
  }
}
