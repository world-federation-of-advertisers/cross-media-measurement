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
import kotlinx.coroutines.runBlocking
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.mockito.kotlin.argumentCaptor
import org.mockito.kotlin.mock
import org.mockito.kotlin.times
import org.mockito.kotlin.verify
import org.wfanet.measurement.common.pack
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.CreateWorkItemRequest
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.GooglePubSubWorkItemsService
import org.wfanet.measurement.securecomputation.datawatcher.v1alpha.DataWatcherConfigKt.controlPlaneConfig
import org.wfanet.measurement.securecomputation.datawatcher.v1alpha.dataWatcherConfig

@RunWith(JUnit4::class)
class DataWatcherTest() {

  @Test
  fun `creates WorkItem when path matches`() {
    runBlocking {
      val topicId = "test-topic-id"
      val mockWorkItemsService: GooglePubSubWorkItemsService = mock {}

      val dataWatcherConfig = dataWatcherConfig {
        sourcePathRegex = "test-bucket://path-to-watch/(.*)"
        this.controlPlaneConfig = controlPlaneConfig {
          queueName = topicId
          appConfig = Any.pack(Int32Value.newBuilder().setValue(5).build())
        }
      }

      val dataWatcher = DataWatcher(mockWorkItemsService, listOf(dataWatcherConfig))

      dataWatcher.receivePath("test-bucket://path-to-watch/some-data")
      val createWorkItemRequestCaptor = argumentCaptor<CreateWorkItemRequest>()
      verify(mockWorkItemsService, times(1)).createWorkItem(createWorkItemRequestCaptor.capture())
      assertThat(createWorkItemRequestCaptor.allValues.single().workItem.queue).isEqualTo(topicId)
    }
  }

  @Test
  fun `does not create WorkItem with path does not match`() {
    runBlocking {
      val topicId = "test-topic-id"
      val mockWorkItemsService: GooglePubSubWorkItemsService = mock {}

      val dataWatcherConfig = dataWatcherConfig {
        sourcePathRegex = "test-bucket://path-to-watch/(.*)"
        this.controlPlaneConfig = controlPlaneConfig {
          queueName = topicId
          appConfig = Any.pack(Int32Value.newBuilder().setValue(5).build())
        }
      }

      val dataWatcher = DataWatcher(mockWorkItemsService, listOf(dataWatcherConfig))
      dataWatcher.receivePath("test-bucket://some-other-path/some-data")

      val createWorkItemRequestCaptor = argumentCaptor<CreateWorkItemRequest>()
      verify(mockWorkItemsService, times(0)).createWorkItem(createWorkItemRequestCaptor.capture())
    }
  }

  companion object {
    private const val BUCKET = "test-bucket"
  }
}
