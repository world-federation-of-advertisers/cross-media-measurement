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

package org.wfanet.measurement.securecomputation.service.internal.testing

import com.google.common.truth.Truth.assertThat
import com.google.common.truth.extensions.proto.ProtoTruth.assertThat
import java.time.Instant
import kotlinx.coroutines.runBlocking
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.common.IdGenerator
import org.wfanet.measurement.common.toInstant
import org.wfanet.measurement.internal.securecomputation.controlplane.WorkItem
import org.wfanet.measurement.internal.securecomputation.controlplane.WorkItemsGrpcKt.WorkItemsCoroutineImplBase
import org.wfanet.measurement.internal.securecomputation.controlplane.createWorkItemRequest
import org.wfanet.measurement.internal.securecomputation.controlplane.workItem
import org.wfanet.measurement.securecomputation.service.internal.QueueMapping

@RunWith(JUnit4::class)
abstract class WorkItemsServiceTest {
  /** Initializes the service under test. */
  abstract fun initService(
    queueMapping: QueueMapping,
    idGenerator: IdGenerator,
  ): WorkItemsCoroutineImplBase

  private fun initService(idGenerator: IdGenerator = IdGenerator.Default) =
    initService(TestConfig.QUEUE_MAPPING, idGenerator)

  @Test
  fun `createWorkItem succeeds`() = runBlocking {
    val service = initService()
    val request = createWorkItemRequest {
      workItem = workItem {
        queueResourceId = "queues/test_queue"
      }
    }

    val response: WorkItem = service.createWorkItem(request)

    assertThat(response)
      .ignoringFields(
        WorkItem.CREATE_TIME_FIELD_NUMBER,
        WorkItem.UPDATE_TIME_FIELD_NUMBER,
        WorkItem.STATE_FIELD_NUMBER,
        WorkItem.WORK_ITEM_RESOURCE_ID_FIELD_NUMBER
      )
      .isEqualTo(request.workItem)
    assertThat(response.createTime.toInstant()).isGreaterThan(Instant.now().minusSeconds(10))
    assertThat(response.updateTime).isEqualTo(response.createTime)
    assertThat(response.state).isEqualTo(WorkItem.State.QUEUED)
  }


}
