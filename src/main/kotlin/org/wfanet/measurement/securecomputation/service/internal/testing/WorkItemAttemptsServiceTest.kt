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

import com.google.common.truth.Truth
import com.google.common.truth.extensions.proto.ProtoTruth
import java.time.Instant
import kotlinx.coroutines.runBlocking
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.common.IdGenerator
import org.wfanet.measurement.common.toInstant
import org.wfanet.measurement.internal.securecomputation.controlplane.WorkItem
import org.wfanet.measurement.internal.securecomputation.controlplane.WorkItemAttempt
import org.wfanet.measurement.internal.securecomputation.controlplane.WorkItemAttemptsGrpcKt.WorkItemAttemptsCoroutineImplBase
import org.wfanet.measurement.internal.securecomputation.controlplane.WorkItemsGrpcKt.WorkItemsCoroutineImplBase
import org.wfanet.measurement.internal.securecomputation.controlplane.createWorkItemAttemptRequest
import org.wfanet.measurement.internal.securecomputation.controlplane.createWorkItemRequest
import org.wfanet.measurement.internal.securecomputation.controlplane.workItem
import org.wfanet.measurement.internal.securecomputation.controlplane.workItemAttempt
import org.wfanet.measurement.securecomputation.service.internal.QueueMapping

@RunWith(JUnit4::class)
abstract class WorkItemAttemptsServiceTest {

  protected data class Services(
    /** Service under test. */
    val service: WorkItemAttemptsCoroutineImplBase,
    val workItemsService: WorkItemsCoroutineImplBase
  )

  /** Initializes the service under test. */
  protected abstract fun initServices(
    queueMapping: QueueMapping,
    idGenerator: IdGenerator
  ): Services

  private fun initServices(idGenerator: IdGenerator = IdGenerator.Default) =
    initServices(TestConfig.QUEUE_MAPPING, idGenerator)

  @Test
  fun `createWorkAttemptItem succeeds`() = runBlocking {
    val services = initServices()
    val workItem: WorkItem = createWorkItem(services.workItemsService)
    val request = createWorkItemAttemptRequest {
      workItemAttempt = workItemAttempt {
        workItemResourceId = workItem.workItemResourceId
      }
    }
    val response = services.service.createWorkItemAttempt(request)

    ProtoTruth.assertThat(response)
      .ignoringFields(
        WorkItemAttempt.CREATE_TIME_FIELD_NUMBER,
        WorkItemAttempt.UPDATE_TIME_FIELD_NUMBER,
        WorkItemAttempt.STATE_FIELD_NUMBER,
        WorkItemAttempt.ATTEMPT_NUMBER_FIELD_NUMBER,
        WorkItemAttempt.WORK_ITEM_ATTEMPT_RESOURCE_ID_FIELD_NUMBER
      )
      .isEqualTo(request.workItemAttempt)
    Truth.assertThat(response.createTime.toInstant()).isGreaterThan(Instant.now().minusSeconds(10))
    ProtoTruth.assertThat(response.updateTime).isEqualTo(response.createTime)
    Truth.assertThat(response.state).isEqualTo(WorkItemAttempt.State.ACTIVE)
  }

  private suspend fun createWorkItem(
    service: WorkItemsCoroutineImplBase,
  ): WorkItem {
    return service.createWorkItem(
      createWorkItemRequest {
        workItem = workItem {
          queueResourceId = "queues/test_queue"
        }
      }
    )
  }

}
