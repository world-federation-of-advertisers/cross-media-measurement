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

package org.wfanet.measurement.securecomputation.deploy.gcloud.spanner

import com.google.cloud.spanner.Value
import com.google.common.truth.Truth.assertThat
import com.google.protobuf.Any
import com.google.protobuf.Message
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.runBlocking
import org.junit.ClassRule
import org.junit.Rule
import org.junit.Test
import org.wfa.measurement.queue.testing.testWork
import org.wfanet.measurement.common.IdGenerator
import org.wfanet.measurement.gcloud.spanner.bufferInsertMutation
import org.wfanet.measurement.gcloud.spanner.bufferUpdateMutation
import org.wfanet.measurement.gcloud.spanner.testing.SpannerEmulatorDatabaseRule
import org.wfanet.measurement.gcloud.spanner.testing.SpannerEmulatorRule
import org.wfanet.measurement.internal.securecomputation.controlplane.WorkItem
import org.wfanet.measurement.internal.securecomputation.controlplane.WorkItemAttempt
import org.wfanet.measurement.internal.securecomputation.controlplane.createWorkItemRequest
import org.wfanet.measurement.internal.securecomputation.controlplane.failWorkItemRequest
import org.wfanet.measurement.internal.securecomputation.controlplane.getWorkItemAttemptRequest
import org.wfanet.measurement.internal.securecomputation.controlplane.workItem
import org.wfanet.measurement.securecomputation.deploy.gcloud.spanner.db.getWorkItemByResourceId
import org.wfanet.measurement.securecomputation.deploy.gcloud.spanner.testing.Schemata
import org.wfanet.measurement.securecomputation.service.internal.QueueMapping
import org.wfanet.measurement.securecomputation.service.internal.WorkItemPublisher
import org.wfanet.measurement.securecomputation.service.internal.testing.TestConfig
import org.wfanet.measurement.securecomputation.service.internal.testing.WorkItemsServiceTest

class SpannerWorkItemsServiceTest : WorkItemsServiceTest() {

  @get:Rule
  val spannerDatabase =
    SpannerEmulatorDatabaseRule(spannerEmulator, Schemata.SECURECOMPUTATION_CHANGELOG_PATH)

  override fun initServices(
    queueMapping: QueueMapping,
    idGenerator: IdGenerator,
    workItemPublisher: WorkItemPublisher,
  ): Services {
    val serviceDispatcher = Dispatchers.Default
    val workItemPublicationRunner =
      WorkItemPublicationRunner(spannerDatabase.databaseClient, queueMapping, workItemPublisher)
    return Services(
      SpannerWorkItemsService(
        spannerDatabase.databaseClient,
        queueMapping,
        idGenerator,
        workItemPublicationRunner,
      ),
      SpannerWorkItemAttemptsService(
        spannerDatabase.databaseClient,
        queueMapping,
        idGenerator,
        serviceDispatcher,
      ),
    )
  }

  @Test
  fun `failWorkItem fails active attempt after more than one page of attempts`() = runBlocking {
    val services =
      initServices(
        TestConfig.QUEUE_MAPPING,
        IdGenerator.Default,
        object : WorkItemPublisher {
          override suspend fun publishMessage(queueName: String, message: Message) {}
        },
      )
    val created =
      services.service.createWorkItem(
        createWorkItemRequest {
          workItem = workItem {
            workItemResourceId = "many-attempts-work-item"
            queueResourceId = "test-topid-id"
            workItemParams = Any.pack(testWork { userName = "UserName" })
          }
        }
      )
    val workItemId =
      spannerDatabase.databaseClient.singleUse().use { readContext ->
        readContext
          .getWorkItemByResourceId(TestConfig.QUEUE_MAPPING, created.workItemResourceId)
          .workItemId
      }
    spannerDatabase.databaseClient.readWriteTransaction().run { transaction ->
      repeat(102) { index ->
        transaction.bufferInsertMutation("WorkItemAttempts") {
          set("WorkItemId").to(workItemId)
          set("WorkItemAttemptId").to(index.toLong() + 1L)
          set("WorkItemAttemptResourceId").to("attempt-$index")
          set("State")
            .to(
              if (index == 101) {
                WorkItemAttempt.State.ACTIVE
              } else {
                WorkItemAttempt.State.FAILED
              }
            )
          set("CreateTime").to(Value.COMMIT_TIMESTAMP)
          set("UpdateTime").to(Value.COMMIT_TIMESTAMP)
        }
      }
      transaction.bufferUpdateMutation("WorkItems") {
        set("WorkItemId").to(workItemId)
        set("State").to(WorkItem.State.RUNNING)
        set("UpdateTime").to(Value.COMMIT_TIMESTAMP)
      }
    }

    services.service.failWorkItem(
      failWorkItemRequest {
        workItemResourceId = created.workItemResourceId
        expectedWorkItemGeneration = created.generation
      }
    )

    val updatedAttempt =
      services.workItemAttemptsService.getWorkItemAttempt(
        getWorkItemAttemptRequest {
          workItemResourceId = created.workItemResourceId
          workItemAttemptResourceId = "attempt-101"
        }
      )
    assertThat(updatedAttempt.state).isEqualTo(WorkItemAttempt.State.FAILED)
  }

  companion object {
    @get:ClassRule @JvmStatic val spannerEmulator = SpannerEmulatorRule()
  }
}
