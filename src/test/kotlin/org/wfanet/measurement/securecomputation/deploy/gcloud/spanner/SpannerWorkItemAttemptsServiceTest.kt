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

import com.google.common.truth.Truth.assertThat
import com.google.protobuf.Any
import com.google.protobuf.Message
import io.grpc.Status
import io.grpc.StatusRuntimeException
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.runBlocking
import org.junit.ClassRule
import org.junit.Rule
import org.junit.Test
import org.wfa.measurement.queue.testing.testWork
import org.wfanet.measurement.common.IdGenerator
import org.wfanet.measurement.gcloud.spanner.testing.SpannerEmulatorDatabaseRule
import org.wfanet.measurement.gcloud.spanner.testing.SpannerEmulatorRule
import org.wfanet.measurement.internal.securecomputation.controlplane.createWorkItemAttemptRequest
import org.wfanet.measurement.internal.securecomputation.controlplane.createWorkItemRequest
import org.wfanet.measurement.internal.securecomputation.controlplane.workItem
import org.wfanet.measurement.internal.securecomputation.controlplane.workItemAttempt
import org.wfanet.measurement.securecomputation.deploy.gcloud.spanner.testing.Schemata
import org.wfanet.measurement.securecomputation.service.internal.QueueMapping
import org.wfanet.measurement.securecomputation.service.internal.WorkItemPublisher
import org.wfanet.measurement.securecomputation.service.internal.testing.TestConfig
import org.wfanet.measurement.securecomputation.service.internal.testing.WorkItemAttemptsServiceTest

class SpannerWorkItemAttemptsServiceTest : WorkItemAttemptsServiceTest() {

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
      SpannerWorkItemAttemptsService(
        spannerDatabase.databaseClient,
        queueMapping,
        idGenerator,
        serviceDispatcher,
      ),
      SpannerWorkItemsService(
        spannerDatabase.databaseClient,
        queueMapping,
        idGenerator,
        workItemPublicationRunner,
      ),
    )
  }

  @Test
  fun `concurrent duplicate deliveries create only one active attempt`() = runBlocking {
    val publisher =
      object : WorkItemPublisher {
        override suspend fun publishMessage(queueName: String, message: Message) {}
      }
    val services = initServices(TestConfig.QUEUE_MAPPING, IdGenerator.Default, publisher)
    val workItem =
      services.workItemsService.createWorkItem(
        createWorkItemRequest {
          this.workItem = workItem {
            workItemResourceId = "duplicate-work-item"
            queueResourceId = "test-topid-id"
            workItemParams = Any.pack(testWork { userName = "UserName" })
          }
        }
      )

    val results =
      listOf("attempt-one", "attempt-two")
        .map { attemptId ->
          async(Dispatchers.Default) {
            runCatching {
              services.service.createWorkItemAttempt(
                createWorkItemAttemptRequest {
                  this.workItemAttempt = workItemAttempt {
                    workItemResourceId = workItem.workItemResourceId
                    workItemAttemptResourceId = attemptId
                  }
                }
              )
            }
          }
        }
        .awaitAll()

    assertThat(results.count { it.isSuccess }).isEqualTo(1)
    val failure = results.single { it.isFailure }.exceptionOrNull() as StatusRuntimeException
    assertThat(failure.status.code).isEqualTo(Status.Code.FAILED_PRECONDITION)
  }

  companion object {
    @get:ClassRule @JvmStatic val spannerEmulator = SpannerEmulatorRule()
  }
}
