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

package org.wfanet.measurement.securecomputation.deploy.gcloud.spanner

import com.google.common.truth.Truth.assertThat
import com.google.protobuf.Any as ProtoAny
import com.google.protobuf.Empty
import com.google.protobuf.Message
import io.grpc.Status
import io.grpc.StatusException
import kotlin.test.assertFailsWith
import kotlinx.coroutines.runBlocking
import org.junit.ClassRule
import org.junit.Rule
import org.junit.Test
import org.junit.rules.RuleChain
import org.wfanet.measurement.common.IdGenerator
import org.wfanet.measurement.common.grpc.errorInfo
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.gcloud.spanner.testing.SpannerEmulatorDatabaseRule
import org.wfanet.measurement.gcloud.spanner.testing.SpannerEmulatorRule
import org.wfanet.measurement.internal.securecomputation.controlplane.WorkItemsGrpcKt.WorkItemsCoroutineStub as InternalWorkItemsCoroutineStub
import org.wfanet.measurement.internal.securecomputation.controlplane.createWorkItemRequest as internalCreateWorkItemRequest
import org.wfanet.measurement.internal.securecomputation.controlplane.workItem as internalWorkItem
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.ListWorkItemsRequest
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.ListWorkItemsRequestKt
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItem
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemsGrpcKt.WorkItemsCoroutineStub
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemsService
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.listWorkItemsRequest
import org.wfanet.measurement.securecomputation.deploy.gcloud.spanner.testing.Schemata
import org.wfanet.measurement.securecomputation.service.internal.WorkItemPublisher
import org.wfanet.measurement.securecomputation.service.internal.testing.TestConfig

/** Checks the public ListWorkItems filter contract against the persistent WorkItem service. */
class ListWorkItemsContractTest {
  private val database =
    SpannerEmulatorDatabaseRule(spannerEmulator, Schemata.SECURECOMPUTATION_CHANGELOG_PATH)
  private val publisher =
    object : WorkItemPublisher {
      override suspend fun publishMessage(queueName: String, message: Message) {}
    }
  private val internalService by lazy {
    SpannerWorkItemsService(
      database.databaseClient,
      TestConfig.TWO_QUEUE_MAPPING,
      IdGenerator.Default,
      WorkItemPublicationRunner(database.databaseClient, TestConfig.TWO_QUEUE_MAPPING, publisher),
    )
  }
  private val internalServer = GrpcTestServerRule { addService(internalService) }
  private val publicServer = GrpcTestServerRule {
    addService(WorkItemsService(InternalWorkItemsCoroutineStub(internalServer.channel)))
  }
  @get:Rule val rules = RuleChain.outerRule(database).around(internalServer).around(publicServer)

  private val workItems by lazy { WorkItemsCoroutineStub(publicServer.channel) }

  @Test
  fun `Queue resource names filter persisted WorkItems across pages`() =
    runBlocking<Unit> {
      create("queued-a-1", QUEUE_A)
      create("queued-b", QUEUE_B)
      create("queued-a-2", QUEUE_A)

      val firstPage = workItems.listWorkItems(request(pageSize = 1, filter = FILTER_A))
      val secondPage =
        workItems.listWorkItems(
          request(pageSize = 1, filter = FILTER_A, pageToken = firstPage.nextPageToken)
        )

      assertThat(firstPage.workItemsList.map { it.name }).containsExactly("workItems/queued-a-1")
      assertThat(firstPage.nextPageToken).isNotEmpty()
      assertThat(secondPage.workItemsList.map { it.name }).containsExactly("workItems/queued-a-2")
      assertThat(secondPage.nextPageToken).isEmpty()
      assertThat((firstPage.workItemsList + secondPage.workItemsList).map { it.queue }.toSet())
        .containsExactly(QUEUE_A)
    }

  @Test
  fun `bare queue ID is rejected`() =
    runBlocking<Unit> {
      val exception =
        assertFailsWith<StatusException> {
          workItems.listWorkItems(
            request(filter = ListWorkItemsRequestKt.filter { queueIn += QUEUE_A })
          )
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
      assertThat(exception.errorInfo?.metadata?.get("fieldName")).isEqualTo("filter.queue_in")
    }

  @Test
  fun `unconfigured Queue is reported by resource name`() =
    runBlocking<Unit> {
      val exception =
        assertFailsWith<StatusException> {
          workItems.listWorkItems(
            request(filter = ListWorkItemsRequestKt.filter { queueIn += "queues/x" })
          )
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
      assertThat(exception.errorInfo?.reason).isEqualTo("QUEUE_NOT_FOUND")
      assertThat(exception.errorInfo?.metadata?.get("queue")).isEqualTo("queues/x")
    }

  @Test
  fun `page token is rejected with a changed filter`() =
    runBlocking<Unit> {
      create("queued-a-1", QUEUE_A)
      create("queued-a-2", QUEUE_A)
      val firstPage = workItems.listWorkItems(request(pageSize = 1, filter = FILTER_A))

      val exception =
        assertFailsWith<StatusException> {
          workItems.listWorkItems(
            request(
              pageSize = 1,
              filter = ListWorkItemsRequestKt.filter { queueIn += "queues/$QUEUE_B" },
              pageToken = firstPage.nextPageToken,
            )
          )
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
      assertThat(exception.errorInfo?.metadata?.get("fieldName")).isEqualTo("page_token")
    }

  private suspend fun create(id: String, queue: String) {
    internalService.createWorkItem(
      internalCreateWorkItemRequest {
        workItem = internalWorkItem {
          workItemResourceId = id
          queueResourceId = queue
          workItemParams = ProtoAny.pack(Empty.getDefaultInstance())
        }
      }
    )
  }

  private fun request(
    pageSize: Int = 0,
    filter: ListWorkItemsRequest.Filter,
    pageToken: String = "",
  ): ListWorkItemsRequest = listWorkItemsRequest {
    this.pageSize = pageSize
    this.filter = filter
    this.pageToken = pageToken
  }

  companion object {
    @ClassRule @JvmField val spannerEmulator = SpannerEmulatorRule()
    private const val QUEUE_A = "test-queue-a"
    private const val QUEUE_B = "test-queue-b"
    private val FILTER_A =
      ListWorkItemsRequestKt.filter {
        queueIn += "queues/$QUEUE_A"
        stateIn += WorkItem.State.QUEUED
      }
  }
}
