/*
 * Copyright 2026 The Cross-Media Measurement Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
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
import java.time.Instant
import java.time.LocalDate
import kotlin.test.assertFailsWith
import kotlinx.coroutines.runBlocking
import org.junit.ClassRule
import org.junit.Rule
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.common.IdGenerator
import org.wfanet.measurement.common.toProtoTime
import org.wfanet.measurement.edpaggregator.vidlabeling.DataAvailabilitySyncWorkItems
import org.wfanet.measurement.gcloud.spanner.testing.SpannerEmulatorDatabaseRule
import org.wfanet.measurement.gcloud.spanner.testing.SpannerEmulatorRule
import org.wfanet.measurement.internal.securecomputation.controlplane.createWorkItemRequest
import org.wfanet.measurement.internal.securecomputation.controlplane.ensureWorkItemRequest
import org.wfanet.measurement.internal.securecomputation.controlplane.workItem
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.CreateWorkItemRequest as PublicCreateWorkItemRequest
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItem as PublicWorkItem
import org.wfanet.measurement.securecomputation.deploy.gcloud.spanner.db.WorkItemSourceObject
import org.wfanet.measurement.securecomputation.deploy.gcloud.spanner.db.getWorkItemByResourceId
import org.wfanet.measurement.securecomputation.deploy.gcloud.spanner.testing.Schemata
import org.wfanet.measurement.securecomputation.service.internal.WorkItemPublisher
import org.wfanet.measurement.securecomputation.service.internal.testing.TestConfig

@RunWith(JUnit4::class)
class InternalAvailabilityWorkItemContractTest {

  @get:Rule
  val spannerDatabase =
    SpannerEmulatorDatabaseRule(spannerEmulator, Schemata.SECURECOMPUTATION_CHANGELOG_PATH)

  @Test
  fun `producer persists complete source identity`() =
    runBlocking<Unit> {
      val publisher = RecordingPublisher()
      val service = buildService(publisher)
      val request = availabilityRequest()

      val created = service.createWorkItem(request.toInternalCreateRequest())
      val persisted =
        spannerDatabase.databaseClient.singleUse().use { readContext ->
          readContext.getWorkItemByResourceId(TestConfig.QUEUE_MAPPING, request.workItemId)
        }

      assertThat(created.state)
        .isEqualTo(
          org.wfanet.measurement.internal.securecomputation.controlplane.WorkItem.State.QUEUED
        )
      assertThat(persisted.sourceObject)
        .isEqualTo(WorkItemSourceObject(SOURCE_DATA_PATH, SOURCE_GENERATION, SOURCE_CREATE_TIME))
      assertThat(publisher.publicationCount).isEqualTo(1)
    }

  @Test
  fun `producer is idempotent only for the same source identity`() =
    runBlocking<Unit> {
      val publisher = RecordingPublisher()
      val service = buildService(publisher)
      val request = availabilityRequest()
      val internalRequest = request.toInternalEnsureRequest()

      val first = service.ensureWorkItem(internalRequest)
      val retried = service.ensureWorkItem(internalRequest)
      val conflicting =
        request.withCreateTime(SOURCE_CREATE_TIME.plusSeconds(1)).toInternalEnsureRequest()
      val exception =
        assertFailsWith<StatusRuntimeException> { service.ensureWorkItem(conflicting) }

      assertThat(retried).isEqualTo(first)
      assertThat(exception.status.code).isEqualTo(Status.Code.ALREADY_EXISTS)
      assertThat(publisher.publicationCount).isEqualTo(1)
    }

  @Test
  fun `producer without creation time fails before publication`() =
    runBlocking<Unit> {
      val publisher = RecordingPublisher()
      val service = buildService(publisher)
      val request = availabilityRequest().withoutCreateTime()

      val exception =
        assertFailsWith<StatusRuntimeException> {
          service.createWorkItem(request.toInternalCreateRequest())
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
      assertThat(publisher.publicationCount).isEqualTo(0)
    }

  private fun buildService(publisher: WorkItemPublisher): SpannerWorkItemsService =
    SpannerWorkItemsService(
      spannerDatabase.databaseClient,
      TestConfig.QUEUE_MAPPING,
      IdGenerator.Default,
      WorkItemPublicationRunner(spannerDatabase.databaseClient, TestConfig.QUEUE_MAPPING, publisher),
    )

  private fun availabilityRequest(): PublicCreateWorkItemRequest =
    DataAvailabilitySyncWorkItems.createRequest(
      "dataProviders/provider-1",
      "dataProviders/provider-1/rawImpressionUploads/upload-1",
      "dataProviders/provider-1/rawImpressionUploads/upload-1/rawImpressionUploadModelLines/line-1",
      "modelProviders/mp/modelSuites/ms/modelLines/ml",
      EVENT_DATE,
      SOURCE_DATA_PATH,
      SOURCE_GENERATION,
      SOURCE_CREATE_TIME,
      emptyMap(),
    )

  private fun PublicCreateWorkItemRequest.withCreateTime(
    createTime: Instant
  ): PublicCreateWorkItemRequest {
    val params = workItem.workItemParams.unpack(PublicWorkItem.WorkItemParams::class.java)
    return toBuilder()
      .setWorkItem(
        workItem
          .toBuilder()
          .setWorkItemParams(
            Any.pack(
              params
                .toBuilder()
                .setDataPathParams(
                  params.dataPathParams.toBuilder().setCreateTime(createTime.toProtoTime())
                )
                .build()
            )
          )
      )
      .build()
  }

  private fun PublicCreateWorkItemRequest.withoutCreateTime(): PublicCreateWorkItemRequest {
    val params = workItem.workItemParams.unpack(PublicWorkItem.WorkItemParams::class.java)
    return toBuilder()
      .setWorkItem(
        workItem
          .toBuilder()
          .setWorkItemParams(
            Any.pack(
              params
                .toBuilder()
                .setDataPathParams(params.dataPathParams.toBuilder().clearCreateTime())
                .build()
            )
          )
      )
      .build()
  }

  private fun PublicCreateWorkItemRequest.toInternalCreateRequest() = createWorkItemRequest {
    workItem = workItem {
      workItemResourceId = workItemId
      queueResourceId = "test-topid-id"
      serializationKey = this@toInternalCreateRequest.workItem.serializationKey
      workItemParams = this@toInternalCreateRequest.workItem.workItemParams
    }
  }

  private fun PublicCreateWorkItemRequest.toInternalEnsureRequest() = ensureWorkItemRequest {
    workItem = toInternalCreateRequest().workItem
  }

  private class RecordingPublisher : WorkItemPublisher {
    var publicationCount = 0
      private set

    override suspend fun publishMessage(queueName: String, message: Message) {
      publicationCount++
    }
  }

  companion object {
    @ClassRule @JvmField val spannerEmulator = SpannerEmulatorRule()

    private const val SOURCE_DATA_PATH = "gs://bucket/model-line/model-line-1/2026-08-15/done"
    private const val SOURCE_GENERATION = 100L
    private val SOURCE_CREATE_TIME = Instant.parse("2026-08-15T10:15:30Z")
    private val EVENT_DATE = LocalDate.of(2026, 8, 15)
  }
}
