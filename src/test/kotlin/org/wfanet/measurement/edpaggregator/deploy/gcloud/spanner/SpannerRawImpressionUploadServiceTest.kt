// Copyright 2026 The Cross-Media Measurement Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner

import com.google.cloud.spanner.Value
import com.google.common.truth.Truth.assertThat
import io.grpc.Status
import io.grpc.StatusRuntimeException
import java.util.UUID
import kotlin.test.assertFailsWith
import kotlinx.coroutines.runBlocking
import org.junit.ClassRule
import org.junit.Rule
import org.junit.Test
import org.wfanet.measurement.common.IdGenerator
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.testing.Schemata
import org.wfanet.measurement.edpaggregator.service.internal.testing.RawImpressionUploadServiceTest
import org.wfanet.measurement.gcloud.spanner.AsyncDatabaseClient
import org.wfanet.measurement.gcloud.spanner.insertMutation
import org.wfanet.measurement.gcloud.spanner.testing.SpannerEmulatorDatabaseRule
import org.wfanet.measurement.gcloud.spanner.testing.SpannerEmulatorRule
import org.wfanet.measurement.internal.edpaggregator.DataAvailabilitySyncLeaseState
import org.wfanet.measurement.internal.edpaggregator.RawImpressionUploadModelLineFailureReason
import org.wfanet.measurement.internal.edpaggregator.RawImpressionUploadModelLineState
import org.wfanet.measurement.internal.edpaggregator.RawImpressionUploadServiceGrpcKt
import org.wfanet.measurement.internal.edpaggregator.RawImpressionUploadState
import org.wfanet.measurement.internal.edpaggregator.VidLabelingEvictionFenceState
import org.wfanet.measurement.internal.edpaggregator.acquireRawImpressionUploadEvictionFenceRequest
import org.wfanet.measurement.internal.edpaggregator.advanceRawImpressionUploadEvictionFenceRequest
import org.wfanet.measurement.internal.edpaggregator.createRawImpressionUploadRequest
import org.wfanet.measurement.internal.edpaggregator.rawImpressionUpload

class SpannerRawImpressionUploadServiceTest : RawImpressionUploadServiceTest() {
  @get:Rule
  val spannerDatabase =
    SpannerEmulatorDatabaseRule(spannerEmulator, Schemata.EDP_AGGREGATOR_CHANGELOG_PATH)

  override fun newService(
    idGenerator: IdGenerator
  ): RawImpressionUploadServiceGrpcKt.RawImpressionUploadServiceCoroutineImplBase {
    val databaseClient: AsyncDatabaseClient = spannerDatabase.databaseClient
    return SpannerRawImpressionUploadService(databaseClient, idGenerator = idGenerator)
  }

  override suspend fun createActiveModelLine(dataProviderResourceId: String) {
    val databaseClient = spannerDatabase.databaseClient
    databaseClient.write(
      listOf(
        insertMutation("RawImpressionUpload") {
          set("DataProviderResourceId").to(dataProviderResourceId)
          set("RawImpressionUploadId").to(1L)
          set("RawImpressionUploadResourceId").to("upload-active")
          set("DoneBlobUri").to("gs://bucket/active/done")
          set("RegistrationComplete").to(true)
          set("State")
            .to(Value.protoEnum(RawImpressionUploadState.RAW_IMPRESSION_UPLOAD_STATE_ACTIVE))
          set("CreateTime").to(Value.COMMIT_TIMESTAMP)
          set("UpdateTime").to(Value.COMMIT_TIMESTAMP)
        }
      )
    )
    databaseClient.write(
      listOf(
        insertMutation("RawImpressionUploadModelLine") {
          set("DataProviderResourceId").to(dataProviderResourceId)
          set("RawImpressionUploadId").to(1L)
          set("RawImpressionUploadModelLineId").to(1L)
          set("RawImpressionUploadModelLineResourceId").to("model-line-active")
          set("CmmsModelLine").to("modelProviders/mp/modelSuites/ms/modelLines/ml")
          set("State")
            .to(
              Value.protoEnum(
                RawImpressionUploadModelLineState.RAW_IMPRESSION_UPLOAD_MODEL_LINE_STATE_LABELING
              )
            )
          set("CreateTime").to(Value.COMMIT_TIMESTAMP)
          set("UpdateTime").to(Value.COMMIT_TIMESTAMP)
        }
      )
    )
  }

  @Test
  fun `eviction fence ignores inactive uploads with incomplete registration`(): Unit = runBlocking {
    val dataProviderResourceId = "data-provider-with-inactive-uploads"
    val databaseClient = spannerDatabase.databaseClient
    databaseClient.write(
      listOf(
        insertMutation("RawImpressionUpload") {
          set("DataProviderResourceId").to(dataProviderResourceId)
          set("RawImpressionUploadId").to(1L)
          set("RawImpressionUploadResourceId").to("completed-upload")
          set("DoneBlobUri").to("gs://bucket/completed/done")
          set("RegistrationComplete").to(false)
          set("State")
            .to(Value.protoEnum(RawImpressionUploadState.RAW_IMPRESSION_UPLOAD_STATE_COMPLETED))
          set("CreateTime").to(Value.COMMIT_TIMESTAMP)
          set("UpdateTime").to(Value.COMMIT_TIMESTAMP)
        },
        insertMutation("RawImpressionUpload") {
          set("DataProviderResourceId").to(dataProviderResourceId)
          set("RawImpressionUploadId").to(2L)
          set("RawImpressionUploadResourceId").to("failed-upload")
          set("DoneBlobUri").to("gs://bucket/failed/done")
          set("RegistrationComplete").to(false)
          set("State")
            .to(Value.protoEnum(RawImpressionUploadState.RAW_IMPRESSION_UPLOAD_STATE_FAILED))
          set("CreateTime").to(Value.COMMIT_TIMESTAMP)
          set("UpdateTime").to(Value.COMMIT_TIMESTAMP)
        },
      )
    )

    newService()
      .acquireRawImpressionUploadEvictionFence(
        acquireRawImpressionUploadEvictionFenceRequest {
          this.dataProviderResourceId = dataProviderResourceId
          evictionOperationId = UUID.randomUUID().toString()
          state = VidLabelingEvictionFenceState.VID_LABELING_EVICTION_FENCE_STATE_EVICTING
        }
      )
  }

  @Test
  fun `approval-pending fence does not wait for active labeling`() = runBlocking {
    createActiveModelLine(TEST_DATA_PROVIDER_ID)

    val response =
      newService()
        .acquireRawImpressionUploadEvictionFence(
          acquireRawImpressionUploadEvictionFenceRequest {
            dataProviderResourceId = TEST_DATA_PROVIDER_ID
            evictionOperationId = EVICTION_OPERATION_ID
            state = VidLabelingEvictionFenceState.VID_LABELING_EVICTION_FENCE_STATE_APPROVAL_PENDING
          }
        )

    assertThat(response.newlyAcquired).isTrue()
  }

  @Test
  fun `approval-pending fence cannot be reacquired for direct eviction`() = runBlocking {
    val service = newService()
    service.acquireRawImpressionUploadEvictionFence(
      acquireRawImpressionUploadEvictionFenceRequest {
        dataProviderResourceId = TEST_DATA_PROVIDER_ID
        evictionOperationId = EVICTION_OPERATION_ID
        state =
          VidLabelingEvictionFenceState.VID_LABELING_EVICTION_FENCE_STATE_APPROVAL_PENDING
      }
    )

    val error =
      assertFailsWith<StatusRuntimeException> {
        service.acquireRawImpressionUploadEvictionFence(
          acquireRawImpressionUploadEvictionFenceRequest {
            dataProviderResourceId = TEST_DATA_PROVIDER_ID
            evictionOperationId = EVICTION_OPERATION_ID
            state = VidLabelingEvictionFenceState.VID_LABELING_EVICTION_FENCE_STATE_EVICTING
          }
        )
      }

    assertThat(error.status.code).isEqualTo(Status.Code.FAILED_PRECONDITION)
  }

  @Test
  fun `evicting fence waits for active data-availability lease`(): Unit = runBlocking {
    insertActiveDataAvailabilitySyncLease()
    val service = newService()
    service.acquireRawImpressionUploadEvictionFence(
      acquireRawImpressionUploadEvictionFenceRequest {
        dataProviderResourceId = TEST_DATA_PROVIDER_ID
        evictionOperationId = EVICTION_OPERATION_ID
        state = VidLabelingEvictionFenceState.VID_LABELING_EVICTION_FENCE_STATE_APPROVAL_PENDING
      }
    )
    service.advanceRawImpressionUploadEvictionFence(
      advanceRawImpressionUploadEvictionFenceRequest {
        dataProviderResourceId = TEST_DATA_PROVIDER_ID
        evictionOperationId = EVICTION_OPERATION_ID
        state = VidLabelingEvictionFenceState.VID_LABELING_EVICTION_FENCE_STATE_DRAINING
      }
    )

    val error =
      assertFailsWith<StatusRuntimeException> {
        service.advanceRawImpressionUploadEvictionFence(
          advanceRawImpressionUploadEvictionFenceRequest {
            dataProviderResourceId = TEST_DATA_PROVIDER_ID
            evictionOperationId = EVICTION_OPERATION_ID
            state = VidLabelingEvictionFenceState.VID_LABELING_EVICTION_FENCE_STATE_EVICTING
          }
        )
      }
    assertThat(error.status.code).isEqualTo(Status.Code.FAILED_PRECONDITION)

    releaseDataAvailabilitySyncLease()
    service.advanceRawImpressionUploadEvictionFence(
      advanceRawImpressionUploadEvictionFenceRequest {
        dataProviderResourceId = TEST_DATA_PROVIDER_ID
        evictionOperationId = EVICTION_OPERATION_ID
        state = VidLabelingEvictionFenceState.VID_LABELING_EVICTION_FENCE_STATE_EVICTING
      }
    )
  }

  @Test
  fun `direct evicting fence rejects active data-availability lease`() = runBlocking {
    insertActiveDataAvailabilitySyncLease()

    val error =
      assertFailsWith<StatusRuntimeException> {
        newService()
          .acquireRawImpressionUploadEvictionFence(
            acquireRawImpressionUploadEvictionFenceRequest {
              dataProviderResourceId = TEST_DATA_PROVIDER_ID
              evictionOperationId = EVICTION_OPERATION_ID
              state = VidLabelingEvictionFenceState.VID_LABELING_EVICTION_FENCE_STATE_EVICTING
            }
          )
      }

    assertThat(error.status.code).isEqualTo(Status.Code.FAILED_PRECONDITION)
  }

  @Test
  fun `approval-pending fence rejects replacement replay`() = runBlocking {
    val doneBlobUri = "gs://bucket/corrected/done"
    createEvictedUpload(TEST_DATA_PROVIDER_ID, "evicted-upload", doneBlobUri, EVICTION_OPERATION_ID)
    val service = newService()
    service.acquireRawImpressionUploadEvictionFence(
      acquireRawImpressionUploadEvictionFenceRequest {
        dataProviderResourceId = TEST_DATA_PROVIDER_ID
        evictionOperationId = EVICTION_OPERATION_ID
        state = VidLabelingEvictionFenceState.VID_LABELING_EVICTION_FENCE_STATE_APPROVAL_PENDING
      }
    )

    val error =
      assertFailsWith<StatusRuntimeException> {
        service.createRawImpressionUpload(
          createRawImpressionUploadRequest {
            dataProviderResourceId = TEST_DATA_PROVIDER_ID
            rawImpressionUpload = rawImpressionUpload {
              this.doneBlobUri = doneBlobUri
              doneBlobGeneration = 2L
              doneBlobCreateTime = com.google.protobuf.timestamp { seconds = 2L }
            }
            requestId = UUID.randomUUID().toString()
            evictionOperationId = EVICTION_OPERATION_ID
          }
        )
      }

    assertThat(error.status.code).isEqualTo(Status.Code.FAILED_PRECONDITION)
  }

  private suspend fun insertActiveDataAvailabilitySyncLease() {
    spannerDatabase.databaseClient.write(
      listOf(
        insertMutation("DataAvailabilitySyncLease") {
          set("DataProviderResourceId").to(TEST_DATA_PROVIDER_ID)
          set("SynchronizationAttemptId").to(SYNCHRONIZATION_ATTEMPT_ID)
          set("State")
            .to(
              Value.protoEnum(
                DataAvailabilitySyncLeaseState.DATA_AVAILABILITY_SYNC_LEASE_STATE_ACTIVE
              )
            )
          set("ExpireTime").to(com.google.cloud.Timestamp.ofTimeSecondsAndNanos(4_102_444_800L, 0))
          set("MutationRequestIds").toStringArray(emptyList())
          set("MutationRequestFingerprints").toBytesArray(emptyList())
          set("CreateTime").to(Value.COMMIT_TIMESTAMP)
          set("UpdateTime").to(Value.COMMIT_TIMESTAMP)
        }
      )
    )
  }

  private suspend fun releaseDataAvailabilitySyncLease() {
    spannerDatabase.databaseClient.write(
      listOf(
        com.google.cloud.spanner.Mutation.newUpdateBuilder("DataAvailabilitySyncLease")
          .set("DataProviderResourceId")
          .to(TEST_DATA_PROVIDER_ID)
          .set("SynchronizationAttemptId")
          .to(SYNCHRONIZATION_ATTEMPT_ID)
          .set("State")
          .to(
            Value.protoEnum(
              DataAvailabilitySyncLeaseState.DATA_AVAILABILITY_SYNC_LEASE_STATE_RELEASED
            )
          )
          .set("UpdateTime")
          .to(Value.COMMIT_TIMESTAMP)
          .build()
      )
    )
  }

  override suspend fun createEvictedUpload(
    dataProviderResourceId: String,
    rawImpressionUploadResourceId: String,
    doneBlobUri: String,
    evictionOperationId: String,
  ) {
    val databaseClient = spannerDatabase.databaseClient
    databaseClient.write(
      listOf(
        insertMutation("RawImpressionUpload") {
          set("DataProviderResourceId").to(dataProviderResourceId)
          set("RawImpressionUploadId").to(2L)
          set("RawImpressionUploadResourceId").to(rawImpressionUploadResourceId)
          set("DoneBlobUri").to(doneBlobUri)
          set("DoneBlobGeneration").to(1L)
          set("DoneBlobCreateTime").to(com.google.cloud.Timestamp.ofTimeSecondsAndNanos(1L, 0))
          set("RegistrationComplete").to(true)
          set("State")
            .to(Value.protoEnum(RawImpressionUploadState.RAW_IMPRESSION_UPLOAD_STATE_FAILED))
          set("CreateTime").to(Value.COMMIT_TIMESTAMP)
          set("UpdateTime").to(Value.COMMIT_TIMESTAMP)
        },
        insertMutation("RawImpressionUploadModelLine") {
          set("DataProviderResourceId").to(dataProviderResourceId)
          set("RawImpressionUploadId").to(2L)
          set("RawImpressionUploadModelLineId").to(1L)
          set("RawImpressionUploadModelLineResourceId").to("model-line-evicted")
          set("CmmsModelLine").to("modelProviders/mp/modelSuites/ms/modelLines/ml")
          set("State")
            .to(
              Value.protoEnum(
                RawImpressionUploadModelLineState.RAW_IMPRESSION_UPLOAD_MODEL_LINE_STATE_FAILED
              )
            )
          set("FailureReason")
            .to(
              Value.protoEnum(
                RawImpressionUploadModelLineFailureReason
                  .RAW_IMPRESSION_UPLOAD_MODEL_LINE_FAILURE_REASON_EVICTED_OUTPUT
              )
            )
          set("EvictionOperationId").to(evictionOperationId)
          set("CreateTime").to(Value.COMMIT_TIMESTAMP)
          set("UpdateTime").to(Value.COMMIT_TIMESTAMP)
        },
      )
    )
  }

  companion object {
    @get:ClassRule @JvmStatic val spannerEmulator = SpannerEmulatorRule()

    private const val EVICTION_OPERATION_ID = "eeeeeeee-eeee-4eee-8eee-eeeeeeeeeeee"
    private const val SYNCHRONIZATION_ATTEMPT_ID = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
    private const val TEST_DATA_PROVIDER_ID = "data-provider"
  }
}
