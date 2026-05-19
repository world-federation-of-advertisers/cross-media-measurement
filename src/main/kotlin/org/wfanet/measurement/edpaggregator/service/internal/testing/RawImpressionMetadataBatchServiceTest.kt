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

package org.wfanet.measurement.edpaggregator.service.internal.testing

import com.google.common.truth.Truth.assertThat
import com.google.common.truth.extensions.proto.ProtoTruth.assertThat
import com.google.rpc.errorInfo
import io.grpc.Status
import io.grpc.StatusRuntimeException
import java.time.Instant
import java.util.UUID
import kotlin.test.assertFailsWith
import kotlinx.coroutines.runBlocking
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.common.IdGenerator
import org.wfanet.measurement.common.grpc.errorInfo
import org.wfanet.measurement.common.toInstant
import org.wfanet.measurement.edpaggregator.service.internal.Errors
import org.wfanet.measurement.internal.edpaggregator.ListRawImpressionMetadataBatchesRequestKt
import org.wfanet.measurement.internal.edpaggregator.ListRawImpressionMetadataBatchesResponse
import org.wfanet.measurement.internal.edpaggregator.RawImpressionBatchState
import org.wfanet.measurement.internal.edpaggregator.RawImpressionMetadataBatch
import org.wfanet.measurement.internal.edpaggregator.RawImpressionMetadataBatchServiceGrpcKt.RawImpressionMetadataBatchServiceCoroutineImplBase
import org.wfanet.measurement.internal.edpaggregator.createRawImpressionMetadataBatchRequest
import org.wfanet.measurement.internal.edpaggregator.deleteRawImpressionMetadataBatchRequest
import org.wfanet.measurement.internal.edpaggregator.getRawImpressionMetadataBatchRequest
import org.wfanet.measurement.internal.edpaggregator.listRawImpressionMetadataBatchesRequest
import org.wfanet.measurement.internal.edpaggregator.markRawImpressionMetadataBatchFailedRequest
import org.wfanet.measurement.internal.edpaggregator.markRawImpressionMetadataBatchProcessedRequest

@RunWith(JUnit4::class)
abstract class RawImpressionMetadataBatchServiceTest {
  private lateinit var service: RawImpressionMetadataBatchServiceCoroutineImplBase

  protected abstract fun newService(
    idGenerator: IdGenerator = IdGenerator.Default
  ): RawImpressionMetadataBatchServiceCoroutineImplBase

  @Before
  fun initService() {
    service = newService()
  }

  @Test
  fun `createRawImpressionMetadataBatch creates a batch`() = runBlocking {
    val startTime: Instant = Instant.now()

    val batch: RawImpressionMetadataBatch =
      service.createRawImpressionMetadataBatch(
        createRawImpressionMetadataBatchRequest {
          dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
          requestId = UUID.randomUUID().toString()
        }
      )

    assertThat(batch.dataProviderResourceId).isEqualTo(DATA_PROVIDER_RESOURCE_ID)
    assertThat(batch.batchResourceId).startsWith("batch-")
    assertThat(batch.state).isEqualTo(RawImpressionBatchState.RAW_IMPRESSION_BATCH_STATE_CREATED)
    assertThat(batch.createTime.toInstant()).isGreaterThan(startTime)
    assertThat(batch.updateTime).isEqualTo(batch.createTime)
  }

  @Test
  fun `createRawImpressionMetadataBatch is idempotent with same request_id`() = runBlocking {
    val requestId: String = UUID.randomUUID().toString()

    val batch1: RawImpressionMetadataBatch =
      service.createRawImpressionMetadataBatch(
        createRawImpressionMetadataBatchRequest {
          dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
          this.requestId = requestId
        }
      )

    val batch2: RawImpressionMetadataBatch =
      service.createRawImpressionMetadataBatch(
        createRawImpressionMetadataBatchRequest {
          dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
          this.requestId = requestId
        }
      )

    assertThat(batch2).isEqualTo(batch1)
  }

  @Test
  fun `createRawImpressionMetadataBatch throws INVALID_ARGUMENT if data_provider_resource_id not set`() =
    runBlocking {
      val exception: StatusRuntimeException =
        assertFailsWith<StatusRuntimeException> {
          service.createRawImpressionMetadataBatch(createRawImpressionMetadataBatchRequest {})
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
      assertThat(exception.errorInfo)
        .isEqualTo(
          errorInfo {
            domain = Errors.DOMAIN
            reason = Errors.Reason.REQUIRED_FIELD_NOT_SET.name
            metadata[Errors.Metadata.FIELD_NAME.key] = "data_provider_resource_id"
          }
        )
    }

  @Test
  fun `createRawImpressionMetadataBatch auto-generates batchResourceId when not provided`() =
    runBlocking {
      val response: RawImpressionMetadataBatch =
        service.createRawImpressionMetadataBatch(
          createRawImpressionMetadataBatchRequest {
            dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
          }
        )

      assertThat(response.batchResourceId).isNotEmpty()
      assertThat(response.batchResourceId).startsWith("batch-")
    }

  @Test
  fun `getRawImpressionMetadataBatch returns a batch`() = runBlocking {
    val created: RawImpressionMetadataBatch =
      service.createRawImpressionMetadataBatch(
        createRawImpressionMetadataBatchRequest {
          dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
        }
      )

    val batch: RawImpressionMetadataBatch =
      service.getRawImpressionMetadataBatch(
        getRawImpressionMetadataBatchRequest {
          dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
          batchResourceId = created.batchResourceId
        }
      )

    assertThat(batch.dataProviderResourceId).isEqualTo(DATA_PROVIDER_RESOURCE_ID)
    assertThat(batch.batchResourceId).isEqualTo(created.batchResourceId)
    assertThat(batch.state).isEqualTo(RawImpressionBatchState.RAW_IMPRESSION_BATCH_STATE_CREATED)
  }

  @Test
  fun `getRawImpressionMetadataBatch throws NOT_FOUND when batch not found`() = runBlocking {
    val exception: StatusRuntimeException =
      assertFailsWith<StatusRuntimeException> {
        service.getRawImpressionMetadataBatch(
          getRawImpressionMetadataBatchRequest {
            dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
            batchResourceId = "nonexistent-batch"
          }
        )
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
    assertThat(exception.errorInfo)
      .isEqualTo(
        errorInfo {
          domain = Errors.DOMAIN
          reason = Errors.Reason.RAW_IMPRESSION_METADATA_BATCH_NOT_FOUND.name
          metadata[Errors.Metadata.DATA_PROVIDER_RESOURCE_ID.key] = DATA_PROVIDER_RESOURCE_ID
          metadata[Errors.Metadata.BATCH_RESOURCE_ID.key] = "nonexistent-batch"
        }
      )
  }

  @Test
  fun `listRawImpressionMetadataBatches returns batches`() = runBlocking {
    service.createRawImpressionMetadataBatch(
      createRawImpressionMetadataBatchRequest { dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID }
    )
    service.createRawImpressionMetadataBatch(
      createRawImpressionMetadataBatchRequest { dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID }
    )

    val response: ListRawImpressionMetadataBatchesResponse =
      service.listRawImpressionMetadataBatches(
        listRawImpressionMetadataBatchesRequest {
          dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
        }
      )

    assertThat(response.rawImpressionMetadataBatchesList).hasSize(2)
  }

  @Test
  fun `listRawImpressionMetadataBatches respects page size`() = runBlocking {
    for (i in 1..3) {
      service.createRawImpressionMetadataBatch(
        createRawImpressionMetadataBatchRequest {
          dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
        }
      )
    }

    val response: ListRawImpressionMetadataBatchesResponse =
      service.listRawImpressionMetadataBatches(
        listRawImpressionMetadataBatchesRequest {
          dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
          pageSize = 2
        }
      )

    assertThat(response.rawImpressionMetadataBatchesList).hasSize(2)
    assertThat(response.hasNextPageToken()).isTrue()
  }

  @Test
  fun `listRawImpressionMetadataBatches filters by state`() = runBlocking {
    val batch1: RawImpressionMetadataBatch =
      service.createRawImpressionMetadataBatch(
        createRawImpressionMetadataBatchRequest {
          dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
        }
      )
    service.createRawImpressionMetadataBatch(
      createRawImpressionMetadataBatchRequest { dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID }
    )

    service.markRawImpressionMetadataBatchProcessed(
      markRawImpressionMetadataBatchProcessedRequest {
        dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
        batchResourceId = batch1.batchResourceId
      }
    )

    val response: ListRawImpressionMetadataBatchesResponse =
      service.listRawImpressionMetadataBatches(
        listRawImpressionMetadataBatchesRequest {
          dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
          filter =
            ListRawImpressionMetadataBatchesRequestKt.filter {
              state = RawImpressionBatchState.RAW_IMPRESSION_BATCH_STATE_PROCESSED
            }
        }
      )

    assertThat(response.rawImpressionMetadataBatchesList).hasSize(1)
    assertThat(response.rawImpressionMetadataBatchesList.first().batchResourceId)
      .isEqualTo(batch1.batchResourceId)
  }

  @Test
  fun `deleteRawImpressionMetadataBatch soft deletes a batch`() = runBlocking {
    val created: RawImpressionMetadataBatch =
      service.createRawImpressionMetadataBatch(
        createRawImpressionMetadataBatchRequest {
          dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
        }
      )

    val deletedBatch: RawImpressionMetadataBatch =
      service.deleteRawImpressionMetadataBatch(
        deleteRawImpressionMetadataBatchRequest {
          dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
          batchResourceId = created.batchResourceId
        }
      )

    assertThat(deletedBatch.hasDeleteTime()).isTrue()
  }

  @Test
  fun `deleteRawImpressionMetadataBatch throws NOT_FOUND when batch not found`() = runBlocking {
    val exception: StatusRuntimeException =
      assertFailsWith<StatusRuntimeException> {
        service.deleteRawImpressionMetadataBatch(
          deleteRawImpressionMetadataBatchRequest {
            dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
            batchResourceId = "nonexistent-batch"
          }
        )
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
  }

  @Test
  fun `deleteRawImpressionMetadataBatch throws NOT_FOUND when batch already deleted`() =
    runBlocking {
      val created: RawImpressionMetadataBatch =
        service.createRawImpressionMetadataBatch(
          createRawImpressionMetadataBatchRequest {
            dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
          }
        )

      service.deleteRawImpressionMetadataBatch(
        deleteRawImpressionMetadataBatchRequest {
          dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
          batchResourceId = created.batchResourceId
        }
      )

      val exception: StatusRuntimeException =
        assertFailsWith<StatusRuntimeException> {
          service.deleteRawImpressionMetadataBatch(
            deleteRawImpressionMetadataBatchRequest {
              dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
              batchResourceId = created.batchResourceId
            }
          )
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
    }

  @Test
  fun `markRawImpressionMetadataBatchProcessed transitions state to PROCESSED`() = runBlocking {
    val created: RawImpressionMetadataBatch =
      service.createRawImpressionMetadataBatch(
        createRawImpressionMetadataBatchRequest {
          dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
        }
      )

    val batch: RawImpressionMetadataBatch =
      service.markRawImpressionMetadataBatchProcessed(
        markRawImpressionMetadataBatchProcessedRequest {
          dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
          batchResourceId = created.batchResourceId
        }
      )

    assertThat(batch.state).isEqualTo(RawImpressionBatchState.RAW_IMPRESSION_BATCH_STATE_PROCESSED)
  }

  @Test
  fun `markRawImpressionMetadataBatchFailed transitions state to FAILED`() = runBlocking {
    val created: RawImpressionMetadataBatch =
      service.createRawImpressionMetadataBatch(
        createRawImpressionMetadataBatchRequest {
          dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
        }
      )

    val batch: RawImpressionMetadataBatch =
      service.markRawImpressionMetadataBatchFailed(
        markRawImpressionMetadataBatchFailedRequest {
          dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
          batchResourceId = created.batchResourceId
        }
      )

    assertThat(batch.state).isEqualTo(RawImpressionBatchState.RAW_IMPRESSION_BATCH_STATE_FAILED)
  }

  @Test
  fun `markRawImpressionMetadataBatchProcessed throws FAILED_PRECONDITION if not in CREATED state`() =
    runBlocking {
      val created: RawImpressionMetadataBatch =
        service.createRawImpressionMetadataBatch(
          createRawImpressionMetadataBatchRequest {
            dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
          }
        )

      service.markRawImpressionMetadataBatchProcessed(
        markRawImpressionMetadataBatchProcessedRequest {
          dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
          batchResourceId = created.batchResourceId
        }
      )

      val exception: StatusRuntimeException =
        assertFailsWith<StatusRuntimeException> {
          service.markRawImpressionMetadataBatchProcessed(
            markRawImpressionMetadataBatchProcessedRequest {
              dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
              batchResourceId = created.batchResourceId
            }
          )
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.FAILED_PRECONDITION)
      assertThat(exception.errorInfo)
        .isEqualTo(
          errorInfo {
            domain = Errors.DOMAIN
            reason = Errors.Reason.RAW_IMPRESSION_METADATA_BATCH_STATE_INVALID.name
            metadata[Errors.Metadata.DATA_PROVIDER_RESOURCE_ID.key] = DATA_PROVIDER_RESOURCE_ID
            metadata[Errors.Metadata.BATCH_RESOURCE_ID.key] = created.batchResourceId
            metadata[Errors.Metadata.RAW_IMPRESSION_BATCH_STATE.key] =
              RawImpressionBatchState.RAW_IMPRESSION_BATCH_STATE_PROCESSED.name
          }
        )
    }

  @Test
  fun `markRawImpressionMetadataBatchProcessed throws NOT_FOUND when batch not found`() =
    runBlocking {
      val exception: StatusRuntimeException =
        assertFailsWith<StatusRuntimeException> {
          service.markRawImpressionMetadataBatchProcessed(
            markRawImpressionMetadataBatchProcessedRequest {
              dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
              batchResourceId = "nonexistent-batch"
            }
          )
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
    }

  @Test
  fun `listRawImpressionMetadataBatches excludes deleted batches by default`() = runBlocking {
    val batch1: RawImpressionMetadataBatch =
      service.createRawImpressionMetadataBatch(
        createRawImpressionMetadataBatchRequest {
          dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
        }
      )
    val batch2: RawImpressionMetadataBatch =
      service.createRawImpressionMetadataBatch(
        createRawImpressionMetadataBatchRequest {
          dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
        }
      )

    service.deleteRawImpressionMetadataBatch(
      deleteRawImpressionMetadataBatchRequest {
        dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
        batchResourceId = batch1.batchResourceId
      }
    )

    val response: ListRawImpressionMetadataBatchesResponse =
      service.listRawImpressionMetadataBatches(
        listRawImpressionMetadataBatchesRequest {
          dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
        }
      )

    assertThat(response.rawImpressionMetadataBatchesList).hasSize(1)
    assertThat(response.rawImpressionMetadataBatchesList.first().batchResourceId)
      .isEqualTo(batch2.batchResourceId)
  }

  @Test
  fun `listRawImpressionMetadataBatches includes deleted batches when showDeleted is true`() =
    runBlocking {
      val batch1: RawImpressionMetadataBatch =
        service.createRawImpressionMetadataBatch(
          createRawImpressionMetadataBatchRequest {
            dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
          }
        )
      service.createRawImpressionMetadataBatch(
        createRawImpressionMetadataBatchRequest {
          dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
        }
      )

      service.deleteRawImpressionMetadataBatch(
        deleteRawImpressionMetadataBatchRequest {
          dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
          batchResourceId = batch1.batchResourceId
        }
      )

      val response: ListRawImpressionMetadataBatchesResponse =
        service.listRawImpressionMetadataBatches(
          listRawImpressionMetadataBatchesRequest {
            dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
            showDeleted = true
          }
        )

      assertThat(response.rawImpressionMetadataBatchesList).hasSize(2)
    }

  @Test
  fun `markRawImpressionMetadataBatchFailed throws FAILED_PRECONDITION if not in CREATED state`() =
    runBlocking {
      val created: RawImpressionMetadataBatch =
        service.createRawImpressionMetadataBatch(
          createRawImpressionMetadataBatchRequest {
            dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
          }
        )

      service.markRawImpressionMetadataBatchFailed(
        markRawImpressionMetadataBatchFailedRequest {
          dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
          batchResourceId = created.batchResourceId
        }
      )

      val exception: StatusRuntimeException =
        assertFailsWith<StatusRuntimeException> {
          service.markRawImpressionMetadataBatchFailed(
            markRawImpressionMetadataBatchFailedRequest {
              dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
              batchResourceId = created.batchResourceId
            }
          )
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.FAILED_PRECONDITION)
    }

  @Test
  fun `markRawImpressionMetadataBatchFailed throws NOT_FOUND when batch not found`() = runBlocking {
    val exception: StatusRuntimeException =
      assertFailsWith<StatusRuntimeException> {
        service.markRawImpressionMetadataBatchFailed(
          markRawImpressionMetadataBatchFailedRequest {
            dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
            batchResourceId = "nonexistent-batch"
          }
        )
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
  }

  @Test
  fun `listRawImpressionMetadataBatches returns remaining batches using page token`() =
    runBlocking {
      for (i in 1..3) {
        service.createRawImpressionMetadataBatch(
          createRawImpressionMetadataBatchRequest {
            dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
            batchResourceId = "batch-$i"
          }
        )
      }

      val firstPage: ListRawImpressionMetadataBatchesResponse =
        service.listRawImpressionMetadataBatches(
          listRawImpressionMetadataBatchesRequest {
            dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
            pageSize = 2
          }
        )

      assertThat(firstPage.rawImpressionMetadataBatchesList).hasSize(2)
      assertThat(firstPage.hasNextPageToken()).isTrue()
      assertThat(firstPage.nextPageToken.after.batchResourceId)
        .isEqualTo(firstPage.rawImpressionMetadataBatchesList.last().batchResourceId)
      assertThat(firstPage.nextPageToken.after.hasCreateTime()).isTrue()
      assertThat(firstPage.nextPageToken.after.createTime)
        .isEqualTo(firstPage.rawImpressionMetadataBatchesList.last().createTime)

      val secondPage: ListRawImpressionMetadataBatchesResponse =
        service.listRawImpressionMetadataBatches(
          listRawImpressionMetadataBatchesRequest {
            dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
            pageSize = 2
            pageToken = firstPage.nextPageToken
          }
        )

      assertThat(secondPage.rawImpressionMetadataBatchesList).hasSize(1)
      assertThat(secondPage.hasNextPageToken()).isFalse()
    }

  @Test
  fun `createRawImpressionMetadataBatch throws INVALID_ARGUMENT for malformed request_id`() =
    runBlocking {
      val exception: StatusRuntimeException =
        assertFailsWith<StatusRuntimeException> {
          service.createRawImpressionMetadataBatch(
            createRawImpressionMetadataBatchRequest {
              dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
              requestId = "not-a-valid-uuid"
            }
          )
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
      assertThat(exception.errorInfo)
        .isEqualTo(
          errorInfo {
            domain = Errors.DOMAIN
            reason = Errors.Reason.INVALID_FIELD_VALUE.name
            metadata[Errors.Metadata.FIELD_NAME.key] = "request_id"
          }
        )
    }

  @Test
  fun `listRawImpressionMetadataBatches page token contains create_time and batch_resource_id`() =
    runBlocking {
      for (i in 1..3) {
        service.createRawImpressionMetadataBatch(
          createRawImpressionMetadataBatchRequest {
            dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
            batchResourceId = "batch-$i"
          }
        )
      }

      val firstPage: ListRawImpressionMetadataBatchesResponse =
        service.listRawImpressionMetadataBatches(
          listRawImpressionMetadataBatchesRequest {
            dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
            pageSize = 2
          }
        )

      assertThat(firstPage.nextPageToken.after.batchResourceId)
        .isEqualTo(firstPage.rawImpressionMetadataBatchesList.last().batchResourceId)
      assertThat(firstPage.nextPageToken.after.hasCreateTime()).isTrue()
      assertThat(firstPage.nextPageToken.after.createTime)
        .isEqualTo(firstPage.rawImpressionMetadataBatchesList.last().createTime)
    }

  companion object {
    private const val DATA_PROVIDER_RESOURCE_ID = "data-provider-1"
  }
}
