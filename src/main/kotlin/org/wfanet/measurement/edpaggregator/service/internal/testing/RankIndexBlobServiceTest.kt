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

package org.wfanet.measurement.edpaggregator.service.internal.testing

import com.google.common.truth.Truth.assertThat
import com.google.common.truth.extensions.proto.ProtoTruth.assertThat
import com.google.protobuf.kotlin.toByteStringUtf8
import com.google.rpc.errorInfo
import com.google.type.Date
import com.google.type.date
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
import org.wfanet.measurement.internal.edpaggregator.BlobType
import org.wfanet.measurement.internal.edpaggregator.EncryptedDek
import org.wfanet.measurement.internal.edpaggregator.ListRankIndexBlobsPageToken
import org.wfanet.measurement.internal.edpaggregator.ListRankIndexBlobsRequestKt
import org.wfanet.measurement.internal.edpaggregator.ListRankIndexBlobsResponse
import org.wfanet.measurement.internal.edpaggregator.RankIndexBlob
import org.wfanet.measurement.internal.edpaggregator.RankIndexBlobServiceGrpcKt.RankIndexBlobServiceCoroutineImplBase
import org.wfanet.measurement.internal.edpaggregator.batchCreateRankIndexBlobsRequest
import org.wfanet.measurement.internal.edpaggregator.batchDeleteRankIndexBlobsRequest
import org.wfanet.measurement.internal.edpaggregator.copy
import org.wfanet.measurement.internal.edpaggregator.createRankIndexBlobRequest
import org.wfanet.measurement.internal.edpaggregator.deleteRankIndexBlobRequest
import org.wfanet.measurement.internal.edpaggregator.encryptedDek
import org.wfanet.measurement.internal.edpaggregator.getRankIndexBlobRequest
import org.wfanet.measurement.internal.edpaggregator.listRankIndexBlobsRequest
import org.wfanet.measurement.internal.edpaggregator.rankIndexBlob

@RunWith(JUnit4::class)
abstract class RankIndexBlobServiceTest {
  private lateinit var service: RankIndexBlobServiceCoroutineImplBase

  protected abstract fun newService(
    idGenerator: IdGenerator = IdGenerator.Default
  ): RankIndexBlobServiceCoroutineImplBase

  /**
   * Creates a parent [RawImpressionUpload] row so that child rank index blob rows can be inserted
   * (interleaved table).
   */
  protected abstract suspend fun createParentUpload(
    dataProviderResourceId: String,
    rawImpressionUploadResourceId: String,
  )

  @Before
  fun initService() {
    service = newService()
    runBlocking { createParentUpload(DATA_PROVIDER_RESOURCE_ID, RAW_IMPRESSION_UPLOAD_RESOURCE_ID) }
  }

  @Test
  fun `createRankIndexBlob creates successfully`() =
    runBlocking<Unit> {
      val startTime: Instant = Instant.now()

      val blob: RankIndexBlob =
        service.createRankIndexBlob(
          createRankIndexBlobRequest {
            requestId = UUID.randomUUID().toString()
            dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
            rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
            rankIndexBlob = newInternalBlob()
          }
        )

      assertThat(blob.dataProviderResourceId).isEqualTo(DATA_PROVIDER_RESOURCE_ID)
      assertThat(blob.rawImpressionUploadResourceId).isEqualTo(RAW_IMPRESSION_UPLOAD_RESOURCE_ID)
      assertThat(blob.rankIndexBlobResourceId).isNotEmpty()
      assertThat(blob.blobType).isEqualTo(BlobType.BLOB_TYPE_SNAPSHOT)
      assertThat(blob.cmmsModelLine).isEqualTo(CMMS_MODEL_LINE)
      assertThat(blob.poolOffset).isEqualTo(0L)
      assertThat(blob.blobUri).isEqualTo(BLOB_URI)
      assertThat(blob.encryptedDek).isEqualTo(ENCRYPTED_DEK)
      assertThat(blob.createTime.toInstant()).isGreaterThan(startTime)
      assertThat(blob.hasDeleteTime()).isFalse()
    }

  @Test
  fun `createRankIndexBlob persists optional max_event_date and blob_checksum`() =
    runBlocking<Unit> {
      val blob: RankIndexBlob =
        service.createRankIndexBlob(
          createRankIndexBlobRequest {
            requestId = UUID.randomUUID().toString()
            dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
            rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
            rankIndexBlob =
              newInternalBlob(blobType = BlobType.BLOB_TYPE_DAY_ONLY).copy {
                maxEventDate = MAX_EVENT_DATE
                blobChecksum = "checksum".toByteStringUtf8()
              }
          }
        )

      val fetched: RankIndexBlob =
        service.getRankIndexBlob(
          getRankIndexBlobRequest {
            dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
            rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
            rankIndexBlobResourceId = blob.rankIndexBlobResourceId
          }
        )

      assertThat(fetched.maxEventDate).isEqualTo(MAX_EVENT_DATE)
      assertThat(fetched.blobChecksum).isEqualTo("checksum".toByteStringUtf8())
    }

  @Test
  fun `createRankIndexBlob is idempotent with same request_id`() =
    runBlocking<Unit> {
      val requestId: String = UUID.randomUUID().toString()

      val blob1: RankIndexBlob =
        service.createRankIndexBlob(
          createRankIndexBlobRequest {
            this.requestId = requestId
            dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
            rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
            rankIndexBlob = newInternalBlob()
          }
        )
      val blob2: RankIndexBlob =
        service.createRankIndexBlob(
          createRankIndexBlobRequest {
            this.requestId = requestId
            dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
            rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
            rankIndexBlob = newInternalBlob()
          }
        )

      assertThat(blob2).isEqualTo(blob1)
    }

  @Test
  fun `createRankIndexBlob throws NOT_FOUND when parent upload missing`() =
    runBlocking<Unit> {
      val exception: StatusRuntimeException =
        assertFailsWith<StatusRuntimeException> {
          service.createRankIndexBlob(
            createRankIndexBlobRequest {
              requestId = UUID.randomUUID().toString()
              dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
              rawImpressionUploadResourceId = "uploads/missing"
              rankIndexBlob = newInternalBlob()
            }
          )
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
    }

  @Test
  fun `createRankIndexBlob throws INVALID_ARGUMENT if data_provider_resource_id not set`() =
    runBlocking<Unit> {
      val exception: StatusRuntimeException =
        assertFailsWith<StatusRuntimeException> {
          service.createRankIndexBlob(
            createRankIndexBlobRequest {
              rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
              rankIndexBlob = newInternalBlob()
            }
          )
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
  fun `createRankIndexBlob throws INVALID_ARGUMENT if rank_index_blob not set`() =
    runBlocking<Unit> {
      val exception: StatusRuntimeException =
        assertFailsWith<StatusRuntimeException> {
          service.createRankIndexBlob(
            createRankIndexBlobRequest {
              dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
              rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
            }
          )
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
      assertThat(exception.errorInfo)
        .isEqualTo(
          errorInfo {
            domain = Errors.DOMAIN
            reason = Errors.Reason.REQUIRED_FIELD_NOT_SET.name
            metadata[Errors.Metadata.FIELD_NAME.key] = "rank_index_blob"
          }
        )
    }

  @Test
  fun `createRankIndexBlob throws INVALID_ARGUMENT if blob_type unspecified`() =
    runBlocking<Unit> {
      val exception: StatusRuntimeException =
        assertFailsWith<StatusRuntimeException> {
          service.createRankIndexBlob(
            createRankIndexBlobRequest {
              dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
              rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
              rankIndexBlob = newInternalBlob().copy { clearBlobType() }
            }
          )
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
      assertThat(exception.errorInfo)
        .isEqualTo(
          errorInfo {
            domain = Errors.DOMAIN
            reason = Errors.Reason.REQUIRED_FIELD_NOT_SET.name
            metadata[Errors.Metadata.FIELD_NAME.key] = "rank_index_blob.blob_type"
          }
        )
    }

  @Test
  fun `createRankIndexBlob throws INVALID_ARGUMENT if cmms_model_line not set`() =
    runBlocking<Unit> {
      val exception: StatusRuntimeException =
        assertFailsWith<StatusRuntimeException> {
          service.createRankIndexBlob(
            createRankIndexBlobRequest {
              dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
              rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
              rankIndexBlob = newInternalBlob().copy { clearCmmsModelLine() }
            }
          )
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    }

  @Test
  fun `createRankIndexBlob throws INVALID_ARGUMENT if blob_uri not set`() =
    runBlocking<Unit> {
      val exception: StatusRuntimeException =
        assertFailsWith<StatusRuntimeException> {
          service.createRankIndexBlob(
            createRankIndexBlobRequest {
              dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
              rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
              rankIndexBlob = newInternalBlob().copy { clearBlobUri() }
            }
          )
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    }

  @Test
  fun `createRankIndexBlob throws INVALID_ARGUMENT if encrypted_dek not set`() =
    runBlocking<Unit> {
      val exception: StatusRuntimeException =
        assertFailsWith<StatusRuntimeException> {
          service.createRankIndexBlob(
            createRankIndexBlobRequest {
              dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
              rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
              rankIndexBlob = newInternalBlob().copy { clearEncryptedDek() }
            }
          )
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    }

  @Test
  fun `createRankIndexBlob throws INVALID_ARGUMENT for malformed request_id`() =
    runBlocking<Unit> {
      val exception: StatusRuntimeException =
        assertFailsWith<StatusRuntimeException> {
          service.createRankIndexBlob(
            createRankIndexBlobRequest {
              requestId = "not-a-valid-uuid"
              dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
              rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
              rankIndexBlob = newInternalBlob()
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
  fun `createRankIndexBlob throws INVALID_ARGUMENT if request_id not set`() =
    runBlocking<Unit> {
      val exception: StatusRuntimeException =
        assertFailsWith<StatusRuntimeException> {
          service.createRankIndexBlob(
            createRankIndexBlobRequest {
              dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
              rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
              rankIndexBlob = newInternalBlob()
            }
          )
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
      assertThat(exception.errorInfo)
        .isEqualTo(
          errorInfo {
            domain = Errors.DOMAIN
            reason = Errors.Reason.REQUIRED_FIELD_NOT_SET.name
            metadata[Errors.Metadata.FIELD_NAME.key] = "request_id"
          }
        )
    }

  @Test
  fun `createRankIndexBlob throws ALREADY_EXISTS for duplicate natural key`() =
    runBlocking<Unit> {
      createBlob(blobType = BlobType.BLOB_TYPE_SNAPSHOT, poolOffset = 0L)

      val exception: StatusRuntimeException =
        assertFailsWith<StatusRuntimeException> {
          createBlob(blobType = BlobType.BLOB_TYPE_SNAPSHOT, poolOffset = 0L)
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.ALREADY_EXISTS)
    }

  @Test
  fun `batchCreateRankIndexBlobs creates multiple`() =
    runBlocking<Unit> {
      val response =
        service.batchCreateRankIndexBlobs(
          batchCreateRankIndexBlobsRequest {
            dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
            rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
            requests += createRankIndexBlobRequest {
              requestId = UUID.randomUUID().toString()
              rankIndexBlob = newInternalBlob(poolOffset = 0L)
            }
            requests += createRankIndexBlobRequest {
              requestId = UUID.randomUUID().toString()
              rankIndexBlob = newInternalBlob(poolOffset = 1L)
            }
          }
        )

      assertThat(response.rankIndexBlobsList).hasSize(2)
      assertThat(response.rankIndexBlobsList.map { it.poolOffset }).containsExactly(0L, 1L)
    }

  @Test
  fun `batchCreateRankIndexBlobs is idempotent on repeated request_ids`() =
    runBlocking<Unit> {
      val requestId = UUID.randomUUID().toString()
      val request = batchCreateRankIndexBlobsRequest {
        dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
        rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
        requests += createRankIndexBlobRequest {
          this.requestId = requestId
          rankIndexBlob = newInternalBlob()
        }
      }

      val first = service.batchCreateRankIndexBlobs(request)
      val second = service.batchCreateRankIndexBlobs(request)

      assertThat(second.rankIndexBlobsList).isEqualTo(first.rankIndexBlobsList)
    }

  @Test
  fun `batchCreateRankIndexBlobs throws NOT_FOUND when parent upload missing`() =
    runBlocking<Unit> {
      val exception: StatusRuntimeException =
        assertFailsWith<StatusRuntimeException> {
          service.batchCreateRankIndexBlobs(
            batchCreateRankIndexBlobsRequest {
              dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
              rawImpressionUploadResourceId = "uploads/missing"
              requests += createRankIndexBlobRequest {
                requestId = UUID.randomUUID().toString()
                rankIndexBlob = newInternalBlob()
              }
            }
          )
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
    }

  @Test
  fun `batchCreateRankIndexBlobs throws INVALID_ARGUMENT when exceeding max batch size`() =
    runBlocking<Unit> {
      val exception: StatusRuntimeException =
        assertFailsWith<StatusRuntimeException> {
          service.batchCreateRankIndexBlobs(
            batchCreateRankIndexBlobsRequest {
              dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
              rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
              for (i in 0 until 51) {
                requests += createRankIndexBlobRequest {
                  rankIndexBlob = newInternalBlob(poolOffset = i.toLong())
                }
              }
            }
          )
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
      assertThat(exception.errorInfo)
        .isEqualTo(
          errorInfo {
            domain = Errors.DOMAIN
            reason = Errors.Reason.INVALID_FIELD_VALUE.name
            metadata[Errors.Metadata.FIELD_NAME.key] = "requests"
          }
        )
    }

  @Test
  fun `batchCreateRankIndexBlobs throws INVALID_ARGUMENT for duplicate request_id`() =
    runBlocking<Unit> {
      val requestId = UUID.randomUUID().toString()

      val exception: StatusRuntimeException =
        assertFailsWith<StatusRuntimeException> {
          service.batchCreateRankIndexBlobs(
            batchCreateRankIndexBlobsRequest {
              dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
              rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
              requests += createRankIndexBlobRequest {
                this.requestId = requestId
                rankIndexBlob = newInternalBlob(poolOffset = 0L)
              }
              requests += createRankIndexBlobRequest {
                this.requestId = requestId
                rankIndexBlob = newInternalBlob(poolOffset = 1L)
              }
            }
          )
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
      assertThat(exception.errorInfo)
        .isEqualTo(
          errorInfo {
            domain = Errors.DOMAIN
            reason = Errors.Reason.INVALID_FIELD_VALUE.name
            metadata[Errors.Metadata.FIELD_NAME.key] = "requests[1].request_id"
          }
        )
    }

  @Test
  fun `batchCreateRankIndexBlobs throws INVALID_ARGUMENT if request_id not set`() =
    runBlocking<Unit> {
      val exception: StatusRuntimeException =
        assertFailsWith<StatusRuntimeException> {
          service.batchCreateRankIndexBlobs(
            batchCreateRankIndexBlobsRequest {
              dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
              rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
              requests += createRankIndexBlobRequest { rankIndexBlob = newInternalBlob() }
            }
          )
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
      assertThat(exception.errorInfo)
        .isEqualTo(
          errorInfo {
            domain = Errors.DOMAIN
            reason = Errors.Reason.REQUIRED_FIELD_NOT_SET.name
            metadata[Errors.Metadata.FIELD_NAME.key] = "requests[0].request_id"
          }
        )
    }

  @Test
  fun `batchCreateRankIndexBlobs reuses existing and inserts new on partial replay`() =
    runBlocking<Unit> {
      val existingRequestId = UUID.randomUUID().toString()
      val existing: RankIndexBlob = createBlob(poolOffset = 0L, requestId = existingRequestId)

      val response =
        service.batchCreateRankIndexBlobs(
          batchCreateRankIndexBlobsRequest {
            dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
            rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
            requests += createRankIndexBlobRequest {
              requestId = existingRequestId
              rankIndexBlob = newInternalBlob(poolOffset = 0L)
            }
            requests += createRankIndexBlobRequest {
              requestId = UUID.randomUUID().toString()
              rankIndexBlob = newInternalBlob(poolOffset = 1L)
            }
          }
        )

      assertThat(response.rankIndexBlobsList).hasSize(2)
      assertThat(response.rankIndexBlobsList[0].rankIndexBlobResourceId)
        .isEqualTo(existing.rankIndexBlobResourceId)
      assertThat(response.rankIndexBlobsList[1].rankIndexBlobResourceId)
        .isNotEqualTo(existing.rankIndexBlobResourceId)
    }

  @Test
  fun `getRankIndexBlob returns existing resource`() =
    runBlocking<Unit> {
      val created: RankIndexBlob = createBlob()

      val blob: RankIndexBlob =
        service.getRankIndexBlob(
          getRankIndexBlobRequest {
            dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
            rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
            rankIndexBlobResourceId = created.rankIndexBlobResourceId
          }
        )

      assertThat(blob).isEqualTo(created)
    }

  @Test
  fun `getRankIndexBlob throws NOT_FOUND when not found`() =
    runBlocking<Unit> {
      val exception: StatusRuntimeException =
        assertFailsWith<StatusRuntimeException> {
          service.getRankIndexBlob(
            getRankIndexBlobRequest {
              dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
              rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
              rankIndexBlobResourceId = "nonexistent-blob"
            }
          )
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
    }

  @Test
  fun `getRankIndexBlob throws INVALID_ARGUMENT if rank_index_blob_resource_id not set`() =
    runBlocking<Unit> {
      val exception: StatusRuntimeException =
        assertFailsWith<StatusRuntimeException> {
          service.getRankIndexBlob(
            getRankIndexBlobRequest {
              dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
              rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
            }
          )
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    }

  @Test
  fun `listRankIndexBlobs returns resources`() =
    runBlocking<Unit> {
      createBlob(poolOffset = 0L)
      createBlob(poolOffset = 1L)

      val response: ListRankIndexBlobsResponse =
        service.listRankIndexBlobs(
          listRankIndexBlobsRequest {
            dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
            rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
          }
        )

      assertThat(response.rankIndexBlobsList).hasSize(2)
    }

  @Test
  fun `listRankIndexBlobs respects page_size`() =
    runBlocking<Unit> {
      for (i in 0..2) {
        createBlob(poolOffset = i.toLong())
      }

      val response: ListRankIndexBlobsResponse =
        service.listRankIndexBlobs(
          listRankIndexBlobsRequest {
            dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
            rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
            pageSize = 2
          }
        )

      assertThat(response.rankIndexBlobsList).hasSize(2)
      assertThat(response.hasNextPageToken()).isTrue()
    }

  @Test
  fun `listRankIndexBlobs paginates with page_token`() =
    runBlocking<Unit> {
      for (i in 0..2) {
        createBlob(poolOffset = i.toLong())
      }

      val firstPage: ListRankIndexBlobsResponse =
        service.listRankIndexBlobs(
          listRankIndexBlobsRequest {
            dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
            rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
            pageSize = 2
          }
        )
      assertThat(firstPage.rankIndexBlobsList).hasSize(2)
      assertThat(firstPage.hasNextPageToken()).isTrue()

      val secondPage: ListRankIndexBlobsResponse =
        service.listRankIndexBlobs(
          listRankIndexBlobsRequest {
            dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
            rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
            pageSize = 2
            pageToken = firstPage.nextPageToken
          }
        )

      assertThat(secondPage.rankIndexBlobsList).hasSize(1)
      assertThat(secondPage.hasNextPageToken()).isFalse()
    }

  @Test
  fun `listRankIndexBlobs filters by blob_type`() =
    runBlocking<Unit> {
      createBlob(blobType = BlobType.BLOB_TYPE_SNAPSHOT, poolOffset = 0L)
      createBlob(blobType = BlobType.BLOB_TYPE_DAY_ONLY, poolOffset = 1L)

      val response: ListRankIndexBlobsResponse =
        service.listRankIndexBlobs(
          listRankIndexBlobsRequest {
            dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
            rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
            filter = ListRankIndexBlobsRequestKt.filter { blobType = BlobType.BLOB_TYPE_DAY_ONLY }
          }
        )

      assertThat(response.rankIndexBlobsList).hasSize(1)
      assertThat(response.rankIndexBlobsList.first().blobType)
        .isEqualTo(BlobType.BLOB_TYPE_DAY_ONLY)
    }

  @Test
  fun `listRankIndexBlobs filters by cmms_model_line`() =
    runBlocking<Unit> {
      createBlob(cmmsModelLine = CMMS_MODEL_LINE, poolOffset = 0L)
      createBlob(cmmsModelLine = CMMS_MODEL_LINE_2, poolOffset = 1L)

      val response: ListRankIndexBlobsResponse =
        service.listRankIndexBlobs(
          listRankIndexBlobsRequest {
            dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
            rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
            filter = ListRankIndexBlobsRequestKt.filter { cmmsModelLine = CMMS_MODEL_LINE_2 }
          }
        )

      assertThat(response.rankIndexBlobsList).hasSize(1)
      assertThat(response.rankIndexBlobsList.first().cmmsModelLine).isEqualTo(CMMS_MODEL_LINE_2)
    }

  @Test
  fun `listRankIndexBlobs filters by pool_offset`() =
    runBlocking<Unit> {
      createBlob(poolOffset = 0L)
      createBlob(poolOffset = 7L)

      val response: ListRankIndexBlobsResponse =
        service.listRankIndexBlobs(
          listRankIndexBlobsRequest {
            dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
            rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
            filter = ListRankIndexBlobsRequestKt.filter { poolOffset = 0L }
          }
        )

      assertThat(response.rankIndexBlobsList).hasSize(1)
      assertThat(response.rankIndexBlobsList.first().poolOffset).isEqualTo(0L)
    }

  @Test
  fun `listRankIndexBlobs filters by max_event_date_on_or_before`() =
    runBlocking<Unit> {
      createBlob(blobType = BlobType.BLOB_TYPE_DAY_ONLY, poolOffset = 0L, maxEventDate = EARLY_DATE)
      createBlob(blobType = BlobType.BLOB_TYPE_DAY_ONLY, poolOffset = 1L, maxEventDate = LATE_DATE)

      val response: ListRankIndexBlobsResponse =
        service.listRankIndexBlobs(
          listRankIndexBlobsRequest {
            dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
            rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
            filter = ListRankIndexBlobsRequestKt.filter { maxEventDateOnOrBefore = EARLY_DATE }
          }
        )

      assertThat(response.rankIndexBlobsList).hasSize(1)
      assertThat(response.rankIndexBlobsList.first().maxEventDate).isEqualTo(EARLY_DATE)
    }

  @Test
  fun `listRankIndexBlobs lists across uploads when upload id omitted`() =
    runBlocking<Unit> {
      createBlob()
      createParentUpload(DATA_PROVIDER_RESOURCE_ID, SECOND_UPLOAD_RESOURCE_ID)
      createBlob(uploadResourceId = SECOND_UPLOAD_RESOURCE_ID)

      val response: ListRankIndexBlobsResponse =
        service.listRankIndexBlobs(
          listRankIndexBlobsRequest { dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID }
        )

      assertThat(response.rankIndexBlobsList).hasSize(2)
    }

  @Test
  fun `listRankIndexBlobs excludes soft-deleted by default`() =
    runBlocking<Unit> {
      val kept: RankIndexBlob = createBlob(poolOffset = 0L)
      val deleted: RankIndexBlob = createBlob(poolOffset = 1L)
      deleteBlob(deleted.rankIndexBlobResourceId)

      val response: ListRankIndexBlobsResponse =
        service.listRankIndexBlobs(
          listRankIndexBlobsRequest {
            dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
            rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
          }
        )

      assertThat(response.rankIndexBlobsList.map { it.rankIndexBlobResourceId })
        .containsExactly(kept.rankIndexBlobResourceId)
    }

  @Test
  fun `listRankIndexBlobs with show_deleted returns active and deleted`() =
    runBlocking<Unit> {
      val kept: RankIndexBlob = createBlob(poolOffset = 0L)
      val deleted: RankIndexBlob = createBlob(poolOffset = 1L)
      deleteBlob(deleted.rankIndexBlobResourceId)

      val response: ListRankIndexBlobsResponse =
        service.listRankIndexBlobs(
          listRankIndexBlobsRequest {
            dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
            rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
            showDeleted = true
          }
        )

      assertThat(response.rankIndexBlobsList.map { it.rankIndexBlobResourceId })
        .containsExactly(kept.rankIndexBlobResourceId, deleted.rankIndexBlobResourceId)
    }

  @Test
  fun `deleteRankIndexBlob soft-deletes the resource`() =
    runBlocking<Unit> {
      val created: RankIndexBlob = createBlob()

      val deleted: RankIndexBlob =
        service.deleteRankIndexBlob(
          deleteRankIndexBlobRequest {
            dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
            rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
            rankIndexBlobResourceId = created.rankIndexBlobResourceId
          }
        )

      assertThat(deleted.rankIndexBlobResourceId).isEqualTo(created.rankIndexBlobResourceId)
      assertThat(deleted.hasDeleteTime()).isTrue()

      val fetched: RankIndexBlob =
        service.getRankIndexBlob(
          getRankIndexBlobRequest {
            dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
            rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
            rankIndexBlobResourceId = created.rankIndexBlobResourceId
          }
        )
      assertThat(fetched.hasDeleteTime()).isTrue()
    }

  @Test
  fun `deleteRankIndexBlob throws NOT_FOUND when not found`() =
    runBlocking<Unit> {
      val exception: StatusRuntimeException =
        assertFailsWith<StatusRuntimeException> {
          service.deleteRankIndexBlob(
            deleteRankIndexBlobRequest {
              dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
              rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
              rankIndexBlobResourceId = "nonexistent-blob"
            }
          )
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
    }

  @Test
  fun `deleteRankIndexBlob is idempotent when already deleted`() =
    runBlocking<Unit> {
      val created: RankIndexBlob = createBlob()
      val firstDelete: RankIndexBlob = deleteBlob(created.rankIndexBlobResourceId)
      assertThat(firstDelete.hasDeleteTime()).isTrue()

      // Deleting again succeeds (AIP-135) and returns the resource unchanged, preserving its
      // original delete_time.
      val secondDelete: RankIndexBlob = deleteBlob(created.rankIndexBlobResourceId)
      assertThat(secondDelete).isEqualTo(firstDelete)
    }

  @Test
  fun `batchDeleteRankIndexBlobs deletes multiple`() =
    runBlocking<Unit> {
      val blob1: RankIndexBlob = createBlob(poolOffset = 0L)
      val blob2: RankIndexBlob = createBlob(poolOffset = 1L)

      val response =
        service.batchDeleteRankIndexBlobs(
          batchDeleteRankIndexBlobsRequest {
            dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
            rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
            requests += deleteRankIndexBlobRequest {
              dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
              rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
              rankIndexBlobResourceId = blob1.rankIndexBlobResourceId
            }
            requests += deleteRankIndexBlobRequest {
              dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
              rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
              rankIndexBlobResourceId = blob2.rankIndexBlobResourceId
            }
          }
        )

      assertThat(response.rankIndexBlobsList).hasSize(2)
      assertThat(response.rankIndexBlobsList.all { it.hasDeleteTime() }).isTrue()
    }

  @Test
  fun `batchDeleteRankIndexBlobs throws NOT_FOUND when one is missing`() =
    runBlocking<Unit> {
      val blob1: RankIndexBlob = createBlob(poolOffset = 0L)

      val exception: StatusRuntimeException =
        assertFailsWith<StatusRuntimeException> {
          service.batchDeleteRankIndexBlobs(
            batchDeleteRankIndexBlobsRequest {
              dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
              rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
              requests += deleteRankIndexBlobRequest {
                rankIndexBlobResourceId = blob1.rankIndexBlobResourceId
              }
              requests += deleteRankIndexBlobRequest {
                rankIndexBlobResourceId = "nonexistent-blob"
              }
            }
          )
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
    }

  @Test
  fun `batchDeleteRankIndexBlobs is idempotent for a mix of active and already-deleted rows`() =
    runBlocking<Unit> {
      val blob1: RankIndexBlob = createBlob(poolOffset = 0L)
      val blob2: RankIndexBlob = createBlob(poolOffset = 1L)
      // blob1 is already soft-deleted; blob2 is still active.
      val alreadyDeleted: RankIndexBlob = deleteBlob(blob1.rankIndexBlobResourceId)

      val response =
        service.batchDeleteRankIndexBlobs(
          batchDeleteRankIndexBlobsRequest {
            dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
            rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
            requests += deleteRankIndexBlobRequest {
              rankIndexBlobResourceId = blob1.rankIndexBlobResourceId
            }
            requests += deleteRankIndexBlobRequest {
              rankIndexBlobResourceId = blob2.rankIndexBlobResourceId
            }
          }
        )

      // Both rows are returned and soft-deleted; the already-deleted one keeps its original
      // delete_time.
      assertThat(response.rankIndexBlobsList).hasSize(2)
      assertThat(response.rankIndexBlobsList.all { it.hasDeleteTime() }).isTrue()
      val returnedBlob1: RankIndexBlob =
        response.rankIndexBlobsList.single {
          it.rankIndexBlobResourceId == blob1.rankIndexBlobResourceId
        }
      assertThat(returnedBlob1.deleteTime).isEqualTo(alreadyDeleted.deleteTime)
    }

  @Test
  fun `batchDeleteRankIndexBlobs throws INVALID_ARGUMENT for duplicate resource id`() =
    runBlocking<Unit> {
      val blob1: RankIndexBlob = createBlob(poolOffset = 0L)

      val exception: StatusRuntimeException =
        assertFailsWith<StatusRuntimeException> {
          service.batchDeleteRankIndexBlobs(
            batchDeleteRankIndexBlobsRequest {
              dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
              rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
              requests += deleteRankIndexBlobRequest {
                rankIndexBlobResourceId = blob1.rankIndexBlobResourceId
              }
              requests += deleteRankIndexBlobRequest {
                rankIndexBlobResourceId = blob1.rankIndexBlobResourceId
              }
            }
          )
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    }

  @Test
  fun `listRankIndexBlobs paginates rows sharing a create time`() =
    runBlocking<Unit> {
      // batchCreate commits all rows in a single transaction, so they share a CreateTime. This
      // forces pagination to disambiguate on RankIndexBlobResourceId (the secondary sort key).
      val createResponse =
        service.batchCreateRankIndexBlobs(
          batchCreateRankIndexBlobsRequest {
            dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
            rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
            for (i in 0 until 5) {
              requests += createRankIndexBlobRequest {
                requestId = UUID.randomUUID().toString()
                rankIndexBlob = newInternalBlob(poolOffset = i.toLong())
              }
            }
          }
        )
      val expectedResourceIds: List<String> =
        createResponse.rankIndexBlobsList.map { it.rankIndexBlobResourceId }

      val collected = mutableListOf<String>()
      var token: ListRankIndexBlobsPageToken? = null
      while (true) {
        val currentToken = token
        val response: ListRankIndexBlobsResponse =
          service.listRankIndexBlobs(
            listRankIndexBlobsRequest {
              dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
              rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
              pageSize = 2
              if (currentToken != null) {
                pageToken = currentToken
              }
            }
          )
        collected += response.rankIndexBlobsList.map { it.rankIndexBlobResourceId }
        if (!response.hasNextPageToken()) {
          break
        }
        token = response.nextPageToken
      }

      assertThat(collected).containsExactlyElementsIn(expectedResourceIds)
    }

  private suspend fun createBlob(
    blobType: BlobType = BlobType.BLOB_TYPE_SNAPSHOT,
    cmmsModelLine: String = CMMS_MODEL_LINE,
    poolOffset: Long = 0L,
    blobUri: String = BLOB_URI,
    uploadResourceId: String = RAW_IMPRESSION_UPLOAD_RESOURCE_ID,
    maxEventDate: Date? = null,
    requestId: String = UUID.randomUUID().toString(),
  ): RankIndexBlob {
    return service.createRankIndexBlob(
      createRankIndexBlobRequest {
        this.requestId = requestId
        dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
        rawImpressionUploadResourceId = uploadResourceId
        rankIndexBlob =
          newInternalBlob(blobType, cmmsModelLine, poolOffset, blobUri).copy {
            if (maxEventDate != null) {
              this.maxEventDate = maxEventDate
            }
          }
      }
    )
  }

  private suspend fun deleteBlob(blobResourceId: String): RankIndexBlob {
    return service.deleteRankIndexBlob(
      deleteRankIndexBlobRequest {
        dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
        rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
        rankIndexBlobResourceId = blobResourceId
      }
    )
  }

  private fun newInternalBlob(
    blobType: BlobType = BlobType.BLOB_TYPE_SNAPSHOT,
    cmmsModelLine: String = CMMS_MODEL_LINE,
    poolOffset: Long = 0L,
    blobUri: String = BLOB_URI,
  ): RankIndexBlob {
    return rankIndexBlob {
      this.blobType = blobType
      this.cmmsModelLine = cmmsModelLine
      this.poolOffset = poolOffset
      this.blobUri = blobUri
      encryptedDek = ENCRYPTED_DEK
    }
  }

  companion object {
    private const val DATA_PROVIDER_RESOURCE_ID = "dataProviders/dp1"
    private const val RAW_IMPRESSION_UPLOAD_RESOURCE_ID = "uploads/upload1"
    private const val SECOND_UPLOAD_RESOURCE_ID = "uploads/upload2"
    private const val CMMS_MODEL_LINE = "modelProviders/mp1/modelSuites/ms1/modelLines/ml1"
    private const val CMMS_MODEL_LINE_2 = "modelProviders/mp1/modelSuites/ms1/modelLines/ml2"
    private const val BLOB_URI = "gs://bucket/rank-index-blob"
    private val ENCRYPTED_DEK: EncryptedDek = encryptedDek {
      kekUri = "kms://kek"
      ciphertext = "ciphertext".toByteStringUtf8()
    }
    private val EARLY_DATE: Date = date {
      year = 2026
      month = 1
      day = 1
    }
    private val LATE_DATE: Date = date {
      year = 2026
      month = 6
      day = 1
    }
    private val MAX_EVENT_DATE: Date = date {
      year = 2026
      month = 3
      day = 15
    }
  }
}
