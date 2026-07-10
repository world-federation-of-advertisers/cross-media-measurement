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
import com.google.type.interval
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
import org.wfanet.measurement.common.toProtoTime
import org.wfanet.measurement.edpaggregator.service.internal.Errors
import org.wfanet.measurement.internal.edpaggregator.ListRawImpressionUploadsRequestKt
import org.wfanet.measurement.internal.edpaggregator.ListRawImpressionUploadsResponse
import org.wfanet.measurement.internal.edpaggregator.RawImpressionUpload
import org.wfanet.measurement.internal.edpaggregator.RawImpressionUploadServiceGrpcKt.RawImpressionUploadServiceCoroutineImplBase
import org.wfanet.measurement.internal.edpaggregator.RawImpressionUploadState
import org.wfanet.measurement.internal.edpaggregator.createRawImpressionUploadRequest
import org.wfanet.measurement.internal.edpaggregator.getRawImpressionUploadRequest
import org.wfanet.measurement.internal.edpaggregator.listRawImpressionUploadsRequest
import org.wfanet.measurement.internal.edpaggregator.rawImpressionUpload

@RunWith(JUnit4::class)
abstract class RawImpressionUploadServiceTest {
  private lateinit var service: RawImpressionUploadServiceCoroutineImplBase

  protected abstract fun newService(
    idGenerator: IdGenerator = IdGenerator.Default
  ): RawImpressionUploadServiceCoroutineImplBase

  @Before
  fun initService() {
    service = newService()
  }

  @Test
  fun `createRawImpressionUpload creates an upload`(): Unit = runBlocking {
    val startTime: Instant = Instant.now()

    val upload: RawImpressionUpload =
      service.createRawImpressionUpload(
        createRawImpressionUploadRequest {
          dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
          rawImpressionUpload = rawImpressionUpload { doneBlobUri = DONE_BLOB_URI }
          requestId = UUID.randomUUID().toString()
        }
      )

    assertThat(upload.rawImpressionUploadResourceId).isNotEmpty()
    assertThat(upload.createTime.toInstant()).isGreaterThan(startTime)
    assertThat(upload.updateTime).isEqualTo(upload.createTime)
    // Verify the entire response, substituting the non-deterministic resource ID and timestamps.
    assertThat(upload)
      .isEqualTo(
        rawImpressionUpload {
          dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
          rawImpressionUploadResourceId = upload.rawImpressionUploadResourceId
          doneBlobUri = DONE_BLOB_URI
          state = RawImpressionUploadState.RAW_IMPRESSION_UPLOAD_STATE_CREATED
          createTime = upload.createTime
          updateTime = upload.updateTime
        }
      )
  }

  @Test
  fun `createRawImpressionUpload is idempotent with same request_id`(): Unit = runBlocking {
    val requestId: String = UUID.randomUUID().toString()

    val upload1: RawImpressionUpload =
      service.createRawImpressionUpload(
        createRawImpressionUploadRequest {
          dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
          rawImpressionUpload = rawImpressionUpload { doneBlobUri = DONE_BLOB_URI }
          this.requestId = requestId
        }
      )

    val upload2: RawImpressionUpload =
      service.createRawImpressionUpload(
        createRawImpressionUploadRequest {
          dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
          rawImpressionUpload = rawImpressionUpload { doneBlobUri = DONE_BLOB_URI }
          this.requestId = requestId
        }
      )

    assertThat(upload2).isEqualTo(upload1)
    // The duplicate request must not have created a second row.
    val response: ListRawImpressionUploadsResponse =
      service.listRawImpressionUploads(
        listRawImpressionUploadsRequest { dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID }
      )
    assertThat(response.rawImpressionUploadsList).hasSize(1)
  }

  @Test
  fun `createRawImpressionUpload throws ALREADY_EXISTS when request_id reused with different done_blob_uri`():
    Unit = runBlocking {
    val requestId: String = UUID.randomUUID().toString()
    service.createRawImpressionUpload(
      createRawImpressionUploadRequest {
        dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
        rawImpressionUpload = rawImpressionUpload { doneBlobUri = DONE_BLOB_URI }
        this.requestId = requestId
      }
    )

    val exception: StatusRuntimeException =
      assertFailsWith<StatusRuntimeException> {
        service.createRawImpressionUpload(
          createRawImpressionUploadRequest {
            dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
            rawImpressionUpload = rawImpressionUpload { doneBlobUri = "$DONE_BLOB_URI-different" }
            this.requestId = requestId
          }
        )
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.ALREADY_EXISTS)
    assertThat(exception.errorInfo)
      .isEqualTo(
        errorInfo {
          domain = Errors.DOMAIN
          reason = Errors.Reason.RAW_IMPRESSION_UPLOAD_ALREADY_EXISTS.name
          metadata[Errors.Metadata.DATA_PROVIDER_RESOURCE_ID.key] = DATA_PROVIDER_RESOURCE_ID
          metadata[Errors.Metadata.CREATE_REQUEST_ID.key] = requestId
        }
      )
  }

  @Test
  fun `createRawImpressionUpload throws INVALID_ARGUMENT if data_provider_resource_id not set`():
    Unit = runBlocking {
    val exception: StatusRuntimeException =
      assertFailsWith<StatusRuntimeException> {
        service.createRawImpressionUpload(
          createRawImpressionUploadRequest {
            rawImpressionUpload = rawImpressionUpload { doneBlobUri = DONE_BLOB_URI }
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
  fun `createRawImpressionUpload throws INVALID_ARGUMENT if done_blob_uri not set`(): Unit =
    runBlocking {
      val exception: StatusRuntimeException =
        assertFailsWith<StatusRuntimeException> {
          service.createRawImpressionUpload(
            createRawImpressionUploadRequest { dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID }
          )
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
      assertThat(exception.errorInfo)
        .isEqualTo(
          errorInfo {
            domain = Errors.DOMAIN
            reason = Errors.Reason.REQUIRED_FIELD_NOT_SET.name
            metadata[Errors.Metadata.FIELD_NAME.key] = "raw_impression_upload.done_blob_uri"
          }
        )
    }

  @Test
  fun `createRawImpressionUpload throws INVALID_ARGUMENT for malformed request_id`(): Unit =
    runBlocking {
      val exception: StatusRuntimeException =
        assertFailsWith<StatusRuntimeException> {
          service.createRawImpressionUpload(
            createRawImpressionUploadRequest {
              dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
              rawImpressionUpload = rawImpressionUpload { doneBlobUri = DONE_BLOB_URI }
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
  fun `createRawImpressionUpload throws INVALID_ARGUMENT if request_id not set`(): Unit =
    runBlocking {
      val exception: StatusRuntimeException =
        assertFailsWith<StatusRuntimeException> {
          service.createRawImpressionUpload(
            createRawImpressionUploadRequest {
              dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
              rawImpressionUpload = rawImpressionUpload { doneBlobUri = DONE_BLOB_URI }
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
  fun `getRawImpressionUpload returns an upload`(): Unit = runBlocking {
    val created: RawImpressionUpload =
      service.createRawImpressionUpload(
        createRawImpressionUploadRequest {
          dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
          rawImpressionUpload = rawImpressionUpload { doneBlobUri = DONE_BLOB_URI }
          requestId = UUID.randomUUID().toString()
        }
      )

    val upload: RawImpressionUpload =
      service.getRawImpressionUpload(
        getRawImpressionUploadRequest {
          dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
          rawImpressionUploadResourceId = created.rawImpressionUploadResourceId
        }
      )

    assertThat(upload).isEqualTo(created)
  }

  @Test
  fun `getRawImpressionUpload throws NOT_FOUND when upload not found`(): Unit = runBlocking {
    val exception: StatusRuntimeException =
      assertFailsWith<StatusRuntimeException> {
        service.getRawImpressionUpload(
          getRawImpressionUploadRequest {
            dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
            rawImpressionUploadResourceId = "nonexistent-upload"
          }
        )
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
    assertThat(exception.errorInfo)
      .isEqualTo(
        errorInfo {
          domain = Errors.DOMAIN
          reason = Errors.Reason.RAW_IMPRESSION_UPLOAD_NOT_FOUND.name
          metadata[Errors.Metadata.DATA_PROVIDER_RESOURCE_ID.key] = DATA_PROVIDER_RESOURCE_ID
          metadata[Errors.Metadata.RAW_IMPRESSION_UPLOAD_RESOURCE_ID.key] = "nonexistent-upload"
        }
      )
  }

  @Test
  fun `listRawImpressionUploads returns uploads`(): Unit = runBlocking {
    createUpload()
    createUpload()

    val response: ListRawImpressionUploadsResponse =
      service.listRawImpressionUploads(
        listRawImpressionUploadsRequest { dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID }
      )

    assertThat(response.rawImpressionUploadsList).hasSize(2)
  }

  @Test
  fun `listRawImpressionUploads respects page size`(): Unit = runBlocking {
    for (i in 1..3) {
      createUpload()
    }

    val response: ListRawImpressionUploadsResponse =
      service.listRawImpressionUploads(
        listRawImpressionUploadsRequest {
          dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
          pageSize = 2
        }
      )

    assertThat(response.rawImpressionUploadsList).hasSize(2)
    assertThat(response.hasNextPageToken()).isTrue()
  }

  @Test
  fun `listRawImpressionUploads filters by state`(): Unit = runBlocking {
    createUpload()
    createUpload()

    val createdResponse: ListRawImpressionUploadsResponse =
      service.listRawImpressionUploads(
        listRawImpressionUploadsRequest {
          dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
          filter =
            ListRawImpressionUploadsRequestKt.filter {
              stateIn += RawImpressionUploadState.RAW_IMPRESSION_UPLOAD_STATE_CREATED
            }
        }
      )
    assertThat(createdResponse.rawImpressionUploadsList).hasSize(2)

    val failedResponse: ListRawImpressionUploadsResponse =
      service.listRawImpressionUploads(
        listRawImpressionUploadsRequest {
          dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
          filter =
            ListRawImpressionUploadsRequestKt.filter {
              stateIn += RawImpressionUploadState.RAW_IMPRESSION_UPLOAD_STATE_FAILED
            }
        }
      )
    assertThat(failedResponse.rawImpressionUploadsList).isEmpty()
  }

  @Test
  fun `listRawImpressionUploads with empty states filter returns all states`(): Unit =
    runBlocking<Unit> {
      val upload1: RawImpressionUpload = createUpload()
      val upload2: RawImpressionUpload = createUpload()

      val response: ListRawImpressionUploadsResponse =
        service.listRawImpressionUploads(
          listRawImpressionUploadsRequest {
            dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
            filter = ListRawImpressionUploadsRequestKt.filter {}
          }
        )

      assertThat(response.rawImpressionUploadsList.map { it.rawImpressionUploadResourceId })
        .containsExactly(
          upload1.rawImpressionUploadResourceId,
          upload2.rawImpressionUploadResourceId,
        )
    }

  @Test
  fun `listRawImpressionUploads filters by create_time interval`(): Unit = runBlocking {
    createUpload()
    createUpload()

    val futureResponse: ListRawImpressionUploadsResponse =
      service.listRawImpressionUploads(
        listRawImpressionUploadsRequest {
          dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
          filter =
            ListRawImpressionUploadsRequestKt.filter {
              createTimeIn = interval { startTime = Instant.now().plusSeconds(3600).toProtoTime() }
            }
        }
      )
    assertThat(futureResponse.rawImpressionUploadsList).isEmpty()

    val pastResponse: ListRawImpressionUploadsResponse =
      service.listRawImpressionUploads(
        listRawImpressionUploadsRequest {
          dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
          filter =
            ListRawImpressionUploadsRequestKt.filter {
              createTimeIn = interval { startTime = Instant.now().minusSeconds(3600).toProtoTime() }
            }
        }
      )
    assertThat(pastResponse.rawImpressionUploadsList).hasSize(2)
  }

  @Test
  fun `listRawImpressionUploads returns remaining uploads using page token`(): Unit = runBlocking {
    val created: List<RawImpressionUpload> = (1..3).map { createUpload() }

    val firstPage: ListRawImpressionUploadsResponse =
      service.listRawImpressionUploads(
        listRawImpressionUploadsRequest {
          dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
          pageSize = 2
        }
      )

    assertThat(firstPage.rawImpressionUploadsList).hasSize(2)
    assertThat(firstPage.hasNextPageToken()).isTrue()
    assertThat(firstPage.nextPageToken.after.rawImpressionUploadResourceId)
      .isEqualTo(firstPage.rawImpressionUploadsList.last().rawImpressionUploadResourceId)
    assertThat(firstPage.nextPageToken.after.hasCreateTime()).isTrue()
    assertThat(firstPage.nextPageToken.after.createTime)
      .isEqualTo(firstPage.rawImpressionUploadsList.last().createTime)

    val secondPage: ListRawImpressionUploadsResponse =
      service.listRawImpressionUploads(
        listRawImpressionUploadsRequest {
          dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
          pageSize = 2
          pageToken = firstPage.nextPageToken
        }
      )

    assertThat(secondPage.rawImpressionUploadsList).hasSize(1)
    assertThat(secondPage.hasNextPageToken()).isFalse()
    // The two pages together cover every created upload exactly once (no overlap, no gaps).
    assertThat(
        (firstPage.rawImpressionUploadsList + secondPage.rawImpressionUploadsList).map {
          it.rawImpressionUploadResourceId
        }
      )
      .containsExactlyElementsIn(created.map { it.rawImpressionUploadResourceId })
  }

  @Test
  fun `getRawImpressionUpload throws INVALID_ARGUMENT if data_provider_resource_id not set`():
    Unit = runBlocking {
    val exception: StatusRuntimeException =
      assertFailsWith<StatusRuntimeException> {
        service.getRawImpressionUpload(
          getRawImpressionUploadRequest { rawImpressionUploadResourceId = "some-upload" }
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
  fun `getRawImpressionUpload throws INVALID_ARGUMENT if raw_impression_upload_resource_id not set`():
    Unit = runBlocking {
    val exception: StatusRuntimeException =
      assertFailsWith<StatusRuntimeException> {
        service.getRawImpressionUpload(
          getRawImpressionUploadRequest { dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID }
        )
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.errorInfo)
      .isEqualTo(
        errorInfo {
          domain = Errors.DOMAIN
          reason = Errors.Reason.REQUIRED_FIELD_NOT_SET.name
          metadata[Errors.Metadata.FIELD_NAME.key] = "raw_impression_upload_resource_id"
        }
      )
  }

  @Test
  fun `listRawImpressionUploads throws INVALID_ARGUMENT if data_provider_resource_id not set`():
    Unit = runBlocking {
    val exception: StatusRuntimeException =
      assertFailsWith<StatusRuntimeException> {
        service.listRawImpressionUploads(listRawImpressionUploadsRequest {})
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
  fun `listRawImpressionUploads throws INVALID_ARGUMENT for negative page_size`(): Unit =
    runBlocking {
      val exception: StatusRuntimeException =
        assertFailsWith<StatusRuntimeException> {
          service.listRawImpressionUploads(
            listRawImpressionUploadsRequest {
              dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
              pageSize = -1
            }
          )
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
      assertThat(exception.errorInfo)
        .isEqualTo(
          errorInfo {
            domain = Errors.DOMAIN
            reason = Errors.Reason.INVALID_FIELD_VALUE.name
            metadata[Errors.Metadata.FIELD_NAME.key] = "page_size"
          }
        )
    }

  @Test
  fun `listRawImpressionUploads filters by create_time interval with end_time`(): Unit =
    runBlocking {
      createUpload()
      createUpload()

      val pastEndResponse: ListRawImpressionUploadsResponse =
        service.listRawImpressionUploads(
          listRawImpressionUploadsRequest {
            dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
            filter =
              ListRawImpressionUploadsRequestKt.filter {
                createTimeIn = interval { endTime = Instant.now().minusSeconds(3600).toProtoTime() }
              }
          }
        )
      assertThat(pastEndResponse.rawImpressionUploadsList).isEmpty()

      val futureEndResponse: ListRawImpressionUploadsResponse =
        service.listRawImpressionUploads(
          listRawImpressionUploadsRequest {
            dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
            filter =
              ListRawImpressionUploadsRequestKt.filter {
                createTimeIn = interval { endTime = Instant.now().plusSeconds(3600).toProtoTime() }
              }
          }
        )
      assertThat(futureEndResponse.rawImpressionUploadsList).hasSize(2)
    }

  @Test
  fun `listRawImpressionUploads filters by create_time interval with start and end window`(): Unit =
    runBlocking {
      createUpload()
      createUpload()

      val windowResponse: ListRawImpressionUploadsResponse =
        service.listRawImpressionUploads(
          listRawImpressionUploadsRequest {
            dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
            filter =
              ListRawImpressionUploadsRequestKt.filter {
                createTimeIn = interval {
                  startTime = Instant.now().minusSeconds(3600).toProtoTime()
                  endTime = Instant.now().plusSeconds(3600).toProtoTime()
                }
              }
          }
        )
      assertThat(windowResponse.rawImpressionUploadsList).hasSize(2)
    }

  private suspend fun createUpload(): RawImpressionUpload =
    service.createRawImpressionUpload(
      createRawImpressionUploadRequest {
        dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
        rawImpressionUpload = rawImpressionUpload { doneBlobUri = DONE_BLOB_URI }
        requestId = UUID.randomUUID().toString()
      }
    )

  companion object {
    private const val DATA_PROVIDER_RESOURCE_ID = "data-provider-1"
    private const val DONE_BLOB_URI = "gs://test-bucket/2026-06-16/done"
  }
}
