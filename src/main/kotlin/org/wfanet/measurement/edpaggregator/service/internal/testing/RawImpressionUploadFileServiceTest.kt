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
import com.google.protobuf.Timestamp
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
import org.wfanet.measurement.common.grpc.errorInfo
import org.wfanet.measurement.common.toInstant
import org.wfanet.measurement.edpaggregator.service.internal.Errors
import org.wfanet.measurement.internal.edpaggregator.BatchCreateRawImpressionUploadFilesResponse
import org.wfanet.measurement.internal.edpaggregator.BatchDeleteRawImpressionUploadFilesResponse
import org.wfanet.measurement.internal.edpaggregator.ListRawImpressionUploadFilesRequestKt
import org.wfanet.measurement.internal.edpaggregator.ListRawImpressionUploadFilesResponse
import org.wfanet.measurement.internal.edpaggregator.RawImpressionUploadFile
import org.wfanet.measurement.internal.edpaggregator.RawImpressionUploadFileServiceGrpcKt.RawImpressionUploadFileServiceCoroutineImplBase
import org.wfanet.measurement.internal.edpaggregator.batchCreateRawImpressionUploadFilesRequest
import org.wfanet.measurement.internal.edpaggregator.batchDeleteRawImpressionUploadFilesRequest
import org.wfanet.measurement.internal.edpaggregator.createRawImpressionUploadFileRequest
import org.wfanet.measurement.internal.edpaggregator.deleteRawImpressionUploadFileRequest
import org.wfanet.measurement.internal.edpaggregator.getRawImpressionUploadFileRequest
import org.wfanet.measurement.internal.edpaggregator.listRawImpressionUploadFilesRequest
import org.wfanet.measurement.internal.edpaggregator.rawImpressionUploadFile

@RunWith(JUnit4::class)
abstract class RawImpressionUploadFileServiceTest {
  private lateinit var fileService: RawImpressionUploadFileServiceCoroutineImplBase

  protected abstract fun newFileService(): RawImpressionUploadFileServiceCoroutineImplBase

  /** Creates a parent [RawImpressionUpload] and returns its resource ID. */
  protected abstract suspend fun createUpload(
    dataProviderResourceId: String = DATA_PROVIDER_RESOURCE_ID
  ): String

  @Before
  fun initService() {
    fileService = newFileService()
  }

  private suspend fun createFile(
    uploadResourceId: String,
    blobUri: String = BLOB_URI,
    requestId: String = "",
  ): RawImpressionUploadFile {
    return fileService.createRawImpressionUploadFile(
      createRawImpressionUploadFileRequest {
        rawImpressionUploadFile = rawImpressionUploadFile {
          dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
          rawImpressionUploadResourceId = uploadResourceId
          this.blobUri = blobUri
        }
        this.requestId = requestId
      }
    )
  }

  @Test
  fun `createRawImpressionUploadFile creates a file`() = runBlocking {
    val uploadId: String = createUpload()
    val startTime: Instant = Instant.now()

    val file: RawImpressionUploadFile =
      createFile(uploadId, requestId = UUID.randomUUID().toString())

    assertThat(file.dataProviderResourceId).isEqualTo(DATA_PROVIDER_RESOURCE_ID)
    assertThat(file.rawImpressionUploadResourceId).isEqualTo(uploadId)
    assertThat(file.fileResourceId).isNotEmpty()
    assertThat(file.blobUri).isEqualTo(BLOB_URI)
    assertThat(file.createTime.toInstant()).isGreaterThan(startTime)
    assertThat(file.updateTime).isEqualTo(file.createTime)
  }

  @Test
  fun `createRawImpressionUploadFile is idempotent with same request_id`() = runBlocking {
    val uploadId: String = createUpload()
    val requestId: String = UUID.randomUUID().toString()

    val file1: RawImpressionUploadFile = createFile(uploadId, requestId = requestId)
    val file2: RawImpressionUploadFile =
      createFile(uploadId, blobUri = "gs://other-uri", requestId = requestId)

    assertThat(file2).isEqualTo(file1)
  }

  @Test
  fun `createRawImpressionUploadFile allows duplicate blob_uri`() = runBlocking {
    val uploadId: String = createUpload()

    val file1: RawImpressionUploadFile = createFile(uploadId)
    val file2: RawImpressionUploadFile = createFile(uploadId)

    assertThat(file1.fileResourceId).isNotEqualTo(file2.fileResourceId)
    assertThat(file1.blobUri).isEqualTo(file2.blobUri)
  }

  @Test
  fun `createRawImpressionUploadFile throws NOT_FOUND when upload not found`() = runBlocking {
    val exception: StatusRuntimeException =
      assertFailsWith<StatusRuntimeException> { createFile("nonexistent-upload") }

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
  fun `createRawImpressionUploadFile throws INVALID_ARGUMENT if data_provider_resource_id not set`() =
    runBlocking {
      val uploadId: String = createUpload()

      val exception: StatusRuntimeException =
        assertFailsWith<StatusRuntimeException> {
          fileService.createRawImpressionUploadFile(
            createRawImpressionUploadFileRequest {
              rawImpressionUploadFile = rawImpressionUploadFile {
                rawImpressionUploadResourceId = uploadId
                blobUri = BLOB_URI
              }
            }
          )
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
      assertThat(exception.errorInfo)
        .isEqualTo(
          errorInfo {
            domain = Errors.DOMAIN
            reason = Errors.Reason.REQUIRED_FIELD_NOT_SET.name
            metadata[Errors.Metadata.FIELD_NAME.key] =
              "raw_impression_upload_file.data_provider_resource_id"
          }
        )
    }

  @Test
  fun `createRawImpressionUploadFile throws INVALID_ARGUMENT if raw_impression_upload_resource_id not set`() =
    runBlocking {
      val exception: StatusRuntimeException =
        assertFailsWith<StatusRuntimeException> {
          fileService.createRawImpressionUploadFile(
            createRawImpressionUploadFileRequest {
              rawImpressionUploadFile = rawImpressionUploadFile {
                dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
                blobUri = BLOB_URI
              }
            }
          )
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
      assertThat(exception.errorInfo)
        .isEqualTo(
          errorInfo {
            domain = Errors.DOMAIN
            reason = Errors.Reason.REQUIRED_FIELD_NOT_SET.name
            metadata[Errors.Metadata.FIELD_NAME.key] =
              "raw_impression_upload_file.raw_impression_upload_resource_id"
          }
        )
    }

  @Test
  fun `createRawImpressionUploadFile throws INVALID_ARGUMENT if blob_uri not set`() = runBlocking {
    val uploadId: String = createUpload()

    val exception: StatusRuntimeException =
      assertFailsWith<StatusRuntimeException> {
        fileService.createRawImpressionUploadFile(
          createRawImpressionUploadFileRequest {
            rawImpressionUploadFile = rawImpressionUploadFile {
              dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
              rawImpressionUploadResourceId = uploadId
            }
          }
        )
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.errorInfo)
      .isEqualTo(
        errorInfo {
          domain = Errors.DOMAIN
          reason = Errors.Reason.REQUIRED_FIELD_NOT_SET.name
          metadata[Errors.Metadata.FIELD_NAME.key] = "raw_impression_upload_file.blob_uri"
        }
      )
  }

  @Test
  fun `createRawImpressionUploadFile throws INVALID_ARGUMENT for malformed request_id`() =
    runBlocking {
      val uploadId: String = createUpload()

      val exception: StatusRuntimeException =
        assertFailsWith<StatusRuntimeException> {
          createFile(uploadId, requestId = "not-a-valid-uuid")
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
  fun `batchCreateRawImpressionUploadFiles creates multiple files`() = runBlocking {
    val uploadId: String = createUpload()

    val response: BatchCreateRawImpressionUploadFilesResponse =
      fileService.batchCreateRawImpressionUploadFiles(
        batchCreateRawImpressionUploadFilesRequest {
          dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
          rawImpressionUploadResourceId = uploadId
          requests += createRawImpressionUploadFileRequest {
            rawImpressionUploadFile = rawImpressionUploadFile { blobUri = "gs://bucket/file1" }
          }
          requests += createRawImpressionUploadFileRequest {
            rawImpressionUploadFile = rawImpressionUploadFile { blobUri = "gs://bucket/file2" }
          }
        }
      )

    assertThat(response.rawImpressionUploadFilesList).hasSize(2)
  }

  @Test
  fun `batchCreateRawImpressionUploadFiles returns empty response for empty requests`() =
    runBlocking {
      val response: BatchCreateRawImpressionUploadFilesResponse =
        fileService.batchCreateRawImpressionUploadFiles(
          batchCreateRawImpressionUploadFilesRequest {}
        )

      assertThat(response.rawImpressionUploadFilesList).isEmpty()
    }

  @Test
  fun `batchCreateRawImpressionUploadFiles throws INVALID_ARGUMENT for duplicate request_id`() =
    runBlocking {
      val uploadId: String = createUpload()
      val requestId: String = UUID.randomUUID().toString()

      val exception: StatusRuntimeException =
        assertFailsWith<StatusRuntimeException> {
          fileService.batchCreateRawImpressionUploadFiles(
            batchCreateRawImpressionUploadFilesRequest {
              dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
              rawImpressionUploadResourceId = uploadId
              requests += createRawImpressionUploadFileRequest {
                rawImpressionUploadFile = rawImpressionUploadFile { blobUri = "gs://bucket/file1" }
                this.requestId = requestId
              }
              requests += createRawImpressionUploadFileRequest {
                rawImpressionUploadFile = rawImpressionUploadFile { blobUri = "gs://bucket/file2" }
                this.requestId = requestId
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
            metadata[Errors.Metadata.FIELD_NAME.key] = "requests.1.request_id"
          }
        )
    }

  @Test
  fun `batchCreateRawImpressionUploadFiles is idempotent with same request_id`() = runBlocking {
    val uploadId: String = createUpload()
    val requestId: String = UUID.randomUUID().toString()

    val createdFile: RawImpressionUploadFile = createFile(uploadId, requestId = requestId)

    val response: BatchCreateRawImpressionUploadFilesResponse =
      fileService.batchCreateRawImpressionUploadFiles(
        batchCreateRawImpressionUploadFilesRequest {
          dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
          rawImpressionUploadResourceId = uploadId
          requests += createRawImpressionUploadFileRequest {
            rawImpressionUploadFile = rawImpressionUploadFile { blobUri = "gs://bucket/other" }
            this.requestId = requestId
          }
        }
      )

    assertThat(response.rawImpressionUploadFilesList).hasSize(1)
    assertThat(response.rawImpressionUploadFilesList.first().fileResourceId)
      .isEqualTo(createdFile.fileResourceId)
  }

  @Test
  fun `batchCreateRawImpressionUploadFiles throws INVALID_ARGUMENT for data_provider mismatch`() =
    runBlocking {
      val uploadId: String = createUpload()

      val exception: StatusRuntimeException =
        assertFailsWith<StatusRuntimeException> {
          fileService.batchCreateRawImpressionUploadFiles(
            batchCreateRawImpressionUploadFilesRequest {
              dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
              rawImpressionUploadResourceId = uploadId
              requests += createRawImpressionUploadFileRequest {
                rawImpressionUploadFile = rawImpressionUploadFile {
                  dataProviderResourceId = "different-provider"
                  blobUri = BLOB_URI
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
            reason = Errors.Reason.DATA_PROVIDER_MISMATCH.name
            metadata[Errors.Metadata.EXPECTED_DATA_PROVIDER_RESOURCE_ID.key] =
              DATA_PROVIDER_RESOURCE_ID
            metadata[Errors.Metadata.DATA_PROVIDER_RESOURCE_ID.key] = "different-provider"
          }
        )
    }

  @Test
  fun `batchCreateRawImpressionUploadFiles throws NOT_FOUND when upload not found`() = runBlocking {
    val exception: StatusRuntimeException =
      assertFailsWith<StatusRuntimeException> {
        fileService.batchCreateRawImpressionUploadFiles(
          batchCreateRawImpressionUploadFilesRequest {
            dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
            rawImpressionUploadResourceId = "nonexistent-upload"
            requests += createRawImpressionUploadFileRequest {
              rawImpressionUploadFile = rawImpressionUploadFile { blobUri = BLOB_URI }
            }
          }
        )
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
  }

  @Test
  fun `getRawImpressionUploadFile returns a file`() = runBlocking {
    val uploadId: String = createUpload()
    val created: RawImpressionUploadFile = createFile(uploadId)

    val file: RawImpressionUploadFile =
      fileService.getRawImpressionUploadFile(
        getRawImpressionUploadFileRequest {
          dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
          rawImpressionUploadResourceId = uploadId
          fileResourceId = created.fileResourceId
        }
      )

    assertThat(file).isEqualTo(created)
  }

  @Test
  fun `getRawImpressionUploadFile throws NOT_FOUND when file not found`() = runBlocking {
    val uploadId: String = createUpload()

    val exception: StatusRuntimeException =
      assertFailsWith<StatusRuntimeException> {
        fileService.getRawImpressionUploadFile(
          getRawImpressionUploadFileRequest {
            dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
            rawImpressionUploadResourceId = uploadId
            fileResourceId = "nonexistent-file"
          }
        )
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
    assertThat(exception.errorInfo)
      .isEqualTo(
        errorInfo {
          domain = Errors.DOMAIN
          reason = Errors.Reason.RAW_IMPRESSION_UPLOAD_FILE_NOT_FOUND.name
          metadata[Errors.Metadata.DATA_PROVIDER_RESOURCE_ID.key] = DATA_PROVIDER_RESOURCE_ID
          metadata[Errors.Metadata.RAW_IMPRESSION_UPLOAD_RESOURCE_ID.key] = uploadId
          metadata[Errors.Metadata.FILE_RESOURCE_ID.key] = "nonexistent-file"
        }
      )
  }

  @Test
  fun `listRawImpressionUploadFiles returns files in an upload`() = runBlocking {
    val uploadId: String = createUpload()
    createFile(uploadId, blobUri = "gs://bucket/file1")
    createFile(uploadId, blobUri = "gs://bucket/file2")

    val response: ListRawImpressionUploadFilesResponse =
      fileService.listRawImpressionUploadFiles(
        listRawImpressionUploadFilesRequest {
          dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
          rawImpressionUploadResourceId = uploadId
        }
      )

    assertThat(response.rawImpressionUploadFilesList).hasSize(2)
  }

  @Test
  fun `listRawImpressionUploadFiles returns files across all uploads`() = runBlocking {
    val uploadA: String = createUpload()
    val uploadB: String = createUpload()
    createFile(uploadA, blobUri = "gs://bucket/a")
    createFile(uploadB, blobUri = "gs://bucket/b")

    val response: ListRawImpressionUploadFilesResponse =
      fileService.listRawImpressionUploadFiles(
        listRawImpressionUploadFilesRequest { dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID }
      )

    assertThat(response.rawImpressionUploadFilesList).hasSize(2)
  }

  @Test
  fun `listRawImpressionUploadFiles respects page size`() = runBlocking {
    val uploadId: String = createUpload()
    for (i in 1..3) {
      createFile(uploadId, blobUri = "gs://bucket/file$i")
    }

    val response: ListRawImpressionUploadFilesResponse =
      fileService.listRawImpressionUploadFiles(
        listRawImpressionUploadFilesRequest {
          dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
          rawImpressionUploadResourceId = uploadId
          pageSize = 2
        }
      )

    assertThat(response.rawImpressionUploadFilesList).hasSize(2)
    assertThat(response.hasNextPageToken()).isTrue()
  }

  @Test
  fun `listRawImpressionUploadFiles returns remaining files using page token`() = runBlocking {
    val uploadId: String = createUpload()
    for (i in 1..3) {
      createFile(uploadId, blobUri = "gs://bucket/file-$i")
    }

    val firstPage: ListRawImpressionUploadFilesResponse =
      fileService.listRawImpressionUploadFiles(
        listRawImpressionUploadFilesRequest {
          dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
          rawImpressionUploadResourceId = uploadId
          pageSize = 2
        }
      )

    assertThat(firstPage.rawImpressionUploadFilesList).hasSize(2)
    assertThat(firstPage.hasNextPageToken()).isTrue()
    assertThat(firstPage.nextPageToken.after.fileResourceId)
      .isEqualTo(firstPage.rawImpressionUploadFilesList.last().fileResourceId)
    assertThat(firstPage.nextPageToken.after.rawImpressionUploadResourceId).isEqualTo(uploadId)
    assertThat(firstPage.nextPageToken.after.hasCreateTime()).isTrue()
    assertThat(firstPage.nextPageToken.after.createTime)
      .isEqualTo(firstPage.rawImpressionUploadFilesList.last().createTime)

    val secondPage: ListRawImpressionUploadFilesResponse =
      fileService.listRawImpressionUploadFiles(
        listRawImpressionUploadFilesRequest {
          dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
          rawImpressionUploadResourceId = uploadId
          pageSize = 2
          pageToken = firstPage.nextPageToken
        }
      )

    assertThat(secondPage.rawImpressionUploadFilesList).hasSize(1)
    assertThat(secondPage.hasNextPageToken()).isFalse()
  }

  @Test
  fun `listRawImpressionUploadFiles excludes deleted files by default`() = runBlocking {
    val uploadId: String = createUpload()
    val file1: RawImpressionUploadFile = createFile(uploadId, blobUri = "gs://bucket/file1")
    val file2: RawImpressionUploadFile = createFile(uploadId, blobUri = "gs://bucket/file2")

    fileService.deleteRawImpressionUploadFile(
      deleteRawImpressionUploadFileRequest {
        dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
        rawImpressionUploadResourceId = uploadId
        fileResourceId = file1.fileResourceId
      }
    )

    val response: ListRawImpressionUploadFilesResponse =
      fileService.listRawImpressionUploadFiles(
        listRawImpressionUploadFilesRequest {
          dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
          rawImpressionUploadResourceId = uploadId
        }
      )

    assertThat(response.rawImpressionUploadFilesList).hasSize(1)
    assertThat(response.rawImpressionUploadFilesList.first().fileResourceId)
      .isEqualTo(file2.fileResourceId)
  }

  @Test
  fun `listRawImpressionUploadFiles includes deleted files when showDeleted is true`() =
    runBlocking {
      val uploadId: String = createUpload()
      val file1: RawImpressionUploadFile = createFile(uploadId, blobUri = "gs://bucket/file-1")
      createFile(uploadId, blobUri = "gs://bucket/file-2")

      fileService.deleteRawImpressionUploadFile(
        deleteRawImpressionUploadFileRequest {
          dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
          rawImpressionUploadResourceId = uploadId
          fileResourceId = file1.fileResourceId
        }
      )

      val response: ListRawImpressionUploadFilesResponse =
        fileService.listRawImpressionUploadFiles(
          listRawImpressionUploadFilesRequest {
            dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
            rawImpressionUploadResourceId = uploadId
            showDeleted = true
          }
        )

      assertThat(response.rawImpressionUploadFilesList).hasSize(2)
    }

  @Test
  fun `listRawImpressionUploadFiles filters by blob_uri_in`() = runBlocking {
    val uploadId: String = createUpload()
    createFile(uploadId, blobUri = "gs://bucket/file1")
    createFile(uploadId, blobUri = "gs://bucket/file2")

    val response: ListRawImpressionUploadFilesResponse =
      fileService.listRawImpressionUploadFiles(
        listRawImpressionUploadFilesRequest {
          dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
          rawImpressionUploadResourceId = uploadId
          filter = ListRawImpressionUploadFilesRequestKt.filter { blobUriIn += "gs://bucket/file1" }
        }
      )

    assertThat(response.rawImpressionUploadFilesList).hasSize(1)
    assertThat(response.rawImpressionUploadFilesList.first().blobUri).isEqualTo("gs://bucket/file1")
  }

  @Test
  fun `listRawImpressionUploadFiles filters by create_time_in end_time`() = runBlocking {
    val uploadId: String = createUpload()
    createFile(uploadId, blobUri = "gs://bucket/file1")
    createFile(uploadId, blobUri = "gs://bucket/file2")

    val response: ListRawImpressionUploadFilesResponse =
      fileService.listRawImpressionUploadFiles(
        listRawImpressionUploadFilesRequest {
          dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
          rawImpressionUploadResourceId = uploadId
          filter =
            ListRawImpressionUploadFilesRequestKt.filter {
              createTimeIn = interval { endTime = FAR_FUTURE }
            }
        }
      )

    assertThat(response.rawImpressionUploadFilesList).hasSize(2)
  }

  @Test
  fun `listRawImpressionUploadFiles filters by create_time_in start_time`() = runBlocking {
    val uploadId: String = createUpload()
    createFile(uploadId, blobUri = "gs://bucket/file1")
    createFile(uploadId, blobUri = "gs://bucket/file2")

    val response: ListRawImpressionUploadFilesResponse =
      fileService.listRawImpressionUploadFiles(
        listRawImpressionUploadFilesRequest {
          dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
          rawImpressionUploadResourceId = uploadId
          filter =
            ListRawImpressionUploadFilesRequestKt.filter {
              createTimeIn = interval { startTime = FAR_FUTURE }
            }
        }
      )

    assertThat(response.rawImpressionUploadFilesList).isEmpty()
  }

  @Test
  fun `deleteRawImpressionUploadFile soft deletes a file`() = runBlocking {
    val uploadId: String = createUpload()
    val created: RawImpressionUploadFile = createFile(uploadId)

    val deletedFile: RawImpressionUploadFile =
      fileService.deleteRawImpressionUploadFile(
        deleteRawImpressionUploadFileRequest {
          dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
          rawImpressionUploadResourceId = uploadId
          fileResourceId = created.fileResourceId
        }
      )

    assertThat(deletedFile.hasDeleteTime()).isTrue()
    assertThat(deletedFile.updateTime).isEqualTo(deletedFile.deleteTime)
  }

  @Test
  fun `deleteRawImpressionUploadFile throws NOT_FOUND when file not found`() = runBlocking {
    val uploadId: String = createUpload()

    val exception: StatusRuntimeException =
      assertFailsWith<StatusRuntimeException> {
        fileService.deleteRawImpressionUploadFile(
          deleteRawImpressionUploadFileRequest {
            dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
            rawImpressionUploadResourceId = uploadId
            fileResourceId = "nonexistent-file"
          }
        )
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
  }

  @Test
  fun `deleteRawImpressionUploadFile throws NOT_FOUND when file already deleted`() = runBlocking {
    val uploadId: String = createUpload()
    val created: RawImpressionUploadFile = createFile(uploadId)

    fileService.deleteRawImpressionUploadFile(
      deleteRawImpressionUploadFileRequest {
        dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
        rawImpressionUploadResourceId = uploadId
        fileResourceId = created.fileResourceId
      }
    )

    val exception: StatusRuntimeException =
      assertFailsWith<StatusRuntimeException> {
        fileService.deleteRawImpressionUploadFile(
          deleteRawImpressionUploadFileRequest {
            dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
            rawImpressionUploadResourceId = uploadId
            fileResourceId = created.fileResourceId
          }
        )
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
  }

  @Test
  fun `batchDeleteRawImpressionUploadFiles deletes multiple files`() = runBlocking {
    val uploadId: String = createUpload()
    val file1: RawImpressionUploadFile = createFile(uploadId, blobUri = "gs://bucket/file1")
    val file2: RawImpressionUploadFile = createFile(uploadId, blobUri = "gs://bucket/file2")

    val response: BatchDeleteRawImpressionUploadFilesResponse =
      fileService.batchDeleteRawImpressionUploadFiles(
        batchDeleteRawImpressionUploadFilesRequest {
          dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
          rawImpressionUploadResourceId = uploadId
          requests += deleteRawImpressionUploadFileRequest {
            dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
            rawImpressionUploadResourceId = uploadId
            fileResourceId = file1.fileResourceId
          }
          requests += deleteRawImpressionUploadFileRequest {
            dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
            rawImpressionUploadResourceId = uploadId
            fileResourceId = file2.fileResourceId
          }
        }
      )

    assertThat(response.rawImpressionUploadFilesList).hasSize(2)
    response.rawImpressionUploadFilesList.forEach { assertThat(it.hasDeleteTime()).isTrue() }
  }

  @Test
  fun `batchDeleteRawImpressionUploadFiles returns empty response for empty requests`() =
    runBlocking {
      val response: BatchDeleteRawImpressionUploadFilesResponse =
        fileService.batchDeleteRawImpressionUploadFiles(
          batchDeleteRawImpressionUploadFilesRequest {}
        )

      assertThat(response.rawImpressionUploadFilesList).isEmpty()
    }

  @Test
  fun `batchDeleteRawImpressionUploadFiles throws NOT_FOUND when file not found`() = runBlocking {
    val uploadId: String = createUpload()

    val exception: StatusRuntimeException =
      assertFailsWith<StatusRuntimeException> {
        fileService.batchDeleteRawImpressionUploadFiles(
          batchDeleteRawImpressionUploadFilesRequest {
            dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
            rawImpressionUploadResourceId = uploadId
            requests += deleteRawImpressionUploadFileRequest {
              dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
              rawImpressionUploadResourceId = uploadId
              fileResourceId = "nonexistent-file"
            }
          }
        )
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
  }

  @Test
  fun `batchDeleteRawImpressionUploadFiles throws INVALID_ARGUMENT for duplicate file_resource_id`() =
    runBlocking {
      val uploadId: String = createUpload()
      val created: RawImpressionUploadFile = createFile(uploadId)

      val exception: StatusRuntimeException =
        assertFailsWith<StatusRuntimeException> {
          fileService.batchDeleteRawImpressionUploadFiles(
            batchDeleteRawImpressionUploadFilesRequest {
              dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
              rawImpressionUploadResourceId = uploadId
              requests += deleteRawImpressionUploadFileRequest {
                dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
                rawImpressionUploadResourceId = uploadId
                fileResourceId = created.fileResourceId
              }
              requests += deleteRawImpressionUploadFileRequest {
                dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
                rawImpressionUploadResourceId = uploadId
                fileResourceId = created.fileResourceId
              }
            }
          )
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    }

  @Test
  fun `batchDeleteRawImpressionUploadFiles throws INVALID_ARGUMENT for upload mismatch`() =
    runBlocking {
      val uploadId: String = createUpload()
      val created: RawImpressionUploadFile = createFile(uploadId)

      val exception: StatusRuntimeException =
        assertFailsWith<StatusRuntimeException> {
          fileService.batchDeleteRawImpressionUploadFiles(
            batchDeleteRawImpressionUploadFilesRequest {
              dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
              rawImpressionUploadResourceId = uploadId
              requests += deleteRawImpressionUploadFileRequest {
                dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
                rawImpressionUploadResourceId = "different-upload"
                fileResourceId = created.fileResourceId
              }
            }
          )
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    }

  companion object {
    private const val DATA_PROVIDER_RESOURCE_ID = "data-provider-1"
    private const val BLOB_URI = "gs://bucket/path/to/file"
    private val FAR_FUTURE: Timestamp = Timestamp.newBuilder().setSeconds(4102444800L).build()
  }
}
