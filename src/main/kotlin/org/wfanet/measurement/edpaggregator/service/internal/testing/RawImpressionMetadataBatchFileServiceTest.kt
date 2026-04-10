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
import org.wfanet.measurement.internal.edpaggregator.BatchCreateRawImpressionMetadataBatchFilesResponse
import org.wfanet.measurement.internal.edpaggregator.BatchDeleteRawImpressionMetadataBatchFilesResponse
import org.wfanet.measurement.internal.edpaggregator.ListRawImpressionMetadataBatchFilesResponse
import org.wfanet.measurement.internal.edpaggregator.RawImpressionMetadataBatch
import org.wfanet.measurement.internal.edpaggregator.RawImpressionMetadataBatchFile
import org.wfanet.measurement.internal.edpaggregator.RawImpressionMetadataBatchFileServiceGrpcKt.RawImpressionMetadataBatchFileServiceCoroutineImplBase
import org.wfanet.measurement.internal.edpaggregator.RawImpressionMetadataBatchServiceGrpcKt.RawImpressionMetadataBatchServiceCoroutineImplBase
import org.wfanet.measurement.internal.edpaggregator.batchCreateRawImpressionMetadataBatchFilesRequest
import org.wfanet.measurement.internal.edpaggregator.batchDeleteRawImpressionMetadataBatchFilesRequest
import org.wfanet.measurement.internal.edpaggregator.createRawImpressionMetadataBatchFileRequest
import org.wfanet.measurement.internal.edpaggregator.createRawImpressionMetadataBatchRequest
import org.wfanet.measurement.internal.edpaggregator.deleteRawImpressionMetadataBatchFileRequest
import org.wfanet.measurement.internal.edpaggregator.getRawImpressionMetadataBatchFileRequest
import org.wfanet.measurement.internal.edpaggregator.listRawImpressionMetadataBatchFilesRequest
import org.wfanet.measurement.internal.edpaggregator.rawImpressionMetadataBatchFile

@RunWith(JUnit4::class)
abstract class RawImpressionMetadataBatchFileServiceTest {
  private lateinit var batchService: RawImpressionMetadataBatchServiceCoroutineImplBase
  private lateinit var fileService: RawImpressionMetadataBatchFileServiceCoroutineImplBase

  protected abstract fun newBatchService(
    idGenerator: IdGenerator = IdGenerator.Default
  ): RawImpressionMetadataBatchServiceCoroutineImplBase

  protected abstract fun newFileService(
    idGenerator: IdGenerator = IdGenerator.Default
  ): RawImpressionMetadataBatchFileServiceCoroutineImplBase

  @Before
  fun initServices() {
    batchService = newBatchService()
    fileService = newFileService()
  }

  private suspend fun createBatch(
    dataProviderResourceId: String = DATA_PROVIDER_RESOURCE_ID
  ): RawImpressionMetadataBatch {
    return batchService.createRawImpressionMetadataBatch(
      createRawImpressionMetadataBatchRequest {
        this.dataProviderResourceId = dataProviderResourceId
      }
    )
  }

  @Test
  fun `createRawImpressionMetadataBatchFile creates a file`() = runBlocking {
    val batch: RawImpressionMetadataBatch = createBatch()
    val startTime: Instant = Instant.now()

    val file: RawImpressionMetadataBatchFile =
      fileService.createRawImpressionMetadataBatchFile(
        createRawImpressionMetadataBatchFileRequest {
          dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
          batchResourceId = batch.batchResourceId
          rawImpressionMetadataBatchFile = rawImpressionMetadataBatchFile { blobUri = BLOB_URI }
          requestId = UUID.randomUUID().toString()
        }
      )

    assertThat(file.dataProviderResourceId).isEqualTo(DATA_PROVIDER_RESOURCE_ID)
    assertThat(file.batchResourceId).isEqualTo(batch.batchResourceId)
    assertThat(file.fileResourceId).startsWith("file-")
    assertThat(file.blobUri).isEqualTo(BLOB_URI)
    assertThat(file.createTime.toInstant()).isGreaterThan(startTime)
    assertThat(file.updateTime).isEqualTo(file.createTime)
  }

  @Test
  fun `createRawImpressionMetadataBatchFile is idempotent with same request_id`() = runBlocking {
    val batch: RawImpressionMetadataBatch = createBatch()
    val requestId: String = UUID.randomUUID().toString()

    val file1: RawImpressionMetadataBatchFile =
      fileService.createRawImpressionMetadataBatchFile(
        createRawImpressionMetadataBatchFileRequest {
          dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
          batchResourceId = batch.batchResourceId
          rawImpressionMetadataBatchFile = rawImpressionMetadataBatchFile { blobUri = BLOB_URI }
          this.requestId = requestId
        }
      )

    val file2: RawImpressionMetadataBatchFile =
      fileService.createRawImpressionMetadataBatchFile(
        createRawImpressionMetadataBatchFileRequest {
          dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
          batchResourceId = batch.batchResourceId
          rawImpressionMetadataBatchFile = rawImpressionMetadataBatchFile {
            blobUri = "gs://other-uri"
          }
          this.requestId = requestId
        }
      )

    assertThat(file2).isEqualTo(file1)
  }

  @Test
  fun `createRawImpressionMetadataBatchFile throws ALREADY_EXISTS for duplicate blob_uri`() =
    runBlocking {
      val batch: RawImpressionMetadataBatch = createBatch()

      fileService.createRawImpressionMetadataBatchFile(
        createRawImpressionMetadataBatchFileRequest {
          dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
          batchResourceId = batch.batchResourceId
          rawImpressionMetadataBatchFile = rawImpressionMetadataBatchFile { blobUri = BLOB_URI }
        }
      )

      val exception: StatusRuntimeException =
        assertFailsWith<StatusRuntimeException> {
          fileService.createRawImpressionMetadataBatchFile(
            createRawImpressionMetadataBatchFileRequest {
              dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
              batchResourceId = batch.batchResourceId
              rawImpressionMetadataBatchFile = rawImpressionMetadataBatchFile { blobUri = BLOB_URI }
            }
          )
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.ALREADY_EXISTS)
      assertThat(exception.errorInfo)
        .isEqualTo(
          errorInfo {
            domain = Errors.DOMAIN
            reason = Errors.Reason.RAW_IMPRESSION_METADATA_BATCH_FILE_ALREADY_EXISTS.name
            metadata[Errors.Metadata.BLOB_URI.key] = BLOB_URI
          }
        )
    }

  @Test
  fun `createRawImpressionMetadataBatchFile throws NOT_FOUND when batch not found`() = runBlocking {
    val exception: StatusRuntimeException =
      assertFailsWith<StatusRuntimeException> {
        fileService.createRawImpressionMetadataBatchFile(
          createRawImpressionMetadataBatchFileRequest {
            dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
            batchResourceId = "nonexistent-batch"
            rawImpressionMetadataBatchFile = rawImpressionMetadataBatchFile { blobUri = BLOB_URI }
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
  fun `createRawImpressionMetadataBatchFile throws INVALID_ARGUMENT if blob_uri not set`() =
    runBlocking {
      val batch: RawImpressionMetadataBatch = createBatch()

      val exception: StatusRuntimeException =
        assertFailsWith<StatusRuntimeException> {
          fileService.createRawImpressionMetadataBatchFile(
            createRawImpressionMetadataBatchFileRequest {
              dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
              batchResourceId = batch.batchResourceId
              rawImpressionMetadataBatchFile = rawImpressionMetadataBatchFile {}
            }
          )
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
      assertThat(exception.errorInfo)
        .isEqualTo(
          errorInfo {
            domain = Errors.DOMAIN
            reason = Errors.Reason.REQUIRED_FIELD_NOT_SET.name
            metadata[Errors.Metadata.FIELD_NAME.key] = "raw_impression_metadata_batch_file.blob_uri"
          }
        )
    }

  @Test
  fun `batchCreateRawImpressionMetadataBatchFiles creates multiple files`() = runBlocking {
    val batch: RawImpressionMetadataBatch = createBatch()

    val response: BatchCreateRawImpressionMetadataBatchFilesResponse =
      fileService.batchCreateRawImpressionMetadataBatchFiles(
        batchCreateRawImpressionMetadataBatchFilesRequest {
          dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
          batchResourceId = batch.batchResourceId
          requests += createRawImpressionMetadataBatchFileRequest {
            dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
            batchResourceId = batch.batchResourceId
            rawImpressionMetadataBatchFile = rawImpressionMetadataBatchFile {
              blobUri = "gs://bucket/file1"
            }
          }
          requests += createRawImpressionMetadataBatchFileRequest {
            dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
            batchResourceId = batch.batchResourceId
            rawImpressionMetadataBatchFile = rawImpressionMetadataBatchFile {
              blobUri = "gs://bucket/file2"
            }
          }
        }
      )

    assertThat(response.rawImpressionMetadataBatchFilesList).hasSize(2)
  }

  @Test
  fun `batchCreateRawImpressionMetadataBatchFiles throws INVALID_ARGUMENT for duplicate blob_uri in batch`() =
    runBlocking {
      val batch: RawImpressionMetadataBatch = createBatch()

      val exception: StatusRuntimeException =
        assertFailsWith<StatusRuntimeException> {
          fileService.batchCreateRawImpressionMetadataBatchFiles(
            batchCreateRawImpressionMetadataBatchFilesRequest {
              dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
              batchResourceId = batch.batchResourceId
              requests += createRawImpressionMetadataBatchFileRequest {
                dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
                batchResourceId = batch.batchResourceId
                rawImpressionMetadataBatchFile = rawImpressionMetadataBatchFile {
                  blobUri = BLOB_URI
                }
              }
              requests += createRawImpressionMetadataBatchFileRequest {
                dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
                batchResourceId = batch.batchResourceId
                rawImpressionMetadataBatchFile = rawImpressionMetadataBatchFile {
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
            reason = Errors.Reason.INVALID_FIELD_VALUE.name
            metadata[Errors.Metadata.FIELD_NAME.key] =
              "requests.1.raw_impression_metadata_batch_file.blob_uri"
          }
        )
    }

  @Test
  fun `getRawImpressionMetadataBatchFile returns a file`() = runBlocking {
    val batch: RawImpressionMetadataBatch = createBatch()

    val created: RawImpressionMetadataBatchFile =
      fileService.createRawImpressionMetadataBatchFile(
        createRawImpressionMetadataBatchFileRequest {
          dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
          batchResourceId = batch.batchResourceId
          rawImpressionMetadataBatchFile = rawImpressionMetadataBatchFile { blobUri = BLOB_URI }
        }
      )

    val file: RawImpressionMetadataBatchFile =
      fileService.getRawImpressionMetadataBatchFile(
        getRawImpressionMetadataBatchFileRequest {
          dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
          batchResourceId = batch.batchResourceId
          fileResourceId = created.fileResourceId
        }
      )

    assertThat(file.dataProviderResourceId).isEqualTo(DATA_PROVIDER_RESOURCE_ID)
    assertThat(file.batchResourceId).isEqualTo(batch.batchResourceId)
    assertThat(file.fileResourceId).isEqualTo(created.fileResourceId)
    assertThat(file.blobUri).isEqualTo(BLOB_URI)
  }

  @Test
  fun `getRawImpressionMetadataBatchFile throws NOT_FOUND when file not found`() = runBlocking {
    val batch: RawImpressionMetadataBatch = createBatch()

    val exception: StatusRuntimeException =
      assertFailsWith<StatusRuntimeException> {
        fileService.getRawImpressionMetadataBatchFile(
          getRawImpressionMetadataBatchFileRequest {
            dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
            batchResourceId = batch.batchResourceId
            fileResourceId = "nonexistent-file"
          }
        )
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
    assertThat(exception.errorInfo)
      .isEqualTo(
        errorInfo {
          domain = Errors.DOMAIN
          reason = Errors.Reason.RAW_IMPRESSION_METADATA_BATCH_FILE_NOT_FOUND.name
          metadata[Errors.Metadata.DATA_PROVIDER_RESOURCE_ID.key] = DATA_PROVIDER_RESOURCE_ID
          metadata[Errors.Metadata.BATCH_RESOURCE_ID.key] = batch.batchResourceId
          metadata[Errors.Metadata.FILE_RESOURCE_ID.key] = "nonexistent-file"
        }
      )
  }

  @Test
  fun `listRawImpressionMetadataBatchFiles returns files in a batch`() = runBlocking {
    val batch: RawImpressionMetadataBatch = createBatch()

    fileService.createRawImpressionMetadataBatchFile(
      createRawImpressionMetadataBatchFileRequest {
        dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
        batchResourceId = batch.batchResourceId
        rawImpressionMetadataBatchFile = rawImpressionMetadataBatchFile {
          blobUri = "gs://bucket/file1"
        }
      }
    )
    fileService.createRawImpressionMetadataBatchFile(
      createRawImpressionMetadataBatchFileRequest {
        dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
        batchResourceId = batch.batchResourceId
        rawImpressionMetadataBatchFile = rawImpressionMetadataBatchFile {
          blobUri = "gs://bucket/file2"
        }
      }
    )

    val response: ListRawImpressionMetadataBatchFilesResponse =
      fileService.listRawImpressionMetadataBatchFiles(
        listRawImpressionMetadataBatchFilesRequest {
          dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
          batchResourceId = batch.batchResourceId
        }
      )

    assertThat(response.rawImpressionMetadataBatchFilesList).hasSize(2)
  }

  @Test
  fun `listRawImpressionMetadataBatchFiles respects page size`() = runBlocking {
    val batch: RawImpressionMetadataBatch = createBatch()

    for (i in 1..3) {
      fileService.createRawImpressionMetadataBatchFile(
        createRawImpressionMetadataBatchFileRequest {
          dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
          batchResourceId = batch.batchResourceId
          rawImpressionMetadataBatchFile = rawImpressionMetadataBatchFile {
            blobUri = "gs://bucket/file$i"
          }
        }
      )
    }

    val response: ListRawImpressionMetadataBatchFilesResponse =
      fileService.listRawImpressionMetadataBatchFiles(
        listRawImpressionMetadataBatchFilesRequest {
          dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
          batchResourceId = batch.batchResourceId
          pageSize = 2
        }
      )

    assertThat(response.rawImpressionMetadataBatchFilesList).hasSize(2)
    assertThat(response.hasNextPageToken()).isTrue()
  }

  @Test
  fun `deleteRawImpressionMetadataBatchFile soft deletes a file`() = runBlocking {
    val batch: RawImpressionMetadataBatch = createBatch()

    val created: RawImpressionMetadataBatchFile =
      fileService.createRawImpressionMetadataBatchFile(
        createRawImpressionMetadataBatchFileRequest {
          dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
          batchResourceId = batch.batchResourceId
          rawImpressionMetadataBatchFile = rawImpressionMetadataBatchFile { blobUri = BLOB_URI }
        }
      )

    val deletedFile: RawImpressionMetadataBatchFile =
      fileService.deleteRawImpressionMetadataBatchFile(
        deleteRawImpressionMetadataBatchFileRequest {
          dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
          batchResourceId = batch.batchResourceId
          fileResourceId = created.fileResourceId
        }
      )

    assertThat(deletedFile.hasDeleteTime()).isTrue()
  }

  @Test
  fun `deleteRawImpressionMetadataBatchFile throws NOT_FOUND when file not found`() = runBlocking {
    val batch: RawImpressionMetadataBatch = createBatch()

    val exception: StatusRuntimeException =
      assertFailsWith<StatusRuntimeException> {
        fileService.deleteRawImpressionMetadataBatchFile(
          deleteRawImpressionMetadataBatchFileRequest {
            dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
            batchResourceId = batch.batchResourceId
            fileResourceId = "nonexistent-file"
          }
        )
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
    assertThat(exception.errorInfo)
      .isEqualTo(
        errorInfo {
          domain = Errors.DOMAIN
          reason = Errors.Reason.RAW_IMPRESSION_METADATA_BATCH_FILE_NOT_FOUND.name
          metadata[Errors.Metadata.DATA_PROVIDER_RESOURCE_ID.key] = DATA_PROVIDER_RESOURCE_ID
          metadata[Errors.Metadata.BATCH_RESOURCE_ID.key] = batch.batchResourceId
          metadata[Errors.Metadata.FILE_RESOURCE_ID.key] = "nonexistent-file"
        }
      )
  }

  @Test
  fun `deleteRawImpressionMetadataBatchFile throws NOT_FOUND when file already deleted`() =
    runBlocking {
      val batch: RawImpressionMetadataBatch = createBatch()

      val created: RawImpressionMetadataBatchFile =
        fileService.createRawImpressionMetadataBatchFile(
          createRawImpressionMetadataBatchFileRequest {
            dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
            batchResourceId = batch.batchResourceId
            rawImpressionMetadataBatchFile = rawImpressionMetadataBatchFile { blobUri = BLOB_URI }
          }
        )

      fileService.deleteRawImpressionMetadataBatchFile(
        deleteRawImpressionMetadataBatchFileRequest {
          dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
          batchResourceId = batch.batchResourceId
          fileResourceId = created.fileResourceId
        }
      )

      val exception: StatusRuntimeException =
        assertFailsWith<StatusRuntimeException> {
          fileService.deleteRawImpressionMetadataBatchFile(
            deleteRawImpressionMetadataBatchFileRequest {
              dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
              batchResourceId = batch.batchResourceId
              fileResourceId = created.fileResourceId
            }
          )
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
      assertThat(exception.errorInfo)
        .isEqualTo(
          errorInfo {
            domain = Errors.DOMAIN
            reason = Errors.Reason.RAW_IMPRESSION_METADATA_BATCH_FILE_NOT_FOUND.name
            metadata[Errors.Metadata.DATA_PROVIDER_RESOURCE_ID.key] = DATA_PROVIDER_RESOURCE_ID
            metadata[Errors.Metadata.BATCH_RESOURCE_ID.key] = batch.batchResourceId
            metadata[Errors.Metadata.FILE_RESOURCE_ID.key] = created.fileResourceId
          }
        )
    }

  @Test
  fun `batchDeleteRawImpressionMetadataBatchFiles deletes multiple files`() = runBlocking {
    val batch: RawImpressionMetadataBatch = createBatch()

    val file1: RawImpressionMetadataBatchFile =
      fileService.createRawImpressionMetadataBatchFile(
        createRawImpressionMetadataBatchFileRequest {
          dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
          batchResourceId = batch.batchResourceId
          rawImpressionMetadataBatchFile = rawImpressionMetadataBatchFile {
            blobUri = "gs://bucket/file1"
          }
        }
      )
    val file2: RawImpressionMetadataBatchFile =
      fileService.createRawImpressionMetadataBatchFile(
        createRawImpressionMetadataBatchFileRequest {
          dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
          batchResourceId = batch.batchResourceId
          rawImpressionMetadataBatchFile = rawImpressionMetadataBatchFile {
            blobUri = "gs://bucket/file2"
          }
        }
      )

    val response: BatchDeleteRawImpressionMetadataBatchFilesResponse =
      fileService.batchDeleteRawImpressionMetadataBatchFiles(
        batchDeleteRawImpressionMetadataBatchFilesRequest {
          requests += deleteRawImpressionMetadataBatchFileRequest {
            dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
            batchResourceId = batch.batchResourceId
            fileResourceId = file1.fileResourceId
          }
          requests += deleteRawImpressionMetadataBatchFileRequest {
            dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
            batchResourceId = batch.batchResourceId
            fileResourceId = file2.fileResourceId
          }
        }
      )

    assertThat(response.rawImpressionMetadataBatchFilesList).hasSize(2)
    response.rawImpressionMetadataBatchFilesList.forEach { assertThat(it.hasDeleteTime()).isTrue() }
  }

  @Test
  fun `batchDeleteRawImpressionMetadataBatchFiles throws NOT_FOUND when file not found`() =
    runBlocking {
      val batch: RawImpressionMetadataBatch = createBatch()

      val exception: StatusRuntimeException =
        assertFailsWith<StatusRuntimeException> {
          fileService.batchDeleteRawImpressionMetadataBatchFiles(
            batchDeleteRawImpressionMetadataBatchFilesRequest {
              requests += deleteRawImpressionMetadataBatchFileRequest {
                dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
                batchResourceId = batch.batchResourceId
                fileResourceId = "nonexistent-file"
              }
            }
          )
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
      assertThat(exception.errorInfo)
        .isEqualTo(
          errorInfo {
            domain = Errors.DOMAIN
            reason = Errors.Reason.RAW_IMPRESSION_METADATA_BATCH_FILE_NOT_FOUND.name
            metadata[Errors.Metadata.DATA_PROVIDER_RESOURCE_ID.key] = DATA_PROVIDER_RESOURCE_ID
            metadata[Errors.Metadata.BATCH_RESOURCE_ID.key] = batch.batchResourceId
            metadata[Errors.Metadata.FILE_RESOURCE_ID.key] = "nonexistent-file"
          }
        )
    }

  @Test
  fun `listRawImpressionMetadataBatchFiles excludes deleted files by default`() = runBlocking {
    val batch: RawImpressionMetadataBatch = createBatch()

    val file1: RawImpressionMetadataBatchFile =
      fileService.createRawImpressionMetadataBatchFile(
        createRawImpressionMetadataBatchFileRequest {
          dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
          batchResourceId = batch.batchResourceId
          rawImpressionMetadataBatchFile = rawImpressionMetadataBatchFile {
            blobUri = "gs://bucket/file1"
          }
        }
      )
    val file2: RawImpressionMetadataBatchFile =
      fileService.createRawImpressionMetadataBatchFile(
        createRawImpressionMetadataBatchFileRequest {
          dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
          batchResourceId = batch.batchResourceId
          rawImpressionMetadataBatchFile = rawImpressionMetadataBatchFile {
            blobUri = "gs://bucket/file2"
          }
        }
      )

    fileService.deleteRawImpressionMetadataBatchFile(
      deleteRawImpressionMetadataBatchFileRequest {
        dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
        batchResourceId = batch.batchResourceId
        fileResourceId = file1.fileResourceId
      }
    )

    val response: ListRawImpressionMetadataBatchFilesResponse =
      fileService.listRawImpressionMetadataBatchFiles(
        listRawImpressionMetadataBatchFilesRequest {
          dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
          batchResourceId = batch.batchResourceId
        }
      )

    assertThat(response.rawImpressionMetadataBatchFilesList).hasSize(1)
    assertThat(response.rawImpressionMetadataBatchFilesList.first().fileResourceId)
      .isEqualTo(file2.fileResourceId)
  }

  @Test
  fun `listRawImpressionMetadataBatchFiles returns remaining files using page token`() =
    runBlocking {
      val batch: RawImpressionMetadataBatch = createBatch()

      for (i in 1..3) {
        fileService.createRawImpressionMetadataBatchFile(
          createRawImpressionMetadataBatchFileRequest {
            dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
            batchResourceId = batch.batchResourceId
            rawImpressionMetadataBatchFile = rawImpressionMetadataBatchFile {
              blobUri = "gs://bucket/file-$i"
            }
          }
        )
      }

      val firstPage: ListRawImpressionMetadataBatchFilesResponse =
        fileService.listRawImpressionMetadataBatchFiles(
          listRawImpressionMetadataBatchFilesRequest {
            dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
            batchResourceId = batch.batchResourceId
            pageSize = 2
          }
        )

      assertThat(firstPage.rawImpressionMetadataBatchFilesList).hasSize(2)
      assertThat(firstPage.hasNextPageToken()).isTrue()
      assertThat(firstPage.nextPageToken.after.fileResourceId)
        .isEqualTo(firstPage.rawImpressionMetadataBatchFilesList.last().fileResourceId)
      assertThat(firstPage.nextPageToken.after.batchResourceId)
        .isEqualTo(firstPage.rawImpressionMetadataBatchFilesList.last().batchResourceId)
      assertThat(firstPage.nextPageToken.after.hasCreateTime()).isTrue()
      assertThat(firstPage.nextPageToken.after.createTime)
        .isEqualTo(firstPage.rawImpressionMetadataBatchFilesList.last().createTime)

      val secondPage: ListRawImpressionMetadataBatchFilesResponse =
        fileService.listRawImpressionMetadataBatchFiles(
          listRawImpressionMetadataBatchFilesRequest {
            dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
            batchResourceId = batch.batchResourceId
            pageSize = 2
            pageToken = firstPage.nextPageToken
          }
        )

      assertThat(secondPage.rawImpressionMetadataBatchFilesList).hasSize(1)
      assertThat(secondPage.hasNextPageToken()).isFalse()
    }

  @Test
  fun `listRawImpressionMetadataBatchFiles includes deleted files when showDeleted is true`() =
    runBlocking {
      val batch: RawImpressionMetadataBatch = createBatch()

      val file1: RawImpressionMetadataBatchFile =
        fileService.createRawImpressionMetadataBatchFile(
          createRawImpressionMetadataBatchFileRequest {
            dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
            batchResourceId = batch.batchResourceId
            rawImpressionMetadataBatchFile = rawImpressionMetadataBatchFile {
              blobUri = "gs://bucket/file-1"
            }
          }
        )
      fileService.createRawImpressionMetadataBatchFile(
        createRawImpressionMetadataBatchFileRequest {
          dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
          batchResourceId = batch.batchResourceId
          rawImpressionMetadataBatchFile = rawImpressionMetadataBatchFile {
            blobUri = "gs://bucket/file-2"
          }
        }
      )

      fileService.deleteRawImpressionMetadataBatchFile(
        deleteRawImpressionMetadataBatchFileRequest {
          dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
          batchResourceId = batch.batchResourceId
          fileResourceId = file1.fileResourceId
        }
      )

      val response: ListRawImpressionMetadataBatchFilesResponse =
        fileService.listRawImpressionMetadataBatchFiles(
          listRawImpressionMetadataBatchFilesRequest {
            dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
            batchResourceId = batch.batchResourceId
            showDeleted = true
          }
        )

      assertThat(response.rawImpressionMetadataBatchFilesList).hasSize(2)
    }

  @Test
  fun `createRawImpressionMetadataBatchFile throws INVALID_ARGUMENT if data_provider_resource_id not set`() =
    runBlocking {
      val exception: StatusRuntimeException =
        assertFailsWith<StatusRuntimeException> {
          fileService.createRawImpressionMetadataBatchFile(
            createRawImpressionMetadataBatchFileRequest {
              batchResourceId = "some-batch"
              rawImpressionMetadataBatchFile = rawImpressionMetadataBatchFile { blobUri = BLOB_URI }
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
  fun `createRawImpressionMetadataBatchFile throws INVALID_ARGUMENT if raw_impression_metadata_batch_file not set`() =
    runBlocking {
      val batch: RawImpressionMetadataBatch = createBatch()

      val exception: StatusRuntimeException =
        assertFailsWith<StatusRuntimeException> {
          fileService.createRawImpressionMetadataBatchFile(
            createRawImpressionMetadataBatchFileRequest {
              dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
              batchResourceId = batch.batchResourceId
            }
          )
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
      assertThat(exception.errorInfo)
        .isEqualTo(
          errorInfo {
            domain = Errors.DOMAIN
            reason = Errors.Reason.REQUIRED_FIELD_NOT_SET.name
            metadata[Errors.Metadata.FIELD_NAME.key] = "raw_impression_metadata_batch_file"
          }
        )
    }

  @Test
  fun `batchCreateRawImpressionMetadataBatchFiles throws INVALID_ARGUMENT for data_provider_resource_id mismatch`() =
    runBlocking {
      val batch: RawImpressionMetadataBatch = createBatch()

      val exception: StatusRuntimeException =
        assertFailsWith<StatusRuntimeException> {
          fileService.batchCreateRawImpressionMetadataBatchFiles(
            batchCreateRawImpressionMetadataBatchFilesRequest {
              dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
              batchResourceId = batch.batchResourceId
              requests += createRawImpressionMetadataBatchFileRequest {
                dataProviderResourceId = "different-provider"
                batchResourceId = batch.batchResourceId
                rawImpressionMetadataBatchFile = rawImpressionMetadataBatchFile {
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
  fun `batchCreateRawImpressionMetadataBatchFiles is idempotent with same request_id`() =
    runBlocking {
      val batch: RawImpressionMetadataBatch = createBatch()
      val requestId: String = UUID.randomUUID().toString()

      val createdFile: RawImpressionMetadataBatchFile =
        fileService.createRawImpressionMetadataBatchFile(
          createRawImpressionMetadataBatchFileRequest {
            dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
            batchResourceId = batch.batchResourceId
            rawImpressionMetadataBatchFile = rawImpressionMetadataBatchFile { blobUri = BLOB_URI }
            this.requestId = requestId
          }
        )

      val response: BatchCreateRawImpressionMetadataBatchFilesResponse =
        fileService.batchCreateRawImpressionMetadataBatchFiles(
          batchCreateRawImpressionMetadataBatchFilesRequest {
            dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
            batchResourceId = batch.batchResourceId
            requests += createRawImpressionMetadataBatchFileRequest {
              dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
              batchResourceId = batch.batchResourceId
              rawImpressionMetadataBatchFile = rawImpressionMetadataBatchFile {
                blobUri = "gs://bucket/other"
              }
              this.requestId = requestId
            }
          }
        )

      assertThat(response.rawImpressionMetadataBatchFilesList).hasSize(1)
      assertThat(response.rawImpressionMetadataBatchFilesList.first().fileResourceId)
        .isEqualTo(createdFile.fileResourceId)
    }

  @Test
  fun `batchCreateRawImpressionMetadataBatchFiles throws ALREADY_EXISTS for pre-existing blob_uri`() =
    runBlocking {
      val batch: RawImpressionMetadataBatch = createBatch()

      fileService.createRawImpressionMetadataBatchFile(
        createRawImpressionMetadataBatchFileRequest {
          dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
          batchResourceId = batch.batchResourceId
          rawImpressionMetadataBatchFile = rawImpressionMetadataBatchFile { blobUri = BLOB_URI }
        }
      )

      val exception: StatusRuntimeException =
        assertFailsWith<StatusRuntimeException> {
          fileService.batchCreateRawImpressionMetadataBatchFiles(
            batchCreateRawImpressionMetadataBatchFilesRequest {
              dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
              batchResourceId = batch.batchResourceId
              requests += createRawImpressionMetadataBatchFileRequest {
                dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
                batchResourceId = batch.batchResourceId
                rawImpressionMetadataBatchFile = rawImpressionMetadataBatchFile {
                  blobUri = BLOB_URI
                }
              }
            }
          )
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.ALREADY_EXISTS)
    }

  @Test
  fun `batchDeleteRawImpressionMetadataBatchFiles throws INVALID_ARGUMENT for batch_resource_id mismatch`() =
    runBlocking {
      val batch: RawImpressionMetadataBatch = createBatch()

      val created: RawImpressionMetadataBatchFile =
        fileService.createRawImpressionMetadataBatchFile(
          createRawImpressionMetadataBatchFileRequest {
            dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
            batchResourceId = batch.batchResourceId
            rawImpressionMetadataBatchFile = rawImpressionMetadataBatchFile { blobUri = BLOB_URI }
          }
        )

      val exception: StatusRuntimeException =
        assertFailsWith<StatusRuntimeException> {
          fileService.batchDeleteRawImpressionMetadataBatchFiles(
            batchDeleteRawImpressionMetadataBatchFilesRequest {
              requests += deleteRawImpressionMetadataBatchFileRequest {
                dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
                batchResourceId = batch.batchResourceId
                fileResourceId = created.fileResourceId
              }
              requests += deleteRawImpressionMetadataBatchFileRequest {
                dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
                batchResourceId = "different-batch"
              }
            }
          )
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    }

  @Test
  fun `batchDeleteRawImpressionMetadataBatchFiles throws INVALID_ARGUMENT for duplicate file_resource_id`() =
    runBlocking {
      val batch: RawImpressionMetadataBatch = createBatch()

      val created: RawImpressionMetadataBatchFile =
        fileService.createRawImpressionMetadataBatchFile(
          createRawImpressionMetadataBatchFileRequest {
            dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
            batchResourceId = batch.batchResourceId
            rawImpressionMetadataBatchFile = rawImpressionMetadataBatchFile { blobUri = BLOB_URI }
          }
        )

      val exception: StatusRuntimeException =
        assertFailsWith<StatusRuntimeException> {
          fileService.batchDeleteRawImpressionMetadataBatchFiles(
            batchDeleteRawImpressionMetadataBatchFilesRequest {
              requests += deleteRawImpressionMetadataBatchFileRequest {
                dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
                batchResourceId = batch.batchResourceId
                fileResourceId = created.fileResourceId
              }
              requests += deleteRawImpressionMetadataBatchFileRequest {
                dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
                batchResourceId = batch.batchResourceId
                fileResourceId = created.fileResourceId
              }
            }
          )
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    }

  @Test
  fun `createRawImpressionMetadataBatchFile throws INVALID_ARGUMENT for malformed request_id`() =
    runBlocking {
      val batch: RawImpressionMetadataBatch = createBatch()

      val exception: StatusRuntimeException =
        assertFailsWith<StatusRuntimeException> {
          fileService.createRawImpressionMetadataBatchFile(
            createRawImpressionMetadataBatchFileRequest {
              dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
              batchResourceId = batch.batchResourceId
              rawImpressionMetadataBatchFile = rawImpressionMetadataBatchFile { blobUri = BLOB_URI }
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
  fun `listRawImpressionMetadataBatchFiles page token contains create_time and batch_resource_id`() =
    runBlocking {
      val batch: RawImpressionMetadataBatch = createBatch()

      for (i in 1..3) {
        fileService.createRawImpressionMetadataBatchFile(
          createRawImpressionMetadataBatchFileRequest {
            dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
            batchResourceId = batch.batchResourceId
            rawImpressionMetadataBatchFile = rawImpressionMetadataBatchFile {
              blobUri = "gs://bucket/file-$i"
            }
          }
        )
      }

      val firstPage: ListRawImpressionMetadataBatchFilesResponse =
        fileService.listRawImpressionMetadataBatchFiles(
          listRawImpressionMetadataBatchFilesRequest {
            dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
            batchResourceId = batch.batchResourceId
            pageSize = 2
          }
        )

      assertThat(firstPage.nextPageToken.after.fileResourceId)
        .isEqualTo(firstPage.rawImpressionMetadataBatchFilesList.last().fileResourceId)
      assertThat(firstPage.nextPageToken.after.batchResourceId)
        .isEqualTo(firstPage.rawImpressionMetadataBatchFilesList.last().batchResourceId)
      assertThat(firstPage.nextPageToken.after.hasCreateTime()).isTrue()
      assertThat(firstPage.nextPageToken.after.createTime)
        .isEqualTo(firstPage.rawImpressionMetadataBatchFilesList.last().createTime)
    }

  @Test
  fun `listRawImpressionMetadataBatchFiles page token contains batch_resource_id across batches`() =
    runBlocking {
      val batchA: RawImpressionMetadataBatch = createBatch()
      val batchB: RawImpressionMetadataBatch = createBatch()

      for (i in 1..3) {
        fileService.createRawImpressionMetadataBatchFile(
          createRawImpressionMetadataBatchFileRequest {
            dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
            batchResourceId = batchA.batchResourceId
            rawImpressionMetadataBatchFile = rawImpressionMetadataBatchFile {
              blobUri = "gs://bucket/batch-a-file-$i"
            }
          }
        )
      }

      val firstPage: ListRawImpressionMetadataBatchFilesResponse =
        fileService.listRawImpressionMetadataBatchFiles(
          listRawImpressionMetadataBatchFilesRequest {
            dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
            batchResourceId = batchA.batchResourceId
            pageSize = 2
          }
        )

      assertThat(firstPage.nextPageToken.after.batchResourceId).isEqualTo(batchA.batchResourceId)
      assertThat(firstPage.nextPageToken.after.fileResourceId)
        .isEqualTo(firstPage.rawImpressionMetadataBatchFilesList.last().fileResourceId)
      assertThat(firstPage.nextPageToken.after.hasCreateTime()).isTrue()
    }

  @Test
  fun `createRawImpressionMetadataBatchFile auto-generates fileResourceId when not provided`() =
    runBlocking {
      val batch: RawImpressionMetadataBatch = createBatch()

      val response: RawImpressionMetadataBatchFile =
        fileService.createRawImpressionMetadataBatchFile(
          createRawImpressionMetadataBatchFileRequest {
            dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
            batchResourceId = batch.batchResourceId
            rawImpressionMetadataBatchFile = rawImpressionMetadataBatchFile {
              blobUri = "gs://bucket/auto-gen-test"
            }
          }
        )

      assertThat(response.fileResourceId).isNotEmpty()
      assertThat(response.fileResourceId).startsWith("file-")
    }

  companion object {
    private const val DATA_PROVIDER_RESOURCE_ID = "data-provider-1"
    private const val BLOB_URI = "gs://bucket/path/to/file"
  }
}
