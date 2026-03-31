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
    dataProviderResourceId: String = DATA_PROVIDER_RESOURCE_ID,
    batchResourceId: String = BATCH_RESOURCE_ID,
  ) {
    batchService.createRawImpressionMetadataBatch(
      createRawImpressionMetadataBatchRequest {
        this.dataProviderResourceId = dataProviderResourceId
        this.batchResourceId = batchResourceId
      }
    )
  }

  @Test
  fun `createRawImpressionMetadataBatchFile creates a file`() = runBlocking {
    createBatch()
    val startTime = Instant.now()

    val file =
      fileService.createRawImpressionMetadataBatchFile(
        createRawImpressionMetadataBatchFileRequest {
          dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
          batchResourceId = BATCH_RESOURCE_ID
          fileResourceId = FILE_RESOURCE_ID
          rawImpressionMetadataBatchFile = rawImpressionMetadataBatchFile {
            blobUri = BLOB_URI
          }
          requestId = UUID.randomUUID().toString()
        }
      )

    assertThat(file.dataProviderResourceId).isEqualTo(DATA_PROVIDER_RESOURCE_ID)
    assertThat(file.batchResourceId).isEqualTo(BATCH_RESOURCE_ID)
    assertThat(file.fileResourceId).isEqualTo(FILE_RESOURCE_ID)
    assertThat(file.blobUri).isEqualTo(BLOB_URI)
    assertThat(file.createTime.toInstant()).isGreaterThan(startTime)
    assertThat(file.updateTime).isEqualTo(file.createTime)
  }

  @Test
  fun `createRawImpressionMetadataBatchFile is idempotent with same request_id`() = runBlocking {
    createBatch()
    val requestId = UUID.randomUUID().toString()

    val file1 =
      fileService.createRawImpressionMetadataBatchFile(
        createRawImpressionMetadataBatchFileRequest {
          dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
          batchResourceId = BATCH_RESOURCE_ID
          fileResourceId = FILE_RESOURCE_ID
          rawImpressionMetadataBatchFile = rawImpressionMetadataBatchFile {
            blobUri = BLOB_URI
          }
          this.requestId = requestId
        }
      )

    val file2 =
      fileService.createRawImpressionMetadataBatchFile(
        createRawImpressionMetadataBatchFileRequest {
          dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
          batchResourceId = BATCH_RESOURCE_ID
          fileResourceId = "file-2"
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
      createBatch()

      fileService.createRawImpressionMetadataBatchFile(
        createRawImpressionMetadataBatchFileRequest {
          dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
          batchResourceId = BATCH_RESOURCE_ID
          fileResourceId = FILE_RESOURCE_ID
          rawImpressionMetadataBatchFile = rawImpressionMetadataBatchFile {
            blobUri = BLOB_URI
          }
        }
      )

      val exception =
        assertFailsWith<StatusRuntimeException> {
          fileService.createRawImpressionMetadataBatchFile(
            createRawImpressionMetadataBatchFileRequest {
              dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
              batchResourceId = BATCH_RESOURCE_ID
              fileResourceId = "file-2"
              rawImpressionMetadataBatchFile = rawImpressionMetadataBatchFile {
                blobUri = BLOB_URI
              }
            }
          )
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.ALREADY_EXISTS)
    }

  @Test
  fun `createRawImpressionMetadataBatchFile throws NOT_FOUND when batch not found`() =
    runBlocking {
      val exception =
        assertFailsWith<StatusRuntimeException> {
          fileService.createRawImpressionMetadataBatchFile(
            createRawImpressionMetadataBatchFileRequest {
              dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
              batchResourceId = "nonexistent-batch"
              fileResourceId = FILE_RESOURCE_ID
              rawImpressionMetadataBatchFile = rawImpressionMetadataBatchFile {
                blobUri = BLOB_URI
              }
            }
          )
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
    }

  @Test
  fun `createRawImpressionMetadataBatchFile throws INVALID_ARGUMENT if blob_uri not set`() =
    runBlocking {
      createBatch()

      val exception =
        assertFailsWith<StatusRuntimeException> {
          fileService.createRawImpressionMetadataBatchFile(
            createRawImpressionMetadataBatchFileRequest {
              dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
              batchResourceId = BATCH_RESOURCE_ID
              fileResourceId = FILE_RESOURCE_ID
              rawImpressionMetadataBatchFile = rawImpressionMetadataBatchFile {}
            }
          )
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    }

  @Test
  fun `batchCreateRawImpressionMetadataBatchFiles creates multiple files`() = runBlocking {
    createBatch()

    val response =
      fileService.batchCreateRawImpressionMetadataBatchFiles(
        batchCreateRawImpressionMetadataBatchFilesRequest {
          dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
          batchResourceId = BATCH_RESOURCE_ID
          requests +=
            createRawImpressionMetadataBatchFileRequest {
              dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
              batchResourceId = BATCH_RESOURCE_ID
              fileResourceId = "file-1"
              rawImpressionMetadataBatchFile = rawImpressionMetadataBatchFile {
                blobUri = "gs://bucket/file1"
              }
            }
          requests +=
            createRawImpressionMetadataBatchFileRequest {
              dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
              batchResourceId = BATCH_RESOURCE_ID
              fileResourceId = "file-2"
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
      createBatch()

      val exception =
        assertFailsWith<StatusRuntimeException> {
          fileService.batchCreateRawImpressionMetadataBatchFiles(
            batchCreateRawImpressionMetadataBatchFilesRequest {
              dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
              batchResourceId = BATCH_RESOURCE_ID
              requests +=
                createRawImpressionMetadataBatchFileRequest {
                  dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
                  batchResourceId = BATCH_RESOURCE_ID
                  fileResourceId = "file-1"
                  rawImpressionMetadataBatchFile = rawImpressionMetadataBatchFile {
                    blobUri = BLOB_URI
                  }
                }
              requests +=
                createRawImpressionMetadataBatchFileRequest {
                  dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
                  batchResourceId = BATCH_RESOURCE_ID
                  fileResourceId = "file-2"
                  rawImpressionMetadataBatchFile = rawImpressionMetadataBatchFile {
                    blobUri = BLOB_URI
                  }
                }
            }
          )
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    }

  @Test
  fun `getRawImpressionMetadataBatchFile returns a file`() = runBlocking {
    createBatch()

    fileService.createRawImpressionMetadataBatchFile(
      createRawImpressionMetadataBatchFileRequest {
        dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
        batchResourceId = BATCH_RESOURCE_ID
        fileResourceId = FILE_RESOURCE_ID
        rawImpressionMetadataBatchFile = rawImpressionMetadataBatchFile {
          blobUri = BLOB_URI
        }
      }
    )

    val file =
      fileService.getRawImpressionMetadataBatchFile(
        getRawImpressionMetadataBatchFileRequest {
          dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
          batchResourceId = BATCH_RESOURCE_ID
          fileResourceId = FILE_RESOURCE_ID
        }
      )

    assertThat(file.dataProviderResourceId).isEqualTo(DATA_PROVIDER_RESOURCE_ID)
    assertThat(file.batchResourceId).isEqualTo(BATCH_RESOURCE_ID)
    assertThat(file.fileResourceId).isEqualTo(FILE_RESOURCE_ID)
    assertThat(file.blobUri).isEqualTo(BLOB_URI)
  }

  @Test
  fun `getRawImpressionMetadataBatchFile throws NOT_FOUND when file not found`() = runBlocking {
    createBatch()

    val exception =
      assertFailsWith<StatusRuntimeException> {
        fileService.getRawImpressionMetadataBatchFile(
          getRawImpressionMetadataBatchFileRequest {
            dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
            batchResourceId = BATCH_RESOURCE_ID
            fileResourceId = "nonexistent-file"
          }
        )
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
  }

  @Test
  fun `listRawImpressionMetadataBatchFiles returns files in a batch`() = runBlocking {
    createBatch()

    fileService.createRawImpressionMetadataBatchFile(
      createRawImpressionMetadataBatchFileRequest {
        dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
        batchResourceId = BATCH_RESOURCE_ID
        fileResourceId = "file-1"
        rawImpressionMetadataBatchFile = rawImpressionMetadataBatchFile {
          blobUri = "gs://bucket/file1"
        }
      }
    )
    fileService.createRawImpressionMetadataBatchFile(
      createRawImpressionMetadataBatchFileRequest {
        dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
        batchResourceId = BATCH_RESOURCE_ID
        fileResourceId = "file-2"
        rawImpressionMetadataBatchFile = rawImpressionMetadataBatchFile {
          blobUri = "gs://bucket/file2"
        }
      }
    )

    val response =
      fileService.listRawImpressionMetadataBatchFiles(
        listRawImpressionMetadataBatchFilesRequest {
          dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
          batchResourceId = BATCH_RESOURCE_ID
        }
      )

    assertThat(response.rawImpressionMetadataBatchFilesList).hasSize(2)
  }

  @Test
  fun `listRawImpressionMetadataBatchFiles respects page size`() = runBlocking {
    createBatch()

    for (i in 1..3) {
      fileService.createRawImpressionMetadataBatchFile(
        createRawImpressionMetadataBatchFileRequest {
          dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
          batchResourceId = BATCH_RESOURCE_ID
          fileResourceId = "file-$i"
          rawImpressionMetadataBatchFile = rawImpressionMetadataBatchFile {
            blobUri = "gs://bucket/file$i"
          }
        }
      )
    }

    val response =
      fileService.listRawImpressionMetadataBatchFiles(
        listRawImpressionMetadataBatchFilesRequest {
          dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
          batchResourceId = BATCH_RESOURCE_ID
          pageSize = 2
        }
      )

    assertThat(response.rawImpressionMetadataBatchFilesList).hasSize(2)
    assertThat(response.hasNextPageToken()).isTrue()
  }

  @Test
  fun `deleteRawImpressionMetadataBatchFile soft deletes a file`() = runBlocking {
    createBatch()

    fileService.createRawImpressionMetadataBatchFile(
      createRawImpressionMetadataBatchFileRequest {
        dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
        batchResourceId = BATCH_RESOURCE_ID
        fileResourceId = FILE_RESOURCE_ID
        rawImpressionMetadataBatchFile = rawImpressionMetadataBatchFile {
          blobUri = BLOB_URI
        }
      }
    )

    val deletedFile =
      fileService.deleteRawImpressionMetadataBatchFile(
        deleteRawImpressionMetadataBatchFileRequest {
          dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
          batchResourceId = BATCH_RESOURCE_ID
          fileResourceId = FILE_RESOURCE_ID
        }
      )

    assertThat(deletedFile.hasDeleteTime()).isTrue()
  }

  @Test
  fun `deleteRawImpressionMetadataBatchFile throws NOT_FOUND when file not found`() = runBlocking {
    createBatch()

    val exception =
      assertFailsWith<StatusRuntimeException> {
        fileService.deleteRawImpressionMetadataBatchFile(
          deleteRawImpressionMetadataBatchFileRequest {
            dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
            batchResourceId = BATCH_RESOURCE_ID
            fileResourceId = "nonexistent-file"
          }
        )
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
  }

  @Test
  fun `deleteRawImpressionMetadataBatchFile throws NOT_FOUND when file already deleted`() =
    runBlocking {
      createBatch()

      fileService.createRawImpressionMetadataBatchFile(
        createRawImpressionMetadataBatchFileRequest {
          dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
          batchResourceId = BATCH_RESOURCE_ID
          fileResourceId = FILE_RESOURCE_ID
          rawImpressionMetadataBatchFile = rawImpressionMetadataBatchFile {
            blobUri = BLOB_URI
          }
        }
      )

      fileService.deleteRawImpressionMetadataBatchFile(
        deleteRawImpressionMetadataBatchFileRequest {
          dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
          batchResourceId = BATCH_RESOURCE_ID
          fileResourceId = FILE_RESOURCE_ID
        }
      )

      val exception =
        assertFailsWith<StatusRuntimeException> {
          fileService.deleteRawImpressionMetadataBatchFile(
            deleteRawImpressionMetadataBatchFileRequest {
              dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
              batchResourceId = BATCH_RESOURCE_ID
              fileResourceId = FILE_RESOURCE_ID
            }
          )
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
    }

  @Test
  fun `batchDeleteRawImpressionMetadataBatchFiles deletes multiple files`() = runBlocking {
    createBatch()

    fileService.createRawImpressionMetadataBatchFile(
      createRawImpressionMetadataBatchFileRequest {
        dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
        batchResourceId = BATCH_RESOURCE_ID
        fileResourceId = "file-1"
        rawImpressionMetadataBatchFile = rawImpressionMetadataBatchFile {
          blobUri = "gs://bucket/file1"
        }
      }
    )
    fileService.createRawImpressionMetadataBatchFile(
      createRawImpressionMetadataBatchFileRequest {
        dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
        batchResourceId = BATCH_RESOURCE_ID
        fileResourceId = "file-2"
        rawImpressionMetadataBatchFile = rawImpressionMetadataBatchFile {
          blobUri = "gs://bucket/file2"
        }
      }
    )

    val response =
      fileService.batchDeleteRawImpressionMetadataBatchFiles(
        batchDeleteRawImpressionMetadataBatchFilesRequest {
          requests +=
            deleteRawImpressionMetadataBatchFileRequest {
              dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
              batchResourceId = BATCH_RESOURCE_ID
              fileResourceId = "file-1"
            }
          requests +=
            deleteRawImpressionMetadataBatchFileRequest {
              dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
              batchResourceId = BATCH_RESOURCE_ID
              fileResourceId = "file-2"
            }
        }
      )

    assertThat(response.rawImpressionMetadataBatchFilesList).hasSize(2)
    response.rawImpressionMetadataBatchFilesList.forEach {
      assertThat(it.hasDeleteTime()).isTrue()
    }
  }

  @Test
  fun `batchDeleteRawImpressionMetadataBatchFiles throws NOT_FOUND when file not found`() =
    runBlocking {
      createBatch()

      val exception =
        assertFailsWith<StatusRuntimeException> {
          fileService.batchDeleteRawImpressionMetadataBatchFiles(
            batchDeleteRawImpressionMetadataBatchFilesRequest {
              requests +=
                deleteRawImpressionMetadataBatchFileRequest {
                  dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
                  batchResourceId = BATCH_RESOURCE_ID
                  fileResourceId = "nonexistent-file"
                }
            }
          )
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
    }

  @Test
  fun `listRawImpressionMetadataBatchFiles excludes deleted files by default`() = runBlocking {
    createBatch()

    fileService.createRawImpressionMetadataBatchFile(
      createRawImpressionMetadataBatchFileRequest {
        dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
        batchResourceId = BATCH_RESOURCE_ID
        fileResourceId = "file-1"
        rawImpressionMetadataBatchFile = rawImpressionMetadataBatchFile {
          blobUri = "gs://bucket/file1"
        }
      }
    )
    fileService.createRawImpressionMetadataBatchFile(
      createRawImpressionMetadataBatchFileRequest {
        dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
        batchResourceId = BATCH_RESOURCE_ID
        fileResourceId = "file-2"
        rawImpressionMetadataBatchFile = rawImpressionMetadataBatchFile {
          blobUri = "gs://bucket/file2"
        }
      }
    )

    fileService.deleteRawImpressionMetadataBatchFile(
      deleteRawImpressionMetadataBatchFileRequest {
        dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
        batchResourceId = BATCH_RESOURCE_ID
        fileResourceId = "file-1"
      }
    )

    val response =
      fileService.listRawImpressionMetadataBatchFiles(
        listRawImpressionMetadataBatchFilesRequest {
          dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
          batchResourceId = BATCH_RESOURCE_ID
        }
      )

    assertThat(response.rawImpressionMetadataBatchFilesList).hasSize(1)
    assertThat(response.rawImpressionMetadataBatchFilesList.first().fileResourceId)
      .isEqualTo("file-2")
  }


  @Test
  fun `listRawImpressionMetadataBatchFiles returns remaining files using page token`() =
    runBlocking {
      createBatch()

      for (i in 1..3) {
        fileService.createRawImpressionMetadataBatchFile(
          createRawImpressionMetadataBatchFileRequest {
            dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
            batchResourceId = BATCH_RESOURCE_ID
            fileResourceId = "file-$i"
            rawImpressionMetadataBatchFile = rawImpressionMetadataBatchFile {
              blobUri = "gs://bucket/file-$i"
            }
          }
        )
      }

      val firstPage =
        fileService.listRawImpressionMetadataBatchFiles(
          listRawImpressionMetadataBatchFilesRequest {
            dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
            batchResourceId = BATCH_RESOURCE_ID
            pageSize = 2
          }
        )

      assertThat(firstPage.rawImpressionMetadataBatchFilesList).hasSize(2)
      assertThat(firstPage.hasNextPageToken()).isTrue()

      val secondPage =
        fileService.listRawImpressionMetadataBatchFiles(
          listRawImpressionMetadataBatchFilesRequest {
            dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
            batchResourceId = BATCH_RESOURCE_ID
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
      createBatch()

      fileService.createRawImpressionMetadataBatchFile(
        createRawImpressionMetadataBatchFileRequest {
          dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
          batchResourceId = BATCH_RESOURCE_ID
          fileResourceId = "file-1"
          rawImpressionMetadataBatchFile = rawImpressionMetadataBatchFile {
            blobUri = "gs://bucket/file-1"
          }
        }
      )
      fileService.createRawImpressionMetadataBatchFile(
        createRawImpressionMetadataBatchFileRequest {
          dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
          batchResourceId = BATCH_RESOURCE_ID
          fileResourceId = "file-2"
          rawImpressionMetadataBatchFile = rawImpressionMetadataBatchFile {
            blobUri = "gs://bucket/file-2"
          }
        }
      )

      fileService.deleteRawImpressionMetadataBatchFile(
        deleteRawImpressionMetadataBatchFileRequest {
          dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
          batchResourceId = BATCH_RESOURCE_ID
          fileResourceId = "file-1"
        }
      )

      val response =
        fileService.listRawImpressionMetadataBatchFiles(
          listRawImpressionMetadataBatchFilesRequest {
            dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
            batchResourceId = BATCH_RESOURCE_ID
            showDeleted = true
          }
        )

      assertThat(response.rawImpressionMetadataBatchFilesList).hasSize(2)
    }
  companion object {
    private const val DATA_PROVIDER_RESOURCE_ID = "data-provider-1"
    private const val BATCH_RESOURCE_ID = "batch-1"
    private const val FILE_RESOURCE_ID = "file-1"
    private const val BLOB_URI = "gs://bucket/path/to/file"
  }
}
