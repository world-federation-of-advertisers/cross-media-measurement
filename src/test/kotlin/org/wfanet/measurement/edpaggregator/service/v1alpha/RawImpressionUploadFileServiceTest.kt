// Copyright 2026 The Cross-Media Measurement Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.wfanet.measurement.edpaggregator.service.v1alpha

import com.google.cloud.spanner.Value
import com.google.common.truth.Truth.assertThat
import com.google.common.truth.extensions.proto.ProtoTruth.assertThat
import com.google.protobuf.timestamp
import com.google.rpc.errorInfo
import com.google.type.interval
import io.grpc.Status
import io.grpc.StatusRuntimeException
import java.time.Instant
import java.util.UUID
import java.util.concurrent.atomic.AtomicLong
import kotlin.coroutines.EmptyCoroutineContext
import kotlin.test.assertFailsWith
import kotlin.test.assertNotNull
import kotlinx.coroutines.runBlocking
import org.junit.Before
import org.junit.ClassRule
import org.junit.Rule
import org.junit.Test
import org.junit.rules.TestRule
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.common.api.ResourceKey
import org.wfanet.measurement.common.grpc.errorInfo
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.common.identity.externalIdToApiId
import org.wfanet.measurement.common.testing.chainRulesSequentially
import org.wfanet.measurement.common.toInstant
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.SpannerRawImpressionUploadFileService
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.testing.Schemata
import org.wfanet.measurement.edpaggregator.service.Errors
import org.wfanet.measurement.edpaggregator.service.RawImpressionUploadFileKey
import org.wfanet.measurement.edpaggregator.service.RawImpressionUploadKey
import org.wfanet.measurement.edpaggregator.v1alpha.ListRawImpressionUploadFilesRequestKt
import org.wfanet.measurement.edpaggregator.v1alpha.batchCreateRawImpressionUploadFilesRequest
import org.wfanet.measurement.edpaggregator.v1alpha.batchDeleteRawImpressionUploadFilesRequest
import org.wfanet.measurement.edpaggregator.v1alpha.createRawImpressionUploadFileRequest
import org.wfanet.measurement.edpaggregator.v1alpha.deleteRawImpressionUploadFileRequest
import org.wfanet.measurement.edpaggregator.v1alpha.getRawImpressionUploadFileRequest
import org.wfanet.measurement.edpaggregator.v1alpha.listRawImpressionUploadFilesRequest
import org.wfanet.measurement.edpaggregator.v1alpha.listRawImpressionUploadFilesResponse
import org.wfanet.measurement.edpaggregator.v1alpha.rawImpressionUploadFile
import org.wfanet.measurement.gcloud.spanner.bufferInsertMutation
import org.wfanet.measurement.gcloud.spanner.testing.SpannerEmulatorDatabaseRule
import org.wfanet.measurement.gcloud.spanner.testing.SpannerEmulatorRule
import org.wfanet.measurement.internal.edpaggregator.RawImpressionUploadFileServiceGrpcKt.RawImpressionUploadFileServiceCoroutineImplBase as InternalFileServiceCoroutineImplBase
import org.wfanet.measurement.internal.edpaggregator.RawImpressionUploadFileServiceGrpcKt.RawImpressionUploadFileServiceCoroutineStub as InternalFileServiceCoroutineStub
import org.wfanet.measurement.internal.edpaggregator.RawImpressionUploadState

@RunWith(JUnit4::class)
class RawImpressionUploadFileServiceTest {
  private lateinit var internalFileService: InternalFileServiceCoroutineImplBase
  private lateinit var fileService: RawImpressionUploadFileService

  val spannerDatabase =
    SpannerEmulatorDatabaseRule(spannerEmulator, Schemata.EDP_AGGREGATOR_CHANGELOG_PATH)

  val grpcTestServerRule = GrpcTestServerRule {
    val spannerDatabaseClient = spannerDatabase.databaseClient
    internalFileService =
      SpannerRawImpressionUploadFileService(spannerDatabaseClient, EmptyCoroutineContext)
    addService(internalFileService)
  }

  @get:Rule
  val serverRuleChain: TestRule = chainRulesSequentially(spannerDatabase, grpcTestServerRule)

  @Before
  fun initService() {
    fileService =
      RawImpressionUploadFileService(InternalFileServiceCoroutineStub(grpcTestServerRule.channel))
  }

  /** Seeds a parent [RawImpressionUpload] and returns its resource name. */
  private suspend fun createUpload(): String {
    val rawImpressionUploadId: Long = uploadIdCounter.incrementAndGet()
    val uploadResourceId: String = UUID.randomUUID().toString()
    spannerDatabase.databaseClient.readWriteTransaction().run { txn ->
      txn.bufferInsertMutation("RawImpressionUpload") {
        set("DataProviderResourceId").to(DATA_PROVIDER_ID)
        set("RawImpressionUploadId").to(rawImpressionUploadId)
        set("RawImpressionUploadResourceId").to(uploadResourceId)
        set("DoneBlobUri").to("gs://bucket/done")
        set("State").to(RawImpressionUploadState.RAW_IMPRESSION_UPLOAD_STATE_CREATED)
        set("CreateTime").to(Value.COMMIT_TIMESTAMP)
        set("UpdateTime").to(Value.COMMIT_TIMESTAMP)
      }
    }
    return RawImpressionUploadKey(DATA_PROVIDER_ID, uploadResourceId).toName()
  }

  @Test
  fun `createRawImpressionUploadFile returns a file`() = runBlocking {
    val uploadName = createUpload()
    val startTime = Instant.now()

    val file =
      fileService.createRawImpressionUploadFile(
        createRawImpressionUploadFileRequest {
          parent = uploadName
          rawImpressionUploadFile = rawImpressionUploadFile {
            sizeBytes = SIZE_BYTES
            blobUri = BLOB_URI_1
          }
          requestId = UUID.randomUUID().toString()
        }
      )

    val fileKey = assertNotNull(RawImpressionUploadFileKey.fromName(file.name))
    assertThat(fileKey.dataProviderId).isEqualTo(DATA_PROVIDER_ID)
    assertThat(fileKey.rawImpressionUploadId).isNotEmpty()
    assertThat(fileKey.fileId).isNotEmpty()
    assertThat(file.blobUri).isEqualTo(BLOB_URI_1)
    assertThat(file.createTime.toInstant()).isGreaterThan(startTime)
    assertThat(file.updateTime).isEqualTo(file.createTime)
  }

  @Test
  fun `createRawImpressionUploadFile throws INVALID_ARGUMENT for empty parent`() = runBlocking {
    val request = createRawImpressionUploadFileRequest {
      rawImpressionUploadFile = rawImpressionUploadFile {
        sizeBytes = SIZE_BYTES
        blobUri = BLOB_URI_1
      }
    }

    val exception =
      assertFailsWith<StatusRuntimeException> { fileService.createRawImpressionUploadFile(request) }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.errorInfo)
      .isEqualTo(
        errorInfo {
          domain = Errors.DOMAIN
          reason = Errors.Reason.REQUIRED_FIELD_NOT_SET.name
          metadata[Errors.Metadata.FIELD_NAME.key] = "parent"
        }
      )
  }

  @Test
  fun `createRawImpressionUploadFile throws NOT_FOUND for nonexistent upload`() = runBlocking {
    val request = createRawImpressionUploadFileRequest {
      parent = "dataProviders/dp1/rawImpressionUploads/nonexistent"
      rawImpressionUploadFile = rawImpressionUploadFile {
        sizeBytes = SIZE_BYTES
        blobUri = BLOB_URI_1
      }
      requestId = UUID.randomUUID().toString()
    }

    val exception =
      assertFailsWith<StatusRuntimeException> { fileService.createRawImpressionUploadFile(request) }
    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
  }

  @Test
  fun `getRawImpressionUploadFile returns a file`() = runBlocking {
    val uploadName = createUpload()
    val created =
      fileService.createRawImpressionUploadFile(
        createRawImpressionUploadFileRequest {
          parent = uploadName
          rawImpressionUploadFile = rawImpressionUploadFile {
            sizeBytes = SIZE_BYTES
            blobUri = BLOB_URI_1
          }
          requestId = UUID.randomUUID().toString()
        }
      )

    val file =
      fileService.getRawImpressionUploadFile(
        getRawImpressionUploadFileRequest { name = created.name }
      )

    assertThat(file).isEqualTo(created)
  }

  @Test
  fun `getRawImpressionUploadFile throws NOT_FOUND for nonexistent file`() = runBlocking {
    val uploadName = createUpload()
    val uploadKey = assertNotNull(RawImpressionUploadKey.fromName(uploadName))
    val request = getRawImpressionUploadFileRequest {
      name =
        RawImpressionUploadFileKey(
            uploadKey.dataProviderId,
            uploadKey.rawImpressionUploadId,
            "nonexistent",
          )
          .toName()
    }

    val exception =
      assertFailsWith<StatusRuntimeException> { fileService.getRawImpressionUploadFile(request) }
    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
  }

  @Test
  fun `listRawImpressionUploadFiles returns files`() = runBlocking {
    val uploadName = createUpload()
    val created =
      fileService.createRawImpressionUploadFile(
        createRawImpressionUploadFileRequest {
          parent = uploadName
          rawImpressionUploadFile = rawImpressionUploadFile {
            sizeBytes = SIZE_BYTES
            blobUri = BLOB_URI_1
          }
          requestId = UUID.randomUUID().toString()
        }
      )

    val response =
      fileService.listRawImpressionUploadFiles(
        listRawImpressionUploadFilesRequest { parent = uploadName }
      )

    assertThat(response)
      .isEqualTo(listRawImpressionUploadFilesResponse { rawImpressionUploadFiles += created })
  }

  @Test
  fun `deleteRawImpressionUploadFile soft deletes a file`() = runBlocking {
    val uploadName = createUpload()
    val created =
      fileService.createRawImpressionUploadFile(
        createRawImpressionUploadFileRequest {
          parent = uploadName
          rawImpressionUploadFile = rawImpressionUploadFile {
            sizeBytes = SIZE_BYTES
            blobUri = BLOB_URI_1
          }
          requestId = UUID.randomUUID().toString()
        }
      )

    val deleted =
      fileService.deleteRawImpressionUploadFile(
        deleteRawImpressionUploadFileRequest { name = created.name }
      )

    assertThat(deleted.name).isEqualTo(created.name)
    assertThat(deleted.hasDeleteTime()).isTrue()
    assertThat(deleted.updateTime.toInstant()).isGreaterThan(created.updateTime.toInstant())
  }

  @Test
  fun `batchCreateRawImpressionUploadFiles creates multiple files`() = runBlocking {
    val uploadName = createUpload()

    val response =
      fileService.batchCreateRawImpressionUploadFiles(
        batchCreateRawImpressionUploadFilesRequest {
          parent = uploadName
          requests += createRawImpressionUploadFileRequest {
            parent = uploadName
            rawImpressionUploadFile = rawImpressionUploadFile {
              sizeBytes = SIZE_BYTES
              blobUri = BLOB_URI_1
            }
            requestId = UUID.randomUUID().toString()
          }
          requests += createRawImpressionUploadFileRequest {
            parent = uploadName
            rawImpressionUploadFile = rawImpressionUploadFile {
              sizeBytes = SIZE_BYTES
              blobUri = BLOB_URI_2
            }
            requestId = UUID.randomUUID().toString()
          }
        }
      )

    assertThat(response.rawImpressionUploadFilesList).hasSize(2)
    val blobUris = response.rawImpressionUploadFilesList.map { it.blobUri }
    assertThat(blobUris).containsExactly(BLOB_URI_1, BLOB_URI_2)
    Unit
  }

  @Test
  fun `batchDeleteRawImpressionUploadFiles deletes multiple files`() = runBlocking {
    val uploadName = createUpload()
    val file1 =
      fileService.createRawImpressionUploadFile(
        createRawImpressionUploadFileRequest {
          parent = uploadName
          rawImpressionUploadFile = rawImpressionUploadFile {
            sizeBytes = SIZE_BYTES
            blobUri = BLOB_URI_1
          }
          requestId = UUID.randomUUID().toString()
        }
      )
    val file2 =
      fileService.createRawImpressionUploadFile(
        createRawImpressionUploadFileRequest {
          parent = uploadName
          rawImpressionUploadFile = rawImpressionUploadFile {
            sizeBytes = SIZE_BYTES
            blobUri = BLOB_URI_2
          }
          requestId = UUID.randomUUID().toString()
        }
      )

    val response =
      fileService.batchDeleteRawImpressionUploadFiles(
        batchDeleteRawImpressionUploadFilesRequest {
          parent = uploadName
          requests += deleteRawImpressionUploadFileRequest { name = file1.name }
          requests += deleteRawImpressionUploadFileRequest { name = file2.name }
        }
      )

    assertThat(response.rawImpressionUploadFilesList).hasSize(2)
    assertThat(response.rawImpressionUploadFilesList.all { it.hasDeleteTime() }).isTrue()
  }

  @Test
  fun `createRawImpressionUploadFile is idempotent with same request_id`() = runBlocking {
    val uploadName = createUpload()
    val requestId = UUID.randomUUID().toString()

    val first =
      fileService.createRawImpressionUploadFile(
        createRawImpressionUploadFileRequest {
          parent = uploadName
          rawImpressionUploadFile = rawImpressionUploadFile {
            sizeBytes = SIZE_BYTES
            blobUri = BLOB_URI_1
          }
          this.requestId = requestId
        }
      )
    val second =
      fileService.createRawImpressionUploadFile(
        createRawImpressionUploadFileRequest {
          parent = uploadName
          rawImpressionUploadFile = rawImpressionUploadFile {
            sizeBytes = SIZE_BYTES
            blobUri = BLOB_URI_2
          }
          this.requestId = requestId
        }
      )

    assertThat(second).isEqualTo(first)
  }

  @Test
  fun `createRawImpressionUploadFile throws INVALID_ARGUMENT for malformed request_id`() =
    runBlocking {
      val uploadName = createUpload()

      val exception =
        assertFailsWith<StatusRuntimeException> {
          fileService.createRawImpressionUploadFile(
            createRawImpressionUploadFileRequest {
              parent = uploadName
              rawImpressionUploadFile = rawImpressionUploadFile {
                sizeBytes = SIZE_BYTES
                blobUri = BLOB_URI_1
              }
              requestId = "not-a-uuid"
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
  fun `createRawImpressionUploadFile throws INVALID_ARGUMENT for missing request_id`() =
    runBlocking {
      val uploadName = createUpload()

      val exception =
        assertFailsWith<StatusRuntimeException> {
          fileService.createRawImpressionUploadFile(
            createRawImpressionUploadFileRequest {
              parent = uploadName
              rawImpressionUploadFile = rawImpressionUploadFile {
                sizeBytes = SIZE_BYTES
                blobUri = BLOB_URI_1
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
            metadata[Errors.Metadata.FIELD_NAME.key] = "request_id"
          }
        )
    }

  @Test
  fun `batchCreateRawImpressionUploadFiles throws INVALID_ARGUMENT for missing request_id`() =
    runBlocking {
      val uploadName = createUpload()

      val exception =
        assertFailsWith<StatusRuntimeException> {
          fileService.batchCreateRawImpressionUploadFiles(
            batchCreateRawImpressionUploadFilesRequest {
              parent = uploadName
              requests += createRawImpressionUploadFileRequest {
                parent = uploadName
                rawImpressionUploadFile = rawImpressionUploadFile {
                  sizeBytes = SIZE_BYTES
                  blobUri = BLOB_URI_1
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
            reason = Errors.Reason.REQUIRED_FIELD_NOT_SET.name
            metadata[Errors.Metadata.FIELD_NAME.key] = "requests.0.request_id"
          }
        )
    }

  @Test
  fun `listRawImpressionUploadFiles paginates using page token`() = runBlocking {
    val uploadName = createUpload()
    val createdNames =
      (1..3).map { i ->
        fileService
          .createRawImpressionUploadFile(
            createRawImpressionUploadFileRequest {
              parent = uploadName
              rawImpressionUploadFile = rawImpressionUploadFile {
                sizeBytes = SIZE_BYTES
                blobUri = "gs://bucket/file-$i"
              }
              requestId = UUID.randomUUID().toString()
            }
          )
          .name
      }

    val firstPage =
      fileService.listRawImpressionUploadFiles(
        listRawImpressionUploadFilesRequest {
          parent = uploadName
          pageSize = 2
        }
      )
    assertThat(firstPage.rawImpressionUploadFilesList).hasSize(2)
    assertThat(firstPage.nextPageToken).isNotEmpty()

    val secondPage =
      fileService.listRawImpressionUploadFiles(
        listRawImpressionUploadFilesRequest {
          parent = uploadName
          pageSize = 2
          pageToken = firstPage.nextPageToken
        }
      )
    assertThat(secondPage.rawImpressionUploadFilesList).hasSize(1)
    assertThat(secondPage.nextPageToken).isEmpty()

    val allNames =
      (firstPage.rawImpressionUploadFilesList + secondPage.rawImpressionUploadFilesList).map {
        it.name
      }
    assertThat(allNames).containsExactlyElementsIn(createdNames)
    Unit
  }

  @Test
  fun `listRawImpressionUploadFiles lists across all uploads with wildcard parent`() = runBlocking {
    val uploadNameA = createUpload()
    val uploadNameB = createUpload()
    val fileA =
      fileService.createRawImpressionUploadFile(
        createRawImpressionUploadFileRequest {
          parent = uploadNameA
          rawImpressionUploadFile = rawImpressionUploadFile {
            sizeBytes = SIZE_BYTES
            blobUri = BLOB_URI_1
          }
          requestId = UUID.randomUUID().toString()
        }
      )
    val fileB =
      fileService.createRawImpressionUploadFile(
        createRawImpressionUploadFileRequest {
          parent = uploadNameB
          rawImpressionUploadFile = rawImpressionUploadFile {
            sizeBytes = SIZE_BYTES
            blobUri = BLOB_URI_2
          }
          requestId = UUID.randomUUID().toString()
        }
      )

    val wildcardParent = RawImpressionUploadKey(DATA_PROVIDER_ID, ResourceKey.WILDCARD_ID).toName()
    val response =
      fileService.listRawImpressionUploadFiles(
        listRawImpressionUploadFilesRequest { parent = wildcardParent }
      )

    assertThat(response.rawImpressionUploadFilesList.map { it.name })
      .containsExactly(fileA.name, fileB.name)
    Unit
  }

  @Test
  fun `listRawImpressionUploadFiles filters by blob_uri_in`() = runBlocking {
    val uploadName = createUpload()
    fileService.createRawImpressionUploadFile(
      createRawImpressionUploadFileRequest {
        parent = uploadName
        rawImpressionUploadFile = rawImpressionUploadFile {
          sizeBytes = SIZE_BYTES
          blobUri = BLOB_URI_1
        }
        requestId = UUID.randomUUID().toString()
      }
    )
    fileService.createRawImpressionUploadFile(
      createRawImpressionUploadFileRequest {
        parent = uploadName
        rawImpressionUploadFile = rawImpressionUploadFile {
          sizeBytes = SIZE_BYTES
          blobUri = BLOB_URI_2
        }
        requestId = UUID.randomUUID().toString()
      }
    )

    val response =
      fileService.listRawImpressionUploadFiles(
        listRawImpressionUploadFilesRequest {
          parent = uploadName
          filter = ListRawImpressionUploadFilesRequestKt.filter { blobUriIn += BLOB_URI_1 }
        }
      )

    assertThat(response.rawImpressionUploadFilesList.map { it.blobUri }).containsExactly(BLOB_URI_1)
    Unit
  }

  @Test
  fun `listRawImpressionUploadFiles filters by create_time_in`() = runBlocking {
    val uploadName = createUpload()
    fileService.createRawImpressionUploadFile(
      createRawImpressionUploadFileRequest {
        parent = uploadName
        rawImpressionUploadFile = rawImpressionUploadFile {
          sizeBytes = SIZE_BYTES
          blobUri = BLOB_URI_1
        }
        requestId = UUID.randomUUID().toString()
      }
    )
    fileService.createRawImpressionUploadFile(
      createRawImpressionUploadFileRequest {
        parent = uploadName
        rawImpressionUploadFile = rawImpressionUploadFile {
          sizeBytes = SIZE_BYTES
          blobUri = BLOB_URI_2
        }
        requestId = UUID.randomUUID().toString()
      }
    )

    val withinRange =
      fileService.listRawImpressionUploadFiles(
        listRawImpressionUploadFilesRequest {
          parent = uploadName
          filter =
            ListRawImpressionUploadFilesRequestKt.filter {
              createTimeIn = interval { endTime = FAR_FUTURE }
            }
        }
      )
    assertThat(withinRange.rawImpressionUploadFilesList).hasSize(2)

    val afterRange =
      fileService.listRawImpressionUploadFiles(
        listRawImpressionUploadFilesRequest {
          parent = uploadName
          filter =
            ListRawImpressionUploadFilesRequestKt.filter {
              createTimeIn = interval { startTime = FAR_FUTURE }
            }
        }
      )
    assertThat(afterRange.rawImpressionUploadFilesList).isEmpty()
  }

  @Test
  fun `batchCreateRawImpressionUploadFiles throws INVALID_ARGUMENT for duplicate request_id`() =
    runBlocking {
      val uploadName = createUpload()
      val requestId = UUID.randomUUID().toString()

      val exception =
        assertFailsWith<StatusRuntimeException> {
          fileService.batchCreateRawImpressionUploadFiles(
            batchCreateRawImpressionUploadFilesRequest {
              parent = uploadName
              requests += createRawImpressionUploadFileRequest {
                parent = uploadName
                rawImpressionUploadFile = rawImpressionUploadFile {
                  sizeBytes = SIZE_BYTES
                  blobUri = BLOB_URI_1
                }
                this.requestId = requestId
              }
              requests += createRawImpressionUploadFileRequest {
                parent = uploadName
                rawImpressionUploadFile = rawImpressionUploadFile {
                  sizeBytes = SIZE_BYTES
                  blobUri = BLOB_URI_2
                }
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
  fun `batchCreateRawImpressionUploadFiles throws INVALID_ARGUMENT for parent mismatch`() =
    runBlocking {
      val uploadName = createUpload()

      val exception =
        assertFailsWith<StatusRuntimeException> {
          fileService.batchCreateRawImpressionUploadFiles(
            batchCreateRawImpressionUploadFilesRequest {
              parent = uploadName
              requests += createRawImpressionUploadFileRequest {
                parent = "dataProviders/dp1/rawImpressionUploads/other"
                rawImpressionUploadFile = rawImpressionUploadFile {
                  sizeBytes = SIZE_BYTES
                  blobUri = BLOB_URI_1
                }
              }
            }
          )
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    }

  @Test
  fun `batchCreateRawImpressionUploadFiles throws INVALID_ARGUMENT for empty requests`() =
    runBlocking {
      val uploadName = createUpload()

      val exception =
        assertFailsWith<StatusRuntimeException> {
          fileService.batchCreateRawImpressionUploadFiles(
            batchCreateRawImpressionUploadFilesRequest { parent = uploadName }
          )
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
      assertThat(exception.errorInfo)
        .isEqualTo(
          errorInfo {
            domain = Errors.DOMAIN
            reason = Errors.Reason.REQUIRED_FIELD_NOT_SET.name
            metadata[Errors.Metadata.FIELD_NAME.key] = "requests"
          }
        )
    }

  @Test
  fun `batchDeleteRawImpressionUploadFiles throws INVALID_ARGUMENT for empty requests`() =
    runBlocking {
      val uploadName = createUpload()

      val exception =
        assertFailsWith<StatusRuntimeException> {
          fileService.batchDeleteRawImpressionUploadFiles(
            batchDeleteRawImpressionUploadFilesRequest { parent = uploadName }
          )
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
      assertThat(exception.errorInfo)
        .isEqualTo(
          errorInfo {
            domain = Errors.DOMAIN
            reason = Errors.Reason.REQUIRED_FIELD_NOT_SET.name
            metadata[Errors.Metadata.FIELD_NAME.key] = "requests"
          }
        )
    }

  @Test
  fun `batchDeleteRawImpressionUploadFiles throws INVALID_ARGUMENT for duplicate name`() =
    runBlocking {
      val uploadName = createUpload()
      val file =
        fileService.createRawImpressionUploadFile(
          createRawImpressionUploadFileRequest {
            parent = uploadName
            rawImpressionUploadFile = rawImpressionUploadFile {
              sizeBytes = SIZE_BYTES
              blobUri = BLOB_URI_1
            }
            requestId = UUID.randomUUID().toString()
          }
        )

      val exception =
        assertFailsWith<StatusRuntimeException> {
          fileService.batchDeleteRawImpressionUploadFiles(
            batchDeleteRawImpressionUploadFilesRequest {
              parent = uploadName
              requests += deleteRawImpressionUploadFileRequest { name = file.name }
              requests += deleteRawImpressionUploadFileRequest { name = file.name }
            }
          )
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
      assertThat(exception.errorInfo)
        .isEqualTo(
          errorInfo {
            domain = Errors.DOMAIN
            reason = Errors.Reason.INVALID_FIELD_VALUE.name
            metadata[Errors.Metadata.FIELD_NAME.key] = "requests.1.name"
          }
        )
    }

  @Test
  fun `batchDeleteRawImpressionUploadFiles throws INVALID_ARGUMENT when name does not belong to parent`() =
    runBlocking {
      val uploadName = createUpload()
      val otherUploadName = createUpload()
      val otherFile =
        fileService.createRawImpressionUploadFile(
          createRawImpressionUploadFileRequest {
            parent = otherUploadName
            rawImpressionUploadFile = rawImpressionUploadFile {
              sizeBytes = SIZE_BYTES
              blobUri = BLOB_URI_1
            }
            requestId = UUID.randomUUID().toString()
          }
        )

      val exception =
        assertFailsWith<StatusRuntimeException> {
          fileService.batchDeleteRawImpressionUploadFiles(
            batchDeleteRawImpressionUploadFilesRequest {
              parent = uploadName
              requests += deleteRawImpressionUploadFileRequest { name = otherFile.name }
            }
          )
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
      assertThat(exception.errorInfo)
        .isEqualTo(
          errorInfo {
            domain = Errors.DOMAIN
            reason = Errors.Reason.INVALID_FIELD_VALUE.name
            metadata[Errors.Metadata.FIELD_NAME.key] = "requests.0.name"
          }
        )
    }

  @Test
  fun `batchDeleteRawImpressionUploadFiles throws NOT_FOUND for nonexistent file`() = runBlocking {
    val uploadName = createUpload()
    val uploadKey = assertNotNull(RawImpressionUploadKey.fromName(uploadName))
    val missingName =
      RawImpressionUploadFileKey(
          uploadKey.dataProviderId,
          uploadKey.rawImpressionUploadId,
          "nonexistent",
        )
        .toName()

    val exception =
      assertFailsWith<StatusRuntimeException> {
        fileService.batchDeleteRawImpressionUploadFiles(
          batchDeleteRawImpressionUploadFilesRequest {
            parent = uploadName
            requests += deleteRawImpressionUploadFileRequest { name = missingName }
          }
        )
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
  }

  @Test
  fun `listRawImpressionUploadFiles throws INVALID_ARGUMENT for negative page_size`() =
    runBlocking {
      val uploadName = createUpload()

      val exception =
        assertFailsWith<StatusRuntimeException> {
          fileService.listRawImpressionUploadFiles(
            listRawImpressionUploadFilesRequest {
              parent = uploadName
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
  fun `listRawImpressionUploadFiles excludes soft-deleted files by default`() = runBlocking {
    val uploadName = createUpload()
    val file1 =
      fileService.createRawImpressionUploadFile(
        createRawImpressionUploadFileRequest {
          parent = uploadName
          rawImpressionUploadFile = rawImpressionUploadFile {
            sizeBytes = SIZE_BYTES
            blobUri = BLOB_URI_1
          }
          requestId = UUID.randomUUID().toString()
        }
      )
    val file2 =
      fileService.createRawImpressionUploadFile(
        createRawImpressionUploadFileRequest {
          parent = uploadName
          rawImpressionUploadFile = rawImpressionUploadFile {
            sizeBytes = SIZE_BYTES
            blobUri = BLOB_URI_2
          }
          requestId = UUID.randomUUID().toString()
        }
      )
    fileService.deleteRawImpressionUploadFile(
      deleteRawImpressionUploadFileRequest { name = file1.name }
    )

    val response =
      fileService.listRawImpressionUploadFiles(
        listRawImpressionUploadFilesRequest { parent = uploadName }
      )

    assertThat(response.rawImpressionUploadFilesList.map { it.name }).containsExactly(file2.name)
    Unit
  }

  @Test
  fun `listRawImpressionUploadFiles includes soft-deleted files when show_deleted is true`() =
    runBlocking {
      val uploadName = createUpload()
      val file1 =
        fileService.createRawImpressionUploadFile(
          createRawImpressionUploadFileRequest {
            parent = uploadName
            rawImpressionUploadFile = rawImpressionUploadFile {
              sizeBytes = SIZE_BYTES
              blobUri = BLOB_URI_1
            }
            requestId = UUID.randomUUID().toString()
          }
        )
      val file2 =
        fileService.createRawImpressionUploadFile(
          createRawImpressionUploadFileRequest {
            parent = uploadName
            rawImpressionUploadFile = rawImpressionUploadFile {
              sizeBytes = SIZE_BYTES
              blobUri = BLOB_URI_2
            }
            requestId = UUID.randomUUID().toString()
          }
        )
      fileService.deleteRawImpressionUploadFile(
        deleteRawImpressionUploadFileRequest { name = file1.name }
      )

      val response =
        fileService.listRawImpressionUploadFiles(
          listRawImpressionUploadFilesRequest {
            parent = uploadName
            showDeleted = true
          }
        )

      assertThat(response.rawImpressionUploadFilesList.map { it.name })
        .containsExactly(file1.name, file2.name)
      Unit
    }

  @Test
  fun `batchCreateRawImpressionUploadFiles throws INVALID_ARGUMENT when batch exceeds max size`() =
    runBlocking {
      val uploadName = createUpload()

      val exception =
        assertFailsWith<StatusRuntimeException> {
          fileService.batchCreateRawImpressionUploadFiles(
            batchCreateRawImpressionUploadFilesRequest {
              parent = uploadName
              // One more than the maximum of 100 allowed per batch.
              repeat(101) { i ->
                requests += createRawImpressionUploadFileRequest {
                  parent = uploadName
                  rawImpressionUploadFile = rawImpressionUploadFile {
                    sizeBytes = SIZE_BYTES
                    blobUri = "gs://bucket/file-$i"
                  }
                  requestId = UUID.randomUUID().toString()
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
  fun `batchDeleteRawImpressionUploadFiles throws INVALID_ARGUMENT when batch exceeds max size`() =
    runBlocking {
      val uploadName = createUpload()
      val uploadKey = assertNotNull(RawImpressionUploadKey.fromName(uploadName))

      val exception =
        assertFailsWith<StatusRuntimeException> {
          fileService.batchDeleteRawImpressionUploadFiles(
            batchDeleteRawImpressionUploadFilesRequest {
              parent = uploadName
              // One more than the maximum of 1000 allowed per batch.
              repeat(1001) { i ->
                requests += deleteRawImpressionUploadFileRequest {
                  name =
                    RawImpressionUploadFileKey(
                        uploadKey.dataProviderId,
                        uploadKey.rawImpressionUploadId,
                        "file-$i",
                      )
                      .toName()
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
  fun `createRawImpressionUploadFile throws ALREADY_EXISTS for duplicate blob_uri`() = runBlocking {
    val uploadName = createUpload()
    fileService.createRawImpressionUploadFile(
      createRawImpressionUploadFileRequest {
        parent = uploadName
        rawImpressionUploadFile = rawImpressionUploadFile {
          sizeBytes = SIZE_BYTES
          blobUri = BLOB_URI_1
        }
        requestId = UUID.randomUUID().toString()
      }
    )
    val exception =
      assertFailsWith<StatusRuntimeException> {
        fileService.createRawImpressionUploadFile(
          createRawImpressionUploadFileRequest {
            parent = uploadName
            rawImpressionUploadFile = rawImpressionUploadFile {
              sizeBytes = SIZE_BYTES
              blobUri = BLOB_URI_1
            }
            requestId = UUID.randomUUID().toString()
          }
        )
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.ALREADY_EXISTS)
  }

  companion object {
    @get:ClassRule @JvmStatic val spannerEmulator = SpannerEmulatorRule()

    private val DATA_PROVIDER_ID = externalIdToApiId(111L)
    private const val BLOB_URI_1 = "gs://bucket/file1"
    private const val BLOB_URI_2 = "gs://bucket/file2"
    private const val SIZE_BYTES = 1024L
    private val FAR_FUTURE = timestamp { seconds = 4102444800L }
    private val uploadIdCounter = AtomicLong(0L)
  }
}
