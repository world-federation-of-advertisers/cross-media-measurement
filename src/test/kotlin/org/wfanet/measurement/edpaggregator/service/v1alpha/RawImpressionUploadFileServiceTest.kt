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
import com.google.rpc.errorInfo
import io.grpc.Status
import io.grpc.StatusRuntimeException
import java.time.Instant
import java.util.UUID
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
    val uploadResourceId: String = UUID.randomUUID().toString()
    spannerDatabase.databaseClient.readWriteTransaction().run { txn ->
      txn.bufferInsertMutation("RawImpressionUpload") {
        set("DataProviderResourceId").to(DATA_PROVIDER_ID)
        set("RawImpressionUploadId").to(uploadResourceId)
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
          rawImpressionUploadFile = rawImpressionUploadFile { blobUri = BLOB_URI_1 }
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
      rawImpressionUploadFile = rawImpressionUploadFile { blobUri = BLOB_URI_1 }
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
      rawImpressionUploadFile = rawImpressionUploadFile { blobUri = BLOB_URI_1 }
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
          rawImpressionUploadFile = rawImpressionUploadFile { blobUri = BLOB_URI_1 }
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
          rawImpressionUploadFile = rawImpressionUploadFile { blobUri = BLOB_URI_1 }
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
          rawImpressionUploadFile = rawImpressionUploadFile { blobUri = BLOB_URI_1 }
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
            rawImpressionUploadFile = rawImpressionUploadFile { blobUri = BLOB_URI_1 }
            requestId = UUID.randomUUID().toString()
          }
          requests += createRawImpressionUploadFileRequest {
            parent = uploadName
            rawImpressionUploadFile = rawImpressionUploadFile { blobUri = BLOB_URI_2 }
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
          rawImpressionUploadFile = rawImpressionUploadFile { blobUri = BLOB_URI_1 }
        }
      )
    val file2 =
      fileService.createRawImpressionUploadFile(
        createRawImpressionUploadFileRequest {
          parent = uploadName
          rawImpressionUploadFile = rawImpressionUploadFile { blobUri = BLOB_URI_2 }
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

  companion object {
    @get:ClassRule @JvmStatic val spannerEmulator = SpannerEmulatorRule()

    private val DATA_PROVIDER_ID = externalIdToApiId(111L)
    private const val BLOB_URI_1 = "gs://bucket/file1"
    private const val BLOB_URI_2 = "gs://bucket/file2"
  }
}
