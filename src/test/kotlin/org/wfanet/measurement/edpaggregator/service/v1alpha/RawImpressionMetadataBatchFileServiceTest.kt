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
import org.wfanet.measurement.api.v2alpha.DataProviderKey
import org.wfanet.measurement.common.grpc.errorInfo
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.common.identity.externalIdToApiId
import org.wfanet.measurement.common.testing.chainRulesSequentially
import org.wfanet.measurement.common.toInstant
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.SpannerRawImpressionMetadataBatchFileService
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.SpannerRawImpressionMetadataBatchService
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.testing.Schemata
import org.wfanet.measurement.edpaggregator.service.Errors
import org.wfanet.measurement.edpaggregator.service.RawImpressionMetadataBatchFileKey
import org.wfanet.measurement.edpaggregator.service.RawImpressionMetadataBatchKey
import org.wfanet.measurement.edpaggregator.v1alpha.batchCreateRawImpressionMetadataBatchFilesRequest
import org.wfanet.measurement.edpaggregator.v1alpha.batchDeleteRawImpressionMetadataBatchFilesRequest
import org.wfanet.measurement.edpaggregator.v1alpha.createRawImpressionMetadataBatchFileRequest
import org.wfanet.measurement.edpaggregator.v1alpha.createRawImpressionMetadataBatchRequest
import org.wfanet.measurement.edpaggregator.v1alpha.deleteRawImpressionMetadataBatchFileRequest
import org.wfanet.measurement.edpaggregator.v1alpha.getRawImpressionMetadataBatchFileRequest
import org.wfanet.measurement.edpaggregator.v1alpha.listRawImpressionMetadataBatchFilesRequest
import org.wfanet.measurement.edpaggregator.v1alpha.listRawImpressionMetadataBatchFilesResponse
import org.wfanet.measurement.edpaggregator.v1alpha.rawImpressionMetadataBatch
import org.wfanet.measurement.edpaggregator.v1alpha.rawImpressionMetadataBatchFile
import org.wfanet.measurement.gcloud.spanner.testing.SpannerEmulatorDatabaseRule
import org.wfanet.measurement.gcloud.spanner.testing.SpannerEmulatorRule
import org.wfanet.measurement.internal.edpaggregator.RawImpressionMetadataBatchFileServiceGrpcKt.RawImpressionMetadataBatchFileServiceCoroutineImplBase as InternalFileServiceCoroutineImplBase
import org.wfanet.measurement.internal.edpaggregator.RawImpressionMetadataBatchFileServiceGrpcKt.RawImpressionMetadataBatchFileServiceCoroutineStub as InternalFileServiceCoroutineStub
import org.wfanet.measurement.internal.edpaggregator.RawImpressionMetadataBatchServiceGrpcKt.RawImpressionMetadataBatchServiceCoroutineImplBase as InternalBatchServiceCoroutineImplBase
import org.wfanet.measurement.internal.edpaggregator.RawImpressionMetadataBatchServiceGrpcKt.RawImpressionMetadataBatchServiceCoroutineStub as InternalBatchServiceCoroutineStub

@RunWith(JUnit4::class)
class RawImpressionMetadataBatchFileServiceTest {
  private lateinit var internalBatchService: InternalBatchServiceCoroutineImplBase
  private lateinit var internalFileService: InternalFileServiceCoroutineImplBase
  private lateinit var batchService: RawImpressionMetadataBatchService
  private lateinit var fileService: RawImpressionMetadataBatchFileService

  val spannerDatabase =
    SpannerEmulatorDatabaseRule(spannerEmulator, Schemata.EDP_AGGREGATOR_CHANGELOG_PATH)

  val grpcTestServerRule = GrpcTestServerRule {
    val spannerDatabaseClient = spannerDatabase.databaseClient
    internalBatchService =
      SpannerRawImpressionMetadataBatchService(spannerDatabaseClient, EmptyCoroutineContext)
    internalFileService =
      SpannerRawImpressionMetadataBatchFileService(spannerDatabaseClient, EmptyCoroutineContext)
    addService(internalBatchService)
    addService(internalFileService)
  }

  @get:Rule
  val serverRuleChain: TestRule = chainRulesSequentially(spannerDatabase, grpcTestServerRule)

  @Before
  fun initService() {
    batchService =
      RawImpressionMetadataBatchService(
        InternalBatchServiceCoroutineStub(grpcTestServerRule.channel)
      )
    fileService =
      RawImpressionMetadataBatchFileService(
        InternalFileServiceCoroutineStub(grpcTestServerRule.channel)
      )
  }

  private suspend fun createBatch(): String {
    val batch =
      batchService.createRawImpressionMetadataBatch(
        createRawImpressionMetadataBatchRequest {
          parent = DATA_PROVIDER_KEY.toName()
          rawImpressionMetadataBatch = rawImpressionMetadataBatch {}
        }
      )
    return batch.name
  }

  @Test
  fun `createRawImpressionMetadataBatchFile returns a file`() = runBlocking {
    val batchName = createBatch()
    val startTime = Instant.now()

    val file =
      fileService.createRawImpressionMetadataBatchFile(
        createRawImpressionMetadataBatchFileRequest {
          parent = batchName
          rawImpressionMetadataBatchFile = rawImpressionMetadataBatchFile { blobUri = BLOB_URI_1 }
        }
      )

    val fileKey = assertNotNull(RawImpressionMetadataBatchFileKey.fromName(file.name))
    assertThat(fileKey.dataProviderId).isEqualTo(DATA_PROVIDER_ID)
    assertThat(fileKey.rawImpressionMetadataBatchId).isNotEmpty()
    assertThat(fileKey.fileId).isNotEmpty()
    assertThat(file.blobUri).isEqualTo(BLOB_URI_1)
    assertThat(file.createTime.toInstant()).isGreaterThan(startTime)
    assertThat(file.updateTime).isEqualTo(file.createTime)
  }

  @Test
  fun `createRawImpressionMetadataBatchFile throws INVALID_ARGUMENT for empty parent`() =
    runBlocking {
      val request = createRawImpressionMetadataBatchFileRequest {
        rawImpressionMetadataBatchFile = rawImpressionMetadataBatchFile { blobUri = BLOB_URI_1 }
      }

      val exception =
        assertFailsWith<StatusRuntimeException> {
          fileService.createRawImpressionMetadataBatchFile(request)
        }
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
  fun `createRawImpressionMetadataBatchFile throws NOT_FOUND for nonexistent batch`() =
    runBlocking {
      val request = createRawImpressionMetadataBatchFileRequest {
        parent = "dataProviders/dp1/rawImpressionMetadataBatches/nonexistent"
        rawImpressionMetadataBatchFile = rawImpressionMetadataBatchFile { blobUri = BLOB_URI_1 }
      }

      val exception =
        assertFailsWith<StatusRuntimeException> {
          fileService.createRawImpressionMetadataBatchFile(request)
        }
      assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
    }

  @Test
  fun `getRawImpressionMetadataBatchFile returns a file`() = runBlocking {
    val batchName = createBatch()
    val created =
      fileService.createRawImpressionMetadataBatchFile(
        createRawImpressionMetadataBatchFileRequest {
          parent = batchName
          rawImpressionMetadataBatchFile = rawImpressionMetadataBatchFile { blobUri = BLOB_URI_1 }
        }
      )

    val file =
      fileService.getRawImpressionMetadataBatchFile(
        getRawImpressionMetadataBatchFileRequest { name = created.name }
      )

    assertThat(file).isEqualTo(created)
  }

  @Test
  fun `getRawImpressionMetadataBatchFile throws NOT_FOUND for nonexistent file`() = runBlocking {
    val batchName = createBatch()
    val batchKey = assertNotNull(RawImpressionMetadataBatchKey.fromName(batchName))
    val request = getRawImpressionMetadataBatchFileRequest {
      name =
        RawImpressionMetadataBatchFileKey(
            batchKey.dataProviderId,
            batchKey.rawImpressionMetadataBatchId,
            "nonexistent",
          )
          .toName()
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        fileService.getRawImpressionMetadataBatchFile(request)
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
  }

  @Test
  fun `listRawImpressionMetadataBatchFiles returns files`() = runBlocking {
    val batchName = createBatch()
    val created =
      fileService.createRawImpressionMetadataBatchFile(
        createRawImpressionMetadataBatchFileRequest {
          parent = batchName
          rawImpressionMetadataBatchFile = rawImpressionMetadataBatchFile { blobUri = BLOB_URI_1 }
        }
      )

    val response =
      fileService.listRawImpressionMetadataBatchFiles(
        listRawImpressionMetadataBatchFilesRequest { parent = batchName }
      )

    assertThat(response)
      .isEqualTo(
        listRawImpressionMetadataBatchFilesResponse { rawImpressionMetadataBatchFiles += created }
      )
  }

  @Test
  fun `deleteRawImpressionMetadataBatchFile soft deletes a file`() = runBlocking {
    val batchName = createBatch()
    val created =
      fileService.createRawImpressionMetadataBatchFile(
        createRawImpressionMetadataBatchFileRequest {
          parent = batchName
          rawImpressionMetadataBatchFile = rawImpressionMetadataBatchFile { blobUri = BLOB_URI_1 }
        }
      )

    val deleted =
      fileService.deleteRawImpressionMetadataBatchFile(
        deleteRawImpressionMetadataBatchFileRequest { name = created.name }
      )

    assertThat(deleted.name).isEqualTo(created.name)
    assertThat(deleted.hasDeleteTime()).isTrue()
    assertThat(deleted.updateTime.toInstant()).isGreaterThan(created.updateTime.toInstant())
  }

  @Test
  fun `batchCreateRawImpressionMetadataBatchFiles creates multiple files`() = runBlocking {
    val batchName = createBatch()

    val response =
      fileService.batchCreateRawImpressionMetadataBatchFiles(
        batchCreateRawImpressionMetadataBatchFilesRequest {
          parent = batchName
          requests += createRawImpressionMetadataBatchFileRequest {
            parent = batchName
            rawImpressionMetadataBatchFile = rawImpressionMetadataBatchFile { blobUri = BLOB_URI_1 }
            requestId = UUID.randomUUID().toString()
          }
          requests += createRawImpressionMetadataBatchFileRequest {
            parent = batchName
            rawImpressionMetadataBatchFile = rawImpressionMetadataBatchFile { blobUri = BLOB_URI_2 }
            requestId = UUID.randomUUID().toString()
          }
        }
      )

    assertThat(response.rawImpressionMetadataBatchFilesList).hasSize(2)
    val blobUris = response.rawImpressionMetadataBatchFilesList.map { it.blobUri }
    assertThat(blobUris).containsExactly(BLOB_URI_1, BLOB_URI_2)
    Unit
  }

  @Test
  fun `batchDeleteRawImpressionMetadataBatchFiles deletes multiple files`() = runBlocking {
    val batchName = createBatch()
    val file1 =
      fileService.createRawImpressionMetadataBatchFile(
        createRawImpressionMetadataBatchFileRequest {
          parent = batchName
          rawImpressionMetadataBatchFile = rawImpressionMetadataBatchFile { blobUri = BLOB_URI_1 }
        }
      )
    val file2 =
      fileService.createRawImpressionMetadataBatchFile(
        createRawImpressionMetadataBatchFileRequest {
          parent = batchName
          rawImpressionMetadataBatchFile = rawImpressionMetadataBatchFile { blobUri = BLOB_URI_2 }
        }
      )

    val response =
      fileService.batchDeleteRawImpressionMetadataBatchFiles(
        batchDeleteRawImpressionMetadataBatchFilesRequest {
          parent = batchName
          names += file1.name
          names += file2.name
        }
      )

    assertThat(response.rawImpressionMetadataBatchFilesList).hasSize(2)
    assertThat(response.rawImpressionMetadataBatchFilesList.all { it.hasDeleteTime() }).isTrue()
  }

  companion object {
    @get:ClassRule @JvmStatic val spannerEmulator = SpannerEmulatorRule()

    private val DATA_PROVIDER_ID = externalIdToApiId(111L)
    private val DATA_PROVIDER_KEY = DataProviderKey(DATA_PROVIDER_ID)
    private const val BLOB_URI_1 = "gs://bucket/file1"
    private const val BLOB_URI_2 = "gs://bucket/file2"
  }
}
