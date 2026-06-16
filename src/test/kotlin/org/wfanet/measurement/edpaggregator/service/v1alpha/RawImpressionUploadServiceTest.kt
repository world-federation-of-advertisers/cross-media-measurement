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
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.SpannerRawImpressionUploadService
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.testing.Schemata
import org.wfanet.measurement.edpaggregator.service.Errors
import org.wfanet.measurement.edpaggregator.service.RawImpressionUploadKey
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUpload
import org.wfanet.measurement.edpaggregator.v1alpha.createRawImpressionUploadRequest
import org.wfanet.measurement.edpaggregator.v1alpha.getRawImpressionUploadRequest
import org.wfanet.measurement.edpaggregator.v1alpha.listRawImpressionUploadsRequest
import org.wfanet.measurement.edpaggregator.v1alpha.listRawImpressionUploadsResponse
import org.wfanet.measurement.edpaggregator.v1alpha.rawImpressionUpload
import org.wfanet.measurement.gcloud.spanner.testing.SpannerEmulatorDatabaseRule
import org.wfanet.measurement.gcloud.spanner.testing.SpannerEmulatorRule
import org.wfanet.measurement.internal.edpaggregator.RawImpressionUploadServiceGrpcKt.RawImpressionUploadServiceCoroutineImplBase as InternalUploadServiceCoroutineImplBase
import org.wfanet.measurement.internal.edpaggregator.RawImpressionUploadServiceGrpcKt.RawImpressionUploadServiceCoroutineStub as InternalUploadServiceCoroutineStub

@RunWith(JUnit4::class)
class RawImpressionUploadServiceTest {
  private lateinit var internalService: InternalUploadServiceCoroutineImplBase
  private lateinit var service: RawImpressionUploadService

  val spannerDatabase =
    SpannerEmulatorDatabaseRule(spannerEmulator, Schemata.EDP_AGGREGATOR_CHANGELOG_PATH)

  val grpcTestServerRule = GrpcTestServerRule {
    val spannerDatabaseClient = spannerDatabase.databaseClient
    internalService =
      SpannerRawImpressionUploadService(spannerDatabaseClient, EmptyCoroutineContext)
    addService(internalService)
  }

  @get:Rule
  val serverRuleChain: TestRule = chainRulesSequentially(spannerDatabase, grpcTestServerRule)

  @Before
  fun initService() {
    service =
      RawImpressionUploadService(InternalUploadServiceCoroutineStub(grpcTestServerRule.channel))
  }

  @Test
  fun `createRawImpressionUpload returns an upload successfully`() = runBlocking {
    val startTime = Instant.now()
    val request = createRawImpressionUploadRequest {
      parent = DATA_PROVIDER_KEY.toName()
      rawImpressionUpload = rawImpressionUpload { doneBlobUri = DONE_BLOB_URI }
      requestId = REQUEST_ID
    }

    val upload = service.createRawImpressionUpload(request)

    val uploadKey = assertNotNull(RawImpressionUploadKey.fromName(upload.name))
    assertThat(uploadKey.dataProviderId).isEqualTo(DATA_PROVIDER_ID)
    assertThat(uploadKey.rawImpressionUploadId).isNotEmpty()
    assertThat(upload.doneBlobUri).isEqualTo(DONE_BLOB_URI)
    assertThat(upload.state).isEqualTo(RawImpressionUpload.State.CREATED)
    assertThat(upload.createTime.toInstant()).isGreaterThan(startTime)
    assertThat(upload.updateTime).isEqualTo(upload.createTime)
  }

  @Test
  fun `createRawImpressionUpload with requestId is idempotent`() = runBlocking {
    val request = createRawImpressionUploadRequest {
      parent = DATA_PROVIDER_KEY.toName()
      rawImpressionUpload = rawImpressionUpload { doneBlobUri = DONE_BLOB_URI }
      requestId = REQUEST_ID
    }
    val existing = service.createRawImpressionUpload(request)

    val duplicate = service.createRawImpressionUpload(request)

    assertThat(duplicate).isEqualTo(existing)
  }

  @Test
  fun `createRawImpressionUpload throws INVALID_ARGUMENT for empty parent`() = runBlocking {
    val request = createRawImpressionUploadRequest {
      rawImpressionUpload = rawImpressionUpload { doneBlobUri = DONE_BLOB_URI }
      requestId = REQUEST_ID
    }

    val exception =
      assertFailsWith<StatusRuntimeException> { service.createRawImpressionUpload(request) }
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
  fun `createRawImpressionUpload throws INVALID_ARGUMENT for malformed parent`() = runBlocking {
    val request = createRawImpressionUploadRequest {
      parent = "invalid-parent"
      rawImpressionUpload = rawImpressionUpload { doneBlobUri = DONE_BLOB_URI }
      requestId = REQUEST_ID
    }

    val exception =
      assertFailsWith<StatusRuntimeException> { service.createRawImpressionUpload(request) }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.errorInfo)
      .isEqualTo(
        errorInfo {
          domain = Errors.DOMAIN
          reason = Errors.Reason.INVALID_FIELD_VALUE.name
          metadata[Errors.Metadata.FIELD_NAME.key] = "parent"
        }
      )
  }

  @Test
  fun `createRawImpressionUpload throws INVALID_ARGUMENT for missing done_blob_uri`() =
    runBlocking {
      val request = createRawImpressionUploadRequest {
        parent = DATA_PROVIDER_KEY.toName()
        rawImpressionUpload = rawImpressionUpload {}
        requestId = REQUEST_ID
      }

      val exception =
        assertFailsWith<StatusRuntimeException> { service.createRawImpressionUpload(request) }
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
  fun `createRawImpressionUpload throws INVALID_ARGUMENT for malformed requestId`() = runBlocking {
    val request = createRawImpressionUploadRequest {
      parent = DATA_PROVIDER_KEY.toName()
      rawImpressionUpload = rawImpressionUpload { doneBlobUri = DONE_BLOB_URI }
      requestId = "invalid-request-id"
    }

    val exception =
      assertFailsWith<StatusRuntimeException> { service.createRawImpressionUpload(request) }
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
  fun `getRawImpressionUpload returns an upload`() = runBlocking {
    val created =
      service.createRawImpressionUpload(
        createRawImpressionUploadRequest {
          parent = DATA_PROVIDER_KEY.toName()
          rawImpressionUpload = rawImpressionUpload { doneBlobUri = DONE_BLOB_URI }
          requestId = REQUEST_ID
        }
      )

    val upload =
      service.getRawImpressionUpload(getRawImpressionUploadRequest { name = created.name })

    assertThat(upload).isEqualTo(created)
  }

  @Test
  fun `getRawImpressionUpload throws INVALID_ARGUMENT for empty name`() = runBlocking {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        service.getRawImpressionUpload(getRawImpressionUploadRequest {})
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.errorInfo)
      .isEqualTo(
        errorInfo {
          domain = Errors.DOMAIN
          reason = Errors.Reason.REQUIRED_FIELD_NOT_SET.name
          metadata[Errors.Metadata.FIELD_NAME.key] = "name"
        }
      )
  }

  @Test
  fun `getRawImpressionUpload throws NOT_FOUND for nonexistent upload`() = runBlocking {
    val request = getRawImpressionUploadRequest {
      name = "dataProviders/dp1/rawImpressionUploads/nonexistent"
    }
    val exception =
      assertFailsWith<StatusRuntimeException> { service.getRawImpressionUpload(request) }
    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
    assertThat(exception.errorInfo)
      .isEqualTo(
        errorInfo {
          domain = Errors.DOMAIN
          reason = Errors.Reason.RAW_IMPRESSION_UPLOAD_NOT_FOUND.name
          metadata[Errors.Metadata.RAW_IMPRESSION_UPLOAD.key] = request.name
        }
      )
  }

  @Test
  fun `listRawImpressionUploads returns uploads`() = runBlocking {
    val created =
      service.createRawImpressionUpload(
        createRawImpressionUploadRequest {
          parent = DATA_PROVIDER_KEY.toName()
          rawImpressionUpload = rawImpressionUpload { doneBlobUri = DONE_BLOB_URI }
        }
      )

    val response =
      service.listRawImpressionUploads(
        listRawImpressionUploadsRequest { parent = DATA_PROVIDER_KEY.toName() }
      )

    assertThat(response)
      .isEqualTo(listRawImpressionUploadsResponse { rawImpressionUploads += created })
  }

  @Test
  fun `listRawImpressionUploads respects page size and pagination`() = runBlocking {
    val created1 =
      service.createRawImpressionUpload(
        createRawImpressionUploadRequest {
          parent = DATA_PROVIDER_KEY.toName()
          rawImpressionUpload = rawImpressionUpload { doneBlobUri = DONE_BLOB_URI }
        }
      )
    val created2 =
      service.createRawImpressionUpload(
        createRawImpressionUploadRequest {
          parent = DATA_PROVIDER_KEY.toName()
          rawImpressionUpload = rawImpressionUpload { doneBlobUri = DONE_BLOB_URI }
        }
      )
    val sortedCreated = listOf(created1, created2).sortedBy { it.name }

    val firstResponse =
      service.listRawImpressionUploads(
        listRawImpressionUploadsRequest {
          parent = DATA_PROVIDER_KEY.toName()
          pageSize = 1
        }
      )

    assertThat(firstResponse.rawImpressionUploadsList).hasSize(1)
    assertThat(firstResponse.nextPageToken).isNotEmpty()

    val secondResponse =
      service.listRawImpressionUploads(
        listRawImpressionUploadsRequest {
          parent = DATA_PROVIDER_KEY.toName()
          pageSize = 1
          pageToken = firstResponse.nextPageToken
        }
      )

    assertThat(secondResponse.rawImpressionUploadsList).hasSize(1)
    assertThat(
        (firstResponse.rawImpressionUploadsList + secondResponse.rawImpressionUploadsList)
          .sortedBy { it.name }
      )
      .isEqualTo(sortedCreated)
  }

  companion object {
    @get:ClassRule @JvmStatic val spannerEmulator = SpannerEmulatorRule()

    private val DATA_PROVIDER_ID = externalIdToApiId(111L)
    private val DATA_PROVIDER_KEY = DataProviderKey(DATA_PROVIDER_ID)
    private val REQUEST_ID = UUID.randomUUID().toString()
    private const val DONE_BLOB_URI = "gs://test-bucket/2026-06-16/done"
  }
}
