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
import com.google.type.interval
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
import org.wfanet.measurement.common.base64UrlEncode
import org.wfanet.measurement.common.grpc.errorInfo
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.common.identity.externalIdToApiId
import org.wfanet.measurement.common.testing.chainRulesSequentially
import org.wfanet.measurement.common.toInstant
import org.wfanet.measurement.common.toProtoTime
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.SpannerRawImpressionUploadService
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.testing.Schemata
import org.wfanet.measurement.edpaggregator.service.Errors
import org.wfanet.measurement.edpaggregator.service.RawImpressionUploadKey
import org.wfanet.measurement.edpaggregator.v1alpha.ListRawImpressionUploadsRequestKt
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
  fun `createRawImpressionUpload returns an upload successfully`(): Unit = runBlocking {
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
    assertThat(upload.createTime.toInstant()).isGreaterThan(startTime)
    assertThat(upload.updateTime).isEqualTo(upload.createTime)
    // Verify the entire response, substituting the non-deterministic resource name and timestamps.
    assertThat(upload)
      .isEqualTo(
        rawImpressionUpload {
          name = upload.name
          state = RawImpressionUpload.State.CREATED
          doneBlobUri = DONE_BLOB_URI
          createTime = upload.createTime
          updateTime = upload.updateTime
        }
      )
    // Verify the upload was persisted by reading it back.
    val fetched =
      service.getRawImpressionUpload(getRawImpressionUploadRequest { name = upload.name })
    assertThat(fetched).isEqualTo(upload)
  }

  @Test
  fun `createRawImpressionUpload with requestId is idempotent`(): Unit = runBlocking {
    val request = createRawImpressionUploadRequest {
      parent = DATA_PROVIDER_KEY.toName()
      rawImpressionUpload = rawImpressionUpload { doneBlobUri = DONE_BLOB_URI }
      requestId = REQUEST_ID
    }
    val existing = service.createRawImpressionUpload(request)

    val duplicate = service.createRawImpressionUpload(request)

    assertThat(duplicate).isEqualTo(existing)
    // The duplicate request must not have created a second row.
    val response =
      service.listRawImpressionUploads(
        listRawImpressionUploadsRequest { parent = DATA_PROVIDER_KEY.toName() }
      )
    assertThat(response.rawImpressionUploadsList).hasSize(1)
  }

  @Test
  fun `createRawImpressionUpload throws INVALID_ARGUMENT for empty parent`(): Unit = runBlocking {
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
  fun `createRawImpressionUpload throws INVALID_ARGUMENT for malformed parent`(): Unit =
    runBlocking {
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
  fun `createRawImpressionUpload throws INVALID_ARGUMENT for missing done_blob_uri`(): Unit =
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
  fun `createRawImpressionUpload throws INVALID_ARGUMENT for malformed requestId`(): Unit =
    runBlocking {
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
  fun `createRawImpressionUpload throws INVALID_ARGUMENT for missing requestId`(): Unit =
    runBlocking {
      val request = createRawImpressionUploadRequest {
        parent = DATA_PROVIDER_KEY.toName()
        rawImpressionUpload = rawImpressionUpload { doneBlobUri = DONE_BLOB_URI }
      }

      val exception =
        assertFailsWith<StatusRuntimeException> { service.createRawImpressionUpload(request) }
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
  fun `getRawImpressionUpload throws INVALID_ARGUMENT for empty name`(): Unit = runBlocking {
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
  fun `getRawImpressionUpload throws NOT_FOUND for nonexistent upload`(): Unit = runBlocking {
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
  fun `listRawImpressionUploads returns uploads`(): Unit = runBlocking {
    val created =
      service.createRawImpressionUpload(
        createRawImpressionUploadRequest {
          parent = DATA_PROVIDER_KEY.toName()
          rawImpressionUpload = rawImpressionUpload { doneBlobUri = DONE_BLOB_URI }
          requestId = UUID.randomUUID().toString()
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
  fun `listRawImpressionUploads respects page size and pagination`(): Unit = runBlocking {
    val created1 =
      service.createRawImpressionUpload(
        createRawImpressionUploadRequest {
          parent = DATA_PROVIDER_KEY.toName()
          rawImpressionUpload = rawImpressionUpload { doneBlobUri = DONE_BLOB_URI }
          requestId = UUID.randomUUID().toString()
        }
      )
    val created2 =
      service.createRawImpressionUpload(
        createRawImpressionUploadRequest {
          parent = DATA_PROVIDER_KEY.toName()
          rawImpressionUpload = rawImpressionUpload { doneBlobUri = DONE_BLOB_URI }
          requestId = UUID.randomUUID().toString()
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

  @Test
  fun `listRawImpressionUploads throws INVALID_ARGUMENT for negative page_size`(): Unit =
    runBlocking {
      val exception =
        assertFailsWith<StatusRuntimeException> {
          service.listRawImpressionUploads(
            listRawImpressionUploadsRequest {
              parent = DATA_PROVIDER_KEY.toName()
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
  fun `listRawImpressionUploads throws INVALID_ARGUMENT for invalid page_token`(): Unit =
    runBlocking {
      val exception =
        assertFailsWith<StatusRuntimeException> {
          service.listRawImpressionUploads(
            listRawImpressionUploadsRequest {
              parent = DATA_PROVIDER_KEY.toName()
              // Valid base64url that does not decode to a valid page-token proto.
              pageToken = byteArrayOf(-1, -1, -1).base64UrlEncode()
            }
          )
        }
      assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
      assertThat(exception.errorInfo)
        .isEqualTo(
          errorInfo {
            domain = Errors.DOMAIN
            reason = Errors.Reason.INVALID_FIELD_VALUE.name
            metadata[Errors.Metadata.FIELD_NAME.key] = "page_token"
          }
        )
    }

  @Test
  fun `listRawImpressionUploads throws INVALID_ARGUMENT for unspecified state filter`(): Unit =
    runBlocking {
      val exception =
        assertFailsWith<StatusRuntimeException> {
          service.listRawImpressionUploads(
            listRawImpressionUploadsRequest {
              parent = DATA_PROVIDER_KEY.toName()
              filter =
                ListRawImpressionUploadsRequestKt.filter {
                  stateIn += RawImpressionUpload.State.STATE_UNSPECIFIED
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
            metadata[Errors.Metadata.FIELD_NAME.key] = "filter.state_in"
          }
        )
    }

  @Test
  fun `listRawImpressionUploads throws INVALID_ARGUMENT for unrecognized state filter`(): Unit =
    runBlocking {
      // An enum number outside the known State values deserializes to UNRECOGNIZED on the server,
      // which must be rejected the same way as STATE_UNSPECIFIED. The DSL cannot add UNRECOGNIZED
      // directly (it has no enum number), so inject the unknown value via the builder.
      val request =
        listRawImpressionUploadsRequest { parent = DATA_PROVIDER_KEY.toName() }
          .toBuilder()
          .apply { filterBuilder.addStateInValue(UNRECOGNIZED_STATE_VALUE) }
          .build()

      val exception =
        assertFailsWith<StatusRuntimeException> { service.listRawImpressionUploads(request) }
      assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
      assertThat(exception.errorInfo)
        .isEqualTo(
          errorInfo {
            domain = Errors.DOMAIN
            reason = Errors.Reason.INVALID_FIELD_VALUE.name
            metadata[Errors.Metadata.FIELD_NAME.key] = "filter.state_in"
          }
        )
    }

  @Test
  fun `listRawImpressionUploads filters by state`(): Unit = runBlocking {
    val created =
      service.createRawImpressionUpload(
        createRawImpressionUploadRequest {
          parent = DATA_PROVIDER_KEY.toName()
          rawImpressionUpload = rawImpressionUpload { doneBlobUri = DONE_BLOB_URI }
          requestId = UUID.randomUUID().toString()
        }
      )

    val createdResponse =
      service.listRawImpressionUploads(
        listRawImpressionUploadsRequest {
          parent = DATA_PROVIDER_KEY.toName()
          filter =
            ListRawImpressionUploadsRequestKt.filter {
              stateIn += RawImpressionUpload.State.CREATED
            }
        }
      )
    assertThat(createdResponse)
      .isEqualTo(listRawImpressionUploadsResponse { rawImpressionUploads += created })

    val failedResponse =
      service.listRawImpressionUploads(
        listRawImpressionUploadsRequest {
          parent = DATA_PROVIDER_KEY.toName()
          filter =
            ListRawImpressionUploadsRequestKt.filter { stateIn += RawImpressionUpload.State.FAILED }
        }
      )
    assertThat(failedResponse.rawImpressionUploadsList).isEmpty()
  }

  @Test
  fun `listRawImpressionUploads filters by create_time interval`(): Unit = runBlocking {
    val created =
      service.createRawImpressionUpload(
        createRawImpressionUploadRequest {
          parent = DATA_PROVIDER_KEY.toName()
          rawImpressionUpload = rawImpressionUpload { doneBlobUri = DONE_BLOB_URI }
          requestId = UUID.randomUUID().toString()
        }
      )

    val pastResponse =
      service.listRawImpressionUploads(
        listRawImpressionUploadsRequest {
          parent = DATA_PROVIDER_KEY.toName()
          filter =
            ListRawImpressionUploadsRequestKt.filter {
              createTimeIn = interval { startTime = Instant.now().minusSeconds(3600).toProtoTime() }
            }
        }
      )
    assertThat(pastResponse)
      .isEqualTo(listRawImpressionUploadsResponse { rawImpressionUploads += created })

    val futureResponse =
      service.listRawImpressionUploads(
        listRawImpressionUploadsRequest {
          parent = DATA_PROVIDER_KEY.toName()
          filter =
            ListRawImpressionUploadsRequestKt.filter {
              createTimeIn = interval { startTime = Instant.now().plusSeconds(3600).toProtoTime() }
            }
        }
      )
    assertThat(futureResponse.rawImpressionUploadsList).isEmpty()
  }

  @Test
  fun `listRawImpressionUploads state filter maps ACTIVE and COMPLETED`(): Unit = runBlocking {
    // Only CREATED uploads are reachable via the public API, so ACTIVE and COMPLETED filters must
    // map through toInternal() and match nothing here. This pins the ACTIVE and COMPLETED state
    // conversions that the CREATED/FAILED filter tests do not exercise.
    val created =
      service.createRawImpressionUpload(
        createRawImpressionUploadRequest {
          parent = DATA_PROVIDER_KEY.toName()
          rawImpressionUpload = rawImpressionUpload { doneBlobUri = DONE_BLOB_URI }
          requestId = UUID.randomUUID().toString()
        }
      )

    val activeResponse =
      service.listRawImpressionUploads(
        listRawImpressionUploadsRequest {
          parent = DATA_PROVIDER_KEY.toName()
          filter =
            ListRawImpressionUploadsRequestKt.filter { stateIn += RawImpressionUpload.State.ACTIVE }
        }
      )
    assertThat(activeResponse.rawImpressionUploadsList).isEmpty()

    val completedResponse =
      service.listRawImpressionUploads(
        listRawImpressionUploadsRequest {
          parent = DATA_PROVIDER_KEY.toName()
          filter =
            ListRawImpressionUploadsRequestKt.filter {
              stateIn += RawImpressionUpload.State.COMPLETED
            }
        }
      )
    assertThat(completedResponse.rawImpressionUploadsList).isEmpty()

    val mixedResponse =
      service.listRawImpressionUploads(
        listRawImpressionUploadsRequest {
          parent = DATA_PROVIDER_KEY.toName()
          filter =
            ListRawImpressionUploadsRequestKt.filter {
              stateIn += RawImpressionUpload.State.CREATED
              stateIn += RawImpressionUpload.State.ACTIVE
              stateIn += RawImpressionUpload.State.COMPLETED
            }
        }
      )
    assertThat(mixedResponse)
      .isEqualTo(listRawImpressionUploadsResponse { rawImpressionUploads += created })
  }

  @Test
  fun `listRawImpressionUploads filters by create_time interval with end_time`(): Unit =
    runBlocking {
      val created =
        service.createRawImpressionUpload(
          createRawImpressionUploadRequest {
            parent = DATA_PROVIDER_KEY.toName()
            rawImpressionUpload = rawImpressionUpload { doneBlobUri = DONE_BLOB_URI }
            requestId = UUID.randomUUID().toString()
          }
        )

      val withinEndResponse =
        service.listRawImpressionUploads(
          listRawImpressionUploadsRequest {
            parent = DATA_PROVIDER_KEY.toName()
            filter =
              ListRawImpressionUploadsRequestKt.filter {
                createTimeIn = interval { endTime = Instant.now().plusSeconds(3600).toProtoTime() }
              }
          }
        )
      assertThat(withinEndResponse)
        .isEqualTo(listRawImpressionUploadsResponse { rawImpressionUploads += created })

      val beforeEndResponse =
        service.listRawImpressionUploads(
          listRawImpressionUploadsRequest {
            parent = DATA_PROVIDER_KEY.toName()
            filter =
              ListRawImpressionUploadsRequestKt.filter {
                createTimeIn = interval { endTime = Instant.now().minusSeconds(3600).toProtoTime() }
              }
          }
        )
      assertThat(beforeEndResponse.rawImpressionUploadsList).isEmpty()
    }

  companion object {
    @get:ClassRule @JvmStatic val spannerEmulator = SpannerEmulatorRule()

    private val DATA_PROVIDER_ID = externalIdToApiId(111L)
    private val DATA_PROVIDER_KEY = DataProviderKey(DATA_PROVIDER_ID)
    private val REQUEST_ID = UUID.randomUUID().toString()
    private const val DONE_BLOB_URI = "gs://test-bucket/2026-06-16/done"
    // An enum number that is not part of RawImpressionUpload.State, forcing UNRECOGNIZED.
    private const val UNRECOGNIZED_STATE_VALUE = 999
  }
}
