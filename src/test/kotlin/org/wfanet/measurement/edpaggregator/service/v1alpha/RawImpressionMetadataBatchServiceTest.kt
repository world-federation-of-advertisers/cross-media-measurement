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
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.SpannerRawImpressionMetadataBatchService
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.testing.Schemata
import org.wfanet.measurement.edpaggregator.service.Errors
import org.wfanet.measurement.edpaggregator.service.RawImpressionMetadataBatchKey
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionMetadataBatch
import org.wfanet.measurement.edpaggregator.v1alpha.createRawImpressionMetadataBatchRequest
import org.wfanet.measurement.edpaggregator.v1alpha.deleteRawImpressionMetadataBatchRequest
import org.wfanet.measurement.edpaggregator.v1alpha.getRawImpressionMetadataBatchRequest
import org.wfanet.measurement.edpaggregator.v1alpha.listRawImpressionMetadataBatchesRequest
import org.wfanet.measurement.edpaggregator.v1alpha.listRawImpressionMetadataBatchesResponse
import org.wfanet.measurement.edpaggregator.v1alpha.markRawImpressionMetadataBatchFailedRequest
import org.wfanet.measurement.edpaggregator.v1alpha.markRawImpressionMetadataBatchProcessedRequest
import org.wfanet.measurement.edpaggregator.v1alpha.rawImpressionMetadataBatch
import org.wfanet.measurement.gcloud.spanner.testing.SpannerEmulatorDatabaseRule
import org.wfanet.measurement.gcloud.spanner.testing.SpannerEmulatorRule
import org.wfanet.measurement.internal.edpaggregator.RawImpressionMetadataBatchServiceGrpcKt.RawImpressionMetadataBatchServiceCoroutineImplBase as InternalBatchServiceCoroutineImplBase
import org.wfanet.measurement.internal.edpaggregator.RawImpressionMetadataBatchServiceGrpcKt.RawImpressionMetadataBatchServiceCoroutineStub as InternalBatchServiceCoroutineStub

@RunWith(JUnit4::class)
class RawImpressionMetadataBatchServiceTest {
  private lateinit var internalService: InternalBatchServiceCoroutineImplBase
  private lateinit var service: RawImpressionMetadataBatchService

  val spannerDatabase =
    SpannerEmulatorDatabaseRule(spannerEmulator, Schemata.EDP_AGGREGATOR_CHANGELOG_PATH)

  val grpcTestServerRule = GrpcTestServerRule {
    val spannerDatabaseClient = spannerDatabase.databaseClient
    internalService =
      SpannerRawImpressionMetadataBatchService(spannerDatabaseClient, EmptyCoroutineContext)
    addService(internalService)
  }

  @get:Rule
  val serverRuleChain: TestRule = chainRulesSequentially(spannerDatabase, grpcTestServerRule)

  @Before
  fun initService() {
    service =
      RawImpressionMetadataBatchService(
        InternalBatchServiceCoroutineStub(grpcTestServerRule.channel)
      )
  }

  @Test
  fun `createRawImpressionMetadataBatch returns a batch successfully`() = runBlocking {
    val startTime = Instant.now()
    val request = createRawImpressionMetadataBatchRequest {
      parent = DATA_PROVIDER_KEY.toName()
      rawImpressionMetadataBatch = rawImpressionMetadataBatch {}
      requestId = REQUEST_ID
    }

    val batch = service.createRawImpressionMetadataBatch(request)

    val batchKey = assertNotNull(RawImpressionMetadataBatchKey.fromName(batch.name))
    assertThat(batchKey.dataProviderId).isEqualTo(DATA_PROVIDER_ID)
    assertThat(batchKey.rawImpressionMetadataBatchId).isNotEmpty()
    assertThat(batch.state).isEqualTo(RawImpressionMetadataBatch.State.CREATED)
    assertThat(batch.createTime.toInstant()).isGreaterThan(startTime)
    assertThat(batch.updateTime).isEqualTo(batch.createTime)
  }

  @Test
  fun `createRawImpressionMetadataBatch with requestId is idempotent`() = runBlocking {
    val request = createRawImpressionMetadataBatchRequest {
      parent = DATA_PROVIDER_KEY.toName()
      rawImpressionMetadataBatch = rawImpressionMetadataBatch {}
      requestId = REQUEST_ID
    }
    val existing = service.createRawImpressionMetadataBatch(request)

    val duplicate = service.createRawImpressionMetadataBatch(request)

    assertThat(duplicate).isEqualTo(existing)
  }

  @Test
  fun `createRawImpressionMetadataBatch throws INVALID_ARGUMENT for empty parent`() = runBlocking {
    val request = createRawImpressionMetadataBatchRequest {
      rawImpressionMetadataBatch = rawImpressionMetadataBatch {}
      requestId = REQUEST_ID
    }

    val exception =
      assertFailsWith<StatusRuntimeException> { service.createRawImpressionMetadataBatch(request) }
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
  fun `createRawImpressionMetadataBatch throws INVALID_ARGUMENT for malformed parent`() =
    runBlocking {
      val request = createRawImpressionMetadataBatchRequest {
        parent = "invalid-parent"
        rawImpressionMetadataBatch = rawImpressionMetadataBatch {}
        requestId = REQUEST_ID
      }

      val exception =
        assertFailsWith<StatusRuntimeException> {
          service.createRawImpressionMetadataBatch(request)
        }
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
  fun `createRawImpressionMetadataBatch throws INVALID_ARGUMENT for malformed requestId`() =
    runBlocking {
      val request = createRawImpressionMetadataBatchRequest {
        parent = DATA_PROVIDER_KEY.toName()
        rawImpressionMetadataBatch = rawImpressionMetadataBatch {}
        requestId = "invalid-request-id"
      }

      val exception =
        assertFailsWith<StatusRuntimeException> {
          service.createRawImpressionMetadataBatch(request)
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
  fun `getRawImpressionMetadataBatch returns a batch`() = runBlocking {
    val created =
      service.createRawImpressionMetadataBatch(
        createRawImpressionMetadataBatchRequest {
          parent = DATA_PROVIDER_KEY.toName()
          rawImpressionMetadataBatch = rawImpressionMetadataBatch {}
          requestId = REQUEST_ID
        }
      )

    val batch =
      service.getRawImpressionMetadataBatch(
        getRawImpressionMetadataBatchRequest { name = created.name }
      )

    assertThat(batch).isEqualTo(created)
  }

  @Test
  fun `getRawImpressionMetadataBatch throws INVALID_ARGUMENT for empty name`() = runBlocking {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        service.getRawImpressionMetadataBatch(getRawImpressionMetadataBatchRequest {})
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
  fun `getRawImpressionMetadataBatch throws NOT_FOUND for nonexistent batch`() = runBlocking {
    val request = getRawImpressionMetadataBatchRequest {
      name = "dataProviders/dp1/rawImpressionMetadataBatches/nonexistent"
    }
    val exception =
      assertFailsWith<StatusRuntimeException> { service.getRawImpressionMetadataBatch(request) }
    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
    assertThat(exception.errorInfo)
      .isEqualTo(
        errorInfo {
          domain = Errors.DOMAIN
          reason = Errors.Reason.RAW_IMPRESSION_METADATA_BATCH_NOT_FOUND.name
          metadata[Errors.Metadata.RAW_IMPRESSION_METADATA_BATCH.key] = request.name
        }
      )
  }

  @Test
  fun `listRawImpressionMetadataBatches returns batches`() = runBlocking {
    val created =
      service.createRawImpressionMetadataBatch(
        createRawImpressionMetadataBatchRequest {
          parent = DATA_PROVIDER_KEY.toName()
          rawImpressionMetadataBatch = rawImpressionMetadataBatch {}
        }
      )

    val response =
      service.listRawImpressionMetadataBatches(
        listRawImpressionMetadataBatchesRequest { parent = DATA_PROVIDER_KEY.toName() }
      )

    assertThat(response)
      .isEqualTo(
        listRawImpressionMetadataBatchesResponse { rawImpressionMetadataBatches += created }
      )
  }

  @Test
  fun `listRawImpressionMetadataBatches respects page size and pagination`() = runBlocking {
    val created1 =
      service.createRawImpressionMetadataBatch(
        createRawImpressionMetadataBatchRequest {
          parent = DATA_PROVIDER_KEY.toName()
          rawImpressionMetadataBatch = rawImpressionMetadataBatch {}
        }
      )
    val created2 =
      service.createRawImpressionMetadataBatch(
        createRawImpressionMetadataBatchRequest {
          parent = DATA_PROVIDER_KEY.toName()
          rawImpressionMetadataBatch = rawImpressionMetadataBatch {}
        }
      )
    val sortedCreated = listOf(created1, created2).sortedBy { it.name }

    val firstResponse =
      service.listRawImpressionMetadataBatches(
        listRawImpressionMetadataBatchesRequest {
          parent = DATA_PROVIDER_KEY.toName()
          pageSize = 1
        }
      )

    assertThat(firstResponse.rawImpressionMetadataBatchesList).hasSize(1)
    assertThat(firstResponse.nextPageToken).isNotEmpty()

    val secondResponse =
      service.listRawImpressionMetadataBatches(
        listRawImpressionMetadataBatchesRequest {
          parent = DATA_PROVIDER_KEY.toName()
          pageSize = 1
          pageToken = firstResponse.nextPageToken
        }
      )

    assertThat(secondResponse.rawImpressionMetadataBatchesList).hasSize(1)
    assertThat(
        (firstResponse.rawImpressionMetadataBatchesList +
            secondResponse.rawImpressionMetadataBatchesList)
          .sortedBy { it.name }
      )
      .isEqualTo(sortedCreated)
  }

  @Test
  fun `deleteRawImpressionMetadataBatch soft deletes a batch`() = runBlocking {
    val created =
      service.createRawImpressionMetadataBatch(
        createRawImpressionMetadataBatchRequest {
          parent = DATA_PROVIDER_KEY.toName()
          rawImpressionMetadataBatch = rawImpressionMetadataBatch {}
        }
      )

    val deleted =
      service.deleteRawImpressionMetadataBatch(
        deleteRawImpressionMetadataBatchRequest { name = created.name }
      )

    assertThat(deleted.name).isEqualTo(created.name)
    assertThat(deleted.hasDeleteTime()).isTrue()
    assertThat(deleted.updateTime.toInstant()).isGreaterThan(created.updateTime.toInstant())
  }

  @Test
  fun `deleteRawImpressionMetadataBatch throws NOT_FOUND for nonexistent batch`() = runBlocking {
    val request = deleteRawImpressionMetadataBatchRequest {
      name = "dataProviders/dp1/rawImpressionMetadataBatches/nonexistent"
    }
    val exception =
      assertFailsWith<StatusRuntimeException> { service.deleteRawImpressionMetadataBatch(request) }
    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
    assertThat(exception.errorInfo)
      .isEqualTo(
        errorInfo {
          domain = Errors.DOMAIN
          reason = Errors.Reason.RAW_IMPRESSION_METADATA_BATCH_NOT_FOUND.name
          metadata[Errors.Metadata.RAW_IMPRESSION_METADATA_BATCH.key] = request.name
        }
      )
  }

  @Test
  fun `markRawImpressionMetadataBatchProcessed transitions state`() = runBlocking {
    val created =
      service.createRawImpressionMetadataBatch(
        createRawImpressionMetadataBatchRequest {
          parent = DATA_PROVIDER_KEY.toName()
          rawImpressionMetadataBatch = rawImpressionMetadataBatch {}
        }
      )

    val processed =
      service.markRawImpressionMetadataBatchProcessed(
        markRawImpressionMetadataBatchProcessedRequest { name = created.name }
      )

    assertThat(processed.state).isEqualTo(RawImpressionMetadataBatch.State.PROCESSED)
    assertThat(processed.name).isEqualTo(created.name)
    assertThat(processed.updateTime.toInstant()).isGreaterThan(created.updateTime.toInstant())
  }

  @Test
  fun `markRawImpressionMetadataBatchFailed transitions state`() = runBlocking {
    val created =
      service.createRawImpressionMetadataBatch(
        createRawImpressionMetadataBatchRequest {
          parent = DATA_PROVIDER_KEY.toName()
          rawImpressionMetadataBatch = rawImpressionMetadataBatch {}
        }
      )

    val failed =
      service.markRawImpressionMetadataBatchFailed(
        markRawImpressionMetadataBatchFailedRequest { name = created.name }
      )

    assertThat(failed.state).isEqualTo(RawImpressionMetadataBatch.State.FAILED)
    assertThat(failed.name).isEqualTo(created.name)
    assertThat(failed.updateTime.toInstant()).isGreaterThan(created.updateTime.toInstant())
  }

  companion object {
    @get:ClassRule @JvmStatic val spannerEmulator = SpannerEmulatorRule()

    private val DATA_PROVIDER_ID = externalIdToApiId(111L)
    private val DATA_PROVIDER_KEY = DataProviderKey(DATA_PROVIDER_ID)
    private val REQUEST_ID = UUID.randomUUID().toString()
  }
}
