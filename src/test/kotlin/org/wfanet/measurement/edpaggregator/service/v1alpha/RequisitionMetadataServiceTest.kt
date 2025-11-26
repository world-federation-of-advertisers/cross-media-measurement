// Copyright 2025 The Cross-Media Measurement Authors
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
import com.google.protobuf.Timestamp
import com.google.protobuf.timestamp
import com.google.rpc.errorInfo
import io.grpc.Status
import io.grpc.StatusRuntimeException
import java.time.Instant
import java.util.UUID
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
import org.wfanet.measurement.api.v2alpha.CanonicalRequisitionKey
import org.wfanet.measurement.api.v2alpha.DataProviderKey
import org.wfanet.measurement.common.IdGenerator
import org.wfanet.measurement.common.base64UrlEncode
import org.wfanet.measurement.common.grpc.errorInfo
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.common.identity.externalIdToApiId
import org.wfanet.measurement.common.testing.chainRulesSequentially
import org.wfanet.measurement.common.toInstant
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.SpannerRequisitionMetadataService
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.testing.Schemata
import org.wfanet.measurement.edpaggregator.service.Errors
import org.wfanet.measurement.edpaggregator.service.RequisitionMetadataKey
import org.wfanet.measurement.edpaggregator.v1alpha.ListRequisitionMetadataRequest
import org.wfanet.measurement.edpaggregator.v1alpha.ListRequisitionMetadataRequestKt
import org.wfanet.measurement.edpaggregator.v1alpha.RequisitionMetadata
import org.wfanet.measurement.edpaggregator.v1alpha.batchCreateRequisitionMetadataRequest
import org.wfanet.measurement.edpaggregator.v1alpha.batchCreateRequisitionMetadataResponse
import org.wfanet.measurement.edpaggregator.v1alpha.copy
import org.wfanet.measurement.edpaggregator.v1alpha.createRequisitionMetadataRequest
import org.wfanet.measurement.edpaggregator.v1alpha.fetchLatestCmmsCreateTimeRequest
import org.wfanet.measurement.edpaggregator.v1alpha.fulfillRequisitionMetadataRequest
import org.wfanet.measurement.edpaggregator.v1alpha.getRequisitionMetadataRequest
import org.wfanet.measurement.edpaggregator.v1alpha.listRequisitionMetadataRequest
import org.wfanet.measurement.edpaggregator.v1alpha.listRequisitionMetadataResponse
import org.wfanet.measurement.edpaggregator.v1alpha.lookupRequisitionMetadataRequest
import org.wfanet.measurement.edpaggregator.v1alpha.markWithdrawnRequisitionMetadataRequest
import org.wfanet.measurement.edpaggregator.v1alpha.queueRequisitionMetadataRequest
import org.wfanet.measurement.edpaggregator.v1alpha.refuseRequisitionMetadataRequest
import org.wfanet.measurement.edpaggregator.v1alpha.requisitionMetadata
import org.wfanet.measurement.edpaggregator.v1alpha.startProcessingRequisitionMetadataRequest
import org.wfanet.measurement.gcloud.spanner.AsyncDatabaseClient
import org.wfanet.measurement.gcloud.spanner.testing.SpannerEmulatorDatabaseRule
import org.wfanet.measurement.gcloud.spanner.testing.SpannerEmulatorRule
import org.wfanet.measurement.internal.edpaggregator.ListRequisitionMetadataPageTokenKt as InternalListRequisitionMetadataPageTokenKt
import org.wfanet.measurement.internal.edpaggregator.RequisitionMetadataServiceGrpcKt.RequisitionMetadataServiceCoroutineImplBase as InternalRequisitionMetadataServiceCoroutineImplBase
import org.wfanet.measurement.internal.edpaggregator.RequisitionMetadataServiceGrpcKt.RequisitionMetadataServiceCoroutineStub as InternalRequisitionMetadataServiceStub
import org.wfanet.measurement.internal.edpaggregator.listRequisitionMetadataPageToken as internalListRequisitionMetadataPageToken
import org.wfanet.measurement.reporting.service.api.v2alpha.ReportKey

@RunWith(JUnit4::class)
class RequisitionMetadataServiceTest {
  private lateinit var internalService: InternalRequisitionMetadataServiceCoroutineImplBase
  private lateinit var service: RequisitionMetadataService

  private fun newInternalService(): InternalRequisitionMetadataServiceCoroutineImplBase {
    val databaseClient: AsyncDatabaseClient = spannerDatabase.databaseClient
    return SpannerRequisitionMetadataService(databaseClient, idGenerator = IdGenerator.Default)
  }

  val spannerDatabase =
    SpannerEmulatorDatabaseRule(spannerEmulator, Schemata.EDP_AGGREGATOR_CHANGELOG_PATH)

  private val grpcTestServerRule = GrpcTestServerRule {
    internalService = newInternalService()
    addService(internalService)
  }

  @get:Rule val ruleChain: TestRule = chainRulesSequentially(spannerDatabase, grpcTestServerRule)

  @Before
  fun initService() {
    service =
      RequisitionMetadataService(InternalRequisitionMetadataServiceStub(grpcTestServerRule.channel))
  }

  @Test
  fun `createRequisitionMetadata with requestId returns a RequisitionMetadata successfully`() =
    runBlocking {
      val startTime = Instant.now()
      val request = createRequisitionMetadataRequest {
        parent = DATA_PROVIDER_KEY.toName()
        requisitionMetadata = NEW_REQUISITION_METADATA
        requestId = REQUEST_ID
      }

      val requisitionMetadata = service.createRequisitionMetadata(request)

      assertThat(requisitionMetadata)
        .comparingExpectedFieldsOnly()
        .isEqualTo(
          requisitionMetadata {
            state = RequisitionMetadata.State.STORED
            cmmsCreateTime = CMMS_CREATE_TIME
            cmmsRequisition = CMMS_REQUISITION_KEY.toName()
            blobUri = BLOB_URI
            blobTypeUrl = BLOB_TYPE
            groupId = GROUP_ID
            report = REPORT_KEY.toName()
          }
        )
      val requisitionMetadataKey =
        assertNotNull(RequisitionMetadataKey.fromName(requisitionMetadata.name))
      assertThat(requisitionMetadataKey.dataProviderId).isEqualTo(DATA_PROVIDER_ID)
      assertThat(requisitionMetadataKey.requisitionMetadataId).isNotEmpty()
      assertThat(requisitionMetadata.createTime.toInstant()).isGreaterThan(startTime)
      assertThat(requisitionMetadata.updateTime).isEqualTo(requisitionMetadata.createTime)
      assertThat(requisitionMetadata.etag).isNotEmpty()
    }

  @Test
  fun `createRequisitionMetadata without requestId returns a RequisitionMetadata successfully`() =
    runBlocking {
      val startTime = Instant.now()
      val request = createRequisitionMetadataRequest {
        parent = DATA_PROVIDER_KEY.toName()
        requisitionMetadata = NEW_REQUISITION_METADATA
        // no request_id
      }

      val requisitionMetadata = service.createRequisitionMetadata(request)

      assertThat(requisitionMetadata)
        .comparingExpectedFieldsOnly()
        .isEqualTo(
          requisitionMetadata {
            state = RequisitionMetadata.State.STORED
            cmmsCreateTime = CMMS_CREATE_TIME
            cmmsRequisition = CMMS_REQUISITION_KEY.toName()
            blobUri = BLOB_URI
            blobTypeUrl = BLOB_TYPE
            groupId = GROUP_ID
            report = REPORT_KEY.toName()
          }
        )
      val requisitionMetadataKey =
        assertNotNull(RequisitionMetadataKey.fromName(requisitionMetadata.name))
      assertThat(requisitionMetadataKey.dataProviderId).isEqualTo(DATA_PROVIDER_ID)
      assertThat(requisitionMetadataKey.requisitionMetadataId).isNotEmpty()
      assertThat(requisitionMetadata.createTime.toInstant()).isGreaterThan(startTime)
      assertThat(requisitionMetadata.updateTime).isEqualTo(requisitionMetadata.createTime)
      assertThat(requisitionMetadata.etag).isNotEmpty()
    }

  @Test
  fun `createRequisitionMetadata with existing requestId returns the existing RequisitionMetadata`() =
    runBlocking {
      val request = createRequisitionMetadataRequest {
        parent = DATA_PROVIDER_KEY.toName()
        requisitionMetadata = NEW_REQUISITION_METADATA
        requestId = REQUEST_ID
      }
      val existingRequisitionMetadata = service.createRequisitionMetadata(request)

      val requisitionMetadata = service.createRequisitionMetadata(request)

      assertThat(requisitionMetadata).isEqualTo(existingRequisitionMetadata)
    }

  @Test
  fun `createRequisitionMetadata throws INVALID_ARGUMENT when parent is missing`() = runBlocking {
    val request = createRequisitionMetadataRequest {
      // missing parent
      requisitionMetadata = NEW_REQUISITION_METADATA
      requestId = REQUEST_ID
    }

    val exception =
      assertFailsWith<StatusRuntimeException> { service.createRequisitionMetadata(request) }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `createRequisitionMetadata throws INVALID_ARGUMENT for invalid cmms_requisition`() =
    runBlocking {
      val request = createRequisitionMetadataRequest {
        parent = DATA_PROVIDER_KEY.toName()
        requisitionMetadata = NEW_REQUISITION_METADATA.copy { cmmsRequisition = "foo" }
        requestId = REQUEST_ID
      }

      val exception =
        assertFailsWith<StatusRuntimeException> { service.createRequisitionMetadata(request) }
      assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    }

  @Test
  fun `createRequisitionMetadata throws INVALID_ARGUMENT for mismatched cmms_requisition parent`() =
    runBlocking {
      val request = createRequisitionMetadataRequest {
        parent = DATA_PROVIDER_KEY.toName()
        requisitionMetadata =
          NEW_REQUISITION_METADATA.copy {
            cmmsRequisition = CanonicalRequisitionKey("other-data-provider", "123").toName()
          }
        requestId = REQUEST_ID
      }

      val exception =
        assertFailsWith<StatusRuntimeException> { service.createRequisitionMetadata(request) }
      assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    }

  @Test
  fun `createRequisitionMetadata throws INVALID_ARGUMENT for invalid report`() = runBlocking {
    val request = createRequisitionMetadataRequest {
      parent = DATA_PROVIDER_KEY.toName()
      requisitionMetadata = NEW_REQUISITION_METADATA.copy { report = "foo" }
      requestId = REQUEST_ID
    }

    val exception =
      assertFailsWith<StatusRuntimeException> { service.createRequisitionMetadata(request) }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `batchCreateRequisitionMetadata returns created RequisitionMetadata`() = runBlocking {
    val request1 = createRequisitionMetadataRequest {
      parent = DATA_PROVIDER_KEY.toName()
      requisitionMetadata = REQUISITION_METADATA
      requestId = UUID.randomUUID().toString()
    }
    val request2 = createRequisitionMetadataRequest {
      parent = DATA_PROVIDER_KEY.toName()
      requisitionMetadata = REQUISITION_METADATA_2
      requestId = UUID.randomUUID().toString()
    }

    val startTime = Instant.now()

    val response =
      service.batchCreateRequisitionMetadata(
        batchCreateRequisitionMetadataRequest {
          parent = DATA_PROVIDER_KEY.toName()
          requests += request1
          requests += request2
        }
      )

    assertThat(response)
      .comparingExpectedFieldsOnly()
      .isEqualTo(
        batchCreateRequisitionMetadataResponse {
          requisitionMetadata +=
            request1.requisitionMetadata.copy {
              clearName()
              clearCreateTime()
              clearUpdateTime()
              clearEtag()
            }
          requisitionMetadata +=
            request2.requisitionMetadata.copy {
              clearName()
              clearCreateTime()
              clearUpdateTime()
              clearEtag()
            }
        }
      )

    assertThat(response.requisitionMetadataList.all { it.name.isNotEmpty() }).isTrue()
    assertThat(response.requisitionMetadataList.all { it.createTime.toInstant() >= startTime })
      .isTrue()
    assertThat(
        response.requisitionMetadataList.all {
          it.updateTime.toInstant() == it.createTime.toInstant()
        }
      )
      .isTrue()
    assertThat(
        response.requisitionMetadataList.all { it.state == RequisitionMetadata.State.STORED }
      )
      .isTrue()
  }

  @Test
  fun `batchCreateRequisitionMetadata is idempotent`() = runBlocking {
    val idempotentRequest = createRequisitionMetadataRequest {
      parent = DATA_PROVIDER_KEY.toName()
      requisitionMetadata = REQUISITION_METADATA
      requestId = UUID.randomUUID().toString()
    }
    val initialResponse =
      service.batchCreateRequisitionMetadata(
        batchCreateRequisitionMetadataRequest {
          parent = DATA_PROVIDER_KEY.toName()
          requests += idempotentRequest
        }
      )
    val existingRequisitionMetadata = initialResponse.requisitionMetadataList.single()

    val newRequest = createRequisitionMetadataRequest {
      parent = DATA_PROVIDER_KEY.toName()
      requisitionMetadata = REQUISITION_METADATA_2
      requestId = UUID.randomUUID().toString()
    }
    val secondResponse =
      service.batchCreateRequisitionMetadata(
        batchCreateRequisitionMetadataRequest {
          parent = DATA_PROVIDER_KEY.toName()
          requests += idempotentRequest
          requests += newRequest
        }
      )

    assertThat(secondResponse.requisitionMetadataList.first())
      .isEqualTo(existingRequisitionMetadata)

    val newRequisitionMetadata = secondResponse.requisitionMetadataList.last()
    assertThat(newRequisitionMetadata.name).isNotEqualTo(existingRequisitionMetadata.name)
    assertThat(newRequisitionMetadata.createTime.toInstant())
      .isGreaterThan(existingRequisitionMetadata.createTime.toInstant())
    assertThat(newRequisitionMetadata.updateTime.toInstant())
      .isGreaterThan(existingRequisitionMetadata.updateTime.toInstant())
    assertThat(newRequisitionMetadata.state).isEqualTo(RequisitionMetadata.State.STORED)
  }

  @Test
  fun `batchCreateRequisitionMetadata throws INVALID_ARGUMENT for missing parent`() = runBlocking {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        service.batchCreateRequisitionMetadata(
          batchCreateRequisitionMetadataRequest {
            requests += createRequisitionMetadataRequest {
              requisitionMetadata = REQUISITION_METADATA
              requestId = REQUEST_ID
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
          metadata[Errors.Metadata.FIELD_NAME.key] = "parent"
        }
      )
  }

  @Test
  fun `batchCreateRequisitionMetadata throws INVALID_ARGUMENT for malformed parent`() =
    runBlocking {
      val exception =
        assertFailsWith<StatusRuntimeException> {
          service.batchCreateRequisitionMetadata(
            batchCreateRequisitionMetadataRequest {
              parent = "invalid-parent"
              requests += createRequisitionMetadataRequest {
                requisitionMetadata = REQUISITION_METADATA
                requestId = REQUEST_ID
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
            metadata[Errors.Metadata.FIELD_NAME.key] = "parent"
          }
        )
    }

  @Test
  fun `batchCreateRequisitionMetadata throws INVALID_ARGUMENT for inconsistent parent`() =
    runBlocking {
      val dataProviderId2 = externalIdToApiId(222L)
      val dataProviderKey2 = DataProviderKey(dataProviderId2)

      val exception =
        assertFailsWith<StatusRuntimeException> {
          service.batchCreateRequisitionMetadata(
            batchCreateRequisitionMetadataRequest {
              parent = dataProviderKey2.toName()
              requests += createRequisitionMetadataRequest {
                parent = DATA_PROVIDER_KEY.toName()
                requisitionMetadata = REQUISITION_METADATA
                requestId = REQUEST_ID
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
            metadata[Errors.Metadata.PARENT.key] = dataProviderKey2.toName()
            metadata[Errors.Metadata.DATA_PROVIDER.key] = DATA_PROVIDER_KEY.toName()
          }
        )
    }

  @Test
  fun `batchCreateRequisitionMetadata throws INVALID_ARGUMENT for malformed request id`() =
    runBlocking {
      val exception =
        assertFailsWith<StatusRuntimeException> {
          service.batchCreateRequisitionMetadata(
            batchCreateRequisitionMetadataRequest {
              parent = DATA_PROVIDER_KEY.toName()
              requests += createRequisitionMetadataRequest {
                parent = DATA_PROVIDER_KEY.toName()
                requisitionMetadata = REQUISITION_METADATA
                requestId = "invalid-request-id"
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
            metadata[Errors.Metadata.FIELD_NAME.key] = "requests.0.request_id"
          }
        )
    }

  @Test
  fun `batchCreateRequisitionMetadata throws INVALID_ARGUMENT for duplicate request id`() =
    runBlocking {
      val exception =
        assertFailsWith<StatusRuntimeException> {
          service.batchCreateRequisitionMetadata(
            batchCreateRequisitionMetadataRequest {
              parent = DATA_PROVIDER_KEY.toName()
              requests += createRequisitionMetadataRequest {
                parent = DATA_PROVIDER_KEY.toName()
                requisitionMetadata = REQUISITION_METADATA
                requestId = REQUEST_ID
              }
              requests += createRequisitionMetadataRequest {
                parent = DATA_PROVIDER_KEY.toName()
                requisitionMetadata = REQUISITION_METADATA_2
                requestId = REQUEST_ID
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
  fun `batchCreateRequisitionMetadata throws INVALID_ARGUMENT for duplicate blob uri`() =
    runBlocking {
      val exception =
        assertFailsWith<StatusRuntimeException> {
          service.batchCreateRequisitionMetadata(
            batchCreateRequisitionMetadataRequest {
              parent = DATA_PROVIDER_KEY.toName()
              requests += createRequisitionMetadataRequest {
                parent = DATA_PROVIDER_KEY.toName()
                requisitionMetadata = REQUISITION_METADATA
                requestId = REQUEST_ID
              }
              requests += createRequisitionMetadataRequest {
                parent = DATA_PROVIDER_KEY.toName()
                requisitionMetadata = REQUISITION_METADATA_3
                requestId = UUID.randomUUID().toString()
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
            metadata[Errors.Metadata.FIELD_NAME.key] = "requests.1.requisition_metadata.blob_uri"
          }
        )
    }

  @Test
  fun `batchCreateRequisitionMetadata throws INVALID_ARGUMENT for duplicate cmms requisition metadata`() =
    runBlocking {
      val exception =
        assertFailsWith<StatusRuntimeException> {
          service.batchCreateRequisitionMetadata(
            batchCreateRequisitionMetadataRequest {
              parent = DATA_PROVIDER_KEY.toName()
              requests += createRequisitionMetadataRequest {
                parent = DATA_PROVIDER_KEY.toName()
                requisitionMetadata = REQUISITION_METADATA
                requestId = REQUEST_ID
              }
              requests += createRequisitionMetadataRequest {
                parent = DATA_PROVIDER_KEY.toName()
                requisitionMetadata =
                  REQUISITION_METADATA_2.copy {
                    cmmsRequisition = REQUISITION_METADATA.cmmsRequisition
                  }
                requestId = UUID.randomUUID().toString()
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
              "requests.1.requisition_metadata.cmms_requisition"
          }
        )
    }

  @Test
  fun `getRequisitionMetadata returns a RequisitionMetadata successfully`() = runBlocking {
    val existingRequisitionMetadata =
      service.createRequisitionMetadata(
        createRequisitionMetadataRequest {
          parent = DATA_PROVIDER_KEY.toName()
          requisitionMetadata = NEW_REQUISITION_METADATA
          requestId = REQUEST_ID
        }
      )

    val request = getRequisitionMetadataRequest { name = existingRequisitionMetadata.name }
    val requisitionMetadata = service.getRequisitionMetadata(request)

    assertThat(requisitionMetadata).isEqualTo(existingRequisitionMetadata)
  }

  @Test
  fun `getRequisitionMetadata throws INVALID_ARGUMENT for invalid name`() = runBlocking {
    val request = getRequisitionMetadataRequest { name = "foo" }
    val exception =
      assertFailsWith<StatusRuntimeException> { service.getRequisitionMetadata(request) }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `lookupRequisitionMetadata by cmmsRequisition returns a RequisitionMetadata successfully`() =
    runBlocking {
      val existingRequisitionMetadata =
        service.createRequisitionMetadata(
          createRequisitionMetadataRequest {
            parent = DATA_PROVIDER_KEY.toName()
            requisitionMetadata = NEW_REQUISITION_METADATA
            requestId = REQUEST_ID
          }
        )

      val request = lookupRequisitionMetadataRequest {
        parent = DATA_PROVIDER_KEY.toName()
        cmmsRequisition = CMMS_REQUISITION_KEY.toName()
      }
      val result = service.lookupRequisitionMetadata(request)

      assertThat(result).isEqualTo(existingRequisitionMetadata)
    }

  @Test
  fun `lookupRequisitionMetadata throws INVALID_ARGUMENT for missing parent`() = runBlocking {
    val request = lookupRequisitionMetadataRequest {
      // missing parent
      cmmsRequisition = CMMS_REQUISITION_KEY.toName()
    }
    val exception =
      assertFailsWith<StatusRuntimeException> { service.lookupRequisitionMetadata(request) }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `lookupRequisitionMetadata throws INVALID_ARGUMENT for missing lookup key`() = runBlocking {
    val request = lookupRequisitionMetadataRequest {
      parent = DATA_PROVIDER_KEY.toName()
      // missing lookup key
    }
    val exception =
      assertFailsWith<StatusRuntimeException> { service.lookupRequisitionMetadata(request) }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `fetchLatestCmmsCreateTime returns default Timestamp when no requisition metadata`() =
    runBlocking {
      // No requisition metadata created

      val request = fetchLatestCmmsCreateTimeRequest { parent = DATA_PROVIDER_KEY.toName() }
      val result = service.fetchLatestCmmsCreateTime(request)

      assertThat(result).isEqualTo(Timestamp.getDefaultInstance())
    }

  @Test
  fun `fetchLatestCmmsCreateTime returns latest Timestamp successfully`() = runBlocking {
    val earlyTimestamp = timestamp { seconds = 1 }
    val lateTimestamp = timestamp { seconds = 2 }

    service.createRequisitionMetadata(
      createRequisitionMetadataRequest {
        parent = DATA_PROVIDER_KEY.toName()
        requisitionMetadata = NEW_REQUISITION_METADATA.copy { cmmsCreateTime = earlyTimestamp }
      }
    )
    service.createRequisitionMetadata(
      createRequisitionMetadataRequest {
        parent = DATA_PROVIDER_KEY.toName()
        requisitionMetadata =
          NEW_REQUISITION_METADATA.copy {
            cmmsCreateTime = lateTimestamp
            cmmsRequisition = CMMS_REQUISITION_KEY.toName() + "second"
            blobUri = BLOB_URI + "second"
          }
      }
    )

    val request = fetchLatestCmmsCreateTimeRequest { parent = DATA_PROVIDER_KEY.toName() }
    val result = service.fetchLatestCmmsCreateTime(request)
    assertThat(result).isEqualTo(lateTimestamp)
  }

  @Test
  fun `fetchLatestCmmsCreateTime throws INVALID_ARGUMENT for missing parent`() = runBlocking {
    val request = fetchLatestCmmsCreateTimeRequest {
      // missing parent
    }
    val exception =
      assertFailsWith<StatusRuntimeException> { service.fetchLatestCmmsCreateTime(request) }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `queueRequisitionMetadata updates state successfully`() = runBlocking {
    val existingRequisitionMetadata =
      service.createRequisitionMetadata(
        createRequisitionMetadataRequest {
          parent = DATA_PROVIDER_KEY.toName()
          requisitionMetadata = NEW_REQUISITION_METADATA
          requestId = REQUEST_ID
        }
      )

    val request = queueRequisitionMetadataRequest {
      name = existingRequisitionMetadata.name
      etag = existingRequisitionMetadata.etag
      workItem = WORK_ITEM
    }
    val requisitionMetadata = service.queueRequisitionMetadata(request)

    assertThat(requisitionMetadata.state).isEqualTo(RequisitionMetadata.State.QUEUED)
    assertThat(requisitionMetadata.workItem).isEqualTo(WORK_ITEM)
    assertThat(requisitionMetadata.updateTime.toInstant())
      .isGreaterThan(existingRequisitionMetadata.updateTime.toInstant())
    assertThat(requisitionMetadata.etag).isNotEqualTo(existingRequisitionMetadata.etag)
  }

  @Test
  fun `queueRequisitionMetadata throws INVALID_ARGUMENT for invalid name`() = runBlocking {
    val request = queueRequisitionMetadataRequest {
      name = "foo"
      etag = ETAG_1
      workItem = WORK_ITEM
    }
    val exception =
      assertFailsWith<StatusRuntimeException> { service.queueRequisitionMetadata(request) }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `queueRequisitionMetadata throws INVALID_ARGUMENT for invalid work_item`() = runBlocking {
    val request = queueRequisitionMetadataRequest {
      name = REQUISITION_METADATA_KEY.toName()
      etag = ETAG_1
      workItem = "foo"
    }
    val exception =
      assertFailsWith<StatusRuntimeException> { service.queueRequisitionMetadata(request) }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `queueRequisitionMetadata throws INVALID_ARGUMENT for missing etag`() = runBlocking {
    val request = queueRequisitionMetadataRequest {
      name = REQUISITION_METADATA_KEY.toName()
      // etag is missing
      workItem = WORK_ITEM
    }
    val exception =
      assertFailsWith<StatusRuntimeException> { service.queueRequisitionMetadata(request) }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `startProcessingRequisitionMetadata updates state successfully`() = runBlocking {
    val existingRequisitionMetadata =
      service.createRequisitionMetadata(
        createRequisitionMetadataRequest {
          parent = DATA_PROVIDER_KEY.toName()
          requisitionMetadata = NEW_REQUISITION_METADATA
          requestId = REQUEST_ID
        }
      )

    val request = startProcessingRequisitionMetadataRequest {
      name = existingRequisitionMetadata.name
      etag = existingRequisitionMetadata.etag
    }
    val requisitionMetadata = service.startProcessingRequisitionMetadata(request)

    assertThat(requisitionMetadata.state).isEqualTo(RequisitionMetadata.State.PROCESSING)
    assertThat(requisitionMetadata.updateTime.toInstant())
      .isGreaterThan(existingRequisitionMetadata.updateTime.toInstant())
    assertThat(requisitionMetadata.etag).isNotEqualTo(existingRequisitionMetadata.etag)
  }

  @Test
  fun `startProcessingRequisitionMetadata throws INVALID_ARGUMENT for invalid name`() =
    runBlocking {
      val request = startProcessingRequisitionMetadataRequest { name = "foo" }
      val exception =
        assertFailsWith<StatusRuntimeException> {
          service.startProcessingRequisitionMetadata(request)
        }
      assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    }

  @Test
  fun `startProcessingRequisitionMetadata throws INVALID_ARGUMENT for missing etag`() =
    runBlocking {
      val request = startProcessingRequisitionMetadataRequest {
        name = REQUISITION_METADATA_KEY.toName()
        // etag is missing
      }
      val exception =
        assertFailsWith<StatusRuntimeException> {
          service.startProcessingRequisitionMetadata(request)
        }
      assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    }

  @Test
  fun `fulfillRequisitionMetadata updates state successfully`() = runBlocking {
    val existingRequisitionMetadata =
      service.createRequisitionMetadata(
        createRequisitionMetadataRequest {
          parent = DATA_PROVIDER_KEY.toName()
          requisitionMetadata = NEW_REQUISITION_METADATA
          requestId = REQUEST_ID
        }
      )

    val request = fulfillRequisitionMetadataRequest {
      name = existingRequisitionMetadata.name
      etag = existingRequisitionMetadata.etag
    }
    val requisitionMetadata = service.fulfillRequisitionMetadata(request)

    assertThat(requisitionMetadata.state).isEqualTo(RequisitionMetadata.State.FULFILLED)
    assertThat(requisitionMetadata.updateTime.toInstant())
      .isGreaterThan(existingRequisitionMetadata.updateTime.toInstant())
    assertThat(requisitionMetadata.etag).isNotEqualTo(existingRequisitionMetadata.etag)
  }

  @Test
  fun `fulfillRequisitionMetadata throws INVALID_ARGUMENT for invalid name`() = runBlocking {
    val request = fulfillRequisitionMetadataRequest { name = "foo" }
    val exception =
      assertFailsWith<StatusRuntimeException> { service.fulfillRequisitionMetadata(request) }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `fulfillRequisitionMetadata throws INVALID_ARGUMENT for missing etag`() = runBlocking {
    val request = fulfillRequisitionMetadataRequest {
      name = REQUISITION_METADATA_KEY.toName()
      // etag is missing
    }
    val exception =
      assertFailsWith<StatusRuntimeException> { service.fulfillRequisitionMetadata(request) }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `markWithdrawnRequisitionMetadata updates state successfully`() = runBlocking {
    val existingRequisitionMetadata =
      service.createRequisitionMetadata(
        createRequisitionMetadataRequest {
          parent = DATA_PROVIDER_KEY.toName()
          requisitionMetadata = NEW_REQUISITION_METADATA
          requestId = REQUEST_ID
        }
      )

    val request = markWithdrawnRequisitionMetadataRequest {
      name = existingRequisitionMetadata.name
      etag = existingRequisitionMetadata.etag
    }
    val requisitionMetadata = service.markWithdrawnRequisitionMetadata(request)

    assertThat(requisitionMetadata.state).isEqualTo(RequisitionMetadata.State.WITHDRAWN)
    assertThat(requisitionMetadata.updateTime.toInstant())
      .isGreaterThan(existingRequisitionMetadata.updateTime.toInstant())
    assertThat(requisitionMetadata.etag).isNotEqualTo(existingRequisitionMetadata.etag)
  }

  @Test
  fun `refuseRequisitionMetadata updates state successfully`() = runBlocking {
    val existingRequisitionMetadata =
      service.createRequisitionMetadata(
        createRequisitionMetadataRequest {
          parent = DATA_PROVIDER_KEY.toName()
          requisitionMetadata = NEW_REQUISITION_METADATA
          requestId = REQUEST_ID
        }
      )

    val request = refuseRequisitionMetadataRequest {
      name = existingRequisitionMetadata.name
      etag = existingRequisitionMetadata.etag
      refusalMessage = REFUSAL_MESSAGE
    }
    val requisitionMetadata = service.refuseRequisitionMetadata(request)

    assertThat(requisitionMetadata.state).isEqualTo(RequisitionMetadata.State.REFUSED)
    assertThat(requisitionMetadata.refusalMessage).isEqualTo(REFUSAL_MESSAGE)
    assertThat(requisitionMetadata.updateTime.toInstant())
      .isGreaterThan(existingRequisitionMetadata.updateTime.toInstant())
    assertThat(requisitionMetadata.etag).isNotEqualTo(existingRequisitionMetadata.etag)
  }

  @Test
  fun `refuseRequisitionMetadata throws INVALID_ARGUMENT for invalid name`() = runBlocking {
    val request = refuseRequisitionMetadataRequest {
      name = "foo"
      etag = ETAG_1
      refusalMessage = REFUSAL_MESSAGE
    }
    val exception =
      assertFailsWith<StatusRuntimeException> { service.refuseRequisitionMetadata(request) }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `refuseRequisitionMetadata throws INVALID_ARGUMENT for missing etag`() = runBlocking {
    val request = refuseRequisitionMetadataRequest {
      name = REQUISITION_METADATA_KEY.toName()
      // missing etag
      refusalMessage = REFUSAL_MESSAGE
    }
    val exception =
      assertFailsWith<StatusRuntimeException> { service.refuseRequisitionMetadata(request) }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `refuseRequisitionMetadata throws INVALID_ARGUMENT for missing refusalMessage`() =
    runBlocking {
      val request = refuseRequisitionMetadataRequest {
        name = REQUISITION_METADATA_KEY.toName()
        etag = ETAG_1
        // missing refusalMessage
      }
      val exception =
        assertFailsWith<StatusRuntimeException> { service.refuseRequisitionMetadata(request) }
      assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    }

  @Test
  fun `listRequisitionMetadata returns RequisitionMetadata`() = runBlocking {
    val created = createRequisitionMetadata(REQUISITION_METADATA)

    val response =
      service.listRequisitionMetadata(
        listRequisitionMetadataRequest { parent = DATA_PROVIDER_KEY.toName() }
      )

    assertThat(response)
      .isEqualTo(listRequisitionMetadataResponse { requisitionMetadata += created })
  }

  @Test
  fun `listRequisitionMetadata with page size returns RequisitionMetadata`() = runBlocking {
    val created = createRequisitionMetadata(REQUISITION_METADATA, REQUISITION_METADATA_2)
    val sortedCreated = created.sortedBy { it.name }

    val internalPageToken = internalListRequisitionMetadataPageToken {
      after =
        InternalListRequisitionMetadataPageTokenKt.after {
          updateTime = sortedCreated[0].updateTime
          requisitionMetadataResourceId =
            RequisitionMetadataKey.fromName(sortedCreated[0].name)!!.requisitionMetadataId
        }
    }

    val response =
      service.listRequisitionMetadata(
        listRequisitionMetadataRequest {
          parent = DATA_PROVIDER_KEY.toName()
          pageSize = 1
        }
      )

    assertThat(response)
      .isEqualTo(
        listRequisitionMetadataResponse {
          requisitionMetadata += sortedCreated[0]
          nextPageToken = internalPageToken.toByteString().base64UrlEncode()
        }
      )
  }

  @Test
  fun `listRequisitionMetadata with page token returns RequisitionMetadata`() = runBlocking {
    val created = createRequisitionMetadata(REQUISITION_METADATA, REQUISITION_METADATA_2)
    val sortedCreated = created.sortedBy { it.name }

    val firstResponse =
      service.listRequisitionMetadata(
        listRequisitionMetadataRequest {
          parent = DATA_PROVIDER_KEY.toName()
          pageSize = 1
        }
      )

    val secondResponse =
      service.listRequisitionMetadata(
        listRequisitionMetadataRequest {
          parent = DATA_PROVIDER_KEY.toName()
          pageSize = 1
          pageToken = firstResponse.nextPageToken
        }
      )

    assertThat(secondResponse)
      .isEqualTo(listRequisitionMetadataResponse { requisitionMetadata += sortedCreated[1] })
  }

  @Test
  fun `listRequisitionMetadata with GroupId filter returns RequisitionMetadata`() = runBlocking {
    val created = createRequisitionMetadata(REQUISITION_METADATA, REQUISITION_METADATA_3)

    val response =
      service.listRequisitionMetadata(
        listRequisitionMetadataRequest {
          parent = DATA_PROVIDER_KEY.toName()
          filter = ListRequisitionMetadataRequestKt.filter { groupId = created[0].groupId }
        }
      )

    assertThat(response)
      .isEqualTo(listRequisitionMetadataResponse { requisitionMetadata += created[0] })
  }

  @Test
  fun `listRequisitionMetadata throws INVALID_ARGUMENT when parent is not set`() = runBlocking {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        service.listRequisitionMetadata(ListRequisitionMetadataRequest.getDefaultInstance())
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
  fun `listRequisitionMetadata throws INVALID_ARGUMENT when parent is malformed`() = runBlocking {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        service.listRequisitionMetadata(
          listRequisitionMetadataRequest { parent = "invalid-parent" }
        )
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
  fun `listRequisitionMetadata throws INVALID_ARGUMENT when page size is invalid`() = runBlocking {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        service.listRequisitionMetadata(
          listRequisitionMetadataRequest {
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
  fun `listRequisitionMetadata throws INVALID_ARGUMENT when page token is malformed`() =
    runBlocking {
      val exception =
        assertFailsWith<StatusRuntimeException> {
          service.listRequisitionMetadata(
            listRequisitionMetadataRequest {
              parent = DATA_PROVIDER_KEY.toName()
              pageToken = "this-is-not-base64-or-a-proto"
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

  private suspend fun createRequisitionMetadata(
    vararg requisitionMetadata: RequisitionMetadata
  ): List<RequisitionMetadata> {
    return requisitionMetadata.map { metadata ->
      service.createRequisitionMetadata(
        createRequisitionMetadataRequest {
          parent = DATA_PROVIDER_KEY.toName()
          this.requisitionMetadata = metadata
          requestId = UUID.randomUUID().toString()
        }
      )
    }
  }

  companion object {
    @get:ClassRule @JvmStatic val spannerEmulator = SpannerEmulatorRule()

    private val DATA_PROVIDER_ID = externalIdToApiId(111L)
    private val DATA_PROVIDER_KEY = DataProviderKey(DATA_PROVIDER_ID)
    private val REQUISITION_METADATA_ID = externalIdToApiId(222L)
    private val REQUISITION_METADATA_ID_2 = externalIdToApiId(444L)
    private val REQUISITION_METADATA_ID_3 = externalIdToApiId(345L)
    private val REQUISITION_METADATA_KEY =
      RequisitionMetadataKey(DATA_PROVIDER_KEY, REQUISITION_METADATA_ID)
    private val REQUISITION_METADATA_KEY_2 =
      RequisitionMetadataKey(DATA_PROVIDER_KEY, REQUISITION_METADATA_ID_2)
    private val REQUISITION_METADATA_KEY_3 =
      RequisitionMetadataKey(DATA_PROVIDER_KEY, REQUISITION_METADATA_ID_3)
    private val CMMS_REQUISITION_ID = externalIdToApiId(333L)
    private val CMMS_REQUISITION_ID_2 = externalIdToApiId(123L)
    private val CMMS_REQUISITION_ID_3 = externalIdToApiId(456L)
    private val CMMS_REQUISITION_KEY =
      CanonicalRequisitionKey(DATA_PROVIDER_KEY, CMMS_REQUISITION_ID)
    private val CMMS_REQUISITION_KEY_2 =
      CanonicalRequisitionKey(DATA_PROVIDER_KEY, CMMS_REQUISITION_ID_2)
    private val CMMS_REQUISITION_KEY_3 =
      CanonicalRequisitionKey(DATA_PROVIDER_KEY, CMMS_REQUISITION_ID_3)
    private val MEASUREMENT_CONSUMER_ID = externalIdToApiId(444L)
    private val REPORT_ID = externalIdToApiId(555L)
    private val REPORT_KEY = ReportKey(MEASUREMENT_CONSUMER_ID, REPORT_ID)
    private val REQUEST_ID = UUID.randomUUID().toString()
    private const val BLOB_URI = "path/to/blob"
    private const val BLOB_URI_2 = "path/to/blob2"
    private const val BLOB_TYPE = "blob.type"
    private const val GROUP_ID = "group_id"
    private const val GROUP_ID_2 = "group_id_2"
    private val CMMS_CREATE_TIME = timestamp { seconds = 12345 }
    private val CREATE_TIME = timestamp { seconds = 12345 }
    private val UPDATE_TIME_1 = timestamp { seconds = 12345 }
    private val UPDATE_TIME_2 = timestamp { seconds = 23456 }
    private val WORK_ITEM = "workItems/${externalIdToApiId(666L)}"
    private val ETAG_1 = UPDATE_TIME_1.toString()
    private val ETAG_2 = UPDATE_TIME_2.toString()
    private val REFUSAL_MESSAGE = "Somehow refused."

    private val NEW_REQUISITION_METADATA = requisitionMetadata {
      // name not set
      cmmsRequisition = CMMS_REQUISITION_KEY.toName()
      report = REPORT_KEY.toName()
      blobUri = BLOB_URI
      blobTypeUrl = BLOB_TYPE
      groupId = GROUP_ID
      cmmsCreateTime = CMMS_CREATE_TIME
      // work_item not set
      // error_message not set
    }

    private val REQUISITION_METADATA =
      NEW_REQUISITION_METADATA.copy {
        name = REQUISITION_METADATA_KEY.toName()
        state = RequisitionMetadata.State.STORED
        createTime = CREATE_TIME
        updateTime = UPDATE_TIME_1
        etag = ETAG_1
      }

    private val REQUISITION_METADATA_2 =
      NEW_REQUISITION_METADATA.copy {
        blobUri = BLOB_URI_2
        cmmsRequisition = CMMS_REQUISITION_KEY_2.toName()
        name = REQUISITION_METADATA_KEY_2.toName()
        state = RequisitionMetadata.State.STORED
        createTime = CREATE_TIME
        updateTime = UPDATE_TIME_1
        etag = ETAG_2
      }

    private val REQUISITION_METADATA_3 =
      NEW_REQUISITION_METADATA.copy {
        cmmsRequisition = CMMS_REQUISITION_KEY_3.toName()
        name = REQUISITION_METADATA_KEY_3.toName()
        state = RequisitionMetadata.State.STORED
        createTime = CREATE_TIME
        updateTime = UPDATE_TIME_1
        groupId = GROUP_ID_2
        etag = ETAG_2
      }
  }
}
