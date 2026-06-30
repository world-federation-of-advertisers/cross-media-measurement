/*
 * Copyright 2026 The Cross-Media Measurement Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.wfanet.measurement.edpaggregator.service.v1alpha

import com.google.cloud.spanner.Mutation
import com.google.cloud.spanner.Value
import com.google.common.truth.Truth.assertThat
import com.google.common.truth.extensions.proto.ProtoTruth.assertThat
import com.google.protobuf.kotlin.toByteStringUtf8
import com.google.rpc.errorInfo
import com.google.type.Date
import com.google.type.date
import io.grpc.Status
import io.grpc.StatusRuntimeException
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
import org.wfanet.measurement.common.api.ResourceKey
import org.wfanet.measurement.common.grpc.errorInfo
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.common.testing.chainRulesSequentially
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.SpannerRankIndexBlobService
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.testing.Schemata
import org.wfanet.measurement.edpaggregator.service.Errors
import org.wfanet.measurement.edpaggregator.service.RankIndexBlobKey
import org.wfanet.measurement.edpaggregator.service.RawImpressionUploadKey
import org.wfanet.measurement.edpaggregator.v1alpha.EncryptedDek
import org.wfanet.measurement.edpaggregator.v1alpha.ListRankIndexBlobsRequestKt
import org.wfanet.measurement.edpaggregator.v1alpha.RankIndexBlob
import org.wfanet.measurement.edpaggregator.v1alpha.batchCreateRankIndexBlobsRequest
import org.wfanet.measurement.edpaggregator.v1alpha.batchDeleteRankIndexBlobsRequest
import org.wfanet.measurement.edpaggregator.v1alpha.copy
import org.wfanet.measurement.edpaggregator.v1alpha.createRankIndexBlobRequest
import org.wfanet.measurement.edpaggregator.v1alpha.deleteRankIndexBlobRequest
import org.wfanet.measurement.edpaggregator.v1alpha.encryptedDek
import org.wfanet.measurement.edpaggregator.v1alpha.getRankIndexBlobRequest
import org.wfanet.measurement.edpaggregator.v1alpha.listRankIndexBlobsRequest
import org.wfanet.measurement.edpaggregator.v1alpha.rankIndexBlob
import org.wfanet.measurement.gcloud.spanner.testing.SpannerEmulatorDatabaseRule
import org.wfanet.measurement.gcloud.spanner.testing.SpannerEmulatorRule
import org.wfanet.measurement.internal.edpaggregator.RankIndexBlobServiceGrpcKt.RankIndexBlobServiceCoroutineImplBase as InternalRankIndexBlobServiceCoroutineImplBase
import org.wfanet.measurement.internal.edpaggregator.RankIndexBlobServiceGrpcKt.RankIndexBlobServiceCoroutineStub as InternalRankIndexBlobServiceCoroutineStub
import org.wfanet.measurement.internal.edpaggregator.RawImpressionUploadState

@RunWith(JUnit4::class)
class RankIndexBlobServiceTest {
  private lateinit var internalService: InternalRankIndexBlobServiceCoroutineImplBase
  private lateinit var service: RankIndexBlobService

  private var nextUploadId: Long = 1L

  val spannerDatabase =
    SpannerEmulatorDatabaseRule(spannerEmulator, Schemata.EDP_AGGREGATOR_CHANGELOG_PATH)

  val grpcTestServerRule = GrpcTestServerRule {
    val spannerDatabaseClient = spannerDatabase.databaseClient
    internalService = SpannerRankIndexBlobService(spannerDatabaseClient, EmptyCoroutineContext)
    addService(internalService)
  }

  @get:Rule
  val serverRuleChain: TestRule = chainRulesSequentially(spannerDatabase, grpcTestServerRule)

  @Before
  fun initService() {
    service =
      RankIndexBlobService(InternalRankIndexBlobServiceCoroutineStub(grpcTestServerRule.channel))
  }

  private suspend fun createParentUpload(
    dataProviderResourceId: String,
    rawImpressionUploadResourceId: String,
  ) {
    val uploadId = nextUploadId++
    val mutation =
      Mutation.newInsertBuilder("RawImpressionUpload")
        .set("DataProviderResourceId")
        .to(dataProviderResourceId)
        .set("RawImpressionUploadId")
        .to(uploadId)
        .set("RawImpressionUploadResourceId")
        .to(rawImpressionUploadResourceId)
        .set("DoneBlobUri")
        .to("gs://bucket/done")
        .set("State")
        .to(Value.protoEnum(RawImpressionUploadState.RAW_IMPRESSION_UPLOAD_STATE_CREATED))
        .set("CreateTime")
        .to(Value.COMMIT_TIMESTAMP)
        .set("UpdateTime")
        .to(Value.COMMIT_TIMESTAMP)
        .build()
    spannerDatabase.databaseClient.write(listOf(mutation))
  }

  private suspend fun createBlob(
    poolOffset: Long = 0L,
    blobType: RankIndexBlob.BlobType = RankIndexBlob.BlobType.SNAPSHOT,
    cmmsModelLine: String = CMMS_MODEL_LINE,
  ): RankIndexBlob {
    return service.createRankIndexBlob(
      createRankIndexBlobRequest {
        parent = UPLOAD_KEY.toName()
        rankIndexBlob = newPublicBlob(poolOffset, blobType, cmmsModelLine)
        requestId = UUID.randomUUID().toString()
      }
    )
  }

  private fun newPublicBlob(
    poolOffset: Long = 0L,
    blobType: RankIndexBlob.BlobType = RankIndexBlob.BlobType.SNAPSHOT,
    cmmsModelLine: String = CMMS_MODEL_LINE,
    blobUri: String = "$BLOB_URI/$cmmsModelLine/$blobType/$poolOffset",
  ): RankIndexBlob {
    return rankIndexBlob {
      this.blobType = blobType
      this.cmmsModelLine = cmmsModelLine
      this.poolOffset = poolOffset
      this.blobUri = blobUri
      blobChecksum = BLOB_CHECKSUM
      encryptedDek = ENCRYPTED_DEK
      maxEventDate = MAX_EVENT_DATE
    }
  }

  @Test
  fun `createRankIndexBlob returns a blob successfully`() =
    runBlocking<Unit> {
      createParentUpload(DATA_PROVIDER_ID, RAW_IMPRESSION_UPLOAD_ID)
      val request = createRankIndexBlobRequest {
        parent = UPLOAD_KEY.toName()
        rankIndexBlob = newPublicBlob()
        requestId = REQUEST_ID
      }

      val blob = service.createRankIndexBlob(request)

      val key = assertNotNull(RankIndexBlobKey.fromName(blob.name))
      assertThat(key.dataProviderId).isEqualTo(DATA_PROVIDER_ID)
      assertThat(key.rawImpressionUploadId).isEqualTo(RAW_IMPRESSION_UPLOAD_ID)
      assertThat(blob.blobType).isEqualTo(RankIndexBlob.BlobType.SNAPSHOT)
      assertThat(blob.cmmsModelLine).isEqualTo(CMMS_MODEL_LINE)
      assertThat(blob.blobUri).isEqualTo(newPublicBlob().blobUri)
      assertThat(blob.encryptedDek).isEqualTo(ENCRYPTED_DEK)
      assertThat(blob.hasCreateTime()).isTrue()
      assertThat(blob.hasDeleteTime()).isFalse()
    }

  @Test
  fun `createRankIndexBlob with requestId is idempotent`() =
    runBlocking<Unit> {
      createParentUpload(DATA_PROVIDER_ID, RAW_IMPRESSION_UPLOAD_ID)
      val request = createRankIndexBlobRequest {
        parent = UPLOAD_KEY.toName()
        rankIndexBlob = newPublicBlob()
        requestId = REQUEST_ID
      }
      val existing = service.createRankIndexBlob(request)

      val duplicate = service.createRankIndexBlob(request)

      assertThat(duplicate).isEqualTo(existing)
    }

  @Test
  fun `createRankIndexBlob throws INVALID_ARGUMENT for empty parent`() =
    runBlocking<Unit> {
      val exception =
        assertFailsWith<StatusRuntimeException> {
          service.createRankIndexBlob(
            createRankIndexBlobRequest {
              rankIndexBlob = newPublicBlob()
              requestId = REQUEST_ID
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
  fun `createRankIndexBlob throws INVALID_ARGUMENT for empty request_id`() =
    runBlocking<Unit> {
      val exception =
        assertFailsWith<StatusRuntimeException> {
          service.createRankIndexBlob(
            createRankIndexBlobRequest {
              parent = UPLOAD_KEY.toName()
              rankIndexBlob = newPublicBlob()
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
  fun `createRankIndexBlob throws INVALID_ARGUMENT for malformed request_id`() =
    runBlocking<Unit> {
      val exception =
        assertFailsWith<StatusRuntimeException> {
          service.createRankIndexBlob(
            createRankIndexBlobRequest {
              parent = UPLOAD_KEY.toName()
              rankIndexBlob = newPublicBlob()
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
  fun `createRankIndexBlob throws INVALID_ARGUMENT when blob_type unspecified`() =
    runBlocking<Unit> {
      val exception =
        assertFailsWith<StatusRuntimeException> {
          service.createRankIndexBlob(
            createRankIndexBlobRequest {
              parent = UPLOAD_KEY.toName()
              rankIndexBlob = newPublicBlob().copy { clearBlobType() }
              requestId = REQUEST_ID
            }
          )
        }
      assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    }

  @Test
  fun `createRankIndexBlob throws INVALID_ARGUMENT when encrypted_dek missing`() =
    runBlocking<Unit> {
      val exception =
        assertFailsWith<StatusRuntimeException> {
          service.createRankIndexBlob(
            createRankIndexBlobRequest {
              parent = UPLOAD_KEY.toName()
              rankIndexBlob = newPublicBlob().copy { clearEncryptedDek() }
              requestId = REQUEST_ID
            }
          )
        }
      assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    }

  @Test
  fun `createRankIndexBlob throws INVALID_ARGUMENT when blob_checksum missing`() =
    runBlocking<Unit> {
      val exception =
        assertFailsWith<StatusRuntimeException> {
          service.createRankIndexBlob(
            createRankIndexBlobRequest {
              parent = UPLOAD_KEY.toName()
              rankIndexBlob = newPublicBlob().copy { clearBlobChecksum() }
              requestId = REQUEST_ID
            }
          )
        }
      assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
      assertThat(exception.errorInfo)
        .isEqualTo(
          errorInfo {
            domain = Errors.DOMAIN
            reason = Errors.Reason.REQUIRED_FIELD_NOT_SET.name
            metadata[Errors.Metadata.FIELD_NAME.key] = "rank_index_blob.blob_checksum"
          }
        )
    }

  @Test
  fun `createRankIndexBlob throws INVALID_ARGUMENT when max_event_date missing`() =
    runBlocking<Unit> {
      val exception =
        assertFailsWith<StatusRuntimeException> {
          service.createRankIndexBlob(
            createRankIndexBlobRequest {
              parent = UPLOAD_KEY.toName()
              rankIndexBlob = newPublicBlob().copy { clearMaxEventDate() }
              requestId = REQUEST_ID
            }
          )
        }
      assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
      assertThat(exception.errorInfo)
        .isEqualTo(
          errorInfo {
            domain = Errors.DOMAIN
            reason = Errors.Reason.REQUIRED_FIELD_NOT_SET.name
            metadata[Errors.Metadata.FIELD_NAME.key] = "rank_index_blob.max_event_date"
          }
        )
    }

  @Test
  fun `createRankIndexBlob throws NOT_FOUND when parent upload missing`() =
    runBlocking<Unit> {
      val exception =
        assertFailsWith<StatusRuntimeException> {
          service.createRankIndexBlob(
            createRankIndexBlobRequest {
              parent = UPLOAD_KEY.toName()
              rankIndexBlob = newPublicBlob()
              requestId = REQUEST_ID
            }
          )
        }
      assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
    }

  @Test
  fun `batchCreateRankIndexBlobs returns blobs`() =
    runBlocking<Unit> {
      createParentUpload(DATA_PROVIDER_ID, RAW_IMPRESSION_UPLOAD_ID)

      val response =
        service.batchCreateRankIndexBlobs(
          batchCreateRankIndexBlobsRequest {
            parent = UPLOAD_KEY.toName()
            requests += createRankIndexBlobRequest {
              rankIndexBlob = newPublicBlob(poolOffset = 0L)
              requestId = UUID.randomUUID().toString()
            }
            requests += createRankIndexBlobRequest {
              rankIndexBlob = newPublicBlob(poolOffset = 1L)
              requestId = UUID.randomUUID().toString()
            }
          }
        )

      assertThat(response.rankIndexBlobsList).hasSize(2)
      assertThat(response.rankIndexBlobsList.map { it.poolOffset }).containsExactly(0L, 1L)
    }

  @Test
  fun `batchCreateRankIndexBlobs throws INVALID_ARGUMENT for empty request_id`() =
    runBlocking<Unit> {
      createParentUpload(DATA_PROVIDER_ID, RAW_IMPRESSION_UPLOAD_ID)

      val exception =
        assertFailsWith<StatusRuntimeException> {
          service.batchCreateRankIndexBlobs(
            batchCreateRankIndexBlobsRequest {
              parent = UPLOAD_KEY.toName()
              requests += createRankIndexBlobRequest { rankIndexBlob = newPublicBlob() }
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
  fun `batchCreateRankIndexBlobs throws INVALID_ARGUMENT when over batch size`() =
    runBlocking<Unit> {
      val exception =
        assertFailsWith<StatusRuntimeException> {
          service.batchCreateRankIndexBlobs(
            batchCreateRankIndexBlobsRequest {
              parent = UPLOAD_KEY.toName()
              for (i in 0 until 51) {
                requests += createRankIndexBlobRequest {
                  rankIndexBlob = newPublicBlob(poolOffset = i.toLong())
                  requestId = UUID.randomUUID().toString()
                }
              }
            }
          )
        }
      assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    }

  @Test
  fun `getRankIndexBlob returns a blob`() =
    runBlocking<Unit> {
      createParentUpload(DATA_PROVIDER_ID, RAW_IMPRESSION_UPLOAD_ID)
      val created = createBlob()

      val blob = service.getRankIndexBlob(getRankIndexBlobRequest { name = created.name })

      assertThat(blob).isEqualTo(created)
    }

  @Test
  fun `getRankIndexBlob throws NOT_FOUND when not found`() =
    runBlocking<Unit> {
      createParentUpload(DATA_PROVIDER_ID, RAW_IMPRESSION_UPLOAD_ID)
      val name =
        RankIndexBlobKey(DATA_PROVIDER_ID, RAW_IMPRESSION_UPLOAD_ID, "missing-blob").toName()

      val exception =
        assertFailsWith<StatusRuntimeException> {
          service.getRankIndexBlob(getRankIndexBlobRequest { this.name = name })
        }
      assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
    }

  @Test
  fun `getRankIndexBlob throws INVALID_ARGUMENT for malformed name`() =
    runBlocking<Unit> {
      val exception =
        assertFailsWith<StatusRuntimeException> {
          service.getRankIndexBlob(getRankIndexBlobRequest { name = "not/a/valid/name" })
        }
      assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    }

  @Test
  fun `listRankIndexBlobs returns blobs`() =
    runBlocking<Unit> {
      createParentUpload(DATA_PROVIDER_ID, RAW_IMPRESSION_UPLOAD_ID)
      createBlob(poolOffset = 0L)
      createBlob(poolOffset = 1L)

      val response =
        service.listRankIndexBlobs(listRankIndexBlobsRequest { parent = UPLOAD_KEY.toName() })

      assertThat(response.rankIndexBlobsList).hasSize(2)
    }

  @Test
  fun `listRankIndexBlobs filters by blob_type`() =
    runBlocking<Unit> {
      createParentUpload(DATA_PROVIDER_ID, RAW_IMPRESSION_UPLOAD_ID)
      createBlob(poolOffset = 0L, blobType = RankIndexBlob.BlobType.SNAPSHOT)
      createBlob(poolOffset = 1L, blobType = RankIndexBlob.BlobType.DAY_ONLY)

      val response =
        service.listRankIndexBlobs(
          listRankIndexBlobsRequest {
            parent = UPLOAD_KEY.toName()
            filter =
              ListRankIndexBlobsRequestKt.filter { blobType = RankIndexBlob.BlobType.DAY_ONLY }
          }
        )

      assertThat(response.rankIndexBlobsList).hasSize(1)
      assertThat(response.rankIndexBlobsList.first().blobType)
        .isEqualTo(RankIndexBlob.BlobType.DAY_ONLY)
    }

  @Test
  fun `listRankIndexBlobs lists across uploads with wildcard parent`() =
    runBlocking<Unit> {
      createParentUpload(DATA_PROVIDER_ID, RAW_IMPRESSION_UPLOAD_ID)
      createBlob()
      createParentUpload(DATA_PROVIDER_ID, SECOND_UPLOAD_ID)
      service.createRankIndexBlob(
        createRankIndexBlobRequest {
          parent = RawImpressionUploadKey(DATA_PROVIDER_ID, SECOND_UPLOAD_ID).toName()
          rankIndexBlob = newPublicBlob(poolOffset = 1L)
          requestId = UUID.randomUUID().toString()
        }
      )

      val response =
        service.listRankIndexBlobs(
          listRankIndexBlobsRequest {
            parent = RawImpressionUploadKey(DATA_PROVIDER_ID, ResourceKey.WILDCARD_ID).toName()
          }
        )

      assertThat(response.rankIndexBlobsList).hasSize(2)
    }

  @Test
  fun `listRankIndexBlobs with show_deleted returns active and deleted`() =
    runBlocking<Unit> {
      createParentUpload(DATA_PROVIDER_ID, RAW_IMPRESSION_UPLOAD_ID)
      val kept = createBlob(poolOffset = 0L)
      val deleted = createBlob(poolOffset = 1L)
      service.deleteRankIndexBlob(deleteRankIndexBlobRequest { name = deleted.name })

      val response =
        service.listRankIndexBlobs(
          listRankIndexBlobsRequest {
            parent = UPLOAD_KEY.toName()
            showDeleted = true
          }
        )

      assertThat(response.rankIndexBlobsList.map { it.name })
        .containsExactly(kept.name, deleted.name)
    }

  @Test
  fun `deleteRankIndexBlob soft-deletes`() =
    runBlocking<Unit> {
      createParentUpload(DATA_PROVIDER_ID, RAW_IMPRESSION_UPLOAD_ID)
      val created = createBlob()

      val deleted = service.deleteRankIndexBlob(deleteRankIndexBlobRequest { name = created.name })

      assertThat(deleted.name).isEqualTo(created.name)
      assertThat(deleted.hasDeleteTime()).isTrue()
    }

  @Test
  fun `deleteRankIndexBlob throws NOT_FOUND when not found`() =
    runBlocking<Unit> {
      createParentUpload(DATA_PROVIDER_ID, RAW_IMPRESSION_UPLOAD_ID)
      val name =
        RankIndexBlobKey(DATA_PROVIDER_ID, RAW_IMPRESSION_UPLOAD_ID, "missing-blob").toName()

      val exception =
        assertFailsWith<StatusRuntimeException> {
          service.deleteRankIndexBlob(deleteRankIndexBlobRequest { this.name = name })
        }
      assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
    }

  @Test
  fun `batchDeleteRankIndexBlobs deletes multiple`() =
    runBlocking<Unit> {
      createParentUpload(DATA_PROVIDER_ID, RAW_IMPRESSION_UPLOAD_ID)
      val blob1 = createBlob(poolOffset = 0L)
      val blob2 = createBlob(poolOffset = 1L)

      val response =
        service.batchDeleteRankIndexBlobs(
          batchDeleteRankIndexBlobsRequest {
            parent = UPLOAD_KEY.toName()
            requests += deleteRankIndexBlobRequest { name = blob1.name }
            requests += deleteRankIndexBlobRequest { name = blob2.name }
          }
        )

      assertThat(response.rankIndexBlobsList.map { it.name })
        .containsExactly(blob1.name, blob2.name)
      assertThat(response.rankIndexBlobsList.all { it.hasDeleteTime() }).isTrue()
    }

  @Test
  fun `batchDeleteRankIndexBlobs throws INVALID_ARGUMENT for duplicate name`() =
    runBlocking<Unit> {
      createParentUpload(DATA_PROVIDER_ID, RAW_IMPRESSION_UPLOAD_ID)
      val blob1 = createBlob()

      val exception =
        assertFailsWith<StatusRuntimeException> {
          service.batchDeleteRankIndexBlobs(
            batchDeleteRankIndexBlobsRequest {
              parent = UPLOAD_KEY.toName()
              requests += deleteRankIndexBlobRequest { name = blob1.name }
              requests += deleteRankIndexBlobRequest { name = blob1.name }
            }
          )
        }
      assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    }

  @Test
  fun `batchCreateRankIndexBlobs throws INVALID_ARGUMENT when request parent does not match`() =
    runBlocking<Unit> {
      createParentUpload(DATA_PROVIDER_ID, RAW_IMPRESSION_UPLOAD_ID)

      val exception =
        assertFailsWith<StatusRuntimeException> {
          service.batchCreateRankIndexBlobs(
            batchCreateRankIndexBlobsRequest {
              parent = UPLOAD_KEY.toName()
              requests += createRankIndexBlobRequest {
                parent = RawImpressionUploadKey(DATA_PROVIDER_ID, SECOND_UPLOAD_ID).toName()
                rankIndexBlob = newPublicBlob()
                requestId = UUID.randomUUID().toString()
              }
            }
          )
        }
      assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    }

  @Test
  fun `batchDeleteRankIndexBlobs throws INVALID_ARGUMENT when name does not match parent`() =
    runBlocking<Unit> {
      createParentUpload(DATA_PROVIDER_ID, RAW_IMPRESSION_UPLOAD_ID)
      val blob = createBlob()
      val otherName = RankIndexBlobKey(DATA_PROVIDER_ID, SECOND_UPLOAD_ID, "other-blob").toName()

      val exception =
        assertFailsWith<StatusRuntimeException> {
          service.batchDeleteRankIndexBlobs(
            batchDeleteRankIndexBlobsRequest {
              parent = UPLOAD_KEY.toName()
              requests += deleteRankIndexBlobRequest { name = blob.name }
              requests += deleteRankIndexBlobRequest { name = otherName }
            }
          )
        }
      assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    }

  @Test
  fun `listRankIndexBlobs throws INVALID_ARGUMENT for negative page_size`() =
    runBlocking<Unit> {
      createParentUpload(DATA_PROVIDER_ID, RAW_IMPRESSION_UPLOAD_ID)

      val exception =
        assertFailsWith<StatusRuntimeException> {
          service.listRankIndexBlobs(
            listRankIndexBlobsRequest {
              parent = UPLOAD_KEY.toName()
              pageSize = -1
            }
          )
        }
      assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    }

  companion object {
    @get:ClassRule @JvmStatic val spannerEmulator = SpannerEmulatorRule()

    private const val DATA_PROVIDER_ID = "dp1"
    private const val RAW_IMPRESSION_UPLOAD_ID = "upload1"
    private const val SECOND_UPLOAD_ID = "upload2"
    private const val CMMS_MODEL_LINE = "modelProviders/mp1/modelSuites/ms1/modelLines/ml1"
    private const val BLOB_URI = "gs://bucket/rank-index-blob"
    private val REQUEST_ID = UUID.randomUUID().toString()
    private val UPLOAD_KEY = RawImpressionUploadKey(DATA_PROVIDER_ID, RAW_IMPRESSION_UPLOAD_ID)
    private val BLOB_CHECKSUM = "checksum".toByteStringUtf8()
    private val ENCRYPTED_DEK: EncryptedDek = encryptedDek {
      kekUri = "kms://kek"
      ciphertext = "ciphertext".toByteStringUtf8()
    }
    private val MAX_EVENT_DATE: Date = date {
      year = 2026
      month = 3
      day = 15
    }
  }
}
