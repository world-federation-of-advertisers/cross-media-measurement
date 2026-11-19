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

package org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner

import com.google.cloud.spanner.Mutation
import com.google.cloud.spanner.Value
import com.google.common.truth.Truth.assertThat
import com.google.common.truth.extensions.proto.ProtoTruth.assertThat
import java.util.UUID
import kotlinx.coroutines.flow.single
import kotlinx.coroutines.runBlocking
import org.junit.ClassRule
import org.junit.Rule
import org.junit.Test
import org.wfanet.measurement.common.IdGenerator
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.testing.Schemata
import org.wfanet.measurement.edpaggregator.service.internal.testing.PoolAssignmentJobServiceTest
import org.wfanet.measurement.gcloud.common.toGcloudTimestamp
import org.wfanet.measurement.gcloud.spanner.AsyncDatabaseClient
import org.wfanet.measurement.gcloud.spanner.getProtoMessage
import org.wfanet.measurement.gcloud.spanner.statement
import org.wfanet.measurement.gcloud.spanner.testing.SpannerEmulatorDatabaseRule
import org.wfanet.measurement.gcloud.spanner.testing.SpannerEmulatorRule
import org.wfanet.measurement.internal.edpaggregator.EncryptedDek
import org.wfanet.measurement.internal.edpaggregator.PoolAssignmentJobServiceGrpcKt
import org.wfanet.measurement.internal.edpaggregator.RawImpressionUploadModelLineState
import org.wfanet.measurement.internal.edpaggregator.RawImpressionUploadState
import org.wfanet.measurement.internal.edpaggregator.batchCreatePoolAssignmentJobsRequest
import org.wfanet.measurement.internal.edpaggregator.createPoolAssignmentJobRequest
import org.wfanet.measurement.internal.edpaggregator.encryptedDek
import org.wfanet.measurement.internal.edpaggregator.getRawImpressionUploadModelLineRequest
import org.wfanet.measurement.internal.edpaggregator.markPoolAssignmentJobSucceededRequest
import org.wfanet.measurement.internal.edpaggregator.poolAssignmentJob

class SpannerPoolAssignmentJobServiceTest : PoolAssignmentJobServiceTest() {
  @get:Rule
  val spannerDatabase =
    SpannerEmulatorDatabaseRule(spannerEmulator, Schemata.EDP_AGGREGATOR_CHANGELOG_PATH)

  private var nextUploadId: Long = 1L
  private var nextModelLineId: Long = 1L

  override fun newService(
    idGenerator: IdGenerator
  ): PoolAssignmentJobServiceGrpcKt.PoolAssignmentJobServiceCoroutineImplBase {
    val databaseClient: AsyncDatabaseClient = spannerDatabase.databaseClient
    return SpannerPoolAssignmentJobService(databaseClient, idGenerator = idGenerator)
  }

  override suspend fun createParentUpload(
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

  override suspend fun createRawImpressionUploadModelLine(
    dataProviderResourceId: String,
    rawImpressionUploadResourceId: String,
    cmmsModelLine: String,
  ) {
    val databaseClient: AsyncDatabaseClient = spannerDatabase.databaseClient
    val uploadId: Long =
      databaseClient.singleUse().use { txn ->
        txn
          .executeQuery(
            statement(
              """
              SELECT RawImpressionUploadId
              FROM RawImpressionUpload
              WHERE DataProviderResourceId = @dataProviderResourceId
                AND RawImpressionUploadResourceId = @rawImpressionUploadResourceId
              """
                .trimIndent()
            ) {
              bind("dataProviderResourceId").to(dataProviderResourceId)
              bind("rawImpressionUploadResourceId").to(rawImpressionUploadResourceId)
            }
          )
          .single()
          .getLong("RawImpressionUploadId")
      }

    val mutation =
      Mutation.newInsertBuilder("RawImpressionUploadModelLine")
        .set("DataProviderResourceId")
        .to(dataProviderResourceId)
        .set("RawImpressionUploadId")
        .to(uploadId)
        .set("RawImpressionUploadModelLineId")
        .to(nextModelLineId++)
        .set("RawImpressionUploadModelLineResourceId")
        .to("ml-${UUID.randomUUID()}")
        .set("CmmsModelLine")
        .to(cmmsModelLine)
        .set("State")
        .to(
          Value.protoEnum(
            RawImpressionUploadModelLineState.RAW_IMPRESSION_UPLOAD_MODEL_LINE_STATE_POOL_ASSIGNING
          )
        )
        .set("PoolOffsets")
        .toInt64Array(emptyList())
        .set("CreateTime")
        .to(Value.COMMIT_TIMESTAMP)
        .set("UpdateTime")
        .to(Value.COMMIT_TIMESTAMP)
        .build()
    databaseClient.write(listOf(mutation))
  }

  override suspend fun getRawImpressionUploadModelLineMergedDek(
    dataProviderResourceId: String,
    rawImpressionUploadResourceId: String,
    cmmsModelLine: String,
  ): EncryptedDek? {
    return spannerDatabase.databaseClient.singleUse().use { txn ->
      val row =
        txn
          .executeQuery(
            statement(
              """
              SELECT ModelLine.EncryptedMergedDek AS EncryptedMergedDek
              FROM RawImpressionUploadModelLine AS ModelLine
              JOIN RawImpressionUpload AS Upload
                USING (DataProviderResourceId, RawImpressionUploadId)
              WHERE ModelLine.DataProviderResourceId = @dataProviderResourceId
                AND Upload.RawImpressionUploadResourceId = @rawImpressionUploadResourceId
                AND ModelLine.CmmsModelLine = @cmmsModelLine
              """
                .trimIndent()
            ) {
              bind("dataProviderResourceId").to(dataProviderResourceId)
              bind("rawImpressionUploadResourceId").to(rawImpressionUploadResourceId)
              bind("cmmsModelLine").to(cmmsModelLine)
            }
          )
          .single()
      if (row.isNull("EncryptedMergedDek")) {
        null
      } else {
        row.getProtoMessage("EncryptedMergedDek", EncryptedDek.getDefaultInstance())
      }
    }
  }

  @Test
  fun `markPoolAssignmentJobSucceeded persists completion without merged encryption`() =
    runBlocking {
      val provider = "keyless-completion"
      val upload = "empty"
      val model = "modelProviders/mp/modelSuites/ms/modelLines/ml"
      createParentUpload(provider, upload)
      createRawImpressionUploadModelLine(provider, upload, model)
      val api = newService(IdGenerator.Default)
      val shard =
        api.createPoolAssignmentJob(
          createPoolAssignmentJobRequest {
            requestId = UUID.randomUUID().toString()
            poolAssignmentJob = poolAssignmentJob {
              dataProviderResourceId = provider
              rawImpressionUploadResourceId = upload
              cmmsModelLine = model
              shardIndex = 0
            }
          }
        )
      api.markPoolAssignmentJobSucceeded(
        markPoolAssignmentJobSucceededRequest {
          requestId = UUID.randomUUID().toString()
          dataProviderResourceId = provider
          rawImpressionUploadResourceId = upload
          poolAssignmentJobResourceId = shard.poolAssignmentJobResourceId
          etag = shard.etag
        }
      )
      val resourceId =
        spannerDatabase.databaseClient.singleUse().use { txn ->
          txn
            .executeQuery(
              statement(
                """
        SELECT RawImpressionUploadModelLineResourceId
        FROM RawImpressionUploadModelLine
        WHERE DataProviderResourceId = @provider AND CmmsModelLine = @model
        """
                  .trimIndent()
              ) {
                bind("provider").to(provider)
                bind("model").to(model)
              }
            )
            .single()
            .getString("RawImpressionUploadModelLineResourceId")
        }
      val parent =
        SpannerRawImpressionUploadModelLineService(spannerDatabase.databaseClient)
          .getRawImpressionUploadModelLine(
            getRawImpressionUploadModelLineRequest {
              dataProviderResourceId = provider
              rawImpressionUploadResourceId = upload
              rawImpressionUploadModelLineResourceId = resourceId
            }
          )
      assertThat(parent.poolAssignmentComplete).isTrue()
      assertThat(parent.hasEncryptedMergedDek()).isFalse()
      assertThat(parent.poolOffsetsList).isEmpty()
      assertThat(parent.hasMaxEventDate()).isFalse()
    }

  @Test
  fun `markPoolAssignmentJobSucceeded replays stored decisions with tied update timestamps`() =
    runBlocking {
      val provider = "tied-completion"
      val upload = "two-shards"
      val model = "modelProviders/mp/modelSuites/ms/modelLines/ml"
      createParentUpload(provider, upload)
      createRawImpressionUploadModelLine(provider, upload, model)
      val api = newService(IdGenerator.Default)
      val shards =
        (0..1).map { index ->
          api.createPoolAssignmentJob(
            createPoolAssignmentJobRequest {
              requestId = UUID.randomUUID().toString()
              poolAssignmentJob = poolAssignmentJob {
                dataProviderResourceId = provider
                rawImpressionUploadResourceId = upload
                cmmsModelLine = model
                shardIndex = index
              }
            }
          )
        }
      val requests =
        shards.map { shard ->
          markPoolAssignmentJobSucceededRequest {
            requestId = UUID.randomUUID().toString()
            dataProviderResourceId = provider
            rawImpressionUploadResourceId = upload
            poolAssignmentJobResourceId = shard.poolAssignmentJobResourceId
            etag = shard.etag
          }
        }
      val early = api.markPoolAssignmentJobSucceeded(requests[0])
      val final = api.markPoolAssignmentJobSucceeded(requests[1])
      assertThat(early.lastShard).isFalse()
      assertThat(final.lastShard).isTrue()
      val ids =
        spannerDatabase.databaseClient.singleUse().use { txn ->
          txn
            .executeQuery(
              statement(
                """
        SELECT RawImpressionUploadId, PoolAssignmentJobId
        FROM PoolAssignmentJob
        WHERE DataProviderResourceId = @provider
          AND PoolAssignmentJobResourceId = @job
        """
                  .trimIndent()
              ) {
                bind("provider").to(provider)
                bind("job").to(shards[0].poolAssignmentJobResourceId)
              }
            )
            .single()
        }
      // Defensive stored-state fixture: overlapping production commits have unique timestamps.
      spannerDatabase.databaseClient.write(
        listOf(
          Mutation.newUpdateBuilder("PoolAssignmentJob")
            .set("DataProviderResourceId")
            .to(provider)
            .set("RawImpressionUploadId")
            .to(ids.getLong("RawImpressionUploadId"))
            .set("PoolAssignmentJobId")
            .to(ids.getLong("PoolAssignmentJobId"))
            .set("UpdateTime")
            .to(final.poolAssignmentJob.updateTime.toGcloudTimestamp())
            .build()
        )
      )
      assertThat(api.markPoolAssignmentJobSucceeded(requests[0]).lastShard).isFalse()
      assertThat(api.markPoolAssignmentJobSucceeded(requests[1])).isEqualTo(final)
    }

  @Test
  fun `getRawImpressionUploadModelLine materializes legacy completion and preserves replay`() =
    runBlocking {
      verifyLegacyCompletion("get")
    }

  @Test
  fun `createPoolAssignmentJob retains legacy finalizer before sibling creation`() = runBlocking {
    verifyLegacyCompletion("create")
  }

  @Test
  fun `batchCreatePoolAssignmentJobs retains legacy finalizer before sibling creation`() =
    runBlocking {
      verifyLegacyCompletion("batch")
    }

  private suspend fun verifyLegacyCompletion(accessFirst: String) {
    val provider = "legacy-completion"
    val upload = "empty"
    val model = "modelProviders/mp/modelSuites/ms/modelLines/ml"
    createParentUpload(provider, upload)
    createRawImpressionUploadModelLine(provider, upload, model)
    val api = newService(IdGenerator.Default)
    suspend fun createShard(index: Int) =
      api.createPoolAssignmentJob(
        createPoolAssignmentJobRequest {
          requestId = UUID.randomUUID().toString()
          poolAssignmentJob = poolAssignmentJob {
            dataProviderResourceId = provider
            rawImpressionUploadResourceId = upload
            cmmsModelLine = model
            shardIndex = index
          }
        }
      )
    val dek = encryptedDek { kekUri = "fake-kms://legacy-output" }
    val requests =
      (0..1).map { index ->
        val shard = createShard(index)
        markPoolAssignmentJobSucceededRequest {
          requestId = UUID.randomUUID().toString()
          dataProviderResourceId = provider
          rawImpressionUploadResourceId = upload
          poolAssignmentJobResourceId = shard.poolAssignmentJobResourceId
          etag = shard.etag
          encryptedDek = dek
        }
      }
    val original = requests.map { api.markPoolAssignmentJobSucceeded(it) }
    spannerDatabase.databaseClient.readWriteTransaction().run { txn ->
      txn.executeUpdate(
        statement(
          """
        UPDATE PoolAssignmentJob
        SET LastShard = NULL, LastShardPoolOffsets = NULL, LastShardMaxEventDate = NULL
        WHERE DataProviderResourceId = @provider
        """
            .trimIndent()
        ) {
          bind("provider").to(provider)
        }
      )
      txn.executeUpdate(
        statement(
          """
        UPDATE RawImpressionUploadModelLine
        SET PoolAssignmentComplete = FALSE, EncryptedMergedDek = @dek
        WHERE DataProviderResourceId = @provider
        """
            .trimIndent()
        ) {
          bind("provider").to(provider)
          bind("dek").to(Value.protoMessage(dek))
        }
      )
    }
    val resourceId =
      spannerDatabase.databaseClient.singleUse().use { txn ->
        txn
          .executeQuery(
            statement(
              "SELECT RawImpressionUploadModelLineResourceId FROM RawImpressionUploadModelLine " +
                "WHERE DataProviderResourceId = @provider AND CmmsModelLine = @model"
            ) {
              bind("provider").to(provider)
              bind("model").to(model)
            }
          )
          .single()
          .getString("RawImpressionUploadModelLineResourceId")
      }
    when (accessFirst) {
      "create" -> createShard(2)
      "batch" ->
        api.batchCreatePoolAssignmentJobs(
          batchCreatePoolAssignmentJobsRequest {
            dataProviderResourceId = provider
            rawImpressionUploadResourceId = upload
            this.requests +=
              (2..3).map { index ->
                createPoolAssignmentJobRequest {
                  requestId = UUID.randomUUID().toString()
                  poolAssignmentJob = poolAssignmentJob {
                    cmmsModelLine = model
                    shardIndex = index
                  }
                }
              }
          }
        )
      "get" -> {}
      else -> error("Unknown first access")
    }
    val normalized =
      SpannerRawImpressionUploadModelLineService(spannerDatabase.databaseClient)
        .getRawImpressionUploadModelLine(
          getRawImpressionUploadModelLineRequest {
            dataProviderResourceId = provider
            rawImpressionUploadResourceId = upload
            rawImpressionUploadModelLineResourceId = resourceId
          }
        )
    assertThat(normalized.poolAssignmentComplete).isTrue()
    assertThat(normalized.hasEncryptedMergedDek()).isFalse()
    assertThat(getRawImpressionUploadModelLineMergedDek(provider, upload, model)).isNull()
    val parent =
      spannerDatabase.databaseClient.singleUse().use { txn ->
        txn
          .executeQuery(
            statement(
              """
        SELECT PoolAssignmentComplete
        FROM RawImpressionUploadModelLine WHERE DataProviderResourceId = @provider
        """
                .trimIndent()
            ) {
              bind("provider").to(provider)
            }
          )
          .single()
      }
    assertThat(parent.getBoolean("PoolAssignmentComplete")).isTrue()
    if (accessFirst == "get") createShard(2)
    for (index in requests.indices) {
      assertThat(api.markPoolAssignmentJobSucceeded(requests[index])).isEqualTo(original[index])
    }
  }

  companion object {
    @get:ClassRule @JvmStatic val spannerEmulator = SpannerEmulatorRule()
  }
}
