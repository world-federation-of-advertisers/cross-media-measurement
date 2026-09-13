/*
 * Copyright 2026 The Cross-Media Measurement Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner

import com.google.cloud.Timestamp
import com.google.cloud.spanner.CommitResponse
import com.google.cloud.spanner.Struct
import com.google.cloud.spanner.Type
import com.google.common.truth.Truth.assertThat
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.runBlocking
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.mockito.kotlin.any
import org.mockito.kotlin.mock
import org.mockito.kotlin.whenever
import org.wfanet.measurement.gcloud.spanner.AsyncDatabaseClient
import org.wfanet.measurement.gcloud.spanner.TransactionWork
import org.wfanet.measurement.internal.edpaggregator.ImpressionMetadataState as State
import org.wfanet.measurement.internal.edpaggregator.batchUndeleteImpressionMetadataRequest
import org.wfanet.measurement.internal.edpaggregator.undeleteImpressionMetadataRequest

@RunWith(JUnit4::class)
class SpannerImpressionMetadataServiceTransactionRetryTest {
  @Test
  fun `batchUndeleteImpressionMetadata does not duplicate results after transaction retry`() =
    runBlocking {
      val rows =
        listOf(
          deletedImpressionMetadataRow(1L, "metadata-1"),
          deletedImpressionMetadataRow(2L, "metadata-2"),
        )
      val transactionContext = mock<AsyncDatabaseClient.TransactionContext>()
      whenever(transactionContext.executeQuery(any(), any())).thenReturn(rows.asFlow())
      val transactionRunner = RetryingTransactionRunner(transactionContext)
      val databaseClient = mock<AsyncDatabaseClient>()
      whenever(databaseClient.readWriteTransaction(any())).thenReturn(transactionRunner)
      val service = SpannerImpressionMetadataService(databaseClient)

      val response =
        service.batchUndeleteImpressionMetadata(
          batchUndeleteImpressionMetadataRequest {
            requests +=
              listOf("metadata-1", "metadata-2").map { resourceId ->
                undeleteImpressionMetadataRequest {
                  dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
                  impressionMetadataResourceId = resourceId
                }
              }
          }
        )

      assertThat(transactionRunner.attempts).isEqualTo(2)
      assertThat(response.impressionMetadataList.map { it.impressionMetadataResourceId })
        .containsExactly("metadata-1", "metadata-2")
        .inOrder()
    }

  private class RetryingTransactionRunner(
    private val transactionContext: AsyncDatabaseClient.TransactionContext
  ) : AsyncDatabaseClient.TransactionRunner {
    var attempts = 0
      private set

    override suspend fun <R> run(doWork: TransactionWork<R>): R {
      attempts++
      doWork(transactionContext)
      attempts++
      return doWork(transactionContext)
    }

    override suspend fun getCommitTimestamp(): Timestamp = COMMIT_TIMESTAMP

    override suspend fun getCommitResponse(): CommitResponse = error("Not used")

    override fun close() {}
  }

  companion object {
    private const val DATA_PROVIDER_RESOURCE_ID = "data-provider-1"
    private val COMMIT_TIMESTAMP = Timestamp.ofTimeSecondsAndNanos(1234L, 0)
    private val ENTITY_KEY_TYPE =
      Type.struct(
        Type.StructField.of("EntityType", Type.string()),
        Type.StructField.of("EntityId", Type.string()),
      )

    private fun deletedImpressionMetadataRow(internalId: Long, resourceId: String): Struct =
      Struct.newBuilder()
        .set("DataProviderResourceId")
        .to(DATA_PROVIDER_RESOURCE_ID)
        .set("ImpressionMetadataId")
        .to(internalId)
        .set("ImpressionMetadataResourceId")
        .to(resourceId)
        .set("BlobUri")
        .to("gs://bucket/$resourceId")
        .set("BlobTypeUrl")
        .to("type.googleapis.com/example.Impression")
        .set("EventGroupReferenceId")
        .to("event-group-1")
        .set("CmmsModelLine")
        .to("modelLines/1")
        .set("IntervalStartTime")
        .to(COMMIT_TIMESTAMP)
        .set("IntervalEndTime")
        .to(COMMIT_TIMESTAMP)
        .set("State")
        .to(State.IMPRESSION_METADATA_STATE_DELETED)
        .set("CreateTime")
        .to(COMMIT_TIMESTAMP)
        .set("UpdateTime")
        .to(COMMIT_TIMESTAMP)
        .set("EntityKeys")
        .toStructArray(ENTITY_KEY_TYPE, emptyList())
        .build()
  }
}
