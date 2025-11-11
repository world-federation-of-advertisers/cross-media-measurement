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

package org.wfanet.panelmatch.client.exchangetasks

import com.google.cloud.bigquery.BigQuery
import com.google.cloud.bigquery.BigQueryError
import com.google.cloud.bigquery.BigQueryException
import com.google.cloud.bigquery.InsertAllRequest
import com.google.cloud.bigquery.InsertAllResponse
import com.google.common.truth.Truth.assertThat
import com.google.common.truth.extensions.proto.ProtoTruth.assertThat
import com.google.protobuf.ByteString
import com.google.protobuf.kotlin.toByteStringUtf8
import java.time.LocalDate
import java.util.Base64
import kotlin.test.assertFailsWith
import kotlinx.coroutines.flow.single
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.mockito.kotlin.any
import org.mockito.kotlin.argumentCaptor
import org.mockito.kotlin.mock
import org.mockito.kotlin.never
import org.mockito.kotlin.times
import org.mockito.kotlin.verify
import org.mockito.kotlin.whenever
import org.wfanet.measurement.storage.StorageClient
import org.wfanet.measurement.storage.testing.InMemoryStorageClient
import org.wfanet.panelmatch.client.authorizedview.BigQueryServiceFactory
import org.wfanet.panelmatch.client.authorizedview.BigQueryStreamingStatus
import org.wfanet.panelmatch.client.authorizedview.encryptedMatchedEvent
import org.wfanet.panelmatch.common.Fingerprinters.sha256
import org.wfanet.panelmatch.common.storage.toByteString
import org.wfanet.panelmatch.common.testing.runBlockingTest

private const val PROJECT_ID = "test-project"
private const val DATASET_ID = "test-dataset"
private const val TABLE_ID = "test-table"
private val EXCHANGE_DATE = LocalDate.of(2025, 1, 15)

@RunWith(JUnit4::class)
class WriteToBigQueryTaskTest {
  private val mockBigQuery: BigQuery = mock()
  private val mockBigQueryServiceFactory: BigQueryServiceFactory = mock()
  private val mockInsertAllResponse: InsertAllResponse = mock()
  private val storageClient = InMemoryStorageClient()

  @Before
  fun setUp() {
    whenever(mockBigQueryServiceFactory.getService(any())).thenReturn(mockBigQuery)
  }

  @Test
  fun `forJoinKeys creates task with correct configuration`() = runBlockingTest {
    // Given
    val task =
      WriteToBigQueryTask.forJoinKeys(
        projectId = PROJECT_ID,
        datasetId = DATASET_ID,
        tableId = TABLE_ID,
        exchangeDate = EXCHANGE_DATE,
        bigQueryServiceFactory = mockBigQueryServiceFactory,
      )

    val joinKeyAndIdCollection = joinKeyAndIdCollection {
      joinKeyAndIds += joinKeyAndId {
        joinKey = joinKey { key = "test-key".toByteStringUtf8() }
        joinKeyIdentifier = joinKeyIdentifier { id = ByteString.copyFromUtf8("1") }
      }
    }
    val inputBlob = createTestBlob("input", joinKeyAndIdCollection.toByteString())

    setupMockBigQueryResponse(hasErrors = false)

    // When
    val result = task.execute(mapOf("input" to inputBlob))

    // Then - verify task executes successfully
    assertThat(result).containsKey("status")
    val status = BigQueryStreamingStatus.parseFrom(result["status"]!!.single().toByteArray())
    assertThat(status.dataType).isEqualTo(BigQueryStreamingStatus.DataType.JOIN_KEYS)
  }

  @Test
  fun `forEncryptedEvents creates task with correct configuration`() = runBlockingTest {
    // Given
    val task =
      WriteToBigQueryTask.forEncryptedEvents(
        projectId = PROJECT_ID,
        datasetId = DATASET_ID,
        tableId = TABLE_ID,
        exchangeDate = EXCHANGE_DATE,
        bigQueryServiceFactory = mockBigQueryServiceFactory,
      )

    val encryptedEvent = encryptedMatchedEvent {
      encryptedJoinKey = ByteString.copyFromUtf8("encrypted-key")
      encryptedEventData = ByteString.copyFromUtf8("encrypted-data")
    }

    val delimitedBytes =
      ByteString.newOutput().use { output ->
        encryptedEvent.writeDelimitedTo(output)
        output.toByteString()
      }

    val inputBlob = createTestBlob("input", delimitedBytes)

    setupMockBigQueryResponse(hasErrors = false)

    // When
    val result = task.execute(mapOf("input" to inputBlob))

    // Then - verify task executes successfully
    assertThat(result).containsKey("status")
    val status = BigQueryStreamingStatus.parseFrom(result["status"]!!.single().toByteArray())
    assertThat(status.dataType).isEqualTo(BigQueryStreamingStatus.DataType.ENCRYPTED_EVENTS)
  }

  @Test
  fun `task uses custom column names when specified`() = runBlockingTest {
    // Given
    val customKeyColumn = "custom_key"
    val customDataColumn = "custom_data"
    val customDateColumn = "custom_date"

    val task =
      WriteToBigQueryTask.forEncryptedEvents(
        projectId = PROJECT_ID,
        datasetId = DATASET_ID,
        tableId = TABLE_ID,
        exchangeDate = EXCHANGE_DATE,
        bigQueryServiceFactory = mockBigQueryServiceFactory,
        keyColumnName = customKeyColumn,
        dataColumnName = customDataColumn,
        dateColumnName = customDateColumn,
      )

    val encryptedEvent = encryptedMatchedEvent {
      encryptedJoinKey = ByteString.copyFromUtf8("key")
      encryptedEventData = ByteString.copyFromUtf8("data")
    }

    val delimitedBytes =
      ByteString.newOutput().use { output ->
        encryptedEvent.writeDelimitedTo(output)
        output.toByteString()
      }

    val inputBlob = createTestBlob("input", delimitedBytes)
    setupMockBigQueryResponse(hasErrors = false)

    // When
    task.execute(mapOf("input" to inputBlob))

    // Then - verify insert request uses custom column names
    val requestCaptor = argumentCaptor<InsertAllRequest>()
    verify(mockBigQuery).insertAll(requestCaptor.capture())

    val insertedRows = requestCaptor.firstValue.rows
    assertThat(insertedRows).isNotEmpty()
    val firstRowContent = insertedRows.first().content
    assertThat(firstRowContent).containsKey(customKeyColumn)
    assertThat(firstRowContent).containsKey(customDataColumn)
    assertThat(firstRowContent).containsKey(customDateColumn)
  }

  @Test
  fun `processJoinKeys handles valid collection`() = runBlockingTest {
    // Given
    val task =
      WriteToBigQueryTask.forJoinKeys(
        projectId = PROJECT_ID,
        datasetId = DATASET_ID,
        tableId = TABLE_ID,
        exchangeDate = EXCHANGE_DATE,
        bigQueryServiceFactory = mockBigQueryServiceFactory,
      )

    val joinKeyAndIdCollection = joinKeyAndIdCollection {
      joinKeyAndIds +=
        (1..10).map { i ->
          joinKeyAndId {
            joinKey = joinKey { key = "key-$i".toByteStringUtf8() }
            joinKeyIdentifier = joinKeyIdentifier { id = ByteString.copyFromUtf8("$i") }
          }
        }
    }

    val inputBlob = createTestBlob("input", joinKeyAndIdCollection.toByteString())
    setupMockBigQueryResponse(hasErrors = false)

    // When
    val result = task.execute(mapOf("input" to inputBlob))

    // Then
    val status = BigQueryStreamingStatus.parseFrom(result["status"]!!.single().toByteArray())
    assertThat(status.status).isEqualTo(BigQueryStreamingStatus.Status.SUCCESS)
    assertThat(status.statistics.rowsWritten).isEqualTo(10)
    assertThat(status.statistics.rowsFailed).isEqualTo(0)
  }

  @Test
  fun `processJoinKeys handles empty collection gracefully`() = runBlockingTest {
    // Given
    val task =
      WriteToBigQueryTask.forJoinKeys(
        projectId = PROJECT_ID,
        datasetId = DATASET_ID,
        tableId = TABLE_ID,
        exchangeDate = EXCHANGE_DATE,
        bigQueryServiceFactory = mockBigQueryServiceFactory,
      )

    val emptyCollection = joinKeyAndIdCollection {}
    val inputBlob = createTestBlob("input", emptyCollection.toByteString())
    setupMockBigQueryResponse(hasErrors = false)

    // When
    val result = task.execute(mapOf("input" to inputBlob))

    // Then - should complete successfully with empty result
    assertThat(result).containsKey("status")
    val status = BigQueryStreamingStatus.parseFrom(result["status"]!!.single().toByteArray())
    assertThat(status.status).isEqualTo(BigQueryStreamingStatus.Status.STATUS_UNSPECIFIED)
    assertThat(status.statistics.rowsWritten).isEqualTo(0)
    assertThat(status.statistics.rowsFailed).isEqualTo(0)
  }

  @Test
  fun `processJoinKeys fails on duplicate keys`() = runBlockingTest {
    // Given
    val task =
      WriteToBigQueryTask.forJoinKeys(
        projectId = PROJECT_ID,
        datasetId = DATASET_ID,
        tableId = TABLE_ID,
        exchangeDate = EXCHANGE_DATE,
        bigQueryServiceFactory = mockBigQueryServiceFactory,
      )

    val collectionWithDuplicateKeys = joinKeyAndIdCollection {
      joinKeyAndIds += joinKeyAndId {
        joinKey = joinKey { key = "duplicate-key".toByteStringUtf8() }
        joinKeyIdentifier = joinKeyIdentifier { id = ByteString.copyFromUtf8("1") }
      }
      joinKeyAndIds += joinKeyAndId {
        joinKey = joinKey { key = "duplicate-key".toByteStringUtf8() } // Duplicate key
        joinKeyIdentifier = joinKeyIdentifier { id = ByteString.copyFromUtf8("2") }
      }
    }

    val inputBlob = createTestBlob("input", collectionWithDuplicateKeys.toByteString())

    // When/Then
    val exception =
      assertFailsWith<IllegalArgumentException> { task.execute(mapOf("input" to inputBlob)) }
    assertThat(exception.message).contains("JoinKeys are not distinct")
  }

  @Test
  fun `processJoinKeys handles duplicate identifiers`() = runBlockingTest {
    // Given
    val task =
      WriteToBigQueryTask.forJoinKeys(
        projectId = PROJECT_ID,
        datasetId = DATASET_ID,
        tableId = TABLE_ID,
        exchangeDate = EXCHANGE_DATE,
        bigQueryServiceFactory = mockBigQueryServiceFactory,
      )

    val collectionWithDuplicateIds = joinKeyAndIdCollection {
      joinKeyAndIds += joinKeyAndId {
        joinKey = joinKey { key = "key-1".toByteStringUtf8() }
        joinKeyIdentifier = joinKeyIdentifier { id = ByteString.copyFromUtf8("1") }
      }
      joinKeyAndIds += joinKeyAndId {
        joinKey = joinKey { key = "key-2".toByteStringUtf8() }
        joinKeyIdentifier = joinKeyIdentifier { id = ByteString.copyFromUtf8("1") } // Duplicate ID
      }
    }

    val inputBlob = createTestBlob("input", collectionWithDuplicateIds.toByteString())
    setupMockBigQueryResponse(hasErrors = false)

    // When
    val result = task.execute(mapOf("input" to inputBlob))

    // Then - should succeed despite duplicate identifiers (no validation for duplicate IDs)
    assertThat(result).containsKey("status")
    val status = BigQueryStreamingStatus.parseFrom(result["status"]!!.single().toByteArray())
    assertThat(status.status).isEqualTo(BigQueryStreamingStatus.Status.SUCCESS)
    assertThat(status.statistics.rowsWritten).isEqualTo(2)
  }

  @Test
  fun `processEncryptedEvents handles valid stream`() = runBlockingTest {
    // Given
    val task =
      WriteToBigQueryTask.forEncryptedEvents(
        projectId = PROJECT_ID,
        datasetId = DATASET_ID,
        tableId = TABLE_ID,
        exchangeDate = EXCHANGE_DATE,
        bigQueryServiceFactory = mockBigQueryServiceFactory,
      )

    // Create a stream of delimited encrypted events
    val events =
      (1..10).map { i ->
        encryptedMatchedEvent {
          encryptedJoinKey = ByteString.copyFromUtf8("key-$i")
          encryptedEventData = ByteString.copyFromUtf8("data-$i")
        }
      }

    val delimitedBytes =
      ByteString.newOutput().use { output ->
        events.forEach { it.writeDelimitedTo(output) }
        output.toByteString()
      }

    val inputBlob = createTestBlob("input", delimitedBytes)
    setupMockBigQueryResponse(hasErrors = false)

    // When
    val result = task.execute(mapOf("input" to inputBlob))

    // Then
    val status = BigQueryStreamingStatus.parseFrom(result["status"]!!.single().toByteArray())
    assertThat(status.status).isEqualTo(BigQueryStreamingStatus.Status.SUCCESS)
    assertThat(status.statistics.rowsWritten).isEqualTo(10)
    assertThat(status.statistics.rowsFailed).isEqualTo(0)
  }

  @Test
  fun `processEncryptedEvents handles empty stream gracefully`() = runBlockingTest {
    // Given
    val task =
      WriteToBigQueryTask.forEncryptedEvents(
        projectId = PROJECT_ID,
        datasetId = DATASET_ID,
        tableId = TABLE_ID,
        exchangeDate = EXCHANGE_DATE,
        bigQueryServiceFactory = mockBigQueryServiceFactory,
      )

    val emptyBlob = createTestBlob("input", ByteString.EMPTY)
    setupMockBigQueryResponse(hasErrors = false)

    // When
    val result = task.execute(mapOf("input" to emptyBlob))

    // Then - should complete successfully with empty result
    assertThat(result).containsKey("status")
    val status = BigQueryStreamingStatus.parseFrom(result["status"]!!.single().toByteArray())
    assertThat(status.status).isEqualTo(BigQueryStreamingStatus.Status.STATUS_UNSPECIFIED)
    assertThat(status.statistics.rowsWritten).isEqualTo(0)
    assertThat(status.statistics.rowsFailed).isEqualTo(0)
  }

  @Test
  fun `processEncryptedEvents filters out empty join key`() = runBlockingTest {
    // Given
    val task =
      WriteToBigQueryTask.forEncryptedEvents(
        projectId = PROJECT_ID,
        datasetId = DATASET_ID,
        tableId = TABLE_ID,
        exchangeDate = EXCHANGE_DATE,
        bigQueryServiceFactory = mockBigQueryServiceFactory,
      )

    val eventWithEmptyKey = encryptedMatchedEvent {
      encryptedJoinKey = ByteString.EMPTY // Empty key
      encryptedEventData = ByteString.copyFromUtf8("data")
    }

    val delimitedBytes =
      ByteString.newOutput().use { output ->
        eventWithEmptyKey.writeDelimitedTo(output)
        output.toByteString()
      }

    val inputBlob = createTestBlob("input", delimitedBytes)
    setupMockBigQueryResponse(hasErrors = false)

    // When
    val result = task.execute(mapOf("input" to inputBlob))

    // Then - should complete successfully but with no events (filtered out)
    assertThat(result).containsKey("status")
    val status = BigQueryStreamingStatus.parseFrom(result["status"]!!.single().toByteArray())
    assertThat(status.status).isEqualTo(BigQueryStreamingStatus.Status.STATUS_UNSPECIFIED)
    assertThat(status.statistics.rowsWritten).isEqualTo(0)
  }

  @Test
  fun `processEncryptedEvents filters out empty event data`() = runBlockingTest {
    // Given
    val task =
      WriteToBigQueryTask.forEncryptedEvents(
        projectId = PROJECT_ID,
        datasetId = DATASET_ID,
        tableId = TABLE_ID,
        exchangeDate = EXCHANGE_DATE,
        bigQueryServiceFactory = mockBigQueryServiceFactory,
      )

    val eventWithEmptyData = encryptedMatchedEvent {
      encryptedJoinKey = ByteString.copyFromUtf8("key")
      encryptedEventData = ByteString.EMPTY // Empty data
    }

    val delimitedBytes =
      ByteString.newOutput().use { output ->
        eventWithEmptyData.writeDelimitedTo(output)
        output.toByteString()
      }

    val inputBlob = createTestBlob("input", delimitedBytes)
    setupMockBigQueryResponse(hasErrors = false)

    // When
    val result = task.execute(mapOf("input" to inputBlob))

    // Then - should complete successfully but with no events (filtered out)
    assertThat(result).containsKey("status")
    val status = BigQueryStreamingStatus.parseFrom(result["status"]!!.single().toByteArray())
    assertThat(status.status).isEqualTo(BigQueryStreamingStatus.Status.STATUS_UNSPECIFIED)
    assertThat(status.statistics.rowsWritten).isEqualTo(0)
  }

  @Test
  fun `streamBatch handles successful insert`() = runBlockingTest {
    // Given
    val task =
      WriteToBigQueryTask.forJoinKeys(
        projectId = PROJECT_ID,
        datasetId = DATASET_ID,
        tableId = TABLE_ID,
        exchangeDate = EXCHANGE_DATE,
        bigQueryServiceFactory = mockBigQueryServiceFactory,
      )

    val joinKeyAndIdCollection = joinKeyAndIdCollection {
      joinKeyAndIds +=
        (1..100).map { i ->
          joinKeyAndId {
            joinKey = joinKey { key = "key-$i".toByteStringUtf8() }
            joinKeyIdentifier = joinKeyIdentifier { id = ByteString.copyFromUtf8("$i") }
          }
        }
    }

    val inputBlob = createTestBlob("input", joinKeyAndIdCollection.toByteString())
    setupMockBigQueryResponse(hasErrors = false)

    // When
    val result = task.execute(mapOf("input" to inputBlob))

    // Then - verify insertAll was called
    verify(mockBigQuery, times(1)).insertAll(any())

    val status = BigQueryStreamingStatus.parseFrom(result["status"]!!.single().toByteArray())
    assertThat(status.status).isEqualTo(BigQueryStreamingStatus.Status.SUCCESS)
    assertThat(status.statistics.rowsWritten).isEqualTo(100)
    assertThat(status.statistics.batchesSent).isEqualTo(1)
  }

  @Test
  fun `streamBatch handles large datasets with multiple batches`() = runBlockingTest {
    // Given
    val task =
      WriteToBigQueryTask.forJoinKeys(
        projectId = PROJECT_ID,
        datasetId = DATASET_ID,
        tableId = TABLE_ID,
        exchangeDate = EXCHANGE_DATE,
        bigQueryServiceFactory = mockBigQueryServiceFactory,
      )

    // Create 10,000 keys to trigger multiple batches (batch size is 5000)
    val joinKeyAndIdCollection = joinKeyAndIdCollection {
      joinKeyAndIds +=
        (1..10000).map { i ->
          joinKeyAndId {
            joinKey = joinKey { key = "key-$i".toByteStringUtf8() }
            joinKeyIdentifier = joinKeyIdentifier { id = ByteString.copyFromUtf8("$i") }
          }
        }
    }

    val inputBlob = createTestBlob("input", joinKeyAndIdCollection.toByteString())
    setupMockBigQueryResponse(hasErrors = false)

    // When
    val result = task.execute(mapOf("input" to inputBlob))

    // Then - verify multiple batches were sent
    verify(mockBigQuery, times(2)).insertAll(any()) // 10,000 / 5,000 = 2 batches

    val status = BigQueryStreamingStatus.parseFrom(result["status"]!!.single().toByteArray())
    assertThat(status.statistics.batchesSent).isEqualTo(2)
  }

  @Test
  fun `streamBatch handles partial failures`() = runBlockingTest {
    // Given
    val task =
      WriteToBigQueryTask.forJoinKeys(
        projectId = PROJECT_ID,
        datasetId = DATASET_ID,
        tableId = TABLE_ID,
        exchangeDate = EXCHANGE_DATE,
        bigQueryServiceFactory = mockBigQueryServiceFactory,
      )

    val joinKeyAndIdCollection = joinKeyAndIdCollection {
      joinKeyAndIds +=
        (1..10).map { i ->
          joinKeyAndId {
            joinKey = joinKey { key = "key-$i".toByteStringUtf8() }
            joinKeyIdentifier = joinKeyIdentifier { id = ByteString.copyFromUtf8("$i") }
          }
        }
    }

    val inputBlob = createTestBlob("input", joinKeyAndIdCollection.toByteString())

    // Setup mock to simulate partial failure
    val errorMap = mapOf(0L to listOf(mock<BigQueryError>()), 2L to listOf(mock<BigQueryError>()))
    whenever(mockInsertAllResponse.hasErrors()).thenReturn(true)
    whenever(mockInsertAllResponse.insertErrors).thenReturn(errorMap)
    whenever(mockBigQuery.insertAll(any())).thenReturn(mockInsertAllResponse)

    // When
    val result = task.execute(mapOf("input" to inputBlob))

    // Then
    val status = BigQueryStreamingStatus.parseFrom(result["status"]!!.single().toByteArray())
    assertThat(status.status).isEqualTo(BigQueryStreamingStatus.Status.PARTIAL_SUCCESS)
    assertThat(status.statistics.rowsWritten).isEqualTo(8) // 10 total - 2 failed
    assertThat(status.statistics.rowsFailed).isEqualTo(2)
    assertThat(status.errorMessage).contains("Failed to stream 2 rows")
  }

  @Test
  fun `streamBatch handles complete failure`() = runBlockingTest {
    // Given
    val task =
      WriteToBigQueryTask.forJoinKeys(
        projectId = PROJECT_ID,
        datasetId = DATASET_ID,
        tableId = TABLE_ID,
        exchangeDate = EXCHANGE_DATE,
        bigQueryServiceFactory = mockBigQueryServiceFactory,
      )

    val joinKeyAndIdCollection = joinKeyAndIdCollection {
      joinKeyAndIds +=
        (1..5).map { i ->
          joinKeyAndId {
            joinKey = joinKey { key = "key-$i".toByteStringUtf8() }
            joinKeyIdentifier = joinKeyIdentifier { id = ByteString.copyFromUtf8("$i") }
          }
        }
    }

    val inputBlob = createTestBlob("input", joinKeyAndIdCollection.toByteString())

    // Setup mock to simulate complete failure
    val errorMap = (0..4).associateBy({ it.toLong() }, { listOf(mock<BigQueryError>()) })
    whenever(mockInsertAllResponse.hasErrors()).thenReturn(true)
    whenever(mockInsertAllResponse.insertErrors).thenReturn(errorMap)
    whenever(mockBigQuery.insertAll(any())).thenReturn(mockInsertAllResponse)

    // When
    val result = task.execute(mapOf("input" to inputBlob))

    // Then
    val status = BigQueryStreamingStatus.parseFrom(result["status"]!!.single().toByteArray())
    assertThat(status.status).isEqualTo(BigQueryStreamingStatus.Status.FAILURE)
    assertThat(status.statistics.rowsWritten).isEqualTo(0)
    assertThat(status.statistics.rowsFailed).isEqualTo(5)
  }

  @Test
  fun `execute handles BigQuery exceptions gracefully`() = runBlockingTest {
    // Given
    val task =
      WriteToBigQueryTask.forJoinKeys(
        projectId = PROJECT_ID,
        datasetId = DATASET_ID,
        tableId = TABLE_ID,
        exchangeDate = EXCHANGE_DATE,
        bigQueryServiceFactory = mockBigQueryServiceFactory,
      )

    val joinKeyAndIdCollection = joinKeyAndIdCollection {
      joinKeyAndIds += joinKeyAndId {
        joinKey = joinKey { key = "key".toByteStringUtf8() }
        joinKeyIdentifier = joinKeyIdentifier { id = ByteString.copyFromUtf8("1") }
      }
    }

    val inputBlob = createTestBlob("input", joinKeyAndIdCollection.toByteString())

    // Setup mock to throw BigQuery exception
    whenever(mockBigQuery.insertAll(any())).thenThrow(BigQueryException(500, "Internal error"))

    // When/Then - should propagate the exception
    assertFailsWith<BigQueryException> { task.execute(mapOf("input" to inputBlob)) }
  }

  @Test
  fun `execute requires input blob`() = runBlockingTest {
    // Given
    val task =
      WriteToBigQueryTask.forJoinKeys(
        projectId = PROJECT_ID,
        datasetId = DATASET_ID,
        tableId = TABLE_ID,
        exchangeDate = EXCHANGE_DATE,
        bigQueryServiceFactory = mockBigQueryServiceFactory,
      )

    // When/Then
    val exception = assertFailsWith<IllegalArgumentException> { task.execute(emptyMap()) }
    assertThat(exception.message).contains("Missing required input")
  }

  @Test
  fun `data is correctly Base64 encoded for BigQuery`() = runBlockingTest {
    // Given
    val task =
      WriteToBigQueryTask.forJoinKeys(
        projectId = PROJECT_ID,
        datasetId = DATASET_ID,
        tableId = TABLE_ID,
        exchangeDate = EXCHANGE_DATE,
        bigQueryServiceFactory = mockBigQueryServiceFactory,
      )

    val testKey = ByteString.copyFromUtf8("test-key-data")
    val joinKeyAndIdCollection = joinKeyAndIdCollection {
      joinKeyAndIds += joinKeyAndId {
        joinKey = joinKey { key = testKey }
        joinKeyIdentifier = joinKeyIdentifier { id = ByteString.copyFromUtf8("1") }
      }
    }

    val inputBlob = createTestBlob("input", joinKeyAndIdCollection.toByteString())
    setupMockBigQueryResponse(hasErrors = false)

    // When
    task.execute(mapOf("input" to inputBlob))

    // Then - verify data is Base64 encoded
    val requestCaptor = argumentCaptor<InsertAllRequest>()
    verify(mockBigQuery).insertAll(requestCaptor.capture())

    val insertedRows = requestCaptor.firstValue.rows
    assertThat(insertedRows).hasSize(1)
    val rowContent = insertedRows.first().content

    val encodedKey = rowContent["encrypted_join_key"] as String
    // forJoinKeys hashes the key with SHA256 before encoding (H(G(Key)))
    val expectedHash = sha256(testKey)
    assertThat(encodedKey).isEqualTo(Base64.getEncoder().encodeToString(expectedHash.toByteArray()))
  }

  @Test
  fun `date is correctly formatted for BigQuery`() = runBlockingTest {
    // Given
    val specificDate = LocalDate.of(2025, 12, 31)
    val task =
      WriteToBigQueryTask.forJoinKeys(
        projectId = PROJECT_ID,
        datasetId = DATASET_ID,
        tableId = TABLE_ID,
        exchangeDate = specificDate,
        bigQueryServiceFactory = mockBigQueryServiceFactory,
      )

    val joinKeyAndIdCollection = joinKeyAndIdCollection {
      joinKeyAndIds += joinKeyAndId {
        joinKey = joinKey { key = "key".toByteStringUtf8() }
        joinKeyIdentifier = joinKeyIdentifier { id = ByteString.copyFromUtf8("1") }
      }
    }

    val inputBlob = createTestBlob("input", joinKeyAndIdCollection.toByteString())
    setupMockBigQueryResponse(hasErrors = false)

    // When
    task.execute(mapOf("input" to inputBlob))

    // Then - verify date format
    val requestCaptor = argumentCaptor<InsertAllRequest>()
    verify(mockBigQuery).insertAll(requestCaptor.capture())

    val rowContent = requestCaptor.firstValue.rows.first().content
    assertThat(rowContent["exchange_date"]).isEqualTo("2025-12-31")
  }

  @Test
  fun `status proto contains correct table information`() = runBlockingTest {
    // Given
    val task =
      WriteToBigQueryTask.forJoinKeys(
        projectId = PROJECT_ID,
        datasetId = DATASET_ID,
        tableId = TABLE_ID,
        exchangeDate = EXCHANGE_DATE,
        bigQueryServiceFactory = mockBigQueryServiceFactory,
      )

    val joinKeyAndIdCollection = joinKeyAndIdCollection {
      joinKeyAndIds += joinKeyAndId {
        joinKey = joinKey { key = "key".toByteStringUtf8() }
        joinKeyIdentifier = joinKeyIdentifier { id = ByteString.copyFromUtf8("1") }
      }
    }

    val inputBlob = createTestBlob("input", joinKeyAndIdCollection.toByteString())
    setupMockBigQueryResponse(hasErrors = false)

    // When
    val result = task.execute(mapOf("input" to inputBlob))

    // Then
    val status = BigQueryStreamingStatus.parseFrom(result["status"]!!.single().toByteArray())
    assertThat(status.tableInfo.projectId).isEqualTo(PROJECT_ID)
    assertThat(status.tableInfo.datasetId).isEqualTo(DATASET_ID)
    assertThat(status.tableInfo.tableId).isEqualTo(TABLE_ID)
  }

  @Test
  fun `status proto contains correct exchange date`() = runBlockingTest {
    // Given
    val specificDate = LocalDate.of(2025, 3, 15)
    val task =
      WriteToBigQueryTask.forJoinKeys(
        projectId = PROJECT_ID,
        datasetId = DATASET_ID,
        tableId = TABLE_ID,
        exchangeDate = specificDate,
        bigQueryServiceFactory = mockBigQueryServiceFactory,
      )

    val joinKeyAndIdCollection = joinKeyAndIdCollection {
      joinKeyAndIds += joinKeyAndId {
        joinKey = joinKey { key = "key".toByteStringUtf8() }
        joinKeyIdentifier = joinKeyIdentifier { id = ByteString.copyFromUtf8("1") }
      }
    }

    val inputBlob = createTestBlob("input", joinKeyAndIdCollection.toByteString())
    setupMockBigQueryResponse(hasErrors = false)

    // When
    val result = task.execute(mapOf("input" to inputBlob))

    // Then
    val status = BigQueryStreamingStatus.parseFrom(result["status"]!!.single().toByteArray())
    assertThat(status.exchangeDate.year).isEqualTo(2025)
    assertThat(status.exchangeDate.month).isEqualTo(3)
    assertThat(status.exchangeDate.day).isEqualTo(15)
  }

  @Test
  fun `bigQueryServiceFactory is called with correct project ID`() = runBlockingTest {
    // Given
    val task =
      WriteToBigQueryTask.forJoinKeys(
        projectId = PROJECT_ID,
        datasetId = DATASET_ID,
        tableId = TABLE_ID,
        exchangeDate = EXCHANGE_DATE,
        bigQueryServiceFactory = mockBigQueryServiceFactory,
      )

    val joinKeyAndIdCollection = joinKeyAndIdCollection {
      joinKeyAndIds += joinKeyAndId {
        joinKey = joinKey { key = "key".toByteStringUtf8() }
        joinKeyIdentifier = joinKeyIdentifier { id = ByteString.copyFromUtf8("1") }
      }
    }

    val inputBlob = createTestBlob("input", joinKeyAndIdCollection.toByteString())
    setupMockBigQueryResponse(hasErrors = false)

    // When
    task.execute(mapOf("input" to inputBlob))

    // Then
    verify(mockBigQueryServiceFactory).getService(PROJECT_ID)
  }

  @Test
  fun `BigQuery service obtained lazily`() {
    // Given
    val task =
      WriteToBigQueryTask.forJoinKeys(
        projectId = PROJECT_ID,
        datasetId = DATASET_ID,
        tableId = TABLE_ID,
        exchangeDate = EXCHANGE_DATE,
        bigQueryServiceFactory = mockBigQueryServiceFactory,
      )

    // Then - service should not be created in constructor
    verify(mockBigQueryServiceFactory, never()).getService(any())
  }

  @Test
  fun `insert ID generation includes unique components`() = runBlockingTest {
    // Given
    val task =
      WriteToBigQueryTask.forJoinKeys(
        projectId = PROJECT_ID,
        datasetId = DATASET_ID,
        tableId = TABLE_ID,
        exchangeDate = EXCHANGE_DATE,
        bigQueryServiceFactory = mockBigQueryServiceFactory,
      )

    val joinKeyAndIdCollection = joinKeyAndIdCollection {
      joinKeyAndIds +=
        (1..3).map { i ->
          joinKeyAndId {
            joinKey = joinKey { key = "key-$i".toByteStringUtf8() }
            joinKeyIdentifier = joinKeyIdentifier { id = ByteString.copyFromUtf8("$i") }
          }
        }
    }

    val inputBlob = createTestBlob("input", joinKeyAndIdCollection.toByteString())
    setupMockBigQueryResponse(hasErrors = false)

    // When
    task.execute(mapOf("input" to inputBlob))

    // Then - verify all insert IDs are unique
    val requestCaptor = argumentCaptor<InsertAllRequest>()
    verify(mockBigQuery).insertAll(requestCaptor.capture())

    val insertIds = requestCaptor.firstValue.rows.map { it.id }
    assertThat(insertIds).containsNoDuplicates()

    // Verify insert IDs contain expected components
    insertIds.forEach { id ->
      assertThat(id).contains("_") // Should contain separators
      assertThat(id.split("_")).hasSize(3) // timestamp_hash_uuid format
    }
  }

  private fun setupMockBigQueryResponse(hasErrors: Boolean) {
    whenever(mockInsertAllResponse.hasErrors()).thenReturn(hasErrors)
    if (!hasErrors) {
      whenever(mockInsertAllResponse.insertErrors).thenReturn(emptyMap())
    }
    whenever(mockBigQuery.insertAll(any())).thenReturn(mockInsertAllResponse)
  }

  private suspend fun createTestBlob(path: String, data: ByteString): StorageClient.Blob {
    return storageClient.writeBlob(path, data)
  }
}
