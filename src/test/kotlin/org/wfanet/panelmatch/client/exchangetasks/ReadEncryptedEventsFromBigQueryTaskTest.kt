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
import com.google.cloud.bigquery.BigQueryException
import com.google.cloud.bigquery.Field
import com.google.cloud.bigquery.FieldValue
import com.google.cloud.bigquery.FieldValueList
import com.google.cloud.bigquery.QueryJobConfiguration
import com.google.cloud.bigquery.Schema
import com.google.cloud.bigquery.StandardSQLTypeName
import com.google.cloud.bigquery.TableResult
import com.google.common.truth.Truth.assertThat
import com.google.common.truth.extensions.proto.ProtoTruth.assertThat
import com.google.protobuf.ByteString
import java.time.LocalDate
import kotlin.test.assertFailsWith
import kotlinx.coroutines.flow.toList
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.mockito.kotlin.any
import org.mockito.kotlin.argumentCaptor
import org.mockito.kotlin.mock
import org.mockito.kotlin.times
import org.mockito.kotlin.verify
import org.mockito.kotlin.whenever
import org.wfanet.measurement.storage.testing.InMemoryStorageClient
import org.wfanet.panelmatch.client.authorizedview.BigQueryServiceFactory
import org.wfanet.panelmatch.client.authorizedview.EncryptedMatchedEvent
import org.wfanet.panelmatch.client.authorizedview.encryptedMatchedEvent
import org.wfanet.panelmatch.common.testing.runBlockingTest

private const val PROJECT_ID = "test-project"
private const val DATASET_ID = "test-dataset"
private const val TABLE_OR_VIEW_ID = "test-view"
private val EXCHANGE_DATE = LocalDate.of(2025, 1, 15)

@RunWith(JUnit4::class)
class ReadEncryptedEventsFromBigQueryTaskTest {
  private val mockBigQuery: BigQuery = mock()
  private val mockBigQueryServiceFactory: BigQueryServiceFactory = mock()
  private val mockTableResult: TableResult = mock()
  private val mockSchema: Schema = mock()
  private val storageClient = InMemoryStorageClient()

  @Before
  fun setUp() {
    whenever(mockBigQueryServiceFactory.getService(any())).thenReturn(mockBigQuery)
  }

  // ============ Constructor and Configuration Tests ============

  @Test
  fun `task uses default column names when not specified`() = runBlockingTest {
    // Given
    val task =
      ReadEncryptedEventsFromBigQueryTask(
        projectId = PROJECT_ID,
        datasetId = DATASET_ID,
        tableOrViewId = TABLE_OR_VIEW_ID,
        exchangeDate = EXCHANGE_DATE,
        bigQueryServiceFactory = mockBigQueryServiceFactory,
      )

    setupMockBigQueryResponse(
      keyColumnName = ReadEncryptedEventsFromBigQueryTask.DEFAULT_ENCRYPTED_JOIN_KEY_COLUMN_NAME,
      eventDataColumnName =
        ReadEncryptedEventsFromBigQueryTask.DEFAULT_ENCRYPTED_EVENT_DATA_COLUMN_NAME,
    )

    // When
    val result = task.execute(emptyMap())
    result["encrypted-events"]!!.toList()

    // Then - verify query uses default column names
    val queryCaptor = argumentCaptor<QueryJobConfiguration>()
    verify(mockBigQuery).query(queryCaptor.capture())
    val query = queryCaptor.firstValue.query

    assertThat(query).contains("`encrypted_join_key`")
    assertThat(query).contains("`encrypted_event_data`")
    assertThat(query).contains("`exchange_date`")
  }

  @Test
  fun `task uses custom column names when specified`() = runBlockingTest {
    // Given
    val customJoinKeyColumn = "custom_join_key"
    val customEventDataColumn = "custom_event_data"
    val customDateColumn = "custom_date"

    val task =
      ReadEncryptedEventsFromBigQueryTask(
        projectId = PROJECT_ID,
        datasetId = DATASET_ID,
        tableOrViewId = TABLE_OR_VIEW_ID,
        exchangeDate = EXCHANGE_DATE,
        bigQueryServiceFactory = mockBigQueryServiceFactory,
        keyColumnName = customJoinKeyColumn,
        eventDataColumnName = customEventDataColumn,
        dateColumnName = customDateColumn,
      )

    setupMockBigQueryResponse(
      keyColumnName = customJoinKeyColumn,
      eventDataColumnName = customEventDataColumn,
    )

    // When
    val result = task.execute(emptyMap())
    result["encrypted-events"]!!.toList()

    // Then - verify query uses custom column names
    val queryCaptor = argumentCaptor<QueryJobConfiguration>()
    verify(mockBigQuery).query(queryCaptor.capture())
    val query = queryCaptor.firstValue.query

    assertThat(query).contains("`$customJoinKeyColumn`")
    assertThat(query).contains("`$customEventDataColumn`")
    assertThat(query).contains("WHERE `$customDateColumn` = DATE('$EXCHANGE_DATE')")
  }

  // ============ Query Construction Tests ============

  @Test
  fun `buildQuery constructs correct SQL with default columns`() = runBlockingTest {
    // Given
    val task =
      ReadEncryptedEventsFromBigQueryTask(
        projectId = PROJECT_ID,
        datasetId = DATASET_ID,
        tableOrViewId = TABLE_OR_VIEW_ID,
        exchangeDate = EXCHANGE_DATE,
        bigQueryServiceFactory = mockBigQueryServiceFactory,
      )

    setupMockBigQueryResponse()

    // When
    val result = task.execute(emptyMap())
    result["encrypted-events"]!!.toList()

    // Then
    val queryCaptor = argumentCaptor<QueryJobConfiguration>()
    verify(mockBigQuery).query(queryCaptor.capture())
    val queryConfig = queryCaptor.firstValue

    assertThat(queryConfig.query)
      .isEqualTo(
        "SELECT `encrypted_join_key`, `encrypted_event_data` " +
          "FROM `$PROJECT_ID.$DATASET_ID.$TABLE_OR_VIEW_ID` " +
          "WHERE `exchange_date` = DATE('$EXCHANGE_DATE')"
      )
    assertThat(queryConfig.useLegacySql()).isFalse()
  }

  @Test
  fun `buildQuery constructs correct SQL with custom columns`() = runBlockingTest {
    // Given
    val customJoinKey = "my_join_key"
    val customEventData = "my_event_data"
    val customDateColumn = "my_date"

    val task =
      ReadEncryptedEventsFromBigQueryTask(
        projectId = PROJECT_ID,
        datasetId = DATASET_ID,
        tableOrViewId = TABLE_OR_VIEW_ID,
        exchangeDate = EXCHANGE_DATE,
        bigQueryServiceFactory = mockBigQueryServiceFactory,
        keyColumnName = customJoinKey,
        eventDataColumnName = customEventData,
        dateColumnName = customDateColumn,
      )

    setupMockBigQueryResponse(keyColumnName = customJoinKey, eventDataColumnName = customEventData)

    // When
    val result = task.execute(emptyMap())
    result["encrypted-events"]!!.toList()

    // Then
    val queryCaptor = argumentCaptor<QueryJobConfiguration>()
    verify(mockBigQuery).query(queryCaptor.capture())
    val queryConfig = queryCaptor.firstValue

    assertThat(queryConfig.query).contains("`$customJoinKey`")
    assertThat(queryConfig.query).contains("`$customEventData`")
    assertThat(queryConfig.query).contains("`$customDateColumn`")
  }

  @Test
  fun `buildQuery formats date correctly`() = runBlockingTest {
    // Test with various dates
    val dates =
      listOf(
        LocalDate.of(2025, 1, 1),
        LocalDate.of(2025, 12, 31),
        LocalDate.of(2024, 2, 29), // Leap year
      )

    dates.forEach { date ->
      // Given
      val task =
        ReadEncryptedEventsFromBigQueryTask(
          projectId = PROJECT_ID,
          datasetId = DATASET_ID,
          tableOrViewId = TABLE_OR_VIEW_ID,
          exchangeDate = date,
          bigQueryServiceFactory = mockBigQueryServiceFactory,
        )

      setupMockBigQueryResponse()

      // When
      val result = task.execute(emptyMap())
      result["encrypted-events"]!!.toList()

      // Then
      val queryCaptor = argumentCaptor<QueryJobConfiguration>()
      verify(mockBigQuery, times(dates.indexOf(date) + 1)).query(queryCaptor.capture())
      val query = queryCaptor.lastValue.query

      assertThat(query).contains("WHERE `exchange_date` = DATE('$date')")
    }
  }

  // ============ Schema Validation Tests ============

  @Test
  fun `validateSchema succeeds with correct schema`() = runBlockingTest {
    // Given
    val task =
      ReadEncryptedEventsFromBigQueryTask(
        projectId = PROJECT_ID,
        datasetId = DATASET_ID,
        tableOrViewId = TABLE_OR_VIEW_ID,
        exchangeDate = EXCHANGE_DATE,
        bigQueryServiceFactory = mockBigQueryServiceFactory,
      )

    setupMockBigQueryResponse()

    // When
    val result = task.execute(emptyMap())
    result["encrypted-events"]!!.toList()

    // Then - no exception thrown
    assertThat(result).containsKey("encrypted-events")
  }

  @Test
  fun `validateSchema throws when join key column missing`() = runBlockingTest {
    // Given
    val task =
      ReadEncryptedEventsFromBigQueryTask(
        projectId = PROJECT_ID,
        datasetId = DATASET_ID,
        tableOrViewId = TABLE_OR_VIEW_ID,
        exchangeDate = EXCHANGE_DATE,
        bigQueryServiceFactory = mockBigQueryServiceFactory,
      )

    // Setup schema without encrypted_join_key
    val eventDataField = Field.of("encrypted_event_data", StandardSQLTypeName.BYTES)
    val schema = Schema.of(eventDataField)
    whenever(mockTableResult.getSchema()).thenReturn(schema)
    whenever(mockBigQuery.query(any<QueryJobConfiguration>())).thenReturn(mockTableResult)

    // When/Then
    val result = task.execute(emptyMap())
    assertFailsWith<IllegalArgumentException> { result["encrypted-events"]!!.toList() }
  }

  @Test
  fun `validateSchema throws when event data column missing`() = runBlockingTest {
    // Given
    val task =
      ReadEncryptedEventsFromBigQueryTask(
        projectId = PROJECT_ID,
        datasetId = DATASET_ID,
        tableOrViewId = TABLE_OR_VIEW_ID,
        exchangeDate = EXCHANGE_DATE,
        bigQueryServiceFactory = mockBigQueryServiceFactory,
      )

    // Setup schema without encrypted_event_data
    val joinKeyField = Field.of("encrypted_join_key", StandardSQLTypeName.BYTES)
    val schema = Schema.of(joinKeyField)
    whenever(mockTableResult.getSchema()).thenReturn(schema)
    whenever(mockBigQuery.query(any<QueryJobConfiguration>())).thenReturn(mockTableResult)

    // When/Then
    val result = task.execute(emptyMap())
    assertFailsWith<IllegalArgumentException> { result["encrypted-events"]!!.toList() }
  }

  @Test
  fun `validateSchema throws when join key column wrong type`() = runBlockingTest {
    // Given
    val task =
      ReadEncryptedEventsFromBigQueryTask(
        projectId = PROJECT_ID,
        datasetId = DATASET_ID,
        tableOrViewId = TABLE_OR_VIEW_ID,
        exchangeDate = EXCHANGE_DATE,
        bigQueryServiceFactory = mockBigQueryServiceFactory,
      )

    // Setup schema with wrong type for join key
    val joinKeyField = Field.of("encrypted_join_key", StandardSQLTypeName.STRING)
    val eventDataField = Field.of("encrypted_event_data", StandardSQLTypeName.BYTES)
    val schema = Schema.of(joinKeyField, eventDataField)
    whenever(mockTableResult.getSchema()).thenReturn(schema)
    whenever(mockBigQuery.query(any<QueryJobConfiguration>())).thenReturn(mockTableResult)

    // When/Then
    val result = task.execute(emptyMap())
    assertFailsWith<IllegalArgumentException> { result["encrypted-events"]!!.toList() }
  }

  @Test
  fun `validateSchema throws when event data column wrong type`() = runBlockingTest {
    // Given
    val task =
      ReadEncryptedEventsFromBigQueryTask(
        projectId = PROJECT_ID,
        datasetId = DATASET_ID,
        tableOrViewId = TABLE_OR_VIEW_ID,
        exchangeDate = EXCHANGE_DATE,
        bigQueryServiceFactory = mockBigQueryServiceFactory,
      )

    // Setup schema with wrong type for event data
    val joinKeyField = Field.of("encrypted_join_key", StandardSQLTypeName.BYTES)
    val eventDataField = Field.of("encrypted_event_data", StandardSQLTypeName.INT64)
    val schema = Schema.of(joinKeyField, eventDataField)
    whenever(mockTableResult.getSchema()).thenReturn(schema)
    whenever(mockBigQuery.query(any<QueryJobConfiguration>())).thenReturn(mockTableResult)

    // When/Then
    val result = task.execute(emptyMap())
    assertFailsWith<IllegalArgumentException> { result["encrypted-events"]!!.toList() }
  }

  // ============ Row Parsing Tests ============

  @Test
  fun `parseRowToEncryptedMatchedEvent parses valid row`() = runBlockingTest {
    // Given
    val task =
      ReadEncryptedEventsFromBigQueryTask(
        projectId = PROJECT_ID,
        datasetId = DATASET_ID,
        tableOrViewId = TABLE_OR_VIEW_ID,
        exchangeDate = EXCHANGE_DATE,
        bigQueryServiceFactory = mockBigQueryServiceFactory,
      )

    val joinKeyBytes = ByteString.copyFromUtf8("test-join-key").toByteArray()
    val eventDataBytes = ByteString.copyFromUtf8("test-event-data").toByteArray()

    setupMockBigQueryResponse(rows = listOf(createMockRow(joinKeyBytes, eventDataBytes)))

    // When
    val result = task.execute(emptyMap())
    val events = parseDelimitedEvents(result["encrypted-events"]!!.toList())

    // Then
    assertThat(events).hasSize(1)
    val event = events[0]
    assertThat(event.encryptedJoinKey).isEqualTo(ByteString.copyFrom(joinKeyBytes))
    assertThat(event.encryptedEventData).isEqualTo(ByteString.copyFrom(eventDataBytes))
  }

  @Test
  fun `parseRowToEncryptedMatchedEvent throws on null join key`() = runBlockingTest {
    // Given
    val task =
      ReadEncryptedEventsFromBigQueryTask(
        projectId = PROJECT_ID,
        datasetId = DATASET_ID,
        tableOrViewId = TABLE_OR_VIEW_ID,
        exchangeDate = EXCHANGE_DATE,
        bigQueryServiceFactory = mockBigQueryServiceFactory,
      )

    val eventDataBytes = ByteString.copyFromUtf8("test-event-data").toByteArray()

    setupMockBigQueryResponse(rows = listOf(createMockRow(null, eventDataBytes)))

    // When/Then
    val result = task.execute(emptyMap())
    assertFailsWith<IllegalArgumentException> { result["encrypted-events"]!!.toList() }
  }

  @Test
  fun `parseRowToEncryptedMatchedEvent throws on null event data`() = runBlockingTest {
    // Given
    val task =
      ReadEncryptedEventsFromBigQueryTask(
        projectId = PROJECT_ID,
        datasetId = DATASET_ID,
        tableOrViewId = TABLE_OR_VIEW_ID,
        exchangeDate = EXCHANGE_DATE,
        bigQueryServiceFactory = mockBigQueryServiceFactory,
      )

    val joinKeyBytes = ByteString.copyFromUtf8("test-join-key").toByteArray()

    setupMockBigQueryResponse(rows = listOf(createMockRow(joinKeyBytes, null)))

    // When/Then
    val result = task.execute(emptyMap())
    assertFailsWith<IllegalArgumentException> { result["encrypted-events"]!!.toList() }
  }

  @Test
  fun `parseRowToEncryptedMatchedEvent handles binary data correctly`() = runBlockingTest {
    // Given
    val task =
      ReadEncryptedEventsFromBigQueryTask(
        projectId = PROJECT_ID,
        datasetId = DATASET_ID,
        tableOrViewId = TABLE_OR_VIEW_ID,
        exchangeDate = EXCHANGE_DATE,
        bigQueryServiceFactory = mockBigQueryServiceFactory,
      )

    // Non-UTF8 binary data
    val joinKeyBytes = byteArrayOf(0x00, 0x01, 0x02, 0x03, 0xFF.toByte(), 0xFE.toByte())
    val eventDataBytes = byteArrayOf(0xDE.toByte(), 0xAD.toByte(), 0xBE.toByte(), 0xEF.toByte())

    setupMockBigQueryResponse(rows = listOf(createMockRow(joinKeyBytes, eventDataBytes)))

    // When
    val result = task.execute(emptyMap())
    val events = parseDelimitedEvents(result["encrypted-events"]!!.toList())

    // Then
    assertThat(events).hasSize(1)
    val event = events[0]
    assertThat(event.encryptedJoinKey.toByteArray()).isEqualTo(joinKeyBytes)
    assertThat(event.encryptedEventData.toByteArray()).isEqualTo(eventDataBytes)
  }

  // ============ Flow Streaming Tests ============

  @Test
  fun `streamAsProto emits individual delimited messages`() = runBlockingTest {
    // Given
    val task =
      ReadEncryptedEventsFromBigQueryTask(
        projectId = PROJECT_ID,
        datasetId = DATASET_ID,
        tableOrViewId = TABLE_OR_VIEW_ID,
        exchangeDate = EXCHANGE_DATE,
        bigQueryServiceFactory = mockBigQueryServiceFactory,
      )

    val rows =
      (1..5).map { i ->
        createMockRow(
          ByteString.copyFromUtf8("join-key-$i").toByteArray(),
          ByteString.copyFromUtf8("event-data-$i").toByteArray(),
        )
      }

    setupMockBigQueryResponse(rows = rows)

    // When
    val result = task.execute(emptyMap())
    val byteStrings = result["encrypted-events"]!!.toList()

    // Then
    assertThat(byteStrings).hasSize(5)

    // Each item should be length-delimited
    byteStrings.forEach { bytes ->
      assertThat(bytes.size()).isGreaterThan(0)
      // Should be able to parse as delimited message
      val event = EncryptedMatchedEvent.parseDelimitedFrom(bytes.newInput())
      assertThat(event).isNotNull()
    }
  }

  @Test
  fun `streamAsProto handles empty results`() = runBlockingTest {
    // Given
    val task =
      ReadEncryptedEventsFromBigQueryTask(
        projectId = PROJECT_ID,
        datasetId = DATASET_ID,
        tableOrViewId = TABLE_OR_VIEW_ID,
        exchangeDate = EXCHANGE_DATE,
        bigQueryServiceFactory = mockBigQueryServiceFactory,
      )

    setupMockBigQueryResponse(rows = emptyList())

    // When
    val result = task.execute(emptyMap())
    val byteStrings = result["encrypted-events"]!!.toList()

    // Then
    assertThat(byteStrings).isEmpty()
  }

  @Test
  fun `streamAsProto handles single page of results`() = runBlockingTest {
    // Given
    val task =
      ReadEncryptedEventsFromBigQueryTask(
        projectId = PROJECT_ID,
        datasetId = DATASET_ID,
        tableOrViewId = TABLE_OR_VIEW_ID,
        exchangeDate = EXCHANGE_DATE,
        bigQueryServiceFactory = mockBigQueryServiceFactory,
      )

    val rows =
      (1..3).map { i ->
        createMockRow(
          ByteString.copyFromUtf8("key-$i").toByteArray(),
          ByteString.copyFromUtf8("data-$i").toByteArray(),
        )
      }

    setupMockBigQueryResponse(rows = rows)

    // When
    val result = task.execute(emptyMap())
    val events = parseDelimitedEvents(result["encrypted-events"]!!.toList())

    // Then
    assertThat(events).hasSize(3)
  }

  @Test
  fun `streamAsProto streams immediately without buffering`() = runBlockingTest {
    // Given
    val task =
      ReadEncryptedEventsFromBigQueryTask(
        projectId = PROJECT_ID,
        datasetId = DATASET_ID,
        tableOrViewId = TABLE_OR_VIEW_ID,
        exchangeDate = EXCHANGE_DATE,
        bigQueryServiceFactory = mockBigQueryServiceFactory,
      )

    val rows =
      (1..100).map { i ->
        createMockRow(
          ByteString.copyFromUtf8("key-$i").toByteArray(),
          ByteString.copyFromUtf8("data-$i").toByteArray(),
        )
      }

    setupMockBigQueryResponse(rows = rows)

    // When
    val result = task.execute(emptyMap())

    // Collect items one by one to verify streaming
    var count = 0
    result["encrypted-events"]!!.collect { byteString ->
      count++
      // Verify we can parse each item immediately
      val event = EncryptedMatchedEvent.parseDelimitedFrom(byteString.newInput())
      assertThat(event).isNotNull()
    }

    // Then
    assertThat(count).isEqualTo(100)
  }

  // ============ Integration and Error Handling Tests ============

  @Test
  fun `execute returns correct output label`() = runBlockingTest {
    // Given
    val task =
      ReadEncryptedEventsFromBigQueryTask(
        projectId = PROJECT_ID,
        datasetId = DATASET_ID,
        tableOrViewId = TABLE_OR_VIEW_ID,
        exchangeDate = EXCHANGE_DATE,
        bigQueryServiceFactory = mockBigQueryServiceFactory,
      )

    setupMockBigQueryResponse()

    // When
    val result = task.execute(emptyMap())

    // Then
    assertThat(result).containsKey("encrypted-events")
    assertThat(result).hasSize(1)
  }

  @Test
  fun `skipReadInput returns true`() {
    // Given
    val task =
      ReadEncryptedEventsFromBigQueryTask(
        projectId = PROJECT_ID,
        datasetId = DATASET_ID,
        tableOrViewId = TABLE_OR_VIEW_ID,
        exchangeDate = EXCHANGE_DATE,
        bigQueryServiceFactory = mockBigQueryServiceFactory,
      )

    // When
    val skipRead = task.skipReadInput()

    // Then
    assertThat(skipRead).isTrue()
  }

  @Test
  fun `execute throws on BigQuery query timeout`() = runBlockingTest {
    // Given
    val task =
      ReadEncryptedEventsFromBigQueryTask(
        projectId = PROJECT_ID,
        datasetId = DATASET_ID,
        tableOrViewId = TABLE_OR_VIEW_ID,
        exchangeDate = EXCHANGE_DATE,
        bigQueryServiceFactory = mockBigQueryServiceFactory,
      )

    whenever(mockBigQuery.query(any<QueryJobConfiguration>()))
      .thenThrow(BigQueryException(504, "Query timed out"))

    // When/Then
    val result = task.execute(emptyMap())
    assertFailsWith<BigQueryException> { result["encrypted-events"]!!.toList() }
  }

  @Test
  fun `execute throws on BigQuery permission errors`() = runBlockingTest {
    // Given
    val task =
      ReadEncryptedEventsFromBigQueryTask(
        projectId = PROJECT_ID,
        datasetId = DATASET_ID,
        tableOrViewId = TABLE_OR_VIEW_ID,
        exchangeDate = EXCHANGE_DATE,
        bigQueryServiceFactory = mockBigQueryServiceFactory,
      )

    val permissionError = BigQueryException(403, "Access Denied: View test-view")
    whenever(mockBigQuery.query(any<QueryJobConfiguration>())).thenThrow(permissionError)

    // When/Then
    val result = task.execute(emptyMap())
    assertFailsWith<BigQueryException> { result["encrypted-events"]!!.toList() }
  }

  @Test
  fun `execute throws on network errors`() = runBlockingTest {
    // Given
    val task =
      ReadEncryptedEventsFromBigQueryTask(
        projectId = PROJECT_ID,
        datasetId = DATASET_ID,
        tableOrViewId = TABLE_OR_VIEW_ID,
        exchangeDate = EXCHANGE_DATE,
        bigQueryServiceFactory = mockBigQueryServiceFactory,
      )

    whenever(mockBigQuery.query(any<QueryJobConfiguration>()))
      .thenThrow(RuntimeException("Network error"))

    // When/Then
    val result = task.execute(emptyMap())
    assertFailsWith<RuntimeException> { result["encrypted-events"]!!.toList() }
  }

  @Test
  fun `bigQueryServiceFactory is called with correct project ID`() = runBlockingTest {
    // Given
    val task =
      ReadEncryptedEventsFromBigQueryTask(
        projectId = PROJECT_ID,
        datasetId = DATASET_ID,
        tableOrViewId = TABLE_OR_VIEW_ID,
        exchangeDate = EXCHANGE_DATE,
        bigQueryServiceFactory = mockBigQueryServiceFactory,
      )

    setupMockBigQueryResponse()

    // When
    val result = task.execute(emptyMap())
    result["encrypted-events"]!!.toList()

    // Then
    verify(mockBigQueryServiceFactory).getService(PROJECT_ID)
  }

  @Test
  fun `BigQuery service obtained lazily`() {
    // Given
    val task =
      ReadEncryptedEventsFromBigQueryTask(
        projectId = PROJECT_ID,
        datasetId = DATASET_ID,
        tableOrViewId = TABLE_OR_VIEW_ID,
        exchangeDate = EXCHANGE_DATE,
        bigQueryServiceFactory = mockBigQueryServiceFactory,
      )

    // Then - service should not be created in constructor
    verify(mockBigQueryServiceFactory, times(0)).getService(any())
  }

  // ============ End-to-End Integration Tests ============

  @Test
  fun `successfully reads and parses encrypted events`() = runBlockingTest {
    // Given
    val task =
      ReadEncryptedEventsFromBigQueryTask(
        projectId = PROJECT_ID,
        datasetId = DATASET_ID,
        tableOrViewId = TABLE_OR_VIEW_ID,
        exchangeDate = EXCHANGE_DATE,
        bigQueryServiceFactory = mockBigQueryServiceFactory,
      )

    // Create sample encrypted data
    val expectedEvents =
      listOf(
        encryptedMatchedEvent {
          encryptedJoinKey = ByteString.copyFromUtf8("encrypted-key-1")
          encryptedEventData = ByteString.copyFromUtf8("encrypted-data-1")
        },
        encryptedMatchedEvent {
          encryptedJoinKey = ByteString.copyFromUtf8("encrypted-key-2")
          encryptedEventData = ByteString.copyFromUtf8("encrypted-data-2")
        },
        encryptedMatchedEvent {
          encryptedJoinKey = ByteString.copyFromUtf8("encrypted-key-3")
          encryptedEventData = ByteString.copyFromUtf8("encrypted-data-3")
        },
      )

    val rows =
      expectedEvents.map { event ->
        createMockRow(event.encryptedJoinKey.toByteArray(), event.encryptedEventData.toByteArray())
      }

    setupMockBigQueryResponse(rows = rows)

    // When
    val result = task.execute(emptyMap())
    val actualEvents = parseDelimitedEvents(result["encrypted-events"]!!.toList())

    // Then
    assertThat(actualEvents).hasSize(3)
    actualEvents.forEachIndexed { index, actualEvent ->
      assertThat(actualEvent).isEqualTo(expectedEvents[index])
    }
  }

  @Test
  fun `handles large result sets`() = runBlockingTest {
    // Given
    val task =
      ReadEncryptedEventsFromBigQueryTask(
        projectId = PROJECT_ID,
        datasetId = DATASET_ID,
        tableOrViewId = TABLE_OR_VIEW_ID,
        exchangeDate = EXCHANGE_DATE,
        bigQueryServiceFactory = mockBigQueryServiceFactory,
      )

    val numberOfRows = 1_000
    val rows =
      (0 until numberOfRows).map { i ->
        createMockRow(
          ByteString.copyFromUtf8("key-$i").toByteArray(),
          ByteString.copyFromUtf8("data-$i").toByteArray(),
        )
      }

    setupMockBigQueryResponse(rows = rows)

    // When
    val result = task.execute(emptyMap())
    val events = parseDelimitedEvents(result["encrypted-events"]!!.toList())

    // Then
    assertThat(events).hasSize(numberOfRows)
  }

  @Test
  fun `filters by exchange date correctly`() = runBlockingTest {
    // Given
    val specificDate = LocalDate.of(2025, 3, 15)
    val task =
      ReadEncryptedEventsFromBigQueryTask(
        projectId = PROJECT_ID,
        datasetId = DATASET_ID,
        tableOrViewId = TABLE_OR_VIEW_ID,
        exchangeDate = specificDate,
        bigQueryServiceFactory = mockBigQueryServiceFactory,
      )

    setupMockBigQueryResponse()

    // When
    val result = task.execute(emptyMap())
    result["encrypted-events"]!!.toList()

    // Then
    val queryCaptor = argumentCaptor<QueryJobConfiguration>()
    verify(mockBigQuery).query(queryCaptor.capture())
    val query = queryCaptor.firstValue.query

    assertThat(query).contains("WHERE `exchange_date` = DATE('2025-03-15')")
  }

  // ============ Helper Methods ============

  private fun setupMockBigQueryResponse(
    rows: List<FieldValueList> = emptyList(),
    keyColumnName: String = "encrypted_join_key",
    eventDataColumnName: String = "encrypted_event_data",
  ) {
    // Setup schema
    val joinKeyField = Field.of(keyColumnName, StandardSQLTypeName.BYTES)
    val eventDataField = Field.of(eventDataColumnName, StandardSQLTypeName.BYTES)
    val schema = Schema.of(joinKeyField, eventDataField)

    // Setup table result - iterateAll() handles pagination automatically
    whenever(mockTableResult.getSchema()).thenReturn(schema)
    whenever(mockTableResult.iterateAll()).thenReturn(rows)

    // Setup BigQuery
    whenever(mockBigQuery.query(any<QueryJobConfiguration>())).thenReturn(mockTableResult)
  }

  private fun createMockRow(joinKeyBytes: ByteArray?, eventDataBytes: ByteArray?): FieldValueList {
    val mockRow: FieldValueList = mock()

    val mockJoinKeyField: FieldValue = mock()
    val mockEventDataField: FieldValue = mock()

    whenever(mockJoinKeyField.getBytesValue()).thenReturn(joinKeyBytes)
    whenever(mockEventDataField.getBytesValue()).thenReturn(eventDataBytes)

    whenever(mockRow.get("encrypted_join_key")).thenReturn(mockJoinKeyField)
    whenever(mockRow.get("encrypted_event_data")).thenReturn(mockEventDataField)
    whenever(mockRow.get("custom_join_key")).thenReturn(mockJoinKeyField)
    whenever(mockRow.get("custom_event_data")).thenReturn(mockEventDataField)
    whenever(mockRow.get("my_join_key")).thenReturn(mockJoinKeyField)
    whenever(mockRow.get("my_event_data")).thenReturn(mockEventDataField)

    return mockRow
  }

  private fun parseDelimitedEvents(byteStrings: List<ByteString>): List<EncryptedMatchedEvent> {
    return byteStrings.map { bytes -> EncryptedMatchedEvent.parseDelimitedFrom(bytes.newInput()) }
  }
}
