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

package org.wfanet.measurement.loadtest.edpaggregator.tools

import com.google.api.gax.core.FixedExecutorProvider
import com.google.cloud.bigquery.BigQuery
import com.google.cloud.bigquery.QueryJobConfiguration
import com.google.cloud.bigquery.QueryParameterValue
import com.google.cloud.bigquery.storage.v1.AppendRowsRequest
import com.google.cloud.bigquery.storage.v1.BigQueryWriteClient
import com.google.cloud.bigquery.storage.v1.Exceptions.AppendSerializationError
import com.google.cloud.bigquery.storage.v1.ProtoRows
import com.google.cloud.bigquery.storage.v1.ProtoSchemaConverter
import com.google.cloud.bigquery.storage.v1.StreamWriter
import com.google.cloud.bigquery.storage.v1.TableName
import com.google.rpc.Code
import java.util.concurrent.Executors
import java.util.logging.Logger
import org.measurement.integration.k8s.testing.HighOverlapImpressionRow
import org.measurement.integration.k8s.testing.highOverlapImpressionRow
import org.wfanet.measurement.api.v2alpha.event_templates.testing.v1.TestEvent
import org.wfanet.measurement.loadtest.dataprovider.EntityKey
import org.wfanet.measurement.loadtest.dataprovider.LabeledEvent

/**
 * Writes high overlap impressions to BigQuery as [HighOverlapImpressionRow]s.
 *
 * [add] is called for each impression as the impressions writer pulls it, so rows mirror what is
 * written to storage. Rows are buffered and appended in batches; [close] flushes the remainder.
 *
 * Not thread safe: the impressions writer consumes one event group at a time.
 */
class HighOverlapImpressionExporter(
  private val bigQuery: BigQuery,
  writeClient: BigQueryWriteClient,
  projectId: String,
  private val datasetId: String,
  private val tableId: String,
  private val modelLine: String,
  private val specFingerprint: String,
) : AutoCloseable {
  private val streamWriter: StreamWriter =
    StreamWriter.newBuilder(
        StreamWriter.getDefaultStreamName(TableName.of(projectId, datasetId, tableId)),
        writeClient,
      )
      .setExecutorProvider(FixedExecutorProvider.create(Executors.newScheduledThreadPool(1)))
      .setEnableConnectionPool(true)
      .setDefaultMissingValueInterpretation(
        AppendRowsRequest.MissingValueInterpretation.DEFAULT_VALUE
      )
      .setWriterSchema(ProtoSchemaConverter.convert(HighOverlapImpressionRow.getDescriptor()))
      .build()

  private var batch: ProtoRows.Builder = ProtoRows.newBuilder()
  private var batchBytes = 0

  var rowCount: Long = 0L
    private set

  /** Removes any rows a previous seeding of [modelLine] left behind. */
  fun deleteExistingRows() {
    val query =
      QueryJobConfiguration.newBuilder(
          "DELETE FROM `$datasetId.$tableId` WHERE model_line = @model_line"
        )
        .addNamedParameter("model_line", QueryParameterValue.string(modelLine))
        .build()
    val deleted = bigQuery.query(query).totalRows
    logger.info("Deleted $deleted existing BigQuery row(s) for $modelLine")
  }

  fun add(
    edpName: String,
    logicalGroup: String,
    entityKey: EntityKey,
    event: LabeledEvent<TestEvent>,
  ) {
    val row = highOverlapImpressionRow {
      modelLine = this@HighOverlapImpressionExporter.modelLine
      specFingerprint = this@HighOverlapImpressionExporter.specFingerprint
      this.edpName = edpName
      this.logicalGroup = logicalGroup
      entityType = entityKey.entityType
      entityId = entityKey.entityId
      cmmsEventGroupReferenceId = "${entityKey.entityType}-${entityKey.entityId}"
      vid = event.vid
      eventTime = event.timestamp.epochSecond * MICROS_PER_SECOND + event.timestamp.nano / 1_000
      gender = event.message.common.gender.name
      ageGroup = event.message.common.ageGroup.name
      usState = event.message.common.usState.name
      signedIn = event.message.common.signedIn
      videoCompletedFraction = event.message.video.completedFraction
      videoViewableFraction = event.message.video.viewableFraction
      displayViewableFraction = event.message.display.viewableFraction
    }
    val serialized = row.toByteString()
    batch.addSerializedRows(serialized)
    batchBytes += serialized.size()
    rowCount++
    if (batch.serializedRowsCount >= MAX_BATCH_ROWS || batchBytes >= MAX_BATCH_BYTES) {
      flush()
    }
  }

  override fun close() {
    try {
      flush()
    } finally {
      streamWriter.close()
    }
    logger.info("Exported $rowCount high overlap impression row(s) to $datasetId.$tableId")
  }

  private fun flush() {
    if (batch.serializedRowsCount == 0) {
      return
    }
    appendWithRetries(batch.build())
    batch = ProtoRows.newBuilder()
    batchBytes = 0
  }

  private fun appendWithRetries(protoRows: ProtoRows) {
    for (attempt in 1..RETRY_COUNT) {
      try {
        val response = streamWriter.append(protoRows).get()
        if (!response.hasError()) {
          return
        }
        check(response.error.code == Code.INTERNAL.number) {
          "Cannot retry failed append: ${response.error}"
        }
        check(attempt < RETRY_COUNT) { "Too many retries appending to $datasetId.$tableId" }
      } catch (e: AppendSerializationError) {
        for (message in e.rowIndexToErrorMessage.values) {
          logger.warning(message)
        }
        throw e
      }
    }
  }

  companion object {
    private val logger: Logger = Logger.getLogger(this::class.java.name)

    private const val MICROS_PER_SECOND = 1_000_000L
    private const val RETRY_COUNT = 3

    // The AppendRows request limit is 10 MiB; leave headroom for request overhead.
    private const val MAX_BATCH_ROWS = 20_000
    private const val MAX_BATCH_BYTES = 8 * 1024 * 1024
  }
}
