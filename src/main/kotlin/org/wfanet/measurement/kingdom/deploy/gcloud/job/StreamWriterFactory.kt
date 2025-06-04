/*
 * Copyright 2024 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.kingdom.deploy.gcloud.job

import com.google.api.gax.core.FixedExecutorProvider
import com.google.api.gax.rpc.ApiException
import com.google.api.gax.rpc.StatusCode
import com.google.cloud.bigquery.storage.v1.AppendRowsRequest
import com.google.cloud.bigquery.storage.v1.BigQueryWriteClient
import com.google.cloud.bigquery.storage.v1.ProtoSchema
import com.google.cloud.bigquery.storage.v1.StreamWriter
import com.google.cloud.bigquery.storage.v1.TableName
import com.google.cloud.bigquery.storage.v1.WriteStream
import com.google.cloud.bigquery.storage.v1.WriteStreamName
import java.util.concurrent.Executors

fun interface StreamWriterFactory {
  fun create(
    projectId: String,
    datasetId: String,
    tableId: String,
    streamId: String?,
    client: BigQueryWriteClient,
    protoSchema: ProtoSchema,
  ): StreamWriter
}

class StreamWriterFactoryImpl : StreamWriterFactory {
  override fun create(
    projectId: String,
    datasetId: String,
    tableId: String,
    streamId: String?,
    client: BigQueryWriteClient,
    protoSchema: ProtoSchema,
  ): StreamWriter {
    val tableName = TableName.of(projectId, datasetId, tableId)

    val streamWriterBuilder =
      if (streamId == null) {
        StreamWriter.newBuilder(StreamWriter.getDefaultStreamName(tableName), client)
      } else {
        val writeStreamName = WriteStreamName.of(projectId, datasetId, tableId, streamId)

        // Stream can be closed if nothing is appended after 3 days
        try {
          client.getWriteStream(writeStreamName)
        } catch (e: ApiException) {
          if (e.statusCode.code.equals(StatusCode.Code.NOT_FOUND)) {
            val writeStream = WriteStream.newBuilder().setType(WriteStream.Type.COMMITTED).setName(writeStreamName.toString()).build()
            client.createWriteStream(tableName, writeStream)
          }

          throw e
        }
        StreamWriter.newBuilder(writeStreamName.toString(), client)
      }

    return streamWriterBuilder
      .setExecutorProvider(FixedExecutorProvider.create(Executors.newScheduledThreadPool(1)))
      .setEnableConnectionPool(true)
      .setDefaultMissingValueInterpretation(
        AppendRowsRequest.MissingValueInterpretation.DEFAULT_VALUE
      )
      .setWriterSchema(protoSchema)
      .build()
  }
}
