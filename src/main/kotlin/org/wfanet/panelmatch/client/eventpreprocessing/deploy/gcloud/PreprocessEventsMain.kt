// Copyright 2021 The Cross-Media Measurement Authors
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

package org.wfanet.panelmatch.client.eventpreprocessing.deploy.gcloud

import com.google.api.services.bigquery.model.TableFieldSchema
import com.google.api.services.bigquery.model.TableRow
import com.google.api.services.bigquery.model.TableSchema
import com.google.protobuf.kotlin.toByteStringUtf8
import java.util.Base64
import java.util.logging.Logger
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions
import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.PipelineResult
import org.apache.beam.sdk.io.FileIO
import org.apache.beam.sdk.io.FileSystems
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.TypedRead.Method
import org.apache.beam.sdk.options.Default
import org.apache.beam.sdk.options.Description
import org.apache.beam.sdk.options.PipelineOptionsFactory
import org.apache.beam.sdk.options.Validation
import org.apache.beam.sdk.values.PCollection
import org.wfanet.panelmatch.client.common.compression.CreateDefaultCompressionParameters
import org.wfanet.panelmatch.client.eventpreprocessing.HardCodedDeterministicCommutativeCipherKeyProvider
import org.wfanet.panelmatch.client.eventpreprocessing.HardCodedHkdfPepperProvider
import org.wfanet.panelmatch.client.eventpreprocessing.HardCodedIdentifierHashPepperProvider
import org.wfanet.panelmatch.client.eventpreprocessing.JniEventPreprocessor
import org.wfanet.panelmatch.client.eventpreprocessing.PreprocessEventsTransform
import org.wfanet.panelmatch.client.eventpreprocessing.UnprocessedEvent
import org.wfanet.panelmatch.client.eventpreprocessing.unprocessedEvent
import org.wfanet.panelmatch.client.privatemembership.DatabaseEntry
import org.wfanet.panelmatch.common.beam.map
import org.wfanet.panelmatch.common.beam.toSingletonView
import org.wfanet.panelmatch.common.compression.CompressionParameters

interface Options : DataflowPipelineOptions {
  @get:Description("Batch Size") @get:Validation.Required var batchSize: Long

  @get:Description("Cryptokey") @get:Validation.Required var cryptokey: String

  @get:Description("Identifier Hash Pepper")
  @get:Validation.Required
  var identifierHashPepper: String

  @get:Description("HKDF Pepper") @get:Validation.Required var hkdfPepper: String

  @get:Description("Table to read from, specified as <project_id>:<dataset_id>.<table_id>")
  @get:Validation.Required
  var bigQueryInputTable: String

  @get:Description(
    "BigQuery table to write to, specified as <project_id>:<dataset_id>.<table_id>. " +
      "The dataset must already exist."
  )
  @get:Validation.Required
  var bigQueryOutputTable: String

  @get:Description(
    "[Optional] URI to read the compression parameters from (e.g. gs://some-bucket/some/file)"
  )
  @get:Default.String("")
  var compressionParametersUri: String
}

/**
 * Runs relevant DoFns to preprocess events in a condensed main function
 *
 * All logic and transforms featured in this pipeline are thoroughly unit tested
 *
 * To test this pipeline, it must be run on Google Cloud Platform on a machine that also has the
 * cross-media-measurement repository cloned and docker installed.
 *
 * To build the pipeline:
 * ```
 * ../cross-media-measurement/tools/bazel-container build //src/main/kotlin/org/wfanet/panelmatch/client/eventpreprocessing/deploy/gcloud:process_events
 * ```
 *
 * And to run it:
 * ```
 * bazel-bin/src/main/kotlin/org/wfanet/panelmatch/client/eventpreprocessing/deploy/gcloud/process_events --batchSize=SIZE --cryptokey=KEY --hkdfPepper=HKDFPEPPER --identifierHashPepper=IDHPEPPER --bigQueryInputTable=INPUT_TABLE --bigQueryOutputTable=OUTPUT_TABLE --project=PROJECT --runner=dataflow --region=us-central1 --tempLocation=TEMP_LOCATION --defaultWorkerLogLevel=DEBUG
 * ```
 */
fun main(args: Array<String>) {
  val options = makeOptions(args)
  FileSystems.setDefaultPipelineOptions(options)

  val pipeline = Pipeline.create(options)
  val unencryptedEvents = readFromBigQuery(options.bigQueryInputTable, pipeline)

  val compressionParameters =
    if (options.compressionParametersUri.isEmpty()) {
      pipeline.apply(CreateDefaultCompressionParameters()).toSingletonView()
    } else {
      pipeline
        .apply(FileIO.match().filepattern(options.compressionParametersUri))
        .apply(FileIO.readMatches())
        .map { CompressionParameters.parseFrom(it.readFullyAsBytes()) }
        .toSingletonView()
    }

  val encryptedEvents =
    unencryptedEvents.apply(
      "PreprocessEvents",
      PreprocessEventsTransform(
        options.batchSize,
        HardCodedIdentifierHashPepperProvider(options.identifierHashPepper.toByteStringUtf8()),
        HardCodedHkdfPepperProvider(options.hkdfPepper.toByteStringUtf8()),
        HardCodedDeterministicCommutativeCipherKeyProvider(options.cryptokey.toByteStringUtf8()),
        JniEventPreprocessor(),
        compressionParameters,
      ),
    )

  writeToBigQuery(encryptedEvents, options.bigQueryOutputTable)

  val pipelineResult = pipeline.run()
  check(pipelineResult.waitUntilFinish() == PipelineResult.State.DONE)
  logMetrics(pipelineResult)
}

private fun readFromBigQuery(
  inputTable: String,
  pipeline: Pipeline,
): PCollection<UnprocessedEvent> {
  // Build the read options proto for the read operation.
  val rowsFromBigQuery =
    pipeline.apply(
      "Read Unencrypted Events",
      BigQueryIO.readTableRows()
        .from(inputTable)
        .withMethod(Method.DIRECT_READ)
        .withSelectedFields(mutableListOf("UserId", "UserEvent")),
    )
  // Convert TableRow to KV<Long,ByteString>
  return rowsFromBigQuery.map(name = "Map to ByteStrings") {
    unprocessedEvent {
      id = (it["UserId"] as String).toByteStringUtf8()
      data = (it["UserEvent"] as String).toByteStringUtf8()
    }
  }
}

private fun writeToBigQuery(encryptedEvents: PCollection<DatabaseEntry>, outputTable: String) {
  // Build the table schema for the output table.
  val fields =
    listOf<TableFieldSchema>(
      TableFieldSchema().setName("EncryptedId").setType("INT64"),
      TableFieldSchema().setName("EncryptedData").setType("BYTES"),
    )
  val schema = TableSchema().setFields(fields)

  // Convert KV<Long,ByteString> to TableRow
  val encryptedTableRows =
    encryptedEvents.map(name = "Map to TableRows") {
      TableRow()
        .set("EncryptedId", it.lookupKey.key)
        .set("EncryptedData", Base64.getEncoder().encode(it.encryptedEntry.data.toByteArray()))
    }

  // Write to BigQueryIO
  encryptedTableRows.apply(
    "Write Encrypted Events",
    BigQueryIO.writeTableRows()
      .to(outputTable)
      .withSchema(schema)
      .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
      .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE),
  )
}

private fun makeOptions(args: Array<String>): Options {
  return PipelineOptionsFactory.fromArgs(*args).withValidation().`as`(Options::class.java)
}

private fun logMetrics(pipelineResult: PipelineResult) {
  val metrics = pipelineResult.metrics().allMetrics()
  val logger = Logger.getLogger("PreprocessEventsMain")
  for (metric in metrics.distributions) {
    with(metric.attempted) {
      logger.info(
        "Distribution '${metric.name}': count=$count, min=$min, mean=$mean, max=$max, sum=$sum"
      )
    }
  }
  for (metric in metrics.counters) {
    logger.info("Counter '${metric.name}': ${metric.attempted}")
  }
  for (metric in metrics.gauges) {
    logger.info("Gauge '${metric.name}': ${metric.attempted.value}")
  }
}
