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
import com.google.protobuf.ByteString
import java.util.Base64
import java.util.logging.Logger
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions
import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.PipelineResult
import org.apache.beam.sdk.io.FileIO
import org.apache.beam.sdk.io.FileIO.Write.FileNaming
import org.apache.beam.sdk.io.FileSystems
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.TypedRead.Method
import org.apache.beam.sdk.options.Description
import org.apache.beam.sdk.options.PipelineOptionsFactory
import org.apache.beam.sdk.options.Validation
import org.apache.beam.sdk.values.KV
import org.apache.beam.sdk.values.PCollection
import org.wfanet.panelmatch.client.common.BrotliEventCompressorTrainer
import org.wfanet.panelmatch.client.eventpreprocessing.HardCodedDeterministicCommutativeCipherKeyProvider
import org.wfanet.panelmatch.client.eventpreprocessing.HardCodedHkdfPepperProvider
import org.wfanet.panelmatch.client.eventpreprocessing.HardCodedIdentifierHashPepperProvider
import org.wfanet.panelmatch.client.eventpreprocessing.preprocessEventsInPipeline
import org.wfanet.panelmatch.common.beam.kvOf
import org.wfanet.panelmatch.common.beam.map
import org.wfanet.panelmatch.common.toByteString

interface Options : DataflowPipelineOptions {
  @get:Description("Batch Size") @get:Validation.Required var batchSize: Int

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
    "File path to write the compression dictionary to (e.g. gs://some-bucket/some/file)"
  )
  @get:Validation.Required
  var dictionaryOutputPath: String
}
/**
 * Runs relevant DoFns to preprocess events in a condensed main function
 *
 * All logic and transforms featured in this pipeline are thoroughly unit tested
 *
 * To test this pipeline, it must be run on Google Cloud Platform on a machine that also has the
 * cross-media-measurement repository cloned and docker installed. The command to run the pipeline
 * on GCP is:
 *
 * ```
 * ../cross-media-measurement/tools/bazel-container build //src/main/kotlin/org/wfanet/panelmatch/client/eventpreprocessing/deploy/gcloud:process_events && bazel-bin/src/main/kotlin/org/wfanet/panelmatch/client/eventpreprocessing/deploy/gcloud/process_events '--batchSize=SIZE' '--cryptokey=KEY' '--hkdfPepper=HKDFPEPPER' '--identifierHashPepper=IDHPEPPER' '--bigQueryInputTable=INPUT_TABLE' '--bigQueryOutputTable=OUTPUT_TABLE' '--dictionaryGcsPath=DICTIONARY_OUTPUT_PATH' '--project=PROJECT' '--runner=dataflow' '--region=us-central1' '--tempLocation=TEMP_LOCATION' '--defaultWorkerLogLevel=DEBUG'
 * ```
 *
 * Where SIZE is the desired batch size, KEY is the desired crypto key, IDHPEPPER is the desired
 * Identifier Hash pepper, HKDFPEPPER is the desired HKDF pepper, INPUT_TABLE is the BigQuery table
 * to read from, OUTPUT_TABLE is the BigQuery table to write to, DICTIONARY_OUTPUT_PATH is the path
 * to write the compression dictionary to, PROJECT is the project name, and TEMP_LOCATION is the
 * desired location to store temp files. Performance and outputs can be tracked on the GCP console.
 */
fun main(args: Array<String>) {
  val options = makeOptions(args)
  FileSystems.setDefaultPipelineOptions(options)

  val pipeline = Pipeline.create(options)
  val unencryptedEvents = readFromBigQuery(options.bigQueryInputTable, pipeline)
  val (encryptedEvents, dictionary) =
    preprocessEventsInPipeline(
      unencryptedEvents,
      options.batchSize,
      HardCodedIdentifierHashPepperProvider(options.identifierHashPepper.toByteString()),
      HardCodedHkdfPepperProvider(options.hkdfPepper.toByteString()),
      HardCodedDeterministicCommutativeCipherKeyProvider(options.cryptokey.toByteString()),
      BrotliEventCompressorTrainer()
    )

  writeToBigQuery(encryptedEvents, options.bigQueryOutputTable)

  val dictionaryFileNaming = FileNaming { _, _, _, _, _ -> options.dictionaryOutputPath }
  dictionary.apply(
    "Write",
    FileIO.write<ByteString>().withNumShards(1).withNaming(dictionaryFileNaming)
  )

  val pipelineResult = pipeline.run()
  check(pipelineResult.waitUntilFinish() == PipelineResult.State.DONE)
  logMetrics(pipelineResult)
}

private fun readFromBigQuery(
  inputTable: String,
  pipeline: Pipeline
): PCollection<KV<ByteString, ByteString>> {
  // Build the read options proto for the read operation.
  val rowsFromBigQuery =
    pipeline.apply(
      "Read Unencrypted Events",
      BigQueryIO.readTableRows()
        .from(inputTable)
        .withMethod(Method.DIRECT_READ)
        .withSelectedFields(mutableListOf("UserId", "UserEvent"))
    )
  // Convert TableRow to KV<Long,ByteString>
  return rowsFromBigQuery.map(name = "Map to ByteStrings") {
    kvOf((it["UserId"] as String).toByteString(), (it["UserEvent"] as String).toByteString())
  }
}

private fun writeToBigQuery(
  encryptedEvents: PCollection<KV<Long, ByteString>>,
  outputTable: String
) {
  // Build the table schema for the output table.
  val fields =
    listOf<TableFieldSchema>(
      TableFieldSchema().setName("EncryptedId").setType("INT64"),
      TableFieldSchema().setName("EncryptedData").setType("BYTES")
    )
  val schema = TableSchema().setFields(fields)

  // Convert KV<Long,ByteString> to TableRow
  val encryptedTableRows =
    encryptedEvents.map(name = "Map to TableRows") {
      TableRow()
        .set("EncryptedId", it.key)
        .set("EncryptedData", Base64.getEncoder().encode(it.value.toByteArray()))
    }

  // Write to BigQueryIO
  encryptedTableRows.apply(
    "Write Encrypted Events",
    BigQueryIO.writeTableRows()
      .to(outputTable)
      .withSchema(schema)
      .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
      .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE)
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
