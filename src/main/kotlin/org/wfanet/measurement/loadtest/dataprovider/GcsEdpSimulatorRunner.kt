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

package org.wfanet.measurement.loadtest.dataprovider

import com.google.cloud.bigquery.BigQueryOptions
import org.wfanet.measurement.common.commandLineMain
import org.wfanet.measurement.gcloud.gcs.GcsFromFlags
import org.wfanet.measurement.gcloud.gcs.GcsStorageClient
import picocli.CommandLine

@CommandLine.Command(
  name = "GcsEdpSimulatorRunner",
  description = ["EdpSimulator Daemon"],
  mixinStandardHelpOptions = true,
  showDefaultValues = true
)
/** Implementation of [EdpSimulator] using the File System to store blobs. */
class GcsEdpSimulatorRunner : EdpSimulatorRunner() {
  @CommandLine.Mixin private lateinit var gcsFlags: GcsFromFlags.Flags

  @CommandLine.Option(
    names = ["--big-query-project-name"],
    description = ["The project name of the big query to be used in this EDP simulator."],
    required = true
  )
  lateinit var bigQueryProjectName: String
    private set

  @CommandLine.Option(
    names = ["--big-query-table-name"],
    description = ["The project name of the big query to be used in this EDP simulator."],
    required = true
  )
  lateinit var bigQueryTableName: String
    private set

  override fun run() {
    val gcs = GcsFromFlags(gcsFlags)
    val bigQuery =
      BigQueryOptions.newBuilder().apply { setProjectId(bigQueryProjectName) }.build().service

    run(
      GcsStorageClient.fromFlags(gcs),
      BigQueryEventQuery(flags.dataProviderDisplayName, bigQuery, bigQueryTableName)
    )
  }
}

fun main(args: Array<String>) = commandLineMain(GcsEdpSimulatorRunner(), args)
