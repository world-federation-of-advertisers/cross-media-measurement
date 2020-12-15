// Copyright 2020 The Measurement System Authors
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

package org.wfanet.measurement.duchy.deploy.gcloud.server

import org.wfanet.measurement.common.commandLineMain
import org.wfanet.measurement.duchy.deploy.common.server.ComputationControlServer
import org.wfanet.measurement.gcloud.gcs.GcsFromFlags
import org.wfanet.measurement.gcloud.gcs.GcsStorageClient
import picocli.CommandLine

/**
 * Implementation of [ComputationControlServer] using Google Cloud Storage (GCS).
 */
@CommandLine.Command(
  name = "GcsComputationControlServer",
  description = [
    "Server daemon for ${ComputationControlServer.SERVICE_NAME} service."
  ],
  mixinStandardHelpOptions = true,
  showDefaultValues = true
)
class GcsComputationControlServer : ComputationControlServer() {
  @CommandLine.Mixin
  private lateinit var gcsFlags: GcsFromFlags.Flags

  override fun run() {
    val gcs = GcsFromFlags(gcsFlags)
    run(GcsStorageClient.fromFlags(gcs))
  }
}

fun main(args: Array<String>) =
  commandLineMain(GcsComputationControlServer(), args)
