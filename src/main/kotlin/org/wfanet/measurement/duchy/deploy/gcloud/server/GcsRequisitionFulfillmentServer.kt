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

package org.wfanet.measurement.duchy.deploy.gcloud.server

import org.wfanet.measurement.common.commandLineMain
import org.wfanet.measurement.duchy.deploy.common.server.RequisitionFulfillmentServer
import org.wfanet.measurement.gcloud.gcs.GcsFromFlags
import org.wfanet.measurement.gcloud.gcs.GcsStorageClient
import picocli.CommandLine

/** Implementation of [RequisitionFulfillmentServer] using Google Cloud Storage (GCS). */
@CommandLine.Command(
  name = "GcsRequisitionFulfillmentServer",
  description = ["Server daemon for ${RequisitionFulfillmentServer.SERVICE_NAME} service."],
  mixinStandardHelpOptions = true,
  showDefaultValues = true
)
class GcsRequisitionFulfillmentServer : RequisitionFulfillmentServer() {
  @CommandLine.Mixin private lateinit var gcsFlags: GcsFromFlags.Flags

  override fun run() {
    val gcs = GcsFromFlags(gcsFlags)
    run(GcsStorageClient.fromFlags(gcs))
  }
}

fun main(args: Array<String>) = commandLineMain(GcsRequisitionFulfillmentServer(), args)
