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

import org.wfanet.measurement.common.commandLineMain
import org.wfanet.measurement.gcloud.gcs.GcsFromFlags
import org.wfanet.measurement.gcloud.gcs.GcsStorageClient
import org.wfanet.measurement.loadtest.storage.SketchStore
import picocli.CommandLine

@CommandLine.Command(
  name = "GcsEdpSimulatorRunner",
  description = ["${EdpSimulator.DAEMON_NAME} Daemon"],
  mixinStandardHelpOptions = true,
  showDefaultValues = true
)
/** Implementation of [EdpSimulator] using the File System to store blobs. */
class GcsEdpSimulatorRunner : EdpSimulator() {
  @CommandLine.Mixin private lateinit var gcsFlags: GcsFromFlags.Flags

  override val sketchStore = SketchStore(GcsStorageClient.fromFlags(GcsFromFlags(gcsFlags)))
}

fun main(args: Array<String>) = commandLineMain(GcsEdpSimulatorRunner(), args)
