// Copyright 2020 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.duchy.deploy.gcloud.daemon.mill.liquidlegionsv1

import org.wfanet.measurement.common.commandLineMain
import org.wfanet.measurement.duchy.deploy.common.daemon.mill.liquidlegionsv1.LiquidLegionsV1MillDaemon
import org.wfanet.measurement.gcloud.gcs.GcsFromFlags
import org.wfanet.measurement.gcloud.gcs.GcsStorageClient
import picocli.CommandLine

@CommandLine.Command(
  name = "GcsLiquidLegionsV1MillDaemon",
  description = ["Liquid Legions V1 Mill daemon."],
  mixinStandardHelpOptions = true,
  showDefaultValues = true
)
class GcsLiquidLegionsV1MillDaemon : LiquidLegionsV1MillDaemon() {
  @CommandLine.Mixin
  private lateinit var gcsFlags: GcsFromFlags.Flags

  override fun run() {
    val gcs = GcsFromFlags(gcsFlags)
    run(GcsStorageClient.fromFlags(gcs))
  }
}

fun main(args: Array<String>) = commandLineMain(GcsLiquidLegionsV1MillDaemon(), args)
