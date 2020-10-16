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

package org.wfanet.measurement.loadtest

import org.wfanet.measurement.common.commandLineMain
import org.wfanet.measurement.gcloud.gcs.GcsFromFlags
import org.wfanet.measurement.gcloud.gcs.GcsStorageClient
import picocli.CommandLine

@CommandLine.Command(
  name = "run_gcs_correctness",
  mixinStandardHelpOptions = true,
  showDefaultValues = true
)
private class GcsCorrectnessRunner : CorrectnessRunner() {
  @CommandLine.Mixin
  private lateinit var gcsFlags: GcsFromFlags.Flags

  override fun run() {
    run(GcsStorageClient.fromFlags(GcsFromFlags(gcsFlags)))
  }
}

fun main(args: Array<String>) = commandLineMain(GcsCorrectnessRunner(), args)
