// Copyright 2024 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.duchy.deploy.gcloud.job.mill.shareshuffle

import org.wfanet.measurement.common.commandLineMain
import org.wfanet.measurement.duchy.deploy.common.job.mill.shareshuffle.HonestMajorityShareShuffleMillJob
import org.wfanet.measurement.gcloud.gcs.GcsFromFlags
import org.wfanet.measurement.gcloud.gcs.GcsStorageClient
import picocli.CommandLine

@CommandLine.Command(
  name = "GcsHonestMajorityShareShuffleMillJob",
  description = ["Honest Majority Share Shuffle Mill"],
  mixinStandardHelpOptions = true,
  showDefaultValues = true,
)
class GcsHonestMajorityShareShuffleMillJob : HonestMajorityShareShuffleMillJob() {
  @CommandLine.Mixin private lateinit var gcsFlags: GcsFromFlags.Flags

  override fun run() {
    val gcs = GcsFromFlags(gcsFlags)
    run(GcsStorageClient.fromFlags(gcs))
  }
}

fun main(args: Array<String>) = commandLineMain(GcsHonestMajorityShareShuffleMillJob(), args)
