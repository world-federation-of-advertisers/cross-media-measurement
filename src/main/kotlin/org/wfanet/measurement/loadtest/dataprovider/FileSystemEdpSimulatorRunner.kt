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

import java.time.Clock
import kotlinx.coroutines.runBlocking
import org.wfanet.measurement.common.commandLineMain
import org.wfanet.measurement.common.throttler.MinimumIntervalThrottler
import org.wfanet.measurement.storage.filesystem.FileSystemStorageClient
import picocli.CommandLine

@CommandLine.Command(
  name = "FileSystemEdpSimulatorRunner",
  description = ["Server daemon for ${EdpSimulator.SERVICE_NAME} service."],
  mixinStandardHelpOptions = true,
  showDefaultValues = true
)
class FileSystemEdpSimulatorRunner : EdpSimulator() {
  override fun run() {
    val throttler = MinimumIntervalThrottler(Clock.systemUTC(), flags.throttlerMinimumInterval)

    val storageClient = FileSystemStorageClient(createTempDir())
    val workflow =
      RequisitionFulfillmentWorkflow(
        flags.externalDataProviderId,
        flags.sketchConfig,
        flags.requisitionsStub,
        flags.requisitionFulfillmentStub,
        storageClient,
      )

    runBlocking { throttler.loopOnReady { workflow.execute() } }
  }
}

fun main(args: Array<String>) = commandLineMain(FileSystemEdpSimulatorRunner(), args)
