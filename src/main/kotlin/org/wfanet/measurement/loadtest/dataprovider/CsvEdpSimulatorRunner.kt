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

import java.io.File
import kotlin.properties.Delegates
import org.wfanet.measurement.common.commandLineMain
import org.wfanet.measurement.loadtest.config.EventGroupMetadata
import picocli.CommandLine

@CommandLine.Command(
  name = "CsvEdpSimulatorRunner",
  description = ["EdpSimulator Daemon"],
  mixinStandardHelpOptions = true,
  showDefaultValues = true,
)
/** Implementation of [EdpSimulatorRunner] using [CsvEventQuery]. */
class CsvEdpSimulatorRunner : EdpSimulatorRunner() {
  @CommandLine.Option(
    names = ["--events-csv"],
    description =
      ["Path to a CSV file specifying the event data that will be returned by this simulator."],
    required = true,
  )
  lateinit var eventsCsv: File
    private set

  @set:CommandLine.Option(
    names = ["--publisher-id"],
    description = ["ID of the publisher within the test dataset"],
    required = true,
  )
  var publisherId by Delegates.notNull<Int>()
    private set

  override fun run() {
    val eventQuery = CsvEventQuery(publisherId, eventsCsv)
    run(eventQuery, mapOf("" to EventGroupMetadata.testMetadata(publisherId)))
  }
}

fun main(args: Array<String>) = commandLineMain(CsvEdpSimulatorRunner(), args)
