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
import org.wfanet.measurement.api.v2alpha.EventGroup
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.SyntheticEventGroupSpec
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.SyntheticPopulationSpec
import org.wfanet.measurement.common.commandLineMain
import org.wfanet.measurement.common.parseTextProto
import picocli.CommandLine

@CommandLine.Command(
  name = "SyntheticGeneratorEdpSimulatorRunner",
  description = ["EdpSimulator Daemon"],
  mixinStandardHelpOptions = true,
  showDefaultValues = true
)
/** Implementation of [EdpSimulatorRunner] using [SyntheticGeneratorEventQuery]. */
class SyntheticGeneratorEdpSimulatorRunner : EdpSimulatorRunner() {
  @CommandLine.Option(
    names = ["--population-spec"],
    description = ["Path to SyntheticPopulationSpec message in text format."],
    required = true,
  )
  private lateinit var populationSpecFile: File

  @CommandLine.Option(
    names = ["--event-group-spec"],
    description = ["Path to SyntheticEventGroupSpec message in text format."],
    required = true,
  )
  private lateinit var eventGroupSpecFile: File

  override fun run() {
    val populationSpec =
      parseTextProto(populationSpecFile, SyntheticPopulationSpec.getDefaultInstance())
    val eventGroupSpec =
      parseTextProto(eventGroupSpecFile, SyntheticEventGroupSpec.getDefaultInstance())

    val eventQuery =
      object : SyntheticGeneratorEventQuery(populationSpec) {
        override fun getSyntheticDataSpec(eventGroup: EventGroup): SyntheticEventGroupSpec {
          return eventGroupSpec
        }
      }

    run(eventQuery, eventGroupSpec)
  }
}

fun main(args: Array<String>) = commandLineMain(SyntheticGeneratorEdpSimulatorRunner(), args)
