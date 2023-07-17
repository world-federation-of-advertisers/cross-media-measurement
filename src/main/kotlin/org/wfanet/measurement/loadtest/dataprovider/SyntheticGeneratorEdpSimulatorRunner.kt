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

import org.wfanet.measurement.api.v2alpha.EventGroup
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.SyntheticEventGroupSpec
import org.wfanet.measurement.common.commandLineMain
import org.wfanet.measurement.loadtest.config.EventGroupMetadata
import picocli.CommandLine

@CommandLine.Command(
  name = "SyntheticGeneratorEdpSimulatorRunner",
  description = ["EdpSimulator Daemon"],
  mixinStandardHelpOptions = true,
  showDefaultValues = true
)
/** Implementation of [EdpSimulatorRunner] using [SyntheticGeneratorEventQuery]. */
class SyntheticGeneratorEdpSimulatorRunner : EdpSimulatorRunner() {

  override fun run() {
    val eventQuery =
      object : SyntheticGeneratorEventQuery(EventGroupMetadata.UK_POPULATION) {
        override fun getSyntheticDataSpec(eventGroup: EventGroup): SyntheticEventGroupSpec {
          return EventGroupMetadata.SYNTHETIC_DATA_SPEC
        }
      }

    run(eventQuery, EventGroupMetadata.SYNTHETIC_DATA_SPEC)
  }
}

fun main(args: Array<String>) = commandLineMain(SyntheticGeneratorEdpSimulatorRunner(), args)
