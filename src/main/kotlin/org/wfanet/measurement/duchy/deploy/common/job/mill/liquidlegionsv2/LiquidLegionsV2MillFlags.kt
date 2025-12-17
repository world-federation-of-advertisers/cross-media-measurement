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

package org.wfanet.measurement.duchy.deploy.common.job.mill.liquidlegionsv2

import kotlin.properties.Delegates
import org.wfanet.measurement.common.identity.DuchyInfoFlags
import org.wfanet.measurement.duchy.deploy.common.CommonDuchyFlags
import org.wfanet.measurement.duchy.deploy.common.ComputationsServiceFlags
import org.wfanet.measurement.duchy.deploy.common.SystemApiFlags
import org.wfanet.measurement.duchy.mill.ClaimedComputationFlags
import org.wfanet.measurement.duchy.mill.MillFlags
import picocli.CommandLine

class LiquidLegionsV2MillFlags : MillFlags() {
  @CommandLine.Mixin
  lateinit var duchy: CommonDuchyFlags
    private set

  @CommandLine.Mixin
  lateinit var duchyInfoFlags: DuchyInfoFlags
    private set

  @CommandLine.Mixin
  lateinit var systemApiFlags: SystemApiFlags
    private set

  @CommandLine.Mixin
  lateinit var computationsServiceFlags: ComputationsServiceFlags
    private set

  @CommandLine.ArgGroup(exclusive = false, heading = "Claimed Computation Flags.%n")
  lateinit var claimedComputationFlags: ClaimedComputationFlags
    private set

  @set:CommandLine.Option(
    names = ["--parallelism"],
    description = ["Maximum number of threads used in crypto actions"],
    defaultValue = "1",
  )
  var parallelism by Delegates.notNull<Int>()
    private set
}
