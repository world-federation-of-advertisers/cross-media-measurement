/*
 * Copyright 2024 The Cross-Media Measurement Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.wfanet.measurement.kingdom.deploy.common.job

import java.io.File
import kotlin.time.Duration
import kotlin.time.Duration.Companion.days
import kotlin.time.Duration.Companion.hours
import org.wfanet.measurement.common.commandLineMain
import org.wfanet.measurement.common.grpc.TlsFlags
import org.wfanet.measurement.kingdom.batch.ProberMeasurementCreation
import org.wfanet.measurement.kingdom.deploy.common.server.KingdomApiServerFlags
import picocli.CommandLine.Command
import picocli.CommandLine.Mixin
import picocli.CommandLine.Option

private class ProberMeasurementCreationFlags {
  @Option(
    names = ["--measurement-consumer"],
    description = ["API resource name of the MeasurementConsumer"],
    required = true,
  )
  lateinit var measurementConsumer: String
    private set

  @Option(
    names = ["--private-key-der-file"],
    description = ["Private key for MeasurementConsumer"],
    required = true,
  )
  lateinit var privateKeyDerFile: File
    private set

  @Option(
    names = ["--api-key"],
    description = ["API authentication key for the MeasurementConsumer"],
    required = true,
  )
  lateinit var apiAuthenticationKey: String
    private set

  @Mixin
  lateinit var tlsFlags: TlsFlags
    private set

  @Mixin
  lateinit var kingdomApiServerFlags: KingdomApiServerFlags
    private set

  @Option(
    names = ["--data-provider"],
    description = ["Data provider API resource name (can be specified multiple times)"],
    required = true,
    arity = "1..*",
  )
  lateinit var dataProvider: List<String>
    private set

  @Option(
    names = ["--simulator-event-group-name"],
    description =
      [
        "QA event group name to use for requisitions and measurements. This identifies that a prober is being launched in a QA environment"
      ],
    required = false,
  )
  lateinit var simulatorEventGroupName: String
    private set

  @Option(
    names = ["--measurement-lookback-duration"],
    description =
      [
        "Subtracted from the current time, specifies the start time for the interval of event data collection"
      ],
    required = true,
  )
  var measurementLookbackDuration: Duration = 1.days
    private set

  @Option(
    names = ["--duration-between-measurements"],
    description =
      [
        "Added to the update time of the most recently completed measurement, determines whether enough time has elapsed to request a new measurement"
      ],
    required = true,
  )
  var durationBetweenMeasurement: Duration = 1.hours
    private set
}

@Command(
  name = "MeasurementSystemProberJob",
  mixinStandardHelpOptions = true,
  showDefaultValues = true,
)
private fun run(@Mixin flags: ProberMeasurementCreationFlags) {
  val proberMeasurementCreation = ProberMeasurementCreation()
  proberMeasurementCreation.run()
}

fun main(args: Array<String>) = commandLineMain(::run, args)
