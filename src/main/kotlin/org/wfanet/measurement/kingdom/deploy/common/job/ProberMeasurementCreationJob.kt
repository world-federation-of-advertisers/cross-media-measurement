package org.wfanet.measurement.kingdom.deploy.common.job

import java.io.File
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

  @Option(
    names = ["--private-key-der-file"],
    description = ["Private key for MeasurementConsumer"],
    required = true,
  )
  lateinit var privateKeyDerFile: File

  @Option(
    names = ["--api-key"],
    description = ["API authentication key for the MeasurementConsumer"],
    required = true,
  )
  lateinit var apiAuthenticationKey: String
    private set

  @Mixin lateinit var tlsFlags: TlsFlags

  @Mixin lateinit var kingdomApiServerFlags: KingdomApiServerFlags

  @Option(
    names = ["--data-providers"],
    description = ["All data provider API resource names"],
    required = true,
  )
  lateinit var dataProviders: List<String>
    private set

  @Option(
    names = ["--simulator-event-group-name"],
    description = ["QA event group name to use for requisitions and measurements"],
    required = false,
  )
  lateinit var simulatorEventGroupName: String

  @Option(
    names = ["--measurement-lookback-duration"],
    description =
      [
        "Subtracted from the current time, specifies the start time for the interval of event data collection"
      ],
    required = true,
  )
  lateinit var measurementLookbackDuration: String
    private set

  @Option(
    names = ["--duration-between-measurements"],
    description =
      [
        "Added to the update time of the most recently completed measurement, determines whether enough time has elapsed to request a new measurement"
      ],
    required = true,
  )
  lateinit var durationBetweenMeasurement: String
}

@Command(
  name = "ProberMeasurementCreationJob",
  mixinStandardHelpOptions = true,
  showDefaultValues = true,
)
private fun run(@Mixin flags: ProberMeasurementCreationFlags) {
  val proberMeasurementCreation = ProberMeasurementCreation()
  proberMeasurementCreation.run()
}

fun main(args: Array<String>) = commandLineMain(::run, args)
