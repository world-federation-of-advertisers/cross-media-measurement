package org.wfanet.measurement.duchy.deploy.common.server

import io.grpc.ManagedChannel
import org.wfanet.measurement.common.commandLineMain
import org.wfanet.measurement.common.grpc.CommonServer
import org.wfanet.measurement.common.grpc.buildChannel
import org.wfanet.measurement.common.identity.DuchyIdFlags
import org.wfanet.measurement.common.identity.DuchyIds
import org.wfanet.measurement.duchy.DuchyPublicKeys
import org.wfanet.measurement.duchy.db.computation.ComputationProtocolStageDetails
import org.wfanet.measurement.duchy.deploy.common.CommonDuchyFlags
import org.wfanet.measurement.duchy.service.internal.computationcontrol.AsyncComputationControlService
import org.wfanet.measurement.internal.duchy.ComputationsGrpcKt.ComputationsCoroutineStub
import picocli.CommandLine

private const val SERVICE_NAME = "AsyncComputationControl"
private const val SERVER_NAME = "${SERVICE_NAME}Server"

class AsyncComputationControlServiceFlags {
  @CommandLine.Mixin
  lateinit var server: CommonServer.Flags
    private set

  @CommandLine.Mixin
  lateinit var duchy: CommonDuchyFlags
    private set

  @CommandLine.Mixin
  lateinit var duchyPublicKeys: DuchyPublicKeys.Flags
    private set

  @CommandLine.Mixin
  lateinit var duchyIdFlags: DuchyIdFlags
    private set

  @CommandLine.Option(
    names = ["--computations-service-target"],
    description = ["Address and port of the Computations service"],
    required = true
  )
  lateinit var computationsServiceTarget: String
    private set
}

@CommandLine.Command(
  name = SERVER_NAME,
  description = ["Server daemon for $SERVICE_NAME service."],
  mixinStandardHelpOptions = true,
  showDefaultValues = true
)
private fun run(
  @CommandLine.Mixin flags: AsyncComputationControlServiceFlags
) {
  val duchyName = flags.duchy.duchyName
  val latestDuchyPublicKeys = DuchyPublicKeys.fromFlags(flags.duchyPublicKeys).latest
  require(latestDuchyPublicKeys.containsKey(duchyName)) {
    "Public key not specified for Duchy $duchyName"
  }
  DuchyIds.setDuchyIdsFromFlags(flags.duchyIdFlags)
  require(latestDuchyPublicKeys.keys.toSet() == DuchyIds.ALL)

  val otherDuchyNames = latestDuchyPublicKeys.keys.filter { it != duchyName }
  val channel: ManagedChannel = buildChannel(flags.computationsServiceTarget)

  CommonServer.fromFlags(
    flags.server,
    SERVER_NAME,
    AsyncComputationControlService(
      ComputationsCoroutineStub(channel),
      ComputationProtocolStageDetails(otherDuchyNames)
    )
  ).start().blockUntilShutdown()
}

fun main(args: Array<String>) = commandLineMain(::run, args)
