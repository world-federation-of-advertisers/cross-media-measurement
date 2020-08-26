package org.wfanet.measurement.service.testing.storage

import org.wfanet.measurement.common.CommonServer
import org.wfanet.measurement.common.commandLineMain
import picocli.CommandLine
import kotlin.properties.Delegates

private class FakeStorageServiceFlags {
  @set:CommandLine.Option(
    names = ["--port", "-p"],
    description = ["TCP port for gRPC server."],
    required = true,
    defaultValue = "8080"
  )
  var port: Int by Delegates.notNull()
    private set
}

@CommandLine.Command(
  name = "gcp_worker_server",
  mixinStandardHelpOptions = true,
  showDefaultValues = true
)
private fun run(@CommandLine.Mixin fakeStorageServiceFlags: FakeStorageServiceFlags) {
  CommonServer(
    "FakeStorageService",
    fakeStorageServiceFlags.port,
    FakeStorageService()
  ).start().blockUntilShutdown()
}

fun main(args: Array<String>) = commandLineMain(::run, args)
