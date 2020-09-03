package org.wfanet.measurement.service.testing.storage

import kotlin.properties.Delegates
import org.wfanet.measurement.common.commandLineMain
import org.wfanet.measurement.service.common.CommonServer
import picocli.CommandLine

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
