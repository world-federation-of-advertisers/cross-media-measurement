package org.wfanet.measurement.service.testing.storage

import org.wfanet.measurement.common.commandLineMain
import org.wfanet.measurement.service.common.CommonServer
import picocli.CommandLine

@CommandLine.Command(
  name = "fake_storage_server",
  mixinStandardHelpOptions = true,
  showDefaultValues = true
)
private fun run(
  @CommandLine.Mixin commonServerFlags: CommonServer.Flags) {
  CommonServer.fromFlags(
    commonServerFlags,
    "FakeStorageService",
    FakeStorageService()
  ).start().blockUntilShutdown()
}

fun main(args: Array<String>) = commandLineMain(::run, args)
