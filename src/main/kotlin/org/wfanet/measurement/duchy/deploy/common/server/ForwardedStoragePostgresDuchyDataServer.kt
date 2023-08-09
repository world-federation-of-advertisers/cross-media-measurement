package org.wfanet.measurement.duchy.deploy.common.server

import org.wfanet.measurement.common.commandLineMain
import org.wfanet.measurement.duchy.deploy.postgres.server.PostgresDuchyDataServer
import org.wfanet.measurement.storage.forwarded.ForwardedStorageFromFlags
import picocli.CommandLine

/** Implementation of [PostgresDuchyDataServer] using Fake Storage Service. */
class ForwardedStoragePostgresDuchyDataServer : PostgresDuchyDataServer() {

  @CommandLine.Mixin private lateinit var forwardedStorageFlags: ForwardedStorageFromFlags.Flags

  override fun run() {
    run(ForwardedStorageFromFlags(forwardedStorageFlags, flags.server.tlsFlags).storageClient)
  }
}

fun main(args: Array<String>) = commandLineMain(ForwardedStoragePostgresDuchyDataServer(), args)
