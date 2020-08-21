// Copyright 2020 The Measurement System Authors
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

package org.wfanet.measurement.service.internal.kingdom

import java.time.Clock
import kotlin.properties.Delegates
import org.wfanet.measurement.common.RandomIdGenerator
import org.wfanet.measurement.common.commandLineMain
import org.wfanet.measurement.common.identity.DuchyIdFlags
import org.wfanet.measurement.common.identity.DuchyIds
import org.wfanet.measurement.db.gcp.SpannerFromFlags
import org.wfanet.measurement.db.kingdom.gcp.GcpKingdomRelationalDatabase
import picocli.CommandLine

private class Flags {
  @set:CommandLine.Option(
    names = ["--port", "-p"],
    description = ["TCP port for gRPC server."],
    required = true,
    defaultValue = "8080"
  )
  var port by Delegates.notNull<Int>()
    private set

  @CommandLine.Option(
    names = ["--server-name"],
    description = ["Name of the gRPC server for logging purposes."],
    required = true,
    defaultValue = "KingdomStorageServer"
  )
  lateinit var nameForLogging: String
    private set
}

@CommandLine.Command(
  name = "gcp_kingdom_storage_server",
  description = [
    "Start the internal Kingdom storage services in a single blocking server.",
    "This brings up its own Cloud Spanner Emulator."
  ],
  mixinStandardHelpOptions = true,
  showDefaultValues = true
)
private fun run(
  @CommandLine.Mixin flags: Flags,
  @CommandLine.Mixin spannerFlags: SpannerFromFlags.Flags,
  @CommandLine.Mixin duchyIdFlags: DuchyIdFlags
) {
  DuchyIds.setDuchyIdsFromFlags(duchyIdFlags)

  val spannerFromFlags = SpannerFromFlags(spannerFlags)
  val clock = Clock.systemUTC()

  val relationalDatabase = GcpKingdomRelationalDatabase(
    clock, RandomIdGenerator(clock), spannerFromFlags.databaseClient
  )

  val server = buildKingdomStorageServer(relationalDatabase, flags.port, flags.nameForLogging)

  server.start().blockUntilShutdown()
}

/** Runs the internal Kingdom storage services in a single server with a Spanner backend. */
fun main(args: Array<String>) = commandLineMain(::run, args)
