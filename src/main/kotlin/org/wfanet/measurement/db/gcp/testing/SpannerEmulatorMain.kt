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

package org.wfanet.measurement.db.gcp.testing

import com.google.cloud.spanner.InstanceConfigId
import com.google.cloud.spanner.InstanceInfo
import java.io.File
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.runBlocking
import org.wfanet.measurement.common.commandLineMain
import org.wfanet.measurement.db.gcp.SpannerFromFlags
import picocli.CommandLine

private class Flags {
  @CommandLine.Option(
    names = ["--spanner-instance-display-name"],
    paramLabel = "<displayName>",
    description = ["Display name of Spanner instance. Defaults to instance name."]
  )
  lateinit var instanceDisplayName: String
    private set
  val hasInstanceDisplayName
    get() = ::instanceDisplayName.isInitialized

  @CommandLine.Option(
    names = ["--schema-file", "-f"],
    description = ["Path to SDL file."],
    required = true
  )
  lateinit var schemaFile: File
    private set
}

@CommandLine.Command(
  name = "spanner_emulator_main",
  description = ["Run Cloud Spanner Emulator on an unused port, creating a temporary database."],
  mixinStandardHelpOptions = true,
  showDefaultValues = true
)
private fun run(
  @CommandLine.Mixin flags: Flags,
  @CommandLine.Mixin spannerFlags: SpannerFromFlags.Flags
) {
  val spannerFromFlags = SpannerFromFlags(spannerFlags)

  SpannerEmulator().use { emulator: SpannerEmulator ->
    emulator.start()
    val emulatorHost = emulator.blockUntilReady()

    val spannerOptions =
      spannerFromFlags.spannerOptions.toBuilder().setEmulatorHost(emulatorHost).build()

    val spanner = spannerOptions.service
    val databaseId = spannerFromFlags.databaseId

    val displayName =
      if (flags.hasInstanceDisplayName) flags.instanceDisplayName else spannerFlags.instanceName
    val instanceInfo =
      InstanceInfo
        .newBuilder(databaseId.instanceId)
        .setDisplayName(displayName)
        .setInstanceConfigId(InstanceConfigId.of(spannerFlags.projectName, "emulator-config"))
        .setNodeCount(1)
        .build()

    val instance = spanner.instanceAdminClient.createInstance(instanceInfo).get()
    println("Instance ${instance.displayName} created")

    val ddl = flags.schemaFile.readText()
    println("Read in schema file: ${flags.schemaFile.absolutePath}")

    createDatabase(instance, ddl, spannerFlags.databaseName)
    println("Database ${spannerFromFlags.databaseId} created")

    // Stay alive so the emulator doesn't terminate:
    println("Idling until killed")
    runBlocking { Channel<Void>().receive() }
  }
}

/**
 * Brings up a Spanner emulator and creates an instance and database.
 *
 * Specify `--help` option to show usage information.
 */
fun main(args: Array<String>) = commandLineMain(::run, args)
