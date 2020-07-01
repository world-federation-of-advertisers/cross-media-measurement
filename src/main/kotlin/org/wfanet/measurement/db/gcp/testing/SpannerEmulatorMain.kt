package org.wfanet.measurement.db.gcp.testing

import com.google.cloud.spanner.InstanceConfigId
import com.google.cloud.spanner.InstanceInfo
import java.io.File
import org.wfanet.measurement.common.Flags
import org.wfanet.measurement.common.stringFlag
import org.wfanet.measurement.db.gcp.SpannerFromFlags

/**
 * Brings up a Spanner emulator and creates an instance and database.
 *
 * Flags:
 *
 *   --spanner-project: the name of the Spanner project to use
 *   --spanner-instance: the name of the Spanner instance to create
 *   --spanner-instance-display-name: the display name of the Spanner instance to create
 *   --spanner-database: the name of the Spanner database to create
 *   --schema-file: path to SDL file
 */
fun main(args: Array<String>) {
  val spannerFlags = SpannerFromFlags()
  val instanceDisplayName = stringFlag("spanner-instance-display-name", "")
  val schemaFile = stringFlag("schema-file", "")
  Flags.parse(args.asIterable())

  SpannerEmulator().use { emulator: SpannerEmulator ->
    emulator.start()
    val emulatorHost = emulator.blockUntilReady()

    val spannerOptions =
      spannerFlags.spannerOptions
        .toBuilder()
        .setEmulatorHost(emulatorHost)
        .build()

    val spanner = spannerOptions.service
    val databaseId = spannerFlags.databaseId

    val instanceInfo =
      InstanceInfo
        .newBuilder(databaseId.instanceId)
        .setDisplayName(instanceDisplayName.value)
        .setInstanceConfigId(InstanceConfigId.of(spannerFlags.projectName, "emulator-config"))
        .setNodeCount(1)
        .build()

    val instance = spanner.instanceAdminClient.createInstance(instanceInfo).get()
    println("Created instance")

    val ddl = File(schemaFile.value).readText()
    println("Read in schema file: ${schemaFile.value}")

    createDatabase(instance, ddl, spannerFlags.databaseName)
    println("Database ${spannerFlags.databaseId} created")

    // Stay alive so the emulator doesn't terminate:
    println("Idling until killed")
    Object().wait()
  }
}
