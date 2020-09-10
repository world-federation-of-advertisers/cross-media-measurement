package org.wfanet.measurement.tools

import com.google.cloud.spanner.ErrorCode
import com.google.cloud.spanner.SpannerException
import java.io.File
import java.util.concurrent.ExecutionException
import java.util.logging.Logger
import kotlin.properties.Delegates
import org.wfanet.measurement.common.commandLineMain
import org.wfanet.measurement.db.gcp.buildSpanner
import org.wfanet.measurement.db.gcp.createDatabase
import org.wfanet.measurement.db.gcp.createInstance
import picocli.CommandLine
import picocli.CommandLine.Command
import picocli.CommandLine.Option

private class Flags {
  @Option(
    names = ["--project-name"],
    description = ["Name of the Spanner project to use."],
    required = true
  )
  lateinit var projectName: String
    private set

  @Option(
    names = ["--instance-name"],
    description = ["Name of the Spanner instance to create."],
    required = true
  )
  lateinit var instanceName: String
    private set

  @Option(
    names = ["--databases"],
    description = ["Map from database names to SDL files"],
    required = true
  )
  lateinit var databases: Map<String, String>

  @set:Option(
    names = ["--create-instance"],
    description = ["If true, creates a new Spanner instance first"],
    defaultValue = "false"
  )
  var createInstance by Delegates.notNull<Boolean>()

  @Option(
    names = ["--instance-display-name"],
    description = ["If --create-instance=true, this is the display name for the instance"],
    required = false
  )
  lateinit var instanceDisplayName: String

  @Option(
    names = ["--instance-config-id"],
    description = ["If --create-instance=true, this is the InstanceConfigId for the instance"],
    required = false
  )
  lateinit var instanceConfigId: String

  @set:Option(
    names = ["--instance-node-count"],
    description = ["If --create-instance=true, this is the number of nodes for the instance"],
    required = false
  )
  var instanceNodeCount by Delegates.notNull<Int>()

  @set:Option(
    names = ["--drop-databases-first"],
    description = ["Drop databases first"],
    defaultValue = "false",
    required = false
  )
  var dropDatabasesFirst by Delegates.notNull<Boolean>()

  @set:Option(
    names = ["--ignore-already-existing-databases"],
    description = ["Drop databases first"],
    defaultValue = "false",
    required = false
  )
  var ignoreAlreadyExistingDatabases by Delegates.notNull<Boolean>()

  @Option(
    names = ["--emulator-host"],
    description = ["Host name and port of the spanner emulator."],
    required = false
  )
  lateinit var spannerEmulatorHost: String
    private set
}

@Command(
  name = "push_spanner_schema_main",
  mixinStandardHelpOptions = true,
  showDefaultValues = true
)
private fun run(@CommandLine.Mixin flags: Flags) {
  val logger = Logger.getLogger("PushSpannerSchema")

  val spanner = buildSpanner(flags.projectName, flags.spannerEmulatorHost)

  if (flags.createInstance) {
    try {
      spanner.createInstance(
        projectName = flags.projectName,
        instanceName = flags.instanceName,
        displayName = flags.instanceDisplayName,
        instanceConfigId = flags.instanceConfigId,
        instanceNodeCount = flags.instanceNodeCount
      )
    } catch (e: ExecutionException) {
      if (e.isAlreadyExistsException) {
        logger.info("Instance already exists")
      } else {
        throw e
      }
    }
  }

  val instance = spanner.instanceAdminClient.getInstance(flags.instanceName)

  for ((databaseName, ddlFilePath) in flags.databases.entries) {
    if (flags.dropDatabasesFirst) {
      logger.info("Dropping database $databaseName")
      instance.getDatabase(databaseName).drop()
    }
    logger.info("Creating database $databaseName from DDL file $ddlFilePath")
    val ddl = File(ddlFilePath).readText()
    try {
      createDatabase(instance, ddl, databaseName)
    } catch (e: ExecutionException) {
      if (flags.ignoreAlreadyExistingDatabases && e.isAlreadyExistsException) {
        logger.info("Database $databaseName already exists")
      } else {
        throw e
      }
    }
  }
}

private val ExecutionException.isAlreadyExistsException: Boolean
  get() = cause.let {
    it != null && it is SpannerException && it.errorCode == ErrorCode.ALREADY_EXISTS
  }

fun main(args: Array<String>) = commandLineMain(::run, args)
