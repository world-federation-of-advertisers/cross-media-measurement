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

package org.wfanet.measurement.tools

import com.google.cloud.spanner.ErrorCode
import com.google.cloud.spanner.SpannerException
import java.io.File
import java.util.concurrent.ExecutionException
import java.util.logging.Logger
import kotlin.properties.Delegates
import org.wfanet.measurement.common.commandLineMain
import org.wfanet.measurement.gcloud.spanner.buildSpanner
import org.wfanet.measurement.gcloud.spanner.createDatabase
import org.wfanet.measurement.gcloud.spanner.createInstance
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
  var spannerEmulatorHost: String ? = null
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
    try {
      if (flags.dropDatabasesFirst) {
        logger.info("Dropping database $databaseName")
        instance.getDatabase(databaseName).drop()
      }
    } catch (e: ExecutionException) {
      logger.info("Database $databaseName doesn't exist but tried to drop")
    }

    logger.info("Creating database $databaseName from DDL file $ddlFilePath")
    File(ddlFilePath).useLines { schemaDefinitionLines ->
      try {
        createDatabase(instance, schemaDefinitionLines, databaseName)
      } catch (e: ExecutionException) {
        if (flags.ignoreAlreadyExistingDatabases && e.isAlreadyExistsException) {
          logger.info("Database $databaseName already exists")
        } else {
          throw e
        }
      }
    }
  }
}

private val ExecutionException.isAlreadyExistsException: Boolean
  get() = cause.let {
    it != null && it is SpannerException && it.errorCode == ErrorCode.ALREADY_EXISTS
  }

fun main(args: Array<String>) = commandLineMain(::run, args)
