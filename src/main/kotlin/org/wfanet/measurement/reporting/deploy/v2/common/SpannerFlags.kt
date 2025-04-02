/*
 * Copyright 2025 The Cross-Media Measurement Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.wfanet.measurement.reporting.deploy.v2.common

import java.time.Duration
import org.wfanet.measurement.gcloud.spanner.SpannerDatabaseConnector
import picocli.CommandLine

/** Common command-line flags for connecting to a single Spanner database. */
class SpannerFlags {
  @CommandLine.Option(
    names = ["--spanner-project"],
    description = ["Name of the Spanner project."],
    required = false,
  )
  lateinit var projectName: String
    private set

  @CommandLine.Option(
    names = ["--spanner-instance"],
    description = ["Name of the Spanner instance."],
    required = false,
  )
  lateinit var instanceName: String
    private set

  @CommandLine.Option(
    names = ["--spanner-database"],
    description = ["Name of the Spanner database."],
    required = false,
  )
  lateinit var databaseName: String
    private set

  @CommandLine.Option(
    names = ["--spanner-ready-timeout"],
    description = ["How long to wait for Spanner to be ready."],
    defaultValue = "10s",
  )
  lateinit var readyTimeout: Duration
    private set

  @CommandLine.Option(
    names = ["--spanner-emulator-host"],
    description = ["Host name and port of the spanner emulator."],
    required = false,
  )
  var emulatorHost: String? = null
    private set

  val jdbcConnectionString: String
    get() {
      val databasePath = "projects/$projectName/instances/$instanceName/databases/$databaseName"
      return if (emulatorHost == null) {
        "jdbc:cloudspanner:/$databasePath"
      } else {
        "jdbc:cloudspanner://$emulatorHost/$databasePath;usePlainText=true;autoConfigEmulator=true"
      }
    }
}

/** Builds a [SpannerDatabaseConnector] from these flags. */
private fun SpannerFlags.toSpannerDatabaseConnector(): SpannerDatabaseConnector {
  return SpannerDatabaseConnector(
    projectName = projectName,
    instanceName = instanceName,
    databaseName = databaseName,
    readyTimeout = readyTimeout,
    emulatorHost = emulatorHost,
  )
}

/**
 * Executes [block] with a [SpannerDatabaseConnector] resource once it's ready, ensuring that the
 * resource is closed.
 */
suspend fun <R> SpannerFlags.usingSpanner(
  block: suspend (spanner: SpannerDatabaseConnector) -> R
): R {
  return toSpannerDatabaseConnector().usingSpanner(block)
}
