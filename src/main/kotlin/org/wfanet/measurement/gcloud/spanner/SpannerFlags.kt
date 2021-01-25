// Copyright 2020 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.gcloud.spanner

import java.time.Duration
import picocli.CommandLine

/**
 * Common command-line flags for connecting to a single Spanner database.
 */
class SpannerFlags {
  @CommandLine.Option(
    names = ["--spanner-project"],
    description = ["Name of the Spanner project."],
    required = true
  )
  lateinit var projectName: String
    private set

  @CommandLine.Option(
    names = ["--spanner-instance"],
    description = ["Name of the Spanner instance."],
    required = true
  )
  lateinit var instanceName: String
    private set

  @CommandLine.Option(
    names = ["--spanner-database"],
    description = ["Name of the Spanner database."],
    required = true
  )
  lateinit var databaseName: String
    private set

  @CommandLine.Option(
    names = ["--spanner-ready-timeout"],
    description = ["How long to wait for Spanner to be ready."],
    defaultValue = "10s"
  )
  lateinit var readyTimeout: Duration
    private set

  @CommandLine.Option(
    names = ["--spanner-emulator-host"],
    description = ["Host name and port of the spanner emulator."],
    required = false
  )
  var spannerEmulatorHost: String? = null
    private set

  /**
   * Builds a [SpannerDatabaseConnector] from these flags.
   */
  private fun toSpannerDatabaseConnector(): SpannerDatabaseConnector {
    return SpannerDatabaseConnector(
      instanceName,
      projectName,
      readyTimeout,
      databaseName,
      spannerEmulatorHost
    )
  }

  /**
   * Executes [block] with a [SpannerDatabaseConnector] resource once it's ready,
   * ensuring that the resource is closed.
   */
  suspend fun <R> usingSpanner(block: suspend (spanner: SpannerDatabaseConnector) -> R): R {
    return toSpannerDatabaseConnector().usingSpanner(block)
  }
}
