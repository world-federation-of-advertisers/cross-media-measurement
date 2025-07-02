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
import kotlin.properties.Delegates
import org.wfanet.measurement.gcloud.spanner.SpannerParams
import picocli.CommandLine

/**
 * Common command-line flags for connecting to a single Spanner database.
 *
 * Copy of [org.wfanet.measurement.gcloud.spanner.SpannerFlags] except there are no required
 * options.
 */
class SpannerFlags : SpannerParams {
  @CommandLine.Option(
    names = ["--spanner-project"],
    description = ["Name of the Spanner project. Required if --basic-reports-enabled is true."],
    required = false,
  )
  override lateinit var projectName: String
    private set

  @CommandLine.Option(
    names = ["--spanner-instance"],
    description = ["Name of the Spanner instance. Required if --basic-reports-enabled is true."],
    required = false,
  )
  override lateinit var instanceName: String
    private set

  @CommandLine.Option(
    names = ["--spanner-database"],
    description = ["Name of the Spanner database. Required if --basic-reports-enabled is true."],
    required = false,
  )
  override lateinit var databaseName: String
    private set

  @CommandLine.Option(
    names = ["--spanner-ready-timeout"],
    description = ["How long to wait for Spanner to be ready."],
    defaultValue = "10s",
  )
  override lateinit var readyTimeout: Duration
    private set

  @CommandLine.Option(
    names = ["--spanner-emulator-host"],
    description = ["Host name and port of the spanner emulator."],
    required = false,
  )
  override var emulatorHost: String? = null
    private set

  @set:CommandLine.Option(
    names = ["--spanner-async-thread-pool-size"],
    description = ["Size of the thread pool for Spanner async operations."],
    defaultValue = "8",
  )
  override var asyncThreadPoolSize: Int by Delegates.notNull()
    private set
}
