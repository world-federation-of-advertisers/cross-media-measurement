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

package org.wfanet.measurement.db.gcp

import com.google.cloud.spanner.DatabaseClient
import com.google.cloud.spanner.DatabaseId
import com.google.cloud.spanner.Instance
import com.google.cloud.spanner.Spanner
import picocli.CommandLine

class SpannerFromFlags(
  private val flags: Flags
) {
  val spanner: Spanner by lazy { buildSpanner(flags.projectName, flags.spannerEmulatorHost) }

  val instance: Instance by lazy {
    spanner.instanceAdminClient.getInstance(flags.instanceName)
  }

  val databaseId: DatabaseId by lazy {
    DatabaseId.of(flags.projectName, flags.instanceName, flags.databaseName)
  }

  val databaseClient: DatabaseClient by lazy {
    spanner.getDatabaseClient(databaseId)
  }

  class Flags {
    @CommandLine.Option(
      names = ["--spanner-project"],
      description = ["Name of the Spanner project to use."],
      required = true
    )
    lateinit var projectName: String
      private set

    @CommandLine.Option(
      names = ["--spanner-instance"],
      description = ["Name of the Spanner instance to create."],
      required = true
    )
    lateinit var instanceName: String
      private set

    @CommandLine.Option(
      names = ["--spanner-database"],
      description = ["Name of the Spanner database to create."],
      required = true
    )
    lateinit var databaseName: String
      private set

    @CommandLine.Option(
      names = ["--spanner-emulator-host"],
      description = ["Host name and port of the spanner emulator."],
      required = false
    )
    lateinit var spannerEmulatorHost: String
      private set
  }
}
