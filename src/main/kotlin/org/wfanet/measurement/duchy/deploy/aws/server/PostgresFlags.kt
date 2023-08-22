// Copyright 2022 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.duchy.deploy.aws.server

import picocli.CommandLine

/** Common command-line flags for connecting to a single Postgres database. */
class PostgresFlags1 {
  @CommandLine.Option(
    names = ["--postgres-database"],
    description = ["Name of the Postgres database."],
    required = false
  )
  var database: String = ""
    private set

  @CommandLine.Option(
    names = ["--postgres-host"],
    description = ["Host name of the Postgres database."],
    required = true
  )
  lateinit var host: String
    private set

  @CommandLine.Option(
    names = ["--postgres-port"],
    description = ["Port of the Postgres database."],
    required = true
  )
  var port: Int = 0
    private set

  @CommandLine.Option(
    names = ["--postgres-credential-secret-name"],
    description = ["Name of the AWS Secrets Manager secret that stores the password of the Postgres database."],
    required = true
  )
  lateinit var credentialSecretName: String
    private set

  @CommandLine.Option(
    names = ["--postgres-region"],
    description = ["AWS region of the postgres."],
    required = true
  )
  lateinit var region: String
    private set

  val jdbcConnectionString: String
    get() {
      return "jdbc:postgresql://$host:$port/$database"
    }
}
