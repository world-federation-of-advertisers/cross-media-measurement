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

package org.wfanet.measurement.kingdom.deploy.gcloud.server

import java.time.Clock
import kotlinx.coroutines.runBlocking
import org.wfanet.measurement.common.commandLineMain
import org.wfanet.measurement.common.identity.RandomIdGenerator
import org.wfanet.measurement.gcloud.spanner.SpannerFlags
import org.wfanet.measurement.kingdom.deploy.common.server.KingdomDataServer
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.SpannerKingdomRelationalDatabase
import picocli.CommandLine

/** Implementation of [KingdomDataServer] using Google Cloud Spanner. */
@CommandLine.Command(
  name = "SpannerKingdomDataServer",
  description = ["Start the internal Kingdom data-layer services in a single blocking server."],
  mixinStandardHelpOptions = true,
  showDefaultValues = true
)
class SpannerKingdomDataServer : KingdomDataServer() {
  @CommandLine.Mixin
  private lateinit var spannerFlags: SpannerFlags

  override fun run() = runBlocking {
    spannerFlags.usingSpanner { spanner ->
      val clock = Clock.systemUTC()

      val database = SpannerKingdomRelationalDatabase(
        clock,
        RandomIdGenerator(clock),
        spanner.databaseClient
      )

      run(database)
    }
  }
}

fun main(args: Array<String>) = commandLineMain(SpannerKingdomDataServer(), args)
