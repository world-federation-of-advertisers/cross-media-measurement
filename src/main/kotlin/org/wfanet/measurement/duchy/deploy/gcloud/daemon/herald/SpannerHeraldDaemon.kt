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

package org.wfanet.measurement.duchy.deploy.gcloud.daemon.herald

import kotlinx.coroutines.runBlocking
import org.wfanet.measurement.common.commandLineMain
import org.wfanet.measurement.duchy.deploy.common.daemon.herald.HeraldDaemon
import org.wfanet.measurement.duchy.deploy.gcloud.spanner.daemon.herald.SpannerContinuationTokenStore
import org.wfanet.measurement.gcloud.spanner.SpannerFlags
import org.wfanet.measurement.gcloud.spanner.usingSpanner
import picocli.CommandLine

@CommandLine.Command(
    name = "SpannerHeraldDaemon",
    description = ["Herald Daemon"],
    mixinStandardHelpOptions = true,
    showDefaultValues = true)
class SpannerHeraldDaemon : HeraldDaemon() {
  @CommandLine.Mixin private lateinit var spannerFlags: SpannerFlags

  override fun run() = runBlocking {
    spannerFlags.usingSpanner { spanner ->
      val databaseClient = spanner.databaseClient
      val continuationTokenStore =
          SpannerContinuationTokenStore(databaseClient, flags.duchy.duchyName)
      run(continuationTokenStore)
    }
  }
}

fun main(args: Array<String>) = commandLineMain(SpannerHeraldDaemon(), args)
