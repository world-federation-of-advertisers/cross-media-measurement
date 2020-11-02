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

package org.wfanet.measurement.duchy.deploy.gcloud.server

import java.time.Clock
import kotlinx.coroutines.runBlocking
import org.wfanet.measurement.common.commandLineMain
import org.wfanet.measurement.common.identity.RandomIdGenerator
import org.wfanet.measurement.duchy.deploy.common.server.MetricValuesServer
import org.wfanet.measurement.duchy.deploy.gcloud.spanner.SpannerMetricValueDatabase
import org.wfanet.measurement.gcloud.spanner.SpannerFlags
import org.wfanet.measurement.storage.forwarded.ForwardedStorageFromFlags
import picocli.CommandLine

/**
 * Implementation of [MetricValuesServer] using Google Cloud Spanner for
 * database and ForwardedStorage service for storage.
 */
@CommandLine.Command(
  name = "SpannerForwardedStorageMetricValuesServer",
  description = ["Run server daemon for ${MetricValuesServer.SERVICE_NAME} service."],
  mixinStandardHelpOptions = true,
  showDefaultValues = true
)
private class SpannerForwardedStorageMetricValuesServer : MetricValuesServer() {
  @CommandLine.Mixin
  private lateinit var forwardedStorageFlags: ForwardedStorageFromFlags.Flags

  @CommandLine.Mixin
  private lateinit var spannerFlags: SpannerFlags

  override fun run() = runBlocking {
    val clock = Clock.systemUTC()
    spannerFlags.usingSpanner { spanner ->
      val metricValueDb =
        SpannerMetricValueDatabase(spanner.databaseClient, RandomIdGenerator(clock))
      val storageClient = ForwardedStorageFromFlags(forwardedStorageFlags).storageClient

      run(metricValueDb, storageClient)
    }
  }
}

fun main(args: Array<String>) = commandLineMain(SpannerForwardedStorageMetricValuesServer(), args)
