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

package org.wfanet.measurement.service.internal.duchy.metricvalues

import java.time.Clock
import java.time.Duration
import org.wfanet.measurement.common.RandomIdGenerator
import org.wfanet.measurement.common.commandLineMain
import org.wfanet.measurement.db.duchy.metricvalue.gcp.SpannerMetricValueDatabase
import org.wfanet.measurement.db.gcp.SpannerFromFlags
import org.wfanet.measurement.db.gcp.isReady
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
  private lateinit var spannerFlags: SpannerFromFlags.Flags

  override fun run() {
    val clock = Clock.systemUTC()
    var spanner = SpannerFromFlags(spannerFlags)

    // TODO: push this retry logic into SpannerFromFlags itself.
    while (!spanner.databaseClient.isReady()) {
      println("Spanner isn't ready yet, sleeping 1s")
      Thread.sleep(Duration.ofSeconds(1).toMillis())
      spanner = SpannerFromFlags(spannerFlags)
    }

    val metricValueDb = SpannerMetricValueDatabase.fromFlags(spanner, RandomIdGenerator(clock))
    val storageClient = ForwardedStorageFromFlags(forwardedStorageFlags).storageClient

    run(metricValueDb, storageClient)
  }
}

fun main(args: Array<String>) = commandLineMain(SpannerForwardedStorageMetricValuesServer(), args)
