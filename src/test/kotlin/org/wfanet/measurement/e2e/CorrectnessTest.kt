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

package org.wfanet.measurement.e2e

import io.grpc.ManagedChannel
import io.grpc.ManagedChannelBuilder
import java.net.URL
import java.time.Clock
import java.time.Duration
import java.time.Instant
import java.time.ZoneOffset
import java.time.format.DateTimeFormatter
import kotlin.test.fail
import kotlinx.coroutines.runBlocking
import org.junit.Before
import org.junit.ClassRule
import org.junit.Rule
import org.junit.Test
import org.junit.rules.TemporaryFolder
import org.wfanet.measurement.api.v1alpha.PublisherDataGrpcKt
import org.wfanet.measurement.api.v1alpha.SketchConfig
import org.wfanet.measurement.common.identity.RandomIdGenerator
import org.wfanet.measurement.common.parseTextProto
import org.wfanet.measurement.gcloud.spanner.SpannerDatabaseConnector
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.SpannerKingdomRelationalDatabase
import org.wfanet.measurement.loadtest.CorrectnessImpl
import org.wfanet.measurement.storage.StorageClient
import org.wfanet.measurement.storage.filesystem.FileSystemStorageClient
import org.wfanet.measurement.tools.ClusterState

/**
 * Runs a correctness test in a Kubernetes-in-Docker (kind) cluster.
 */
class CorrectnessTest {
  private val publisherDataServer = "a-publisher-data-server"
  private val spannerEmulator = "spanner-emulator"
  private val pushSpannerSchemaJob = "push-spanner-schema-job"
  lateinit var storageClient: StorageClient

  @Rule
  @JvmField
  val tempDirectory = TemporaryFolder()

  @Before
  fun initClient() {
    storageClient = FileSystemStorageClient(tempDirectory.root)
  }

  @Test
  fun testCorrectnessInKind() {
    val clusterState = ClusterState(KindRule.clusterName)

    // TODO(fashing): Replace with k8s readiness checks or something similar.
    // Poll statuses until cluster is ready
    val maxAttempts = 300
    var isReady = false
    for (numAttempts in 1..maxAttempts) {
      val podStatuses = clusterState.getPodStatuses()
        .filter { !it.key.startsWith("correctness-test-job-") }
      val podsRunningOrSucceeded =
        podStatuses.isNotEmpty() && podStatuses.values.all { it == "Running" || it == "Succeeded" }
      val haveSchemaPushesSucceeded =
        clusterState.jobsSucceeded(('a'..'c').map { "$it-$pushSpannerSchemaJob" })
      isReady = podsRunningOrSucceeded && haveSchemaPushesSucceeded
      if (isReady) {
        break
      }
      Thread.sleep(1000L)
    }
    if (!isReady) {
      fail("Timed out waiting for cluster to be ready")
    }
    // Wait for servers to be ready to accept traffic
    Thread.sleep(30000L)

    val nodeIp = clusterState.getNodeIp()
    val nodePorts = clusterState.getNodePorts()

    val spannerEmulatorHost = "$nodeIp:${nodePorts.getValue(spannerEmulator).getValue("grpc")}"

    val channel: ManagedChannel =
      ManagedChannelBuilder
        .forTarget("$nodeIp:${nodePorts.getValue(publisherDataServer).getValue("port")}")
        .usePlaintext()
        .build()
    val publisherDataStub = PublisherDataGrpcKt.PublisherDataCoroutineStub(channel)

    var runId = ""
    val dataProviderCount = 2
    val campaignCount = 1
    val generatedSetSize = 1000
    val universeSize = 10000000000

    if (runId.isBlank()) {
      // Set the runId to current timestamp.
      runId = DateTimeFormatter
        .ofPattern("yyyy-MM-ddHH-mm-ss-SSS")
        .withZone(ZoneOffset.UTC)
        .format(Instant.now())
    }

    val sketchConfig = resource.openStream().use { input ->
      parseTextProto(input.bufferedReader(), SketchConfig.getDefaultInstance())
    }

    val clock = Clock.systemUTC()
    runBlocking {
      SpannerDatabaseConnector(
        emulatorHost = spannerEmulatorHost,
        instanceName = "emulator-instance",
        projectName = "ads-open-measurement",
        databaseName = "kingdom",
        readyTimeout = Duration.ofSeconds(10L)
      ).use { spanner ->
        val relationalDatabase =
          SpannerKingdomRelationalDatabase(clock, RandomIdGenerator(clock), spanner.databaseClient)

        val correctness = CorrectnessImpl(
          dataProviderCount = dataProviderCount,
          campaignCount = campaignCount,
          generatedSetSize = generatedSetSize,
          universeSize = universeSize,
          runId = runId,
          sketchConfig = sketchConfig,
          storageClient = storageClient,
          publisherDataStub = publisherDataStub
        )

        correctness.process(relationalDatabase)
      }
    }
  }

  companion object {
    @ClassRule
    @JvmField
    val kindRule = KindRule()

    private const val configPath =
      "/org/wfanet/measurement/loadtest/config/liquid_legions_sketch_config.textproto"
    private val resource: URL = this::class.java.getResource(configPath)
  }
}
