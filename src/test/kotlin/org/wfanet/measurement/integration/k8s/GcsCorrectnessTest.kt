/*
 * Copyright 2023 The Cross-Media Measurement Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.wfanet.measurement.integration.k8s

import com.google.cloud.storage.StorageOptions
import io.grpc.ManagedChannel
import java.nio.file.Paths
import java.time.Duration
import java.util.UUID
import org.junit.ClassRule
import org.junit.rules.TemporaryFolder
import org.junit.rules.TestRule
import org.junit.runner.Description
import org.junit.runners.model.Statement
import org.measurement.integration.k8s.testing.CorrectnessTestConfig
import org.measurement.integration.k8s.testing.GoogleCloudStorageConfig
import org.wfanet.measurement.api.v2alpha.CertificatesGrpcKt
import org.wfanet.measurement.api.v2alpha.DataProvidersGrpcKt
import org.wfanet.measurement.api.v2alpha.EventGroupsGrpcKt
import org.wfanet.measurement.api.v2alpha.MeasurementConsumersGrpcKt
import org.wfanet.measurement.api.v2alpha.MeasurementsGrpcKt
import org.wfanet.measurement.api.v2alpha.RequisitionsGrpcKt
import org.wfanet.measurement.common.grpc.buildMutualTlsChannel
import org.wfanet.measurement.common.grpc.withDefaultDeadline
import org.wfanet.measurement.common.parseTextProto
import org.wfanet.measurement.common.testing.chainRulesSequentially
import org.wfanet.measurement.gcloud.gcs.GcsStorageClient
import org.wfanet.measurement.loadtest.config.EventFilters
import org.wfanet.measurement.loadtest.frontend.FrontendSimulator
import org.wfanet.measurement.loadtest.frontend.MeasurementConsumerData
import org.wfanet.measurement.loadtest.storage.SketchStore

/**
 * Test for correctness of an existing CMMS on Kubernetes where the EDP simulator sketches are
 * accessible via a [GcsStorageClient].
 *
 * This currently assumes that the CMMS instance is using the certificates and keys from this Bazel
 * workspace.
 */
class GcsCorrectnessTest : AbstractCorrectnessTest(measurementSystem) {
  private class RunningMeasurementSystem : MeasurementSystem, TestRule {
    override val runId: String by lazy { UUID.randomUUID().toString() }

    private lateinit var _testHarness: FrontendSimulator
    override val testHarness: FrontendSimulator
      get() = _testHarness

    private val channels = mutableListOf<ManagedChannel>()

    override fun apply(base: Statement, description: Description): Statement {
      return object : Statement() {
        override fun evaluate() {
          try {
            _testHarness = createTestHarness()
            base.evaluate()
          } finally {
            shutDownChannels()
          }
        }
      }
    }

    private fun createTestHarness(): FrontendSimulator {
      val measurementConsumerData =
        MeasurementConsumerData(
          TEST_CONFIG.measurementConsumer,
          MC_SIGNING_KEY,
          MC_ENCRYPTION_PRIVATE_KEY,
          TEST_CONFIG.apiAuthenticationKey
        )

      val publicApiChannel =
        buildMutualTlsChannel(
            TEST_CONFIG.kingdomPublicApiTarget,
            MEASUREMENT_CONSUMER_SIGNING_CERTS,
            TEST_CONFIG.kingdomPublicApiCertHost.ifEmpty { null }
          )
          .also { channels.add(it) }
          .withDefaultDeadline(RPC_DEADLINE_DURATION)

      val gcs = StorageOptions.newBuilder().setProjectId(STORAGE_CONFIG.project).build().service
      val storageClient = GcsStorageClient(gcs, STORAGE_CONFIG.bucket)

      return FrontendSimulator(
        measurementConsumerData,
        OUTPUT_DP_PARAMS,
        DataProvidersGrpcKt.DataProvidersCoroutineStub(publicApiChannel),
        EventGroupsGrpcKt.EventGroupsCoroutineStub(publicApiChannel),
        MeasurementsGrpcKt.MeasurementsCoroutineStub(publicApiChannel),
        RequisitionsGrpcKt.RequisitionsCoroutineStub(publicApiChannel),
        MeasurementConsumersGrpcKt.MeasurementConsumersCoroutineStub(publicApiChannel),
        CertificatesGrpcKt.CertificatesCoroutineStub(publicApiChannel),
        SketchStore(storageClient),
        RESULT_POLLING_DELAY,
        MEASUREMENT_CONSUMER_SIGNING_CERTS.trustedCertificates,
        EventFilters.EVENT_TEMPLATES_TO_FILTERS_MAP
      )
    }

    private fun shutDownChannels() {
      for (channel in channels) {
        channel.shutdown()
      }
    }
  }

  companion object {
    private val RESULT_POLLING_DELAY = Duration.ofSeconds(10)
    private val RPC_DEADLINE_DURATION = Duration.ofSeconds(30)
    private val CONFIG_PATH =
      Paths.get("src", "test", "kotlin", "org", "wfanet", "measurement", "integration", "k8s")
    private const val TEST_CONFIG_NAME = "correctness_test_config.textproto"
    private const val STORAGE_CONFIG_NAME = "gcs_config.textproto"

    private val TEST_CONFIG: CorrectnessTestConfig by lazy {
      val configFile = getRuntimePath(CONFIG_PATH.resolve(TEST_CONFIG_NAME)).toFile()
      parseTextProto(configFile, CorrectnessTestConfig.getDefaultInstance())
    }

    private val STORAGE_CONFIG: GoogleCloudStorageConfig by lazy {
      val configFile = getRuntimePath(CONFIG_PATH.resolve(STORAGE_CONFIG_NAME)).toFile()
      parseTextProto(configFile, GoogleCloudStorageConfig.getDefaultInstance())
    }

    private val tempDir = TemporaryFolder()
    private val measurementSystem = RunningMeasurementSystem()

    @ClassRule @JvmField val chainedRule = chainRulesSequentially(tempDir, measurementSystem)
  }
}
