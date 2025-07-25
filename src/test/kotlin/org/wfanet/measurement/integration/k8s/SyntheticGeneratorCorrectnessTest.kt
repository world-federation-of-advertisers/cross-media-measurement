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

import io.grpc.ManagedChannel
import java.nio.file.Paths
import java.util.UUID
import org.junit.ClassRule
import org.junit.rules.TemporaryFolder
import org.junit.rules.TestRule
import org.junit.runner.Description
import org.junit.runners.model.Statement
import org.measurement.integration.k8s.testing.CorrectnessTestConfig
import org.wfanet.measurement.api.v2alpha.CertificatesGrpcKt
import org.wfanet.measurement.api.v2alpha.DataProvidersGrpcKt
import org.wfanet.measurement.api.v2alpha.EventGroupsGrpcKt
import org.wfanet.measurement.api.v2alpha.MeasurementConsumersGrpcKt
import org.wfanet.measurement.api.v2alpha.MeasurementsGrpcKt
import org.wfanet.measurement.api.v2alpha.ProtocolConfig
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.SyntheticEventGroupSpec
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.SyntheticPopulationSpec
import org.wfanet.measurement.common.grpc.buildMutualTlsChannel
import org.wfanet.measurement.common.parseTextProto
import org.wfanet.measurement.common.testing.chainRulesSequentially
import org.wfanet.measurement.integration.common.SyntheticGenerationSpecs
import org.wfanet.measurement.loadtest.measurementconsumer.EventQueryMeasurementConsumerSimulator
import org.wfanet.measurement.loadtest.measurementconsumer.MeasurementConsumerData
import org.wfanet.measurement.loadtest.reporting.ReportingUserSimulator
import org.wfanet.measurement.reporting.v2alpha.MetricCalculationSpecsGrpcKt
import org.wfanet.measurement.reporting.v2alpha.ReportsGrpcKt

/**
 * Test for correctness of an existing CMMS on Kubernetes with EDP simulators.
 *
 * The computation composition is using ACDP by assumption.
 *
 * This currently assumes that the CMMS instance is using the certificates and keys from this Bazel
 * workspace. It also assumes that there is a Reporting system connected to the CMMS.
 */
class SyntheticGeneratorCorrectnessTest : AbstractCorrectnessTest(measurementSystem) {
  private class RunningMeasurementSystem : MeasurementSystem, TestRule {
    override val syntheticPopulationSpec: SyntheticPopulationSpec =
      SyntheticGenerationSpecs.SYNTHETIC_POPULATION_SPEC_LARGE
    override val syntheticEventGroupSpecs: List<SyntheticEventGroupSpec> =
      SyntheticGenerationSpecs.SYNTHETIC_DATA_SPECS_LARGE_2M

    override val runId: String by lazy { UUID.randomUUID().toString() }

    private lateinit var _testHarness: EventQueryMeasurementConsumerSimulator
    private lateinit var _reportingTestHarness: ReportingUserSimulator

    override val testHarness: EventQueryMeasurementConsumerSimulator
      get() = _testHarness

    override val reportingTestHarness: ReportingUserSimulator
      get() = _reportingTestHarness

    private val channels = mutableListOf<ManagedChannel>()

    override fun apply(base: Statement, description: Description): Statement {
      return object : Statement() {
        override fun evaluate() {
          try {
            _testHarness = createTestHarness()
            _reportingTestHarness = createReportingTestHarness()
            base.evaluate()
          } finally {
            shutDownChannels()
          }
        }
      }
    }

    private fun createTestHarness(): EventQueryMeasurementConsumerSimulator {
      val measurementConsumerData =
        MeasurementConsumerData(
          TEST_CONFIG.measurementConsumer,
          MC_SIGNING_KEY,
          MC_ENCRYPTION_PRIVATE_KEY,
          TEST_CONFIG.apiAuthenticationKey,
        )

      val publicApiChannel =
        buildMutualTlsChannel(
            TEST_CONFIG.kingdomPublicApiTarget,
            MEASUREMENT_CONSUMER_SIGNING_CERTS,
            TEST_CONFIG.kingdomPublicApiCertHost.ifEmpty { null },
          )
          .also { channels.add(it) }

      return EventQueryMeasurementConsumerSimulator(
        measurementConsumerData,
        OUTPUT_DP_PARAMS,
        DataProvidersGrpcKt.DataProvidersCoroutineStub(publicApiChannel),
        EventGroupsGrpcKt.EventGroupsCoroutineStub(publicApiChannel),
        MeasurementsGrpcKt.MeasurementsCoroutineStub(publicApiChannel),
        MeasurementConsumersGrpcKt.MeasurementConsumersCoroutineStub(publicApiChannel),
        CertificatesGrpcKt.CertificatesCoroutineStub(publicApiChannel),
        MEASUREMENT_CONSUMER_SIGNING_CERTS.trustedCertificates,
        buildEventQuery(TEST_CONFIG.dataProvidersList),
        ProtocolConfig.NoiseMechanism.CONTINUOUS_GAUSSIAN,
      )
    }

    private fun createReportingTestHarness(): ReportingUserSimulator {
      val publicApiChannel =
        buildMutualTlsChannel(
            TEST_CONFIG.reportingPublicApiTarget,
            REPORTING_SIGNING_CERTS,
            TEST_CONFIG.reportingPublicApiCertHost,
          )
          .also { channels.add(it) }

      return ReportingUserSimulator(
        measurementConsumerName = TEST_CONFIG.measurementConsumer,
        dataProvidersClient = DataProvidersGrpcKt.DataProvidersCoroutineStub(publicApiChannel),
        eventGroupsClient =
          org.wfanet.measurement.reporting.v2alpha.EventGroupsGrpcKt.EventGroupsCoroutineStub(
            publicApiChannel
          ),
        reportingSetsClient =
          org.wfanet.measurement.reporting.v2alpha.ReportingSetsGrpcKt.ReportingSetsCoroutineStub(
            publicApiChannel
          ),
        metricCalculationSpecsClient =
          MetricCalculationSpecsGrpcKt.MetricCalculationSpecsCoroutineStub(publicApiChannel),
        reportsClient = ReportsGrpcKt.ReportsCoroutineStub(publicApiChannel),
      )
    }

    private fun shutDownChannels() {
      for (channel in channels) {
        channel.shutdown()
      }
    }
  }

  companion object {
    private val CONFIG_PATH =
      Paths.get("src", "test", "kotlin", "org", "wfanet", "measurement", "integration", "k8s")
    private const val TEST_CONFIG_NAME = "correctness_test_config.textproto"

    private val TEST_CONFIG: CorrectnessTestConfig by lazy {
      val configFile = getRuntimePath(CONFIG_PATH.resolve(TEST_CONFIG_NAME)).toFile()
      parseTextProto(configFile, CorrectnessTestConfig.getDefaultInstance())
    }

    private val tempDir = TemporaryFolder()
    private val measurementSystem = RunningMeasurementSystem()

    @ClassRule @JvmField val chainedRule = chainRulesSequentially(tempDir, measurementSystem)
  }
}
