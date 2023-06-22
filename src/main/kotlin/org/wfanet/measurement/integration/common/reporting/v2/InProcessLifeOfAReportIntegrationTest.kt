/*
 * Copyright 2023 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.integration.common.reporting.v2

import com.google.common.truth.extensions.proto.ProtoTruth.assertThat
import java.io.File
import java.nio.file.Paths
import kotlinx.coroutines.runBlocking
import org.junit.After
import org.junit.Rule
import org.junit.Test
import org.junit.rules.TestRule
import org.junit.runners.model.Statement
import org.wfanet.measurement.api.v2alpha.MeasurementConsumersGrpcKt.MeasurementConsumersCoroutineStub
import org.wfanet.measurement.api.v2alpha.getMeasurementConsumerRequest
import org.wfanet.measurement.api.withAuthenticationKey
import org.wfanet.measurement.common.crypto.readCertificateCollection
import org.wfanet.measurement.common.crypto.subjectKeyIdentifier
import org.wfanet.measurement.common.getRuntimePath
import org.wfanet.measurement.common.testing.ProviderRule
import org.wfanet.measurement.common.testing.chainRulesSequentially
import org.wfanet.measurement.config.reporting.EncryptionKeyPairConfig
import org.wfanet.measurement.config.reporting.EncryptionKeyPairConfigKt.keyPair
import org.wfanet.measurement.config.reporting.EncryptionKeyPairConfigKt.principalKeyPairs
import org.wfanet.measurement.config.reporting.MeasurementConsumerConfig
import org.wfanet.measurement.config.reporting.encryptionKeyPairConfig
import org.wfanet.measurement.config.reporting.measurementConsumerConfig
import org.wfanet.measurement.integration.common.InProcessCmmsComponents
import org.wfanet.measurement.integration.common.InProcessDuchy
import org.wfanet.measurement.integration.common.reporting.v2.identity.withPrincipalName
import org.wfanet.measurement.kingdom.deploy.common.service.DataServices
import org.wfanet.measurement.reporting.deploy.v2.common.server.InternalReportingServer
import org.wfanet.measurement.reporting.v2alpha.ReportingSetsGrpcKt.ReportingSetsCoroutineStub
import org.wfanet.measurement.reporting.v2alpha.listReportingSetsRequest
import org.wfanet.measurement.storage.StorageClient

/**
 * Test that everything is wired up properly.
 *
 * This is abstract so that different implementations of dependencies can all run the same tests
 * easily.
 */
abstract class InProcessLifeOfAReportIntegrationTest {
  abstract val kingdomDataServicesRule: ProviderRule<DataServices>

  /** Provides a function from Duchy to the dependencies needed to start the Duchy to the test. */
  abstract val duchyDependenciesRule: ProviderRule<(String) -> InProcessDuchy.DuchyDependencies>

  abstract val storageClient: StorageClient

  private val inProcessCmmsComponents: InProcessCmmsComponents by lazy {
    InProcessCmmsComponents(kingdomDataServicesRule, duchyDependenciesRule, storageClient)
  }

  private val inProcessCmmsComponentsStartup: TestRule by lazy {
    TestRule { statement, _ ->
      object : Statement() {
        override fun evaluate() {
          InProcessCmmsComponents.initConfig()
          inProcessCmmsComponents.startDaemons()
          statement.evaluate()
        }
      }
    }
  }

  abstract val internalReportingServerServices: InternalReportingServer.Services

  private val reportingServer: InProcessReportingServer by lazy {
    val encryptionKeyPairConfigGenerator: () -> EncryptionKeyPairConfig = {
      val measurementConsumerData = inProcessCmmsComponents.getMeasurementConsumerData()

      encryptionKeyPairConfig {
        principalKeyPairs += principalKeyPairs {
          principal = measurementConsumerData.name
          keyPairs += keyPair {
            publicKeyFile = "mc_enc_public.tink"
            privateKeyFile = "mc_enc_private.tink"
          }
        }
      }
    }

    val measurementConsumerConfigGenerator: suspend () -> MeasurementConsumerConfig = {
      val measurementConsumerData = inProcessCmmsComponents.getMeasurementConsumerData()

      val measurementConsumer =
        publicKingdomMeasurementConsumersClient
          .withAuthenticationKey(measurementConsumerData.apiAuthenticationKey)
          .getMeasurementConsumer(
            getMeasurementConsumerRequest { name = measurementConsumerData.name }
          )

      measurementConsumerConfig {
        apiKey = measurementConsumerData.apiAuthenticationKey
        signingCertificateName = measurementConsumer.certificate
        signingPrivateKeyPath = MC_SIGNING_PRIVATE_KEY_PATH
      }
    }

    InProcessReportingServer(
      internalReportingServerServices,
      { inProcessCmmsComponents.kingdom.publicApiChannel },
      encryptionKeyPairConfigGenerator,
      SECRETS_DIR,
      measurementConsumerConfigGenerator,
      TRUSTED_CERTIFICATES,
      verboseGrpcLogging = false,
    )
  }

  @get:Rule
  val ruleChain: TestRule by lazy {
    chainRulesSequentially(
      inProcessCmmsComponents,
      inProcessCmmsComponentsStartup,
      reportingServer
    )
  }

  private val publicKingdomMeasurementConsumersClient by lazy {
    MeasurementConsumersCoroutineStub(inProcessCmmsComponents.kingdom.publicApiChannel)
  }

  private val publicReportingSetsClient by lazy {
    ReportingSetsCoroutineStub(reportingServer.publicApiChannel)
  }

  @After
  fun stopComponents() {
    inProcessCmmsComponents.stopEdpSimulators()
    inProcessCmmsComponents.stopDuchyDaemons()
  }

  @Test
  fun `connection test`() = runBlocking {
    // TODO(@tristanvuong2021): Add tests covering different scenarios
    val measurementConsumerName = inProcessCmmsComponents.getMeasurementConsumerData().name
    val reportingSets =
      publicReportingSetsClient
        .withPrincipalName(measurementConsumerName)
        .listReportingSets(listReportingSetsRequest { parent = measurementConsumerName })
    assertThat(reportingSets.reportingSetsList).isEmpty()
  }

  companion object {
    private val SECRETS_DIR: File =
      getRuntimePath(
          Paths.get(
            "wfa_measurement_system",
            "src",
            "main",
            "k8s",
            "testing",
            "secretfiles",
          )
        )!!
        .toFile()

    private val TRUSTED_CERTIFICATES =
      readCertificateCollection(SECRETS_DIR.resolve("all_root_certs.pem")).associateBy {
        it.subjectKeyIdentifier!!
      }

    private const val MC_SIGNING_PRIVATE_KEY_PATH = "mc_cs_private.der"
  }
}
