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

package org.wfanet.measurement.integration.common

import com.google.protobuf.ByteString
import java.security.cert.X509Certificate
import kotlinx.coroutines.runBlocking
import org.junit.rules.TestRule
import org.junit.runner.Description
import org.junit.runners.model.Statement
import org.wfanet.measurement.api.v2alpha.EventGroup
import org.wfanet.measurement.common.crypto.subjectKeyIdentifier
import org.wfanet.measurement.common.crypto.tink.TinkPrivateKeyHandle
import org.wfanet.measurement.common.testing.ProviderRule
import org.wfanet.measurement.common.testing.chainRulesSequentially
import org.wfanet.measurement.gcloud.testing.CloudFunctionProcessDetails
import org.wfanet.measurement.gcloud.testing.FunctionsFrameworkInvokerProcess
import org.wfanet.measurement.loadtest.resourcesetup.EntityContent
import org.wfanet.measurement.loadtest.resourcesetup.Resources
import org.wfanet.measurement.securecomputation.service.internal.Services

class InProcessEdpAggregatorComponents(private val internalServicesRule: ProviderRule<Services>) :
  TestRule {

  private val internalServices: Services
    get() = internalServicesRule.value

  val secureComputationPublicApi =
    InProcessSecureComputationPublicApi(internalServicesProvider = { internalServices })

  private val dataWatcherDetails: CloudFunctionProcessDetails by lazy {
    val authority = secureComputationPublicApi.publicApiChannel.authority()
    DATA_WATCHER_CONFIG.copy(
      envVars =
        DATA_WATCHER_CONFIG.envVars +
          mapOf("CONTROL_PLANE_TARGET" to authority, "CONTROL_PLANE_CERT_HOST" to authority.split(":")[0])
    )
  }

  private val dataWatcher: FunctionsFrameworkInvokerProcess by lazy {
    FunctionsFrameworkInvokerProcess(
      javaBinaryPath = dataWatcherDetails.functionBinaryPath,
      classTarget = dataWatcherDetails.classTarget,
    )
  }

  val ruleChain: TestRule by lazy {
    chainRulesSequentially(internalServicesRule, secureComputationPublicApi)
  }

  private lateinit var edpDisplayNameToResourceMap: Map<String, Resources.Resource>
  lateinit var eventGroups: List<EventGroup>

  private suspend fun createAllResources() {}

  fun getDataProviderResourceNames(): List<String> {
    return edpDisplayNameToResourceMap.values.map { it.name }
  }

  fun startDaemons() = runBlocking {
    // Create all resources
    createAllResources()
    dataWatcher.start(dataWatcherDetails.envVars)
  }

  fun stopDaemons() {}

  override fun apply(statement: Statement, description: Description): Statement {
    return ruleChain.apply(statement, description)
  }

  companion object {
    val MC_ENTITY_CONTENT: EntityContent = createEntityContent(MC_DISPLAY_NAME)
    val MC_ENCRYPTION_PRIVATE_KEY: TinkPrivateKeyHandle =
      loadEncryptionPrivateKey("${MC_DISPLAY_NAME}_enc_private.tink")
    val TRUSTED_CERTIFICATES: Map<ByteString, X509Certificate> =
      loadTestCertCollection("all_root_certs.pem").associateBy {
        checkNotNull(it.subjectKeyIdentifier)
      }

    @JvmStatic fun initConfig() {}
  }
}
