// Copyright 2021 The Cross-Media Measurement Authors
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

package org.wfanet.panelmatch.integration

import io.grpc.Channel
import io.grpc.Status
import java.time.Clock
import java.time.Duration
import kotlinx.coroutines.CoroutineScope
import org.wfanet.measurement.api.v2alpha.DataProviderKey
import org.wfanet.measurement.api.v2alpha.ExchangeStepAttemptsGrpcKt.ExchangeStepAttemptsCoroutineStub
import org.wfanet.measurement.api.v2alpha.ExchangeStepsGrpcKt.ExchangeStepsCoroutineStub
import org.wfanet.measurement.api.v2alpha.ExchangeWorkflow
import org.wfanet.measurement.api.v2alpha.ModelProviderKey
import org.wfanet.measurement.api.v2alpha.ResourceKey
import org.wfanet.measurement.common.grpc.failGrpc
import org.wfanet.measurement.common.identity.withPrincipalName
import org.wfanet.measurement.common.throttler.MinimumIntervalThrottler
import org.wfanet.measurement.common.throttler.Throttler
import org.wfanet.panelmatch.client.common.ExchangeContext
import org.wfanet.panelmatch.client.common.Identity
import org.wfanet.panelmatch.client.deploy.ExchangeWorkflowDaemon
import org.wfanet.panelmatch.client.exchangetasks.ExchangeTaskMapper
import org.wfanet.panelmatch.client.launcher.ApiClient
import org.wfanet.panelmatch.client.launcher.GrpcApiClient
import org.wfanet.panelmatch.client.storage.PrivateStorageSelector
import org.wfanet.panelmatch.client.storage.SharedStorageSelector
import org.wfanet.panelmatch.client.storage.StorageDetails
import org.wfanet.panelmatch.client.storage.StorageFactory
import org.wfanet.panelmatch.common.Timeout
import org.wfanet.panelmatch.common.asTimeout
import org.wfanet.panelmatch.common.certificates.CertificateManager
import org.wfanet.panelmatch.common.certificates.testing.TestCertificateManager
import org.wfanet.panelmatch.common.secrets.SecretMap

/** Executes ExchangeWorkflows for InProcess Integration testing. */
class ExchangeWorkflowDaemonForTest(
  override val privateStorageSelector: PrivateStorageSelector,
  override val sharedStorageSelector: SharedStorageSelector,
  override val clock: Clock,
  override val scope: CoroutineScope,
  override val validExchangeWorkflows: SecretMap,
  private val channel: Channel,
  private val providerKey: ResourceKey,
  private val taskTimeoutDuration: Duration,
  private val pollingInterval: Duration,
  override val rootCertificates: SecretMap,
  override val privateKeys: SecretMap,
  override val privateStorageFactories:
    Map<StorageDetails.PlatformCase, ExchangeContext.(StorageDetails) -> StorageFactory>,
  override val privateStorageInformation: SecretMap,
  override val sharedStorageFactories:
    Map<StorageDetails.PlatformCase, ExchangeContext.(StorageDetails) -> StorageFactory>,
  override val sharedStorageInformation: SecretMap,
) : ExchangeWorkflowDaemon() {

  override val certificateManager: CertificateManager by lazy { TestCertificateManager() }

  override val identity: Identity =
    when (providerKey) {
      is ModelProviderKey ->
        Identity(providerKey.modelProviderId, ExchangeWorkflow.Party.MODEL_PROVIDER)
      is DataProviderKey ->
        Identity(providerKey.dataProviderId, ExchangeWorkflow.Party.DATA_PROVIDER)
      else ->
        failGrpc(Status.UNAUTHENTICATED) {
          "Caller identity is neither DataProvider nor ModelProvider"
        }
    }

  override val apiClient: ApiClient by lazy {
    val exchangeStepsClient =
      ExchangeStepsCoroutineStub(channel).withPrincipalName(providerKey.toName())
    val exchangeStepAttemptsClient =
      ExchangeStepAttemptsCoroutineStub(channel).withPrincipalName(providerKey.toName())

    GrpcApiClient(identity, exchangeStepsClient, exchangeStepAttemptsClient, clock)
  }

  override val throttler: Throttler by lazy { MinimumIntervalThrottler(clock, pollingInterval) }

  override val exchangeTaskMapper: ExchangeTaskMapper by lazy {
    InProcessExchangeTaskMapper(
      privateStorageSelector = privateStorageSelector,
      sharedStorageSelector = sharedStorageSelector,
      certificateManager = certificateManager,
      inputTaskThrottler = throttler
    )
  }

  override val taskTimeout: Timeout by lazy { taskTimeoutDuration.asTimeout() }
}
