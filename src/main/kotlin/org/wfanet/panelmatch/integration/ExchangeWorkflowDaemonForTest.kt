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

import com.google.protobuf.ByteString
import io.grpc.Channel
import java.nio.file.Path
import java.time.Clock
import java.time.Duration
import kotlinx.coroutines.runBlocking
import org.apache.beam.sdk.options.PipelineOptionsFactory
import org.wfanet.measurement.api.v2alpha.DataProviderKey
import org.wfanet.measurement.api.v2alpha.ExchangeStepAttemptsGrpcKt.ExchangeStepAttemptsCoroutineStub
import org.wfanet.measurement.api.v2alpha.ExchangeStepsGrpcKt.ExchangeStepsCoroutineStub
import org.wfanet.measurement.api.v2alpha.ModelProviderKey
import org.wfanet.measurement.common.api.ResourceKey
import org.wfanet.measurement.common.identity.withPrincipalName
import org.wfanet.measurement.common.throttler.MinimumIntervalThrottler
import org.wfanet.measurement.common.throttler.Throttler
import org.wfanet.measurement.storage.StorageClient
import org.wfanet.measurement.storage.filesystem.FileSystemStorageClient
import org.wfanet.panelmatch.client.common.Identity
import org.wfanet.panelmatch.client.common.TaskParameters
import org.wfanet.panelmatch.client.deploy.DaemonStorageClientDefaults
import org.wfanet.panelmatch.client.deploy.ExchangeWorkflowDaemon
import org.wfanet.panelmatch.client.deploy.ProductionExchangeTaskMapper
import org.wfanet.panelmatch.client.eventpreprocessing.PreprocessingParameters
import org.wfanet.panelmatch.client.exchangetasks.ExchangeTaskMapper
import org.wfanet.panelmatch.client.internal.ExchangeWorkflow.Party
import org.wfanet.panelmatch.client.launcher.ApiClient
import org.wfanet.panelmatch.client.launcher.GrpcApiClient
import org.wfanet.panelmatch.client.storage.FileSystemStorageFactory
import org.wfanet.panelmatch.client.storage.StorageDetails
import org.wfanet.panelmatch.client.storage.StorageDetails.PlatformCase
import org.wfanet.panelmatch.client.storage.StorageDetailsProvider
import org.wfanet.panelmatch.common.ExchangeDateKey
import org.wfanet.panelmatch.common.Timeout
import org.wfanet.panelmatch.common.asTimeout
import org.wfanet.panelmatch.common.certificates.CertificateManager
import org.wfanet.panelmatch.common.certificates.testing.TestCertificateManager
import org.wfanet.panelmatch.common.secrets.SecretMap
import org.wfanet.panelmatch.common.storage.StorageFactory
import org.wfanet.panelmatch.common.storage.testing.FakeTinkKeyStorageProvider
import org.wfanet.panelmatch.common.storage.toByteString

/** Executes ExchangeWorkflows for InProcess Integration testing. */
class ExchangeWorkflowDaemonForTest(
  v2alphaChannel: Channel,
  provider: ResourceKey,
  private val exchangeDateKey: ExchangeDateKey,
  privateDirectory: Path,
  override val clock: Clock = Clock.systemUTC(),
  pollingInterval: Duration = Duration.ofMillis(100),
  taskTimeoutDuration: Duration = Duration.ofMinutes(2),
) : ExchangeWorkflowDaemon() {

  private val rootStorageClient: StorageClient by lazy {
    FileSystemStorageClient(privateDirectory.toFile())
  }

  private val tinkKeyUri = "fake-tink-key-uri"

  /** This can be customized per deployment. */
  private val defaults by lazy {
    DaemonStorageClientDefaults(rootStorageClient, tinkKeyUri, FakeTinkKeyStorageProvider())
  }

  /** This can be customized per deployment. */
  override val validExchangeWorkflows: SecretMap
    get() = defaults.validExchangeWorkflows

  /** This can be customized per deployment. */
  override val rootCertificates: SecretMap
    get() = defaults.rootCertificates

  /** This can be customized per deployment. */
  override val privateStorageInfo: StorageDetailsProvider
    get() = defaults.privateStorageInfo

  /** This can be customized per deployment. */
  override val sharedStorageInfo: StorageDetailsProvider
    get() = defaults.sharedStorageInfo

  override val certificateManager: CertificateManager = TestCertificateManager

  override val identity: Identity by lazy {
    when (provider) {
      is DataProviderKey -> Identity(provider.dataProviderId, Party.DATA_PROVIDER)
      is ModelProviderKey -> Identity(provider.modelProviderId, Party.MODEL_PROVIDER)
      else -> error("Invalid provider: $provider")
    }
  }

  override val maxParallelClaimedExchangeSteps = null

  override val apiClient: ApiClient by lazy {
    val providerName = provider.toName()
    val exchangeStepsClient =
      ExchangeStepsCoroutineStub(v2alphaChannel).withPrincipalName(providerName)
    val exchangeStepAttemptsClient =
      ExchangeStepAttemptsCoroutineStub(v2alphaChannel).withPrincipalName(providerName)

    GrpcApiClient(identity, exchangeStepsClient, exchangeStepAttemptsClient, clock)
  }

  override val throttler: Throttler = MinimumIntervalThrottler(clock, pollingInterval)

  override val runMode: RunMode = RunMode.DAEMON

  private val preprocessingParameters =
    PreprocessingParameters(maxByteSize = 1024 * 1024, fileCount = 1)

  private val taskContext: TaskParameters = TaskParameters(setOf(preprocessingParameters))

  override val exchangeTaskMapper: ExchangeTaskMapper by lazy {
    ProductionExchangeTaskMapper(
      privateStorageSelector = privateStorageSelector,
      sharedStorageSelector = sharedStorageSelector,
      certificateManager = certificateManager,
      inputTaskThrottler = MinimumIntervalThrottler(clock, Duration.ofMillis(250)),
      makePipelineOptions = PipelineOptionsFactory::create,
      taskContext = taskContext,
    )
  }

  override val privateStorageFactories:
    Map<PlatformCase, (StorageDetails, ExchangeDateKey) -> StorageFactory> =
    mapOf(PlatformCase.FILE to ::FileSystemStorageFactory)

  /** Reads a blob from private storage for an exchange. */
  fun readPrivateBlob(blobKey: String): ByteString? = runBlocking {
    privateStorageSelector.getStorageClient(exchangeDateKey).getBlob(blobKey)?.toByteString()
  }

  override val sharedStorageFactories:
    Map<PlatformCase, (StorageDetails, ExchangeDateKey) -> StorageFactory> =
    mapOf(PlatformCase.FILE to ::FileSystemStorageFactory)

  override val taskTimeout: Timeout = taskTimeoutDuration.asTimeout()
}
