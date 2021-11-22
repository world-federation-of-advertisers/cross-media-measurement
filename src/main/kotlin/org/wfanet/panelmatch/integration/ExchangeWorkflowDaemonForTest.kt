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
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.runBlocking
import org.wfanet.measurement.api.v2alpha.ExchangeStepAttemptsGrpcKt.ExchangeStepAttemptsCoroutineStub
import org.wfanet.measurement.api.v2alpha.ExchangeStepsGrpcKt.ExchangeStepsCoroutineStub
import org.wfanet.measurement.api.v2alpha.ResourceKey
import org.wfanet.measurement.common.identity.withPrincipalName
import org.wfanet.measurement.common.throttler.MinimumIntervalThrottler
import org.wfanet.measurement.common.throttler.Throttler
import org.wfanet.measurement.common.toByteString
import org.wfanet.panelmatch.client.common.ExchangeContext
import org.wfanet.panelmatch.client.common.Identity
import org.wfanet.panelmatch.client.deploy.ExchangeWorkflowDaemon
import org.wfanet.panelmatch.client.exchangetasks.ExchangeTaskMapper
import org.wfanet.panelmatch.client.launcher.ApiClient
import org.wfanet.panelmatch.client.launcher.GrpcApiClient
import org.wfanet.panelmatch.client.storage.FileSystemStorageFactory
import org.wfanet.panelmatch.client.storage.StorageDetails
import org.wfanet.panelmatch.client.storage.StorageDetails.PlatformCase
import org.wfanet.panelmatch.client.storage.StorageDetailsKt.fileStorage
import org.wfanet.panelmatch.client.storage.StorageDetailsProvider
import org.wfanet.panelmatch.client.storage.StorageFactory
import org.wfanet.panelmatch.client.storage.storageDetails
import org.wfanet.panelmatch.common.ExchangeDateKey
import org.wfanet.panelmatch.common.Timeout
import org.wfanet.panelmatch.common.asTimeout
import org.wfanet.panelmatch.common.certificates.CertificateAuthority
import org.wfanet.panelmatch.common.certificates.CertificateManager
import org.wfanet.panelmatch.common.certificates.testing.TestCertificateAuthority
import org.wfanet.panelmatch.common.certificates.testing.TestCertificateManager
import org.wfanet.panelmatch.common.secrets.SecretMap
import org.wfanet.panelmatch.common.secrets.testing.TestSecretMap
import org.wfanet.panelmatch.common.storage.createBlob
import org.wfanet.panelmatch.common.storage.toByteString

/** Executes ExchangeWorkflows for InProcess Integration testing. */
class ExchangeWorkflowDaemonForTest(
  v2alphaChannel: Channel,
  provider: ResourceKey,
  partnerProvider: ResourceKey,
  private val exchangeDateKey: ExchangeDateKey,
  serializedExchangeWorkflow: ByteString,
  privateDirectory: Path,
  sharedDirectory: Path,
  override val scope: CoroutineScope,
  override val clock: Clock = Clock.systemUTC(),
  pollingInterval: Duration = Duration.ofMillis(250),
  taskTimeoutDuration: Duration = Duration.ofSeconds(30),
) : ExchangeWorkflowDaemon() {
  private val recurringExchangeId = exchangeDateKey.recurringExchangeId

  override val certificateManager: CertificateManager = TestCertificateManager

  override val identity: Identity = Identity.fromResourceKey(provider)

  override val apiClient: ApiClient by lazy {
    val providerName = provider.toName()
    val exchangeStepsClient =
      ExchangeStepsCoroutineStub(v2alphaChannel).withPrincipalName(providerName)
    val exchangeStepAttemptsClient =
      ExchangeStepAttemptsCoroutineStub(v2alphaChannel).withPrincipalName(providerName)

    GrpcApiClient(identity, exchangeStepsClient, exchangeStepAttemptsClient, clock)
  }

  override val rootCertificates: SecretMap =
    TestSecretMap(
      partnerProvider.toName() to TestCertificateManager.CERTIFICATE.encoded.toByteString()
    )

  override val validExchangeWorkflows: SecretMap =
    TestSecretMap(recurringExchangeId to serializedExchangeWorkflow)

  override val throttler: Throttler = MinimumIntervalThrottler(clock, pollingInterval)

  override val exchangeTaskMapper: ExchangeTaskMapper by lazy {
    InProcessExchangeTaskMapper(
      privateStorageSelector = privateStorageSelector,
      sharedStorageSelector = sharedStorageSelector,
      certificateManager = certificateManager,
      inputTaskThrottler = MinimumIntervalThrottler(clock, Duration.ofMillis(250))
    )
  }

  override val certificateAuthority: CertificateAuthority = TestCertificateAuthority

  override val privateStorageFactories:
    Map<PlatformCase, ExchangeDateKey.(StorageDetails) -> StorageFactory> =
    mapOf(PlatformCase.FILE to { storageDetails -> FileSystemStorageFactory(storageDetails, this) })

  private val privateStorageDetails = storageDetails {
    file = fileStorage { path = privateDirectory.toString() }
    visibility = StorageDetails.Visibility.PRIVATE
  }
  override val privateStorageInfo: StorageDetailsProvider =
    StorageDetailsProvider(
      TestSecretMap(recurringExchangeId to privateStorageDetails.toByteString())
    )

  /** Writes [contents] into private storage for an exchange. */
  fun writePrivateBlob(blobKey: String, contents: ByteString) =
    runBlocking(Dispatchers.IO) {
      privateStorageSelector.getStorageClient(exchangeDateKey).createBlob(blobKey, contents)
    }

  /** Reads a blob from private storage for an exchange. */
  fun readPrivateBlob(blobKey: String): ByteString? = runBlocking {
    privateStorageSelector.getStorageClient(exchangeDateKey).getBlob(blobKey)?.toByteString()
  }

  override val sharedStorageFactories:
    Map<PlatformCase, ExchangeContext.(StorageDetails) -> StorageFactory> =
    mapOf(
      PlatformCase.FILE to
        { storageDetails ->
          FileSystemStorageFactory(storageDetails, this.exchangeDateKey)
        }
    )

  private val sharedStorageDetails = storageDetails {
    file = fileStorage { path = sharedDirectory.toString() }
    visibility = StorageDetails.Visibility.SHARED
  }
  override val sharedStorageInfo: StorageDetailsProvider =
    StorageDetailsProvider(
      TestSecretMap(recurringExchangeId to sharedStorageDetails.toByteString())
    )

  override val taskTimeout: Timeout = taskTimeoutDuration.asTimeout()
}
