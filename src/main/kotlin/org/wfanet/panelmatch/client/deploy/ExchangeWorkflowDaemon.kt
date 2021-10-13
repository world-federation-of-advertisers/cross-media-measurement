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

package org.wfanet.panelmatch.client.deploy

import java.security.PrivateKey
import java.security.cert.X509Certificate
import kotlinx.coroutines.runBlocking
import org.wfanet.measurement.common.logAndSuppressExceptionSuspend
import org.wfanet.measurement.common.throttler.Throttler
import org.wfanet.measurement.storage.StorageClient
import org.wfanet.panelmatch.client.common.BrotliCompressorFactory
import org.wfanet.panelmatch.client.exchangetasks.ExchangeTaskMapperForJoinKeyExchange
import org.wfanet.panelmatch.client.launcher.ApiClient
import org.wfanet.panelmatch.client.launcher.CoroutineLauncher
import org.wfanet.panelmatch.client.launcher.ExchangeStepLauncher
import org.wfanet.panelmatch.client.launcher.ExchangeStepValidator
import org.wfanet.panelmatch.client.launcher.ExchangeTaskExecutor
import org.wfanet.panelmatch.client.launcher.Identity
import org.wfanet.panelmatch.client.privatemembership.JniPrivateMembershipCryptor
import org.wfanet.panelmatch.client.privatemembership.JniQueryResultsDecryptor
import org.wfanet.panelmatch.client.storage.VerifiedStorageClient
import org.wfanet.panelmatch.common.Timeout
import org.wfanet.panelmatch.common.crypto.JniDeterministicCommutativeCipher
import org.wfanet.panelmatch.common.secrets.SecretMap

/** Runs ExchangeWorkflows. */
abstract class ExchangeWorkflowDaemon : Runnable {

  /** Identity of the party executing this daemon. */
  abstract val identity: Identity

  /** Kingdom [ApiClient]. */
  abstract val apiClient: ApiClient

  // TODO derive `localCertificate`
  abstract val localCertificate: X509Certificate

  // TODO derive `partnerCertificate`
  abstract val partnerCertificate: X509Certificate

  // TODO derive `uriPrefix`
  abstract val uriPrefix: String

  /** [VerifiedStorageClient] for writing to local (non-shared) storage. */
  abstract val privateStorage: StorageClient

  /** [SecretMap] from RecurringExchange ID to serialized ExchangeWorkflow. */
  abstract val validExchangeWorkflows: SecretMap

  /** Limits how often to poll. */
  abstract val throttler: Throttler

  /** How long a task should be allowed to run for before being cancelled. */
  abstract val taskTimeout: Timeout

  // TODO: this will be refactored very shortly. In general, this (a) doesn't work and (b) is unsafe
  abstract val privateKey: PrivateKey

  override fun run() {
    val exchangeTaskMapper =
      ExchangeTaskMapperForJoinKeyExchange(
        compressorFactory = BrotliCompressorFactory(),
        getDeterministicCommutativeCryptor = ::JniDeterministicCommutativeCipher,
        getPrivateMembershipCryptor = ::JniPrivateMembershipCryptor,
        getQueryResultsDecryptor = ::JniQueryResultsDecryptor,
        localCertificate = localCertificate,
        partnerCertificate = partnerCertificate,
        privateStorage = privateStorage,
        uriPrefix = uriPrefix,
        privateKey = privateKey
      )

    val stepExecutor =
      ExchangeTaskExecutor(
        apiClient = apiClient,
        timeout = taskTimeout,
        privateStorage = privateStorage,
        getExchangeTaskForStep = exchangeTaskMapper::getExchangeTaskForStep
      )

    val launcher = CoroutineLauncher(stepExecutor = stepExecutor)

    val exchangeStepLauncher =
      ExchangeStepLauncher(
        apiClient = apiClient,
        validator = ExchangeStepValidator(identity.party, validExchangeWorkflows),
        jobLauncher = launcher
      )

    runBlocking {
      throttler.loopOnReady {
        logAndSuppressExceptionSuspend { exchangeStepLauncher.findAndRunExchangeStep() }
      }
    }
  }
}
