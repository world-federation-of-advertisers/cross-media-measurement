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

package org.wfanet.panelmatch.client.storage.testing

import java.time.LocalDate
import org.wfanet.measurement.storage.StorageClient
import org.wfanet.measurement.storage.testing.InMemoryStorageClient
import org.wfanet.panelmatch.client.common.ExchangeContext
import org.wfanet.panelmatch.client.common.ExchangeStepAttemptKey
import org.wfanet.panelmatch.client.internal.ExchangeWorkflow
import org.wfanet.panelmatch.client.internal.ExchangeWorkflowKt.exchangeIdentifiers
import org.wfanet.panelmatch.client.internal.ExchangeWorkflowKt.step
import org.wfanet.panelmatch.client.internal.exchangeWorkflow
import org.wfanet.panelmatch.client.storage.PrivateStorageSelector
import org.wfanet.panelmatch.client.storage.SharedStorageSelector
import org.wfanet.panelmatch.client.storage.SigningStorageClient
import org.wfanet.panelmatch.client.storage.StorageDetails
import org.wfanet.panelmatch.client.storage.StorageDetails.PlatformCase
import org.wfanet.panelmatch.client.storage.StorageDetailsProvider
import org.wfanet.panelmatch.client.storage.VerifyingStorageClient
import org.wfanet.panelmatch.common.ExchangeDateKey
import org.wfanet.panelmatch.common.certificates.testing.TestCertificateManager
import org.wfanet.panelmatch.common.secrets.MutableSecretMap
import org.wfanet.panelmatch.common.storage.StorageFactory
import org.wfanet.panelmatch.common.storage.testing.InMemoryStorageFactory
import org.wfanet.panelmatch.protocol.namedSignature

private fun makeTestStorageFactoryMap(
  underlyingStorage: StorageFactory
): Map<PlatformCase, (StorageDetails, ExchangeDateKey) -> StorageFactory> {
  val builder: (StorageDetails, ExchangeDateKey) -> StorageFactory = { _, _ -> underlyingStorage }
  return mapOf(
    PlatformCase.FILE to builder,
    PlatformCase.AWS to builder,
    PlatformCase.GCS to builder,
  )
}

fun makeTestPrivateStorageSelector(
  secretMap: MutableSecretMap,
  underlyingClient: InMemoryStorageClient,
): PrivateStorageSelector {
  return PrivateStorageSelector(
    makeTestStorageFactoryMap(InMemoryStorageFactory(underlyingClient)),
    StorageDetailsProvider(secretMap),
  )
}

fun makeTestSharedStorageSelector(
  secretMap: MutableSecretMap,
  underlyingClient: InMemoryStorageClient,
): SharedStorageSelector {
  return SharedStorageSelector(
    TestCertificateManager,
    makeTestStorageFactoryMap(InMemoryStorageFactory(underlyingClient)),
    StorageDetailsProvider(secretMap),
  )
}

private val WORKFLOW = exchangeWorkflow {
  exchangeIdentifiers = exchangeIdentifiers {
    modelProviderId = "some-model-provider"
    dataProviderId = "some-data-provider"
  }
  steps += step { party = ExchangeWorkflow.Party.DATA_PROVIDER }
}

private val TEST_CONTEXT =
  ExchangeContext(
    ExchangeStepAttemptKey(
      "some-recurring-exchange-id",
      "some-exchange-id",
      "some-step-id",
      "some-attempt-id",
    ),
    LocalDate.of(2020, 10, 6),
    WORKFLOW,
    WORKFLOW.stepsList.first(),
  )

fun makeTestVerifyingStorageClient(
  underlyingClient: StorageClient = InMemoryStorageClient()
): VerifyingStorageClient {
  return VerifyingStorageClient({ underlyingClient }, TestCertificateManager.CERTIFICATE)
}

fun makeTestSigningStorageClient(
  underlyingClient: StorageClient = InMemoryStorageClient()
): SigningStorageClient {
  return SigningStorageClient(
    { underlyingClient },
    TestCertificateManager.CERTIFICATE,
    TestCertificateManager.PRIVATE_KEY,
    namedSignature { certificateName = TestCertificateManager.RESOURCE_NAME },
  )
}
