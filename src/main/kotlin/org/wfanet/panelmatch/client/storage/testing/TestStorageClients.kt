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
import org.wfanet.measurement.api.v2alpha.ExchangeStepAttemptKey
import org.wfanet.measurement.api.v2alpha.ExchangeWorkflow
import org.wfanet.measurement.api.v2alpha.ExchangeWorkflowKt.exchangeIdentifiers
import org.wfanet.measurement.api.v2alpha.ExchangeWorkflowKt.step
import org.wfanet.measurement.api.v2alpha.exchangeWorkflow
import org.wfanet.measurement.storage.StorageClient
import org.wfanet.measurement.storage.testing.InMemoryStorageClient
import org.wfanet.panelmatch.client.common.ExchangeContext
import org.wfanet.panelmatch.client.storage.PrivateStorageSelector
import org.wfanet.panelmatch.client.storage.SharedStorageSelector
import org.wfanet.panelmatch.client.storage.StorageDetails
import org.wfanet.panelmatch.client.storage.StorageDetailsProvider
import org.wfanet.panelmatch.client.storage.StorageFactory
import org.wfanet.panelmatch.client.storage.VerifiedStorageClient
import org.wfanet.panelmatch.common.ExchangeDateKey
import org.wfanet.panelmatch.common.certificates.testing.TestCertificateManager
import org.wfanet.panelmatch.common.secrets.SecretMap

fun makeTestPrivateStorageSelector(
  secretMap: SecretMap,
  underlyingClient: InMemoryStorageClient
): PrivateStorageSelector {

  val rootInMemoryStorageFactory = InMemoryStorageFactory(underlyingClient)
  val builder: ExchangeDateKey.(StorageDetails) -> StorageFactory = { rootInMemoryStorageFactory }

  return PrivateStorageSelector(
    mapOf(
      StorageDetails.PlatformCase.FILE to builder,
      StorageDetails.PlatformCase.AWS to builder,
      StorageDetails.PlatformCase.GCS to builder
    ),
    StorageDetailsProvider(secretMap)
  )
}

fun makeTestSharedStorageSelector(
  secretMap: SecretMap,
  underlyingClient: InMemoryStorageClient
): SharedStorageSelector {

  val rootInMemoryStorageFactory = InMemoryStorageFactory(underlyingClient)
  val builder: ExchangeContext.(StorageDetails) -> StorageFactory = { rootInMemoryStorageFactory }

  return SharedStorageSelector(
    TestCertificateManager,
    mapOf(
      StorageDetails.PlatformCase.FILE to builder,
      StorageDetails.PlatformCase.AWS to builder,
      StorageDetails.PlatformCase.GCS to builder
    ),
    StorageDetailsProvider(secretMap)
  )
}

private val WORKFLOW = exchangeWorkflow {
  exchangeIdentifiers =
    exchangeIdentifiers {
      modelProvider = "some-model-provider"
      dataProvider = "some-data-provider"
    }
  steps += step { party = ExchangeWorkflow.Party.DATA_PROVIDER }
}

private val TEST_CONTEXT =
  ExchangeContext(
    ExchangeStepAttemptKey(
      "some-recurring-exchange-id",
      "some-exchange-id",
      "some-step-id",
      "some-attempt-id"
    ),
    LocalDate.of(2020, 10, 6),
    WORKFLOW,
    WORKFLOW.stepsList.first()
  )

fun makeTestVerifiedStorageClient(
  underlyingClient: StorageClient = InMemoryStorageClient()
): VerifiedStorageClient {
  return VerifiedStorageClient(underlyingClient, TEST_CONTEXT, TestCertificateManager)
}
