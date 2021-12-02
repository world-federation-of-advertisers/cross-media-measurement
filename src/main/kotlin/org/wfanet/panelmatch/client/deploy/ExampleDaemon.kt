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

import org.apache.beam.sdk.options.PipelineOptions
import org.apache.beam.sdk.options.PipelineOptionsFactory
import org.wfanet.measurement.storage.StorageClient
import org.wfanet.panelmatch.client.storage.StorageDetailsProvider
import org.wfanet.panelmatch.common.secrets.MutableSecretMap
import org.wfanet.panelmatch.common.secrets.SecretMap
import picocli.CommandLine.Mixin
import picocli.CommandLine.Option

/** Example base class for [ExchangeWorkflowDaemon] implementations. */
abstract class ExampleDaemon : ExchangeWorkflowDaemonFromFlags() {
  @Option(names = ["--tink-key-uri"], description = ["KMS URI for Tink"], required = true)
  private lateinit var tinkKeyUri: String

  @Mixin private lateinit var blobSizeFlags: BlobSizeFlags

  /** This MUST be customized per deployment. */
  abstract val rootStorageClient: StorageClient

  /** This should be customized per deployment. */
  override val pipelineOptions: PipelineOptions = PipelineOptionsFactory.create()

  /** This can be customized per deployment. */
  private val defaults by lazy { DaemonStorageClientDefaults(rootStorageClient, tinkKeyUri) }

  /** This can be customized per deployment. */
  override val validExchangeWorkflows: SecretMap
    get() = defaults.validExchangeWorkflows

  /** This can be customized per deployment. */
  override val privateKeys: MutableSecretMap
    get() = defaults.privateKeys

  /** This can be customized per deployment. */
  override val rootCertificates: SecretMap
    get() = defaults.rootCertificates

  /** This can be customized per deployment. */
  override val privateStorageInfo: StorageDetailsProvider
    get() = defaults.privateStorageInfo

  /** This can be customized per deployment. */
  override val sharedStorageInfo: StorageDetailsProvider
    get() = defaults.sharedStorageInfo

  /** This can be customized per deployment. */
  override val privateStorageFactories = defaultStorageFactories

  /** This can be customized per deployment. */
  override val sharedStorageFactories =
    defaultStorageFactories.mapValues { blobSizeFlags.wrapStorageFactory(it.value) }
}
