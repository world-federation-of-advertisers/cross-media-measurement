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

package org.wfanet.panelmatch.client.deploy.example

import org.apache.beam.sdk.options.PipelineOptions
import org.apache.beam.sdk.options.PipelineOptionsFactory
import org.wfanet.measurement.storage.StorageClient
import org.wfanet.panelmatch.client.deploy.BlobSizeFlags
import org.wfanet.panelmatch.client.deploy.ExchangeWorkflowDaemonFromFlags
import picocli.CommandLine.Mixin
import picocli.CommandLine.Option

/** Example base class for [ExchangeWorkflowDaemonFromFlags] implementations. */
abstract class ExampleDaemon : ExchangeWorkflowDaemonFromFlags() {
  @Option(names = ["--tink-key-uri"], description = ["KMS URI for Tink"], required = true)
  protected lateinit var tinkKeyUri: String

  @Mixin private lateinit var blobSizeFlags: BlobSizeFlags

  /** This MUST be customized per deployment. */
  abstract val rootStorageClient: StorageClient

  /** This should be customized per deployment. */
  override fun makePipelineOptions(): PipelineOptions = PipelineOptionsFactory.create()

  /** This can be customized per deployment. */
  override val privateStorageFactories = exampleStorageFactories

  /** This can be customized per deployment. */
  override val sharedStorageFactories by lazy {
    exampleStorageFactories.mapValues { blobSizeFlags.wrapStorageFactory(it.value) }
  }
}
