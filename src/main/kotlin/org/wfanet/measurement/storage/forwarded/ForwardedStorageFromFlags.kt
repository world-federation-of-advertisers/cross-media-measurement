// Copyright 2020 The Measurement System Authors
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

package org.wfanet.measurement.storage.forwarded

import org.wfanet.measurement.common.grpc.buildChannel
import org.wfanet.measurement.internal.testing.ForwardedStorageGrpcKt.ForwardedStorageCoroutineStub
import org.wfanet.measurement.storage.StorageClient
import picocli.CommandLine

/**
 * Client access provider for Forwarded Storage via command-line flags.
 */
class ForwardedStorageFromFlags(private val flags: Flags) {

  val storageClient: StorageClient by lazy {
    ForwardedStorageClient(
      ForwardedStorageCoroutineStub(buildChannel(flags.forwardedStorageServiceTarget))
    )
  }

  class Flags {
    @CommandLine.Option(
      names = ["--forwarded-storage-service-target"],
      description = ["gRPC target (authority string or URI) for ForwardedStorage service."],
      required = true
    )
    lateinit var forwardedStorageServiceTarget: String
      private set
  }
}
