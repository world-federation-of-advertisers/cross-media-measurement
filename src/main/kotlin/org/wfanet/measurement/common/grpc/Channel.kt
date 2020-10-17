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

package org.wfanet.measurement.common.grpc

import io.grpc.ManagedChannel
import io.grpc.ManagedChannelBuilder
import java.time.Duration

/**
 * Builds a [ManagedChannel] for the specified target.
 *
 * @param target the URI or authority string for the target server
 */
fun buildChannel(target: String): ManagedChannel {
  return ManagedChannelBuilder.forTarget(target)
    // TODO: Remove this once TLS has been configured for our servers.
    .usePlaintext()
    .build()
}

/**
 * Builds a [ManagedChannel] for the specified target.
 *
 * @param target the URI or authority string for the target server
 * @param shutdownTimeout duration of time to allow a channel to finish
 *     processing on server shutdown
 */
fun buildChannel(target: String, shutdownTimeout: Duration): ManagedChannel {
  return buildChannel(target).also {
    Runtime.getRuntime().addShutdownHook(it, shutdownTimeout)
  }
}
