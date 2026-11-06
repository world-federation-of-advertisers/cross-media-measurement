/*
 * Copyright 2026 The Cross-Media Measurement Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.wfanet.measurement.reporting.deploy.v2.common

import org.wfanet.measurement.common.grpc.ProtobufServiceConfig

/** Overrides the channel timeout while preserving its existing retry and backoff policy. */
fun serviceConfigWithTimeout(timeoutSeconds: Long): ProtobufServiceConfig {
  require(timeoutSeconds > 0) { "API timeout must be positive" }
  val timeout = com.google.protobuf.Duration.newBuilder().setSeconds(timeoutSeconds).build()
  return ProtobufServiceConfig.DEFAULT.copy {
    for (index in methodConfig.indices) {
      methodConfig[index] = methodConfig[index].toBuilder().setTimeout(timeout).build()
    }
  }
}
