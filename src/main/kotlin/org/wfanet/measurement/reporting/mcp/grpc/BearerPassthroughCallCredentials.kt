/*
 * Copyright 2025 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.reporting.mcp.grpc

import io.grpc.CallCredentials
import io.grpc.Metadata
import java.util.concurrent.Executor

/**
 * gRPC [CallCredentials] that forwards a bearer token to the downstream Reporting public API.
 *
 * The token is attached as-is to the `authorization` metadata key with a `Bearer ` prefix.
 */
class BearerPassthroughCallCredentials(
  private val bearerToken: String,
) : CallCredentials() {
  override fun applyRequestMetadata(
    requestInfo: RequestInfo,
    appExecutor: Executor,
    applier: MetadataApplier,
  ) {
    val headers = Metadata()
    headers.put(AUTHORIZATION_KEY, "Bearer $bearerToken")
    applier.apply(headers)
  }

  companion object {
    private val AUTHORIZATION_KEY: Metadata.Key<String> =
      Metadata.Key.of("authorization", Metadata.ASCII_STRING_MARSHALLER)
  }
}
