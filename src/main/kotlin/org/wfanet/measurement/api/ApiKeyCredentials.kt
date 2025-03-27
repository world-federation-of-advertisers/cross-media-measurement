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

package org.wfanet.measurement.api

import io.grpc.CallCredentials
import io.grpc.Metadata
import io.grpc.stub.AbstractStub
import java.util.concurrent.Executor

class ApiKeyCredentials(val apiAuthenticationKey: String) : CallCredentials() {
  override fun applyRequestMetadata(
    requestInfo: RequestInfo,
    appExecutor: Executor,
    applier: MetadataApplier,
  ) {
    val headers =
      Metadata().apply {
        put(ApiKeyConstants.API_AUTHENTICATION_KEY_METADATA_KEY, apiAuthenticationKey)
      }
    applier.apply(headers)
  }

  companion object {
    fun fromHeaders(headers: Metadata): ApiKeyCredentials? {
      val apiAuthenticationKey =
        headers[ApiKeyConstants.API_AUTHENTICATION_KEY_METADATA_KEY] ?: return null
      return ApiKeyCredentials(apiAuthenticationKey)
    }
  }
}

fun <T : AbstractStub<T>> T.withApiKeyCredentials(apiAuthenticationKey: String) =
  withCallCredentials(ApiKeyCredentials(apiAuthenticationKey))
