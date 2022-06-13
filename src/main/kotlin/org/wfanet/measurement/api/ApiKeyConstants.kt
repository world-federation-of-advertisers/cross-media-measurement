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

package org.wfanet.measurement.api

import io.grpc.Metadata
import io.grpc.stub.AbstractStub
import io.grpc.stub.MetadataUtils
import org.wfanet.measurement.internal.kingdom.MeasurementConsumer

object ApiKeyConstants {
  /** Metadata key for the api authentication key for a [MeasurementConsumer]. */
  val API_AUTHENTICATION_KEY_METADATA_KEY: Metadata.Key<String> =
    Metadata.Key.of("x-api-key", Metadata.ASCII_STRING_MARSHALLER)
}

fun <T : AbstractStub<T>> T.withAuthenticationKey(authenticationKey: String? = null): T {
  val extraHeaders = Metadata()
  authenticationKey?.let {
    extraHeaders.put(ApiKeyConstants.API_AUTHENTICATION_KEY_METADATA_KEY, it)
  }
  return withInterceptors(MetadataUtils.newAttachHeadersInterceptor(extraHeaders))
}
