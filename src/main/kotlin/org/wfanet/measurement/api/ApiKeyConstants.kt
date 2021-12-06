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

import io.grpc.Context
import io.grpc.Metadata
import org.wfanet.measurement.internal.kingdom.MeasurementConsumer

object ApiKeyConstants {
  /** Context key for a [MeasurementConsumer]. */
  val CONTEXT_MEASUREMENT_CONSUMER_KEY: Context.Key<MeasurementConsumer> =
    Context.key("measurement_consumer")

  /** Context key for a provided api authentication key. */
  val CONTEXT_API_AUTHENTICATION_KEY_KEY: Context.Key<Long> = Context.key("api_authentication_key")

  /** Metadata key for the api authentication key for a [MeasurementConsumer]. */
  val API_AUTHENTICATION_KEY_METADATA_KEY: Metadata.Key<String> =
    Metadata.Key.of("x-api-key", Metadata.ASCII_STRING_MARSHALLER)
}
