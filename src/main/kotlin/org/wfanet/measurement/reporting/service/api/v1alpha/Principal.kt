// Copyright 2022 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.reporting.service.api.v1alpha

import org.wfanet.measurement.api.v2alpha.DuchyKey
import org.wfanet.measurement.api.v2alpha.MeasurementConsumerKey
import org.wfanet.measurement.common.api.ResourceKey

/**
 * Identifies the sender of an inbound gRPC request.
 *
 * TODO: once using Kotlin 1.5, switch to a sealed interface.
 */
sealed class Principal<T : ResourceKey> {
  abstract val resourceKey: T

  class MeasurementConsumer(override val resourceKey: MeasurementConsumerKey) :
    Principal<MeasurementConsumerKey>()

  companion object {
    fun fromName(name: String): Principal<*>? {
      return when (name.substringBefore('/')) {
        MeasurementConsumerKey.COLLECTION_NAME ->
          MeasurementConsumerKey.fromName(name)?.let(::MeasurementConsumer)
        else -> null
      }
    }
  }
}
