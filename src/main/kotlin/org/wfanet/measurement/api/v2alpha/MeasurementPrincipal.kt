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

package org.wfanet.measurement.api.v2alpha

import org.wfanet.measurement.common.api.ResourcePrincipal

/** Identifies the sender of an inbound gRPC request. */
sealed interface MeasurementPrincipal : ResourcePrincipal {
  companion object {
    fun fromName(name: String): MeasurementPrincipal? {
      return when (name.substringBefore('/')) {
        DataProviderKey.COLLECTION_NAME ->
          DataProviderKey.fromName(name)?.let(::DataProviderPrincipal)
        ModelProviderKey.COLLECTION_NAME ->
          ModelProviderKey.fromName(name)?.let(::ModelProviderPrincipal)
        MeasurementConsumerKey.COLLECTION_NAME ->
          MeasurementConsumerKey.fromName(name)?.let(::MeasurementConsumerPrincipal)
        AccountKey.COLLECTION_NAME -> AccountKey.fromName(name)?.let(::AccountPrincipal)
        DuchyKey.COLLECTION_NAME -> DuchyKey.fromName(name)?.let(::DuchyPrincipal)
        else -> null
      }
    }
  }
}

data class DataProviderPrincipal(override val resourceKey: DataProviderKey) : MeasurementPrincipal

data class ModelProviderPrincipal(override val resourceKey: ModelProviderKey) :
  MeasurementPrincipal

data class MeasurementConsumerPrincipal(override val resourceKey: MeasurementConsumerKey) :
  MeasurementPrincipal

data class AccountPrincipal(override val resourceKey: AccountKey) : MeasurementPrincipal

data class DuchyPrincipal(override val resourceKey: DuchyKey) : MeasurementPrincipal
