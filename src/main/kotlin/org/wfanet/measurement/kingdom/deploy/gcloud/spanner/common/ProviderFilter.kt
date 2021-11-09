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

package org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common

import io.grpc.Status
import org.wfanet.measurement.common.grpc.failGrpc
import org.wfanet.measurement.internal.kingdom.Provider

fun providerFilter(provider: Provider, param: String): String {
  @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA")
  return when (provider.type) {
    Provider.Type.DATA_PROVIDER -> "DataProviders.ExternalDataProviderId = @$param"
    Provider.Type.MODEL_PROVIDER -> "ModelProviders.ExternalModelProviderId = @$param"
    Provider.Type.TYPE_UNSPECIFIED, Provider.Type.UNRECOGNIZED ->
      failGrpc(Status.INVALID_ARGUMENT) {
        "external_data_provider_id or external_model_provider_id must be provided."
      }
  }
}

fun stepProviderFilter(stepProvider: Provider, param: String): String {
  @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA")
  return when (stepProvider.type) {
    Provider.Type.DATA_PROVIDER ->
      "ExchangeSteps.DataProviderId = (SELECT DataProviderId FROM DataProviders " +
        "WHERE ExternalDataProviderId = @$param)"
    Provider.Type.MODEL_PROVIDER ->
      "ExchangeSteps.ModelProviderId = (SELECT ModelProviderId FROM ModelProviders " +
        "WHERE ExternalModelProviderId = @$param)"
    Provider.Type.TYPE_UNSPECIFIED, Provider.Type.UNRECOGNIZED ->
      failGrpc(Status.INVALID_ARGUMENT) {
        "external_data_provider_id or external_model_provider_id must be provided."
      }
  }
}
