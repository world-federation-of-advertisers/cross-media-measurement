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

package org.wfanet.measurement.kingdom.deploy.gcloud.spanner

import io.grpc.Status
import java.time.Clock
import org.wfanet.measurement.common.grpc.failGrpc
import org.wfanet.measurement.common.grpc.grpcRequire
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.common.identity.IdGenerator
import org.wfanet.measurement.gcloud.spanner.AsyncDatabaseClient
import org.wfanet.measurement.internal.kingdom.DataProvider
import org.wfanet.measurement.internal.kingdom.DataProvidersGrpcKt.DataProvidersCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.GetDataProviderRequest
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.DataProviderReader
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.writers.CreateDataProvider

class SpannerDataProvidersService(
  private val clock: Clock,
  private val idGenerator: IdGenerator,
  private val client: AsyncDatabaseClient
) : DataProvidersCoroutineImplBase() {
  override suspend fun createDataProvider(request: DataProvider): DataProvider {
    grpcRequire(
      !request.details.apiVersion.isEmpty() &&
        !request.details.publicKey.isEmpty() &&
        !request.details.publicKeySignature.isEmpty()
    ) { "Details field of DataProvider is missing fields." }
    return CreateDataProvider(request).execute(client, idGenerator, clock)
  }
  override suspend fun getDataProvider(request: GetDataProviderRequest): DataProvider {
    return DataProviderReader()
      .readExternalIdOrNull(client.singleUse(), ExternalId(request.externalDataProviderId))
      ?.dataProvider
      ?: failGrpc(Status.NOT_FOUND) { "DataProvider not found" }
  }
}
