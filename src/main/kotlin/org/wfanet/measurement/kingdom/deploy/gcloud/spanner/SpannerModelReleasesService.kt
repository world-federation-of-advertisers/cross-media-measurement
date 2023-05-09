/*
 * Copyright 2023 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.kingdom.deploy.gcloud.spanner

import io.grpc.Status
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.map
import org.wfanet.measurement.common.grpc.failGrpc
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.common.identity.IdGenerator
import org.wfanet.measurement.gcloud.spanner.AsyncDatabaseClient
import org.wfanet.measurement.internal.kingdom.GetModelReleaseRequest
import org.wfanet.measurement.internal.kingdom.ModelRelease
import org.wfanet.measurement.internal.kingdom.ModelReleasesGrpcKt.ModelReleasesCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.StreamModelReleasesRequest
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.queries.StreamModelReleases
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.ModelReleaseReader
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.writers.CreateModelRelease

class SpannerModelReleasesService(
  private val idGenerator: IdGenerator,
  private val client: AsyncDatabaseClient
) : ModelReleasesCoroutineImplBase() {

  override suspend fun createModelRelease(request: ModelRelease): ModelRelease {
    return CreateModelRelease(request).execute(client, idGenerator)
  }

  override suspend fun getModelRelease(request: GetModelReleaseRequest): ModelRelease {
    return ModelReleaseReader()
      .readByExternalModelReleaseId(
        client.singleUse(),
        ExternalId(request.externalModelReleaseId),
        ExternalId(request.externalModelSuiteId),
        ExternalId(request.externalModelProviderId)
      )
      ?.modelRelease
      ?: failGrpc(Status.NOT_FOUND) { "ModelRelease not found." }
  }

  override fun streamModelReleases(request: StreamModelReleasesRequest): Flow<ModelRelease> {
    return StreamModelReleases(request.filter, request.limit).execute(client.singleUse()).map {
      it.modelRelease
    }
  }
}
