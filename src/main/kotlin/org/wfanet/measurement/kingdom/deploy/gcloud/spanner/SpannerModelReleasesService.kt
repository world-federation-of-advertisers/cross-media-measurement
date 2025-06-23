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
import kotlin.coroutines.CoroutineContext
import kotlin.coroutines.EmptyCoroutineContext
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.map
import org.wfanet.measurement.common.grpc.failGrpc
import org.wfanet.measurement.common.grpc.grpcRequire
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.common.identity.IdGenerator
import org.wfanet.measurement.gcloud.spanner.AsyncDatabaseClient
import org.wfanet.measurement.internal.kingdom.GetModelReleaseRequest
import org.wfanet.measurement.internal.kingdom.ModelRelease
import org.wfanet.measurement.internal.kingdom.ModelReleasesGrpcKt.ModelReleasesCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.StreamModelReleasesRequest
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.ModelReleaseNotFoundException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.ModelSuiteNotFoundException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.queries.StreamModelReleases
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.ModelReleaseReader
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.writers.CreateModelRelease

class SpannerModelReleasesService(
  private val idGenerator: IdGenerator,
  private val client: AsyncDatabaseClient,
  coroutineContext: CoroutineContext = EmptyCoroutineContext,
) : ModelReleasesCoroutineImplBase(coroutineContext) {

  override suspend fun createModelRelease(request: ModelRelease): ModelRelease {
    try {
      return CreateModelRelease(request).execute(client, idGenerator)
    } catch (e: ModelSuiteNotFoundException) {
      throw e.asStatusRuntimeException(Status.Code.NOT_FOUND, "ModelSuite not found.")
    }
  }

  override suspend fun getModelRelease(request: GetModelReleaseRequest): ModelRelease {
    val externalModelReleaseId = ExternalId(request.externalModelReleaseId)
    val externalModelSuiteId = ExternalId(request.externalModelSuiteId)
    val externalModelProviderId = ExternalId(request.externalModelProviderId)
    return ModelReleaseReader()
      .readByExternalIds(
        client.singleUse(),
        externalModelReleaseId,
        externalModelSuiteId,
        externalModelProviderId,
      )
      ?.modelRelease
      ?: throw ModelReleaseNotFoundException(
          externalModelProviderId,
          externalModelSuiteId,
          externalModelReleaseId,
        )
        .asStatusRuntimeException(Status.Code.NOT_FOUND, "ModelRelease not found.")
  }

  override fun streamModelReleases(request: StreamModelReleasesRequest): Flow<ModelRelease> {
    grpcRequire(request.limit >= 0) { "Limit cannot be less than 0" }
    if (
      request.filter.hasAfter() &&
        (!request.filter.after.hasCreateTime() ||
          request.filter.after.externalModelReleaseId == 0L ||
          request.filter.after.externalModelSuiteId == 0L ||
          request.filter.after.externalModelProviderId == 0L)
    ) {
      failGrpc(Status.INVALID_ARGUMENT) { "Missing After filter fields" }
    }
    return StreamModelReleases(request.filter, request.limit).execute(client.singleUse()).map {
      it.modelRelease
    }
  }
}
