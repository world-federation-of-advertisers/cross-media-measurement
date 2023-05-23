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

package org.wfanet.measurement.kingdom.service.api.v2alpha

import org.wfanet.measurement.api.v2alpha.ModelReleasesGrpcKt.ModelReleasesCoroutineImplBase as ModelReleasesCoroutineService
import io.grpc.Status
import org.wfanet.measurement.api.v2alpha.CreateModelReleaseRequest
import org.wfanet.measurement.api.v2alpha.GetModelReleaseRequest
import org.wfanet.measurement.api.v2alpha.ListModelReleasesRequest
import org.wfanet.measurement.api.v2alpha.ListModelReleasesResponse
import org.wfanet.measurement.api.v2alpha.MeasurementPrincipal
import org.wfanet.measurement.api.v2alpha.ModelProviderPrincipal
import org.wfanet.measurement.api.v2alpha.ModelRelease
import org.wfanet.measurement.api.v2alpha.ModelSuiteKey
import org.wfanet.measurement.api.v2alpha.principalFromCurrentContext
import org.wfanet.measurement.common.grpc.failGrpc
import org.wfanet.measurement.common.grpc.grpcRequireNotNull
import org.wfanet.measurement.internal.kingdom.ModelReleasesGrpcKt.ModelReleasesCoroutineStub

private const val DEFAULT_PAGE_SIZE = 50
private const val MAX_PAGE_SIZE = 1000
class ModelReleasesService(private val internalClient: ModelReleasesCoroutineStub) :
  ModelReleasesCoroutineService() {

  override suspend fun createModelRelease(request: CreateModelReleaseRequest): ModelRelease {
    val parentKey =
      grpcRequireNotNull(ModelSuiteKey.fromName(request.parent)) {
        "Parent is either unspecified or invalid"
      }

    when (val principal: MeasurementPrincipal = principalFromCurrentContext) {
      is ModelProviderPrincipal -> {
        if (principal.resourceKey.modelProviderId != parentKey.modelProviderId) {
          failGrpc(Status.PERMISSION_DENIED) { "Cannot create ModelRelease for another ModelProvider" }
        }
      }
      else -> {
        failGrpc(Status.PERMISSION_DENIED) { "Caller does not have permission to create ModelRelease" }
      }
    }

    val createModelReleaseRequest = request.modelRelease.toInternal(parentKey)
    return try {
      internalClient.createModelLine(createModelLineRequest).toModelLine()
    } catch (ex: StatusException) {
      when (ex.status.code) {
        Status.Code.NOT_FOUND -> failGrpc(Status.NOT_FOUND, ex) { "ModelProvider not found" }
        Status.Code.INVALID_ARGUMENT ->
          failGrpc(Status.NOT_FOUND, ex) { ex.message ?: "Required field unspecified or invalid" }
        else -> failGrpc(Status.UNKNOWN, ex) { "Unknown exception" }
      }
    }
  }

  override suspend fun getModelRelease(request: GetModelReleaseRequest): ModelRelease {
    return super.getModelRelease(request)
  }

  override suspend fun listModelReleases(request: ListModelReleasesRequest): ListModelReleasesResponse {
    return super.listModelReleases(request)
  }

}
