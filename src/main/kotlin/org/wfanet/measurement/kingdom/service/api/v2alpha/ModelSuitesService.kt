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

import org.wfanet.measurement.internal.kingdom.ModelSuitesGrpcKt.ModelSuitesCoroutineStub
import org.wfanet.measurement.api.v2alpha.ModelSuitesGrpcKt.ModelSuitesCoroutineImplBase as ModelSuitesCoroutineService
import org.wfanet.measurement.api.v2alpha.CreateModelSuiteRequest
import org.wfanet.measurement.api.v2alpha.GetModelSuiteRequest
import org.wfanet.measurement.api.v2alpha.ListModelSuitesRequest
import org.wfanet.measurement.api.v2alpha.ListModelSuitesResponse
import org.wfanet.measurement.api.v2alpha.ModelSuite

class ModelSuitesService(private val internalClient: ModelSuitesCoroutineStub) : ModelSuitesCoroutineService() {

  override suspend fun createModelSuite(request: CreateModelSuiteRequest): ModelSuite {
    return super.createModelSuite(request)
  }

  override suspend fun getModelSuite(request: GetModelSuiteRequest): ModelSuite {
    return super.getModelSuite(request)
  }

  override suspend fun listModelSuites(request: ListModelSuitesRequest): ListModelSuitesResponse {
    return super.listModelSuites(request)
  }

}
