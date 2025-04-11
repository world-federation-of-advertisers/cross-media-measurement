/*
 * Copyright 2025 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.reporting.service.api.v2alpha

import org.wfanet.measurement.access.client.v1alpha.Authorization
import org.wfanet.measurement.access.client.v1alpha.check
import org.wfanet.measurement.api.v2alpha.DataProvidersGrpcKt.DataProvidersCoroutineStub
import org.wfanet.measurement.api.v2alpha.EventGroupsGrpcKt.EventGroupsCoroutineStub
import org.wfanet.measurement.api.v2alpha.ModelLinesGrpcKt.ModelLinesCoroutineStub
import org.wfanet.measurement.api.v2alpha.ModelRolloutsGrpcKt.ModelRolloutsCoroutineStub
import org.wfanet.measurement.reporting.v2alpha.ListValidModelLinesRequest
import org.wfanet.measurement.reporting.v2alpha.ListValidModelLinesResponse
import org.wfanet.measurement.reporting.v2alpha.ModelLinesGrpcKt.ModelLinesCoroutineImplBase

class ModelLinesService(
  private val kingdomModelLinesStub: ModelLinesCoroutineStub,
  private val kingdomModelRolloutsStub: ModelRolloutsCoroutineStub,
  private val kingdomEventGroupsStub: EventGroupsCoroutineStub,
  private val kingdomDataProvidersStub: DataProvidersCoroutineStub,
  private val modelSuite: String,
  private val authorization: Authorization,
  ) : ModelLinesCoroutineImplBase() {
  override suspend fun listValidModelLines(request: ListValidModelLinesRequest): ListValidModelLinesResponse {
    authorization.check(modelSuite, Permission.LIST)

    return super.listValidModelLines(request)
  }

  object Permission {
    private const val TYPE = "reporting.modelLines"
    const val LIST = "$TYPE.list"
  }
}
