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
import java.time.Clock
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.map
import org.wfanet.measurement.common.grpc.grpcRequire
import org.wfanet.measurement.common.identity.IdGenerator
import org.wfanet.measurement.gcloud.spanner.AsyncDatabaseClient
import org.wfanet.measurement.internal.kingdom.ModelRollout
import org.wfanet.measurement.internal.kingdom.ModelRolloutsGrpcKt.ModelRolloutsCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.StreamModelRolloutsRequest
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.ModelLineNotFoundException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.ModelRolloutInvalidArgsException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.queries.StreamModelRollouts
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.writers.CreateModelRollout

class SpannerModelRolloutsService(
  private val clock: Clock,
  private val idGenerator: IdGenerator,
  private val client: AsyncDatabaseClient
) : ModelRolloutsCoroutineImplBase() {

  override suspend fun createModelRollout(request: ModelRollout): ModelRollout {
    grpcRequire(request.hasRolloutPeriodStartTime()) {
      "RolloutPeriodStartTime field of ModelRollout is missing."
    }
    grpcRequire(request.hasRolloutPeriodEndTime()) {
      "RolloutPeriodEndTime field of ModelRollout is missing."
    }
    grpcRequire(request.externalModelReleaseId > 0L) {
      "ExternalModelReleaseId field of ModelRollout is missing."
    }
    try {
      return CreateModelRollout(request, clock).execute(client, idGenerator)
    } catch (e: ModelRolloutInvalidArgsException) {
      e.throwStatusRuntimeException(Status.INVALID_ARGUMENT)
    } catch (e: ModelLineNotFoundException) {
      e.throwStatusRuntimeException(Status.NOT_FOUND) { "ModelLine not found." }
    }
  }

  override fun streamModelRollouts(request: StreamModelRolloutsRequest): Flow<ModelRollout> {
    return StreamModelRollouts(request.filter, request.limit).execute(client.singleUse()).map {
      it.modelRollout
    }
  }
}
