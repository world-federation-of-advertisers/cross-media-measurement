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
import org.wfanet.measurement.common.grpc.grpcRequire
import org.wfanet.measurement.common.identity.IdGenerator
import org.wfanet.measurement.gcloud.spanner.AsyncDatabaseClient
import org.wfanet.measurement.internal.kingdom.DeleteModelOutageRequest
import org.wfanet.measurement.internal.kingdom.ModelOutage
import org.wfanet.measurement.internal.kingdom.ModelOutagesGrpcKt.ModelOutagesCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.StreamModelOutagesRequest
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.ModelLineNotFoundException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.writers.CreateModelOutage

class SpannerModelOutagesService(
  private val clock: Clock,
  private val idGenerator: IdGenerator,
  private val client: AsyncDatabaseClient
) : ModelOutagesCoroutineImplBase() {

  override suspend fun createModelOutage(request: ModelOutage): ModelOutage {
    grpcRequire(request.hasModelOutageStartTime() && request.hasModelOutageEndTime()) {
      "Outage interval is missing."
    }
    try {
      return CreateModelOutage(request).execute(client, idGenerator)
    } catch (e: ModelLineNotFoundException) {
      e.throwStatusRuntimeException(Status.NOT_FOUND) { "ModelLine not found." }
    }
  }

  override suspend fun deleteModelOutage(request: DeleteModelOutageRequest): ModelOutage {
    return super.deleteModelOutage(request)
  }

  override fun streamModelOutages(request: StreamModelOutagesRequest): Flow<ModelOutage> {
    return super.streamModelOutages(request)
  }
}
