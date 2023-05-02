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

import org.wfanet.measurement.common.grpc.grpcRequire
import org.wfanet.measurement.common.identity.IdGenerator
import org.wfanet.measurement.gcloud.spanner.AsyncDatabaseClient
import org.wfanet.measurement.internal.kingdom.ModelLine
import org.wfanet.measurement.internal.kingdom.ModelLinesGrpcKt.ModelLinesCoroutineImplBase
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.writers.CreateModelLine

class SpannerModelLinesService(
  private val idGenerator: IdGenerator,
  private val client: AsyncDatabaseClient
) : ModelLinesCoroutineImplBase() {

  override suspend fun createModelLine(request: ModelLine): ModelLine {
    grpcRequire(request.activeStartTime.isInitialized()) {
      "ActiveStartTime field of ModelLine is missing fields."
    }
    grpcRequire(request.type != ModelLine.Type.UNRECOGNIZED) {
      "Unrecognized ModelLine's type ${request.type}"
    }
    return CreateModelLine(request).execute(client, idGenerator)
  }
}
