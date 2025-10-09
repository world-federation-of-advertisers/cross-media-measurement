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

package org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner

import kotlin.coroutines.CoroutineContext
import org.wfanet.measurement.common.IdGenerator
import org.wfanet.measurement.edpaggregator.service.internal.Services
import org.wfanet.measurement.gcloud.spanner.AsyncDatabaseClient

object InternalApiServices {
  fun build(
    databaseClient: AsyncDatabaseClient,
    coroutineContext: CoroutineContext,
    idGenerator: IdGenerator = IdGenerator.Default,
  ): Services {
    return Services(
      SpannerRequisitionMetadataService(databaseClient, coroutineContext, idGenerator),
      SpannerImpressionMetadataService(databaseClient, coroutineContext),
    )
  }
}
