/*
 * Copyright 2024 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.access.deploy.gcloud.spanner

import kotlin.coroutines.CoroutineContext
import org.wfanet.measurement.access.common.TlsClientPrincipalMapping
import org.wfanet.measurement.access.service.internal.PermissionMapping
import org.wfanet.measurement.access.service.internal.Services
import org.wfanet.measurement.common.IdGenerator
import org.wfanet.measurement.gcloud.spanner.AsyncDatabaseClient

object InternalApiServices {
  fun build(
    databaseClient: AsyncDatabaseClient,
    permissionMapping: PermissionMapping,
    tlsClientMapping: TlsClientPrincipalMapping,
    coroutineContext: CoroutineContext,
    idGenerator: IdGenerator = IdGenerator.Default,
  ): Services {
    return Services(
      SpannerPrincipalsService(databaseClient, tlsClientMapping, coroutineContext, idGenerator),
      SpannerPermissionsService(
        databaseClient,
        permissionMapping,
        tlsClientMapping,
        coroutineContext,
      ),
      SpannerRolesService(databaseClient, permissionMapping, coroutineContext, idGenerator),
      SpannerPoliciesService(databaseClient, tlsClientMapping, coroutineContext, idGenerator),
    )
  }
}
