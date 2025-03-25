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

package org.wfanet.measurement.integration.common

import org.junit.rules.TestRule
import org.wfanet.measurement.access.common.TlsClientPrincipalMapping
import org.wfanet.measurement.access.service.internal.PermissionMapping
import org.wfanet.measurement.access.service.internal.Services as AccessInternalServices

interface AccessServicesFactory : TestRule {
  fun create(
    permissionMapping: PermissionMapping,
    tlsClientMapping: TlsClientPrincipalMapping,
  ): AccessInternalServices
}
