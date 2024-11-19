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

package org.wfanet.measurement.access.deploy.common.testing

import com.google.common.truth.extensions.proto.ProtoTruth.assertThat
import kotlinx.coroutines.runBlocking
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.internal.access.Principal
import org.wfanet.measurement.internal.access.PrincipalKt.tlsClient
import org.wfanet.measurement.internal.access.PrincipalsGrpcKt
import org.wfanet.measurement.internal.access.lookupPrincipalRequest
import org.wfanet.measurement.internal.access.principal

@RunWith(JUnit4::class)
abstract class PrincipalsServiceTest {
  protected val tlsClientMapping = TestConfig.TLS_CLIENT_MAPPING

  /**
   * Service under test.
   *
   * This must be initialized using [tlsClientMapping].
   */
  protected abstract val service: PrincipalsGrpcKt.PrincipalsCoroutineImplBase

  @Test
  fun `lookupPrincipal returns TLS client principal`() {
    val request = lookupPrincipalRequest {
      tlsClient = tlsClient { authorityKeyIdentifier = TestConfig.MC_AUTHORITY_KEY_IDENTIFIER }
    }

    val response: Principal = runBlocking { service.lookupPrincipal(request) }

    assertThat(response)
      .isEqualTo(
        principal {
          principalResourceId = TestConfig.MC_PRINCIPAL_RESOURCE_ID
          tlsClient = request.tlsClient
        }
      )
  }
}
