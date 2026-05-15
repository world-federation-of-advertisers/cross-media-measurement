// Copyright 2026 The Cross-Media Measurement Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.wfanet.measurement.integration.common

import com.google.common.truth.Truth.assertThat
import com.google.crypto.tink.Aead
import com.google.crypto.tink.KeyTemplates
import com.google.crypto.tink.KeysetHandle
import com.google.crypto.tink.aead.AeadConfig
import com.google.protobuf.ByteString
import io.grpc.inprocess.InProcessChannelBuilder
import io.grpc.inprocess.InProcessServerBuilder
import io.grpc.testing.GrpcCleanupRule
import java.security.cert.X509Certificate
import org.junit.BeforeClass
import org.junit.Rule
import org.junit.Test
import org.junit.runner.Description
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.junit.runners.model.Statement
import org.wfanet.measurement.common.crypto.tink.testing.FakeKmsClient
import org.wfanet.measurement.common.testing.ProviderRule
import org.wfanet.measurement.system.v1alpha.ComputationLogEntriesGrpcKt.ComputationLogEntriesCoroutineStub

@RunWith(JUnit4::class)
class InProcessDuchyTest {

  @get:Rule val grpcCleanup = GrpcCleanupRule()

  private fun createChannel(name: String) =
    grpcCleanup.register(InProcessChannelBuilder.forName(name).directExecutor().build()).also {
      grpcCleanup.register(InProcessServerBuilder.forName(name).directExecutor().build().start())
    }

  private val emptyDuchyDependenciesRule =
    object :
      ProviderRule<
        (String, ComputationLogEntriesCoroutineStub) -> InProcessDuchy.DuchyDependencies
      > {
      override val value:
        (String, ComputationLogEntriesCoroutineStub) -> InProcessDuchy.DuchyDependencies
        get() = throw UnsupportedOperationException("Not used in construction tests")

      override fun apply(base: Statement, description: Description): Statement = base
    }

  @Test
  fun `constructor accepts FakeKmsClient without configured keys`() {
    val duchy =
      InProcessDuchy(
        externalDuchyId = AGGREGATOR_NAME,
        kingdomSystemApiChannel = createChannel("system-api-test-1"),
        kingdomPublicApiChannel = createChannel("public-api-test-1"),
        duchyDependenciesRule = emptyDuchyDependenciesRule,
        trustedCertificates = emptyMap<ByteString, X509Certificate>(),
        trusTeeKmsClient = FakeKmsClient(),
      )

    assertThat(duchy.externalDuchyId).isEqualTo(AGGREGATOR_NAME)
  }

  @Test
  fun `constructor accepts FakeKmsClient as trusTeeKmsClient`() {
    val fakeKmsClient = FakeKmsClient()
    val kekUri = FakeKmsClient.KEY_URI_PREFIX + "test-key"
    val kekHandle = KeysetHandle.generateNew(KeyTemplates.get("AES128_GCM"))
    fakeKmsClient.setAead(kekUri, kekHandle.getPrimitive(Aead::class.java))

    val duchy =
      InProcessDuchy(
        externalDuchyId = AGGREGATOR_NAME,
        kingdomSystemApiChannel = createChannel("system-api-test-2"),
        kingdomPublicApiChannel = createChannel("public-api-test-2"),
        duchyDependenciesRule = emptyDuchyDependenciesRule,
        trustedCertificates = emptyMap<ByteString, X509Certificate>(),
        trusTeeKmsClient = fakeKmsClient,
      )

    assertThat(duchy.externalDuchyId).isEqualTo(AGGREGATOR_NAME)
  }

  companion object {
    @JvmStatic
    @BeforeClass
    fun initTink() {
      AeadConfig.register()
    }
  }
}
