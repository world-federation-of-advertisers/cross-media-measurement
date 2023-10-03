/*
 * Copyright 2023 The Cross-Media Measurement Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.wfanet.panelmatch.integration.k8s

import com.google.protobuf.ByteString
import com.google.protobuf.kotlin.toByteStringUtf8
import java.net.InetSocketAddress
import java.time.Duration
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.time.withTimeout
import kotlinx.coroutines.withContext
import org.junit.Rule
import org.junit.Test
import org.junit.rules.TestRule
import org.junit.runner.Description
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.junit.runners.model.Statement
import org.wfanet.measurement.api.v2alpha.ExchangeWorkflow
import org.wfanet.measurement.api.v2alpha.copy
import org.wfanet.measurement.common.grpc.buildMutualTlsChannel
import org.wfanet.measurement.common.k8s.testing.PortForwarder
import org.wfanet.measurement.common.toProtoDate
import org.wfanet.measurement.integration.common.createEntityContent
import org.wfanet.measurement.internal.testing.ForwardedStorageGrpcKt
import org.wfanet.measurement.storage.forwarded.ForwardedStorageClient
import org.wfanet.panelmatch.client.common.joinKeyAndIdOf
import org.wfanet.panelmatch.client.exchangetasks.JoinKeyAndIdCollection
import org.wfanet.panelmatch.client.exchangetasks.joinKeyAndIdCollection
import org.wfanet.panelmatch.common.storage.toByteString

@RunWith(JUnit4::class)
class DoubleBlindWorkflowTest : AbstractPanelMatchCorrectnessTest() {

  private val PLAINTEXT_JOIN_KEYS = joinKeyAndIdCollection {
    joinKeyAndIds +=
      joinKeyAndIdOf("join-key-1".toByteStringUtf8(), "join-key-id-1".toByteStringUtf8())
    joinKeyAndIds +=
      joinKeyAndIdOf("join-key-2".toByteStringUtf8(), "join-key-id-2".toByteStringUtf8())
  }

  private val EDP_COMMUTATIVE_DETERMINISTIC_KEY = "some-key".toByteStringUtf8()

  override val workflow: ExchangeWorkflow by lazy {
    loadTestData("double_blind_exchange_workflow.textproto", ExchangeWorkflow.getDefaultInstance())
      .copy { firstExchangeDate = EXCHANGE_DATE.toProtoDate() }
  }

  /*val workflow: ExchangeWorkflow by lazy {
    loadTestData(
      "mini_exchange_workflow.textproto",
      ExchangeWorkflow.getDefaultInstance()
    ).copy { firstExchangeDate = EXCHANGE_DATE.toProtoDate() }
  }*/

  override val initialDataProviderInputs: Map<String, ByteString> =
    mapOf("edp-commutative-deterministic-key" to EDP_COMMUTATIVE_DETERMINISTIC_KEY)

  /*val initialDataProviderInputs: Map<String, ByteString> =
  mapOf("edp-hkdf-pepper" to "some-hkdf-pepper".toByteStringUtf8())*/

  // val initialModelProviderInputs: Map<String, ByteString> = emptyMap()

  override val initialModelProviderInputs: Map<String, ByteString> =
    mapOf("mp-plaintext-join-keys" to PLAINTEXT_JOIN_KEYS.toByteString())

  @Rule @JvmField val testRule = LocalSetup()

  inner class LocalSetup() : TestRule {

    val dataProviderContent = createEntityContent("edp1")
    val modelProviderContent = createEntityContent("mp1")
    override fun apply(base: Statement, description: Description): Statement {
      return object : Statement() {
        override fun evaluate() {
          runBlocking {
            withTimeout(Duration.ofMinutes(5)) {
              runResourceSetup(
                dataProviderContent,
                modelProviderContent,
                workflow,
                initialDataProviderInputs,
                initialModelProviderInputs
              )
            }
          }
          base.evaluate()
        }
      }
    }
  }

  // val logger = Logger.getLogger(this::class.java.name)
  @Test(timeout = 3 * 60 * 1000) fun `Double blind workflow test`() = runBlocking { runTest() }

  override suspend fun validate() {

    PortForwarder(getPod(MP_PRIVATE_STORAGE_DEPLOYMENT_NAME), SERVER_PORT).use { mpStorageForwarder
      ->
      val mpStorageAddress: InetSocketAddress =
        withContext(Dispatchers.IO) { mpStorageForwarder.start() }
      val mpStorageChannel = buildMutualTlsChannel(mpStorageAddress.toTarget(), MP_SIGNING_CERTS)
      val mpForwardedStorage =
        ForwardedStorageClient(
          ForwardedStorageGrpcKt.ForwardedStorageCoroutineStub(mpStorageChannel)
        )

      val result = mpForwardedStorage.getBlob("mp-decrypted-join-keys")
      JoinKeyAndIdCollection.parseFrom(result?.toByteString())

      mpStorageChannel.shutdown()
      mpStorageForwarder.stop()
    }
  }
}
