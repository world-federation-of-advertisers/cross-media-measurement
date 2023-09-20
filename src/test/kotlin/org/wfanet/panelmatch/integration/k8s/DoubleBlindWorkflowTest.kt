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

import com.google.common.truth.Truth
import com.google.protobuf.ByteString
import com.google.protobuf.kotlin.toByteStringUtf8
import java.time.Duration
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.time.withTimeout
import org.junit.Rule
import org.junit.Test
import org.junit.rules.TestRule
import org.junit.runner.Description
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.junit.runners.model.Statement
import org.wfanet.measurement.api.v2alpha.ExchangeWorkflow
import org.wfanet.measurement.integration.common.createEntityContent

@RunWith(JUnit4::class)
class DoubleBlindWorkflowTest : AbstractPanelMatchCorrectnessTest() {

  private val EDP_COMMUTATIVE_DETERMINISTIC_KEY = "some-key".toByteStringUtf8()

  val workflow: ExchangeWorkflow by lazy {
    loadTestData(
      "double_blind_exchange_workflow.textproto",
      ExchangeWorkflow.getDefaultInstance()
    )
  }

  val initialDataProviderInputs: Map<String, ByteString> =
    mapOf("edp-commutative-deterministic-key" to EDP_COMMUTATIVE_DETERMINISTIC_KEY)

  @Rule
  @JvmField
  val methodLevelRule = LocalSetup()

  inner class LocalSetup() : TestRule {

    val dataProviderContent = createEntityContent("edp1")
    val modelProviderContent = createEntityContent("mp1")
    override fun apply(base: Statement, description: Description): Statement {
      return object : Statement() {
        override fun evaluate() {
          runBlocking {
            withTimeout(Duration.ofMinutes(5)) {
              runResourceSetup(dataProviderContent, modelProviderContent, workflow, initialDataProviderInputs)
            }
          }
          base.evaluate()
        }
      }
    }
  }

  //val logger = Logger.getLogger(this::class.java.name)
  @Test(timeout = 1 * 60 * 1000)
  fun `my custom test`() = runBlocking {
    logger.info("----------------------------------- That's my first TEST !!!!")
    println("------------------------------------ That's my first TEST !!!!")
    Truth.assertThat("ciao").isEqualTo("miao")
  }

}
