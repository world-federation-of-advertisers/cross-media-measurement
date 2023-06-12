// Copyright 2021 The Cross-Media Measurement Authors
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

package org.wfanet.panelmatch.integration

import com.google.common.truth.Truth.assertThat
import com.google.protobuf.ByteString
import com.google.protobuf.kotlin.toByteStringUtf8
import org.junit.runner.RunWith
import org.junit.runners.JUnit4

private val HKDF_PEPPER = "some-hkdf-pepper".toByteStringUtf8()

@RunWith(JUnit4::class)
class MiniWorkflowTest : AbstractInProcessPanelMatchIntegrationTest() {
  override val exchangeWorkflowResourcePath: String = "config/mini_exchange_workflow.textproto"

  override val initialDataProviderInputs: Map<String, ByteString> =
    mapOf("edp-hkdf-pepper" to HKDF_PEPPER)

  override val initialModelProviderInputs: Map<String, ByteString> = emptyMap()

  override fun validateFinalState(
    dataProviderDaemon: ExchangeWorkflowDaemonForTest,
    modelProviderDaemon: ExchangeWorkflowDaemonForTest
  ) {
    assertThat(modelProviderDaemon.readPrivateBlob("mp-hkdf-pepper")).isEqualTo(HKDF_PEPPER)
  }
}
