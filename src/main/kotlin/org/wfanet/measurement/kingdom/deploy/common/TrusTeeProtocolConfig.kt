// Copyright 2025 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.kingdom.deploy.common

import java.io.File
import org.wfanet.measurement.common.parseTextProto
import org.wfanet.measurement.internal.kingdom.ProtocolConfig
import org.wfanet.measurement.internal.kingdom.TrusTeeProtocolConfigConfig
import picocli.CommandLine

object TrusTeeProtocolConfig {
  const val NAME = "trustee"

  lateinit var protocolConfig: ProtocolConfig.TrusTee
    private set

  lateinit var duchyId: String

  fun initializeFromFlags(flags: TrusTeeProtocolConfigFlags) {
    require(!TrusTeeProtocolConfig::protocolConfig.isInitialized)
    val configMessage =
      flags.config.reader().use {
        parseTextProto(it, TrusTeeProtocolConfigConfig.getDefaultInstance())
      }

    protocolConfig = configMessage.protocolConfig
    duchyId = configMessage.duchyId
  }

  fun setForTest(protocolConfig: ProtocolConfig.TrusTee, duchyId: String) {
    require(!TrusTeeProtocolConfig::protocolConfig.isInitialized)

    TrusTeeProtocolConfig.protocolConfig = protocolConfig
    TrusTeeProtocolConfig.duchyId = duchyId
  }
}

class TrusTeeProtocolConfigFlags {
  @CommandLine.Option(
    names = ["--trustee-protocol-config-config"],
    description = ["TrusTeeProtocolConfigConfig proto message in text format."],
    required = true,
  )
  lateinit var config: File
    private set
}
