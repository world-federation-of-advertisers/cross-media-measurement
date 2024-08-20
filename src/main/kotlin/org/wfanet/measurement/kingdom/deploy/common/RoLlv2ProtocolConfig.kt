// Copyright 2023 The Cross-Media Measurement Authors
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
import kotlin.properties.Delegates
import org.wfanet.measurement.common.parseTextProto
import org.wfanet.measurement.internal.kingdom.DuchyProtocolConfig
import org.wfanet.measurement.internal.kingdom.Llv2ProtocolConfigConfig
import org.wfanet.measurement.internal.kingdom.ProtocolConfig
import picocli.CommandLine

object RoLlv2ProtocolConfig {
  const val NAME = "rollv2"
  lateinit var protocolConfig: ProtocolConfig.LiquidLegionsV2
    private set

  lateinit var duchyProtocolConfig: DuchyProtocolConfig.LiquidLegionsV2
    private set

  lateinit var requiredExternalDuchyIds: Set<String>
    private set

  var minimumNumberOfRequiredDuchies: Int by Delegates.notNull()
    private set

  fun initializeFromFlags(flags: RoLlv2ProtocolConfigFlags) {
    require(!RoLlv2ProtocolConfig::protocolConfig.isInitialized)
    require(!RoLlv2ProtocolConfig::duchyProtocolConfig.isInitialized)
    require(!RoLlv2ProtocolConfig::requiredExternalDuchyIds.isInitialized)
    val configMessage =
      flags.config.reader().use {
        parseTextProto(it, Llv2ProtocolConfigConfig.getDefaultInstance())
      }
    protocolConfig = configMessage.protocolConfig
    duchyProtocolConfig = configMessage.duchyProtocolConfig
    requiredExternalDuchyIds = configMessage.requiredExternalDuchyIdsList.toSet()
    minimumNumberOfRequiredDuchies = configMessage.minimumDuchyParticipantCount
  }

  fun setForTest(
    protocolConfig: ProtocolConfig.LiquidLegionsV2,
    duchyProtocolConfig: DuchyProtocolConfig.LiquidLegionsV2,
    requiredExternalDuchyIds: Set<String>,
    minimumNumberOfRequiredDuchies: Int,
  ) {
    require(!RoLlv2ProtocolConfig::protocolConfig.isInitialized)
    require(!RoLlv2ProtocolConfig::duchyProtocolConfig.isInitialized)
    require(!RoLlv2ProtocolConfig::requiredExternalDuchyIds.isInitialized)
    RoLlv2ProtocolConfig.protocolConfig = protocolConfig
    RoLlv2ProtocolConfig.duchyProtocolConfig = duchyProtocolConfig
    RoLlv2ProtocolConfig.requiredExternalDuchyIds = requiredExternalDuchyIds
    RoLlv2ProtocolConfig.minimumNumberOfRequiredDuchies = minimumNumberOfRequiredDuchies
  }
}

class RoLlv2ProtocolConfigFlags {
  @CommandLine.Option(
    names = ["--ro-llv2-protocol-config-config"],
    description = ["Llv2ProtocolConfigConfig proto message in text format."],
    required = true,
  )
  lateinit var config: File
    private set
}
