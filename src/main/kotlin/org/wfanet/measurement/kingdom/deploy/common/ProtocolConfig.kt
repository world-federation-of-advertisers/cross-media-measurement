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

object Llv2ProtocolConfig {
  const val name = "llv2"
  lateinit var protocolConfig: ProtocolConfig.LiquidLegionsV2
    private set
  lateinit var duchyProtocolConfig: DuchyProtocolConfig.LiquidLegionsV2
    private set
  lateinit var requiredExternalDuchyIds: Set<String>
    private set

  var minimumNumberOfRequiredDuchies: Int by Delegates.notNull()
    private set

  fun initializeFromFlags(flags: Llv2ProtocolConfigFlags) {
    require(!Llv2ProtocolConfig::protocolConfig.isInitialized)
    require(!Llv2ProtocolConfig::duchyProtocolConfig.isInitialized)
    require(!Llv2ProtocolConfig::requiredExternalDuchyIds.isInitialized)
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
    minimumNumberOfRequiredDuchies: Int
  ) {
    require(!Llv2ProtocolConfig::protocolConfig.isInitialized)
    require(!Llv2ProtocolConfig::duchyProtocolConfig.isInitialized)
    require(!Llv2ProtocolConfig::requiredExternalDuchyIds.isInitialized)
    Llv2ProtocolConfig.protocolConfig = protocolConfig
    Llv2ProtocolConfig.duchyProtocolConfig = duchyProtocolConfig
    Llv2ProtocolConfig.requiredExternalDuchyIds = requiredExternalDuchyIds
    Llv2ProtocolConfig.minimumNumberOfRequiredDuchies = minimumNumberOfRequiredDuchies
  }
}

class Llv2ProtocolConfigFlags {
  @CommandLine.Option(
    names = ["--llv2-protocol-config-config"],
    description = ["Llv2ProtocolConfigConfig proto message in text format."],
    required = true
  )
  lateinit var config: File
    private set
}

object Rollv2ProtocolConfig {
  var enabled: Boolean by Delegates.notNull()
    private set

  const val name = "rollv2"
  lateinit var protocolConfig: ProtocolConfig.LiquidLegionsV2
    private set
  lateinit var duchyProtocolConfig: DuchyProtocolConfig.LiquidLegionsV2
    private set
  lateinit var requiredExternalDuchyIds: Set<String>
    private set

  var minimumNumberOfRequiredDuchies: Int by Delegates.notNull()
    private set

  fun initializeFromFlags(flags: Rollv2ProtocolConfigFlags) {
    require(!Rollv2ProtocolConfig::protocolConfig.isInitialized)
    require(!Rollv2ProtocolConfig::duchyProtocolConfig.isInitialized)
    require(!Rollv2ProtocolConfig::requiredExternalDuchyIds.isInitialized)
    val configMessage =
      flags.config.reader().use {
        parseTextProto(it, Llv2ProtocolConfigConfig.getDefaultInstance())
      }
    protocolConfig = configMessage.protocolConfig
    duchyProtocolConfig = configMessage.duchyProtocolConfig
    requiredExternalDuchyIds = configMessage.requiredExternalDuchyIdsList.toSet()
    minimumNumberOfRequiredDuchies = configMessage.minimumDuchyParticipantCount
    enabled = flags.enableRollv2Protocol
  }

  fun setForTest(
    protocolConfig: ProtocolConfig.LiquidLegionsV2,
    duchyProtocolConfig: DuchyProtocolConfig.LiquidLegionsV2,
    requiredExternalDuchyIds: Set<String>,
    minimumNumberOfRequiredDuchies: Int,
    enabled: Boolean,
  ) {
    Rollv2ProtocolConfig.enabled = enabled

    require(!Rollv2ProtocolConfig::protocolConfig.isInitialized)
    require(!Rollv2ProtocolConfig::duchyProtocolConfig.isInitialized)
    require(!Rollv2ProtocolConfig::requiredExternalDuchyIds.isInitialized)
    Rollv2ProtocolConfig.protocolConfig = protocolConfig
    Rollv2ProtocolConfig.duchyProtocolConfig = duchyProtocolConfig
    Rollv2ProtocolConfig.requiredExternalDuchyIds = requiredExternalDuchyIds
    Rollv2ProtocolConfig.minimumNumberOfRequiredDuchies = minimumNumberOfRequiredDuchies
  }
}

class Rollv2ProtocolConfigFlags {
  @CommandLine.Option(
    names = ["--rollv2-protocol-config-config"],
    description = ["Llv2ProtocolConfigConfig proto message in text format."],
    required = true
  )
  lateinit var config: File
    private set

  @CommandLine.Option(
    names = ["--enable-rollv2-protocol"],
    description = ["Determine whether enable reach-only liquid legions v2 protocol."],
    required = false
  )
  var enableRollv2Protocol: Boolean = false
    private set
}
