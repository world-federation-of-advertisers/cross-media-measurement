// Copyright 2024 The Cross-Media Measurement Authors
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
import org.wfanet.measurement.internal.kingdom.HmssProtocolConfigConfig
import org.wfanet.measurement.internal.kingdom.ProtocolConfig
import picocli.CommandLine

object HmssProtocolConfig {
  const val NAME = "hmss"

  const val DUCHY_COUNT = 3

  lateinit var protocolConfig: ProtocolConfig.HonestMajorityShareShuffle
    private set

  lateinit var firstNonAggregatorDuchyId: String
    private set

  lateinit var secondNonAggregatorDuchyId: String
    private set

  lateinit var aggregatorDuchyId: String
    private set

  fun initializeFromFlags(flags: HmssProtocolConfigFlags) {
    require(!HmssProtocolConfig::protocolConfig.isInitialized)
    require(!HmssProtocolConfig::firstNonAggregatorDuchyId.isInitialized)
    require(!HmssProtocolConfig::secondNonAggregatorDuchyId.isInitialized)
    require(!HmssProtocolConfig::aggregatorDuchyId.isInitialized)
    val configMessage =
      flags.config.reader().use {
        parseTextProto(it, HmssProtocolConfigConfig.getDefaultInstance())
      }

    protocolConfig = configMessage.protocolConfig
    firstNonAggregatorDuchyId = configMessage.firstNonAggregatorDuchyId
    secondNonAggregatorDuchyId = configMessage.secondNonAggregatorDuchyId
    aggregatorDuchyId = configMessage.aggregatorDuchyId
  }

  fun setForTest(
    protocolConfig: ProtocolConfig.HonestMajorityShareShuffle,
    firstNonAggregatorDuchyId: String,
    secondNonAggregatorDuchyId: String,
    aggregatorDuchyId: String,
  ) {
    require(!HmssProtocolConfig::protocolConfig.isInitialized)

    HmssProtocolConfig.protocolConfig = protocolConfig
    HmssProtocolConfig.firstNonAggregatorDuchyId = firstNonAggregatorDuchyId
    HmssProtocolConfig.secondNonAggregatorDuchyId = secondNonAggregatorDuchyId
    HmssProtocolConfig.aggregatorDuchyId = aggregatorDuchyId
  }
}

class HmssProtocolConfigFlags {
  @CommandLine.Option(
    names = ["--hmss-protocol-config-config"],
    description = ["HmssProtocolConfigConfig proto message in text format."],
    required = true,
  )
  lateinit var config: File
    private set
}
