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

package org.wfanet.measurement.integration.common

import com.google.protobuf.Message
import java.nio.file.Files
import java.nio.file.Paths
import org.wfanet.measurement.common.getRuntimePath
import org.wfanet.measurement.common.parseTextProto
import org.wfanet.measurement.config.DuchyRpcConfig
import org.wfanet.measurement.internal.duchy.config.ProtocolsSetupConfig
import org.wfanet.measurement.internal.kingdom.DuchyIdConfig
import org.wfanet.measurement.internal.kingdom.Llv2ProtocolConfigConfig

val DUCHY_ID_CONFIG: DuchyIdConfig =
  loadTextProto("duchy_id_config.textproto", DuchyIdConfig.getDefaultInstance())
val DUCHY_RPC_CONFIG: DuchyRpcConfig =
  loadTextProto("duchy_rpc_config.textproto", DuchyRpcConfig.getDefaultInstance())
val AGGREGATOR_PROTOCOLS_SETUP_CONFIG: ProtocolsSetupConfig =
  loadTextProto(
    "aggregator_protocols_setup_config.textproto",
    ProtocolsSetupConfig.getDefaultInstance()
  )
val NON_AGGREGATOR_PROTOCOLS_SETUP_CONFIG: ProtocolsSetupConfig =
  loadTextProto(
    "non_aggregator_protocols_setup_config.textproto",
    ProtocolsSetupConfig.getDefaultInstance()
  )
val LLV2_PROTOCOL_CONFIG_CONFIG: Llv2ProtocolConfigConfig =
  loadTextProto(
    "llv2_protocol_config_config.textproto",
    Llv2ProtocolConfigConfig.getDefaultInstance()
  )
val LLV2_AGGREGATOR_NAME =
  AGGREGATOR_PROTOCOLS_SETUP_CONFIG.liquidLegionsV2.externalAggregatorDuchyId!!
val ALL_DUCHY_NAMES = DUCHY_ID_CONFIG.duchiesList.map { it.externalDuchyId }

private fun <T : Message> loadTextProto(fileName: String, default: T): T {
  val runfilesRelativePath =
    Paths.get("wfa_measurement_system", "src", "main", "k8s", "configs", fileName)
  val path = checkNotNull(getRuntimePath(runfilesRelativePath))
  return Files.newBufferedReader(path).use { reader -> parseTextProto(reader, default) }
}
