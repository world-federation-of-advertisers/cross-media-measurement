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

package org.wfanet.measurement.kingdom.deploy.common.server

import com.google.protobuf.DescriptorProtos
import com.google.protobuf.Descriptors
import java.io.File
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.runInterruptible
import org.wfanet.measurement.common.ProtoReflection
import org.wfanet.measurement.common.grpc.CommonServer
import org.wfanet.measurement.common.grpc.ServiceFlags
import org.wfanet.measurement.common.identity.DuchyInfo
import org.wfanet.measurement.common.identity.DuchyInfoFlags
import org.wfanet.measurement.kingdom.deploy.common.DuchyIds
import org.wfanet.measurement.kingdom.deploy.common.DuchyIdsFlags
import org.wfanet.measurement.kingdom.deploy.common.HmssProtocolConfig
import org.wfanet.measurement.kingdom.deploy.common.HmssProtocolConfigFlags
import org.wfanet.measurement.kingdom.deploy.common.Llv2ProtocolConfig
import org.wfanet.measurement.kingdom.deploy.common.Llv2ProtocolConfigFlags
import org.wfanet.measurement.kingdom.deploy.common.RoLlv2ProtocolConfig
import org.wfanet.measurement.kingdom.deploy.common.RoLlv2ProtocolConfigFlags
import org.wfanet.measurement.kingdom.deploy.common.TrusTeeProtocolConfig
import org.wfanet.measurement.kingdom.deploy.common.TrusTeeProtocolConfigFlags
import org.wfanet.measurement.kingdom.deploy.common.service.DataServices
import org.wfanet.measurement.kingdom.deploy.common.service.toList
import picocli.CommandLine

abstract class KingdomDataServer : Runnable {
  @CommandLine.Mixin private lateinit var serverFlags: CommonServer.Flags

  @CommandLine.Mixin private lateinit var serviceFlags: ServiceFlags

  @CommandLine.Mixin private lateinit var duchyInfoFlags: DuchyInfoFlags

  @CommandLine.Mixin private lateinit var duchyIdsFlags: DuchyIdsFlags

  @CommandLine.Mixin private lateinit var llv2ProtocolConfigFlags: Llv2ProtocolConfigFlags

  @CommandLine.Mixin private lateinit var roLlv2ProtocolConfigFlags: RoLlv2ProtocolConfigFlags

  @CommandLine.Mixin private lateinit var hmssProtocolConfigFlags: HmssProtocolConfigFlags

  @CommandLine.Mixin private lateinit var trusteeProtocolConfigFlags: TrusTeeProtocolConfigFlags

  @CommandLine.Option(
    names = ["--known-event-group-metadata-type"],
    description =
      [
        "File path to FileDescriptorSet containing known EventGroup metadata types.",
        "This is in addition to standard protobuf well-known types.",
        "Can be specified multiple times.",
      ],
    required = false,
    defaultValue = "",
  )
  private fun setKnownEventGroupMetadataTypes(fileDescriptorSetFiles: List<File>) {
    val fileDescriptorSets =
      fileDescriptorSetFiles.map { file ->
        file.inputStream().use { input -> DescriptorProtos.FileDescriptorSet.parseFrom(input) }
      }
    knownEventGroupMetadataTypes = ProtoReflection.buildFileDescriptors(fileDescriptorSets)
  }

  protected lateinit var knownEventGroupMetadataTypes: List<Descriptors.FileDescriptor>
    private set

  protected suspend fun run(dataServices: DataServices) {
    DuchyInfo.initializeFromFlags(duchyInfoFlags)
    DuchyIds.initializeFromFlags(duchyIdsFlags)
    Llv2ProtocolConfig.initializeFromFlags(llv2ProtocolConfigFlags)
    RoLlv2ProtocolConfig.initializeFromFlags(roLlv2ProtocolConfigFlags)
    HmssProtocolConfig.initializeFromFlags(hmssProtocolConfigFlags)
    TrusTeeProtocolConfig.initializeFromFlags(trusteeProtocolConfigFlags)

    val services =
      dataServices.buildDataServices(serviceFlags.executor.asCoroutineDispatcher()).toList()
    val server = CommonServer.fromFlags(serverFlags, this::class.simpleName!!, services)

    runInterruptible { server.start().blockUntilShutdown() }
  }
}
