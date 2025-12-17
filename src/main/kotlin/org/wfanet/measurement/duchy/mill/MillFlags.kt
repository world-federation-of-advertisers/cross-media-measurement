/*
 * Copyright 2024 The Cross-Media Measurement Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.wfanet.measurement.duchy.mill

import java.io.File
import java.time.Duration
import kotlin.properties.Delegates
import org.wfanet.measurement.common.grpc.TlsFlags
import org.wfanet.measurement.internal.duchy.ComputationTypeEnum.ComputationType
import picocli.CommandLine

class ClaimedComputationFlags {
  @CommandLine.Option(
    names = ["--claimed-computation-id"],
    description = ["Global Computation ID of the claimed work item"],
    required = true,
  )
  lateinit var claimedGlobalComputationId: String
    private set

  @set:CommandLine.Option(
    names = ["--claimed-computation-version"],
    description = ["Token version of the the claimed work item"],
    required = true,
  )
  var claimedComputationVersion by Delegates.notNull<Long>()
    private set

  @CommandLine.Option(
    names = ["--claimed-computation-type"],
    description = ["Computation type (protocol) of the claimed work item"],
    required = true,
  )
  lateinit var claimedComputationType: ComputationType
    private set
}

abstract class MillFlags {
  @CommandLine.Mixin
  lateinit var tlsFlags: TlsFlags
    private set

  @CommandLine.Option(
    names = ["--mill-id"],
    description = ["ID of this Mill instance. Defaults to HOSTNAME."],
  )
  var millId: String = System.getenv("HOSTNAME")
    private set

  @CommandLine.Option(
    names = ["--duchy-computation-control-target"],
    description = ["Key-value pair of Duchy ID to ComputationControl service target."],
    required = false,
  )
  var computationControlServiceTargets: Map<String, String> = emptyMap()
    private set

  @CommandLine.Option(
    names = ["--channel-shutdown-timeout"],
    defaultValue = "3s",
    description = ["How long to allow for the gRPC channel to shutdown."],
  )
  lateinit var channelShutdownTimeout: Duration
    private set

  @CommandLine.Option(
    names = ["--work-lock-duration"],
    defaultValue = "5m",
    description = ["How long to hold work locks."],
  )
  lateinit var workLockDuration: Duration
    private set

  @set:CommandLine.Option(
    names = ["--bytes-per-chunk"],
    description = ["The number of bytes in a chunk when sending rpc result to other duchy."],
    defaultValue = "32768", // 32 KiB. See https://github.com/grpc/grpc.github.io/issues/371.
  )
  var requestChunkSizeBytes by Delegates.notNull<Int>()
    private set

  @CommandLine.Option(
    names = ["--consent-signaling-certificate-resource-name"],
    description = ["The resource name of the duchy's consent signaling certificate."],
    required = true,
  )
  lateinit var csCertificateName: String
    private set

  @CommandLine.Option(
    names = ["--consent-signaling-private-key-der-file"],
    description = ["The duchy's consent signaling private key (DER format) file."],
    required = true,
  )
  lateinit var csPrivateKeyDerFile: File
    private set

  @CommandLine.Option(
    names = ["--consent-signaling-certificate-der-file"],
    description = ["The duchy's consent signaling certificate (DER format) file."],
    required = true,
  )
  lateinit var csCertificateDerFile: File
    private set
}
