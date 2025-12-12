/*
 * Copyright 2023 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.reporting.deploy.v2.common

import java.time.Duration
import kotlin.properties.Delegates
import picocli.CommandLine

class ReportingApiServerFlags {
  @CommandLine.Mixin
  lateinit var internalApiFlags: InternalApiFlags
    private set

  @CommandLine.Option(
    names = ["--access-api-target"],
    description = ["gRPC target of the Access public API server"],
    required = true,
  )
  lateinit var accessApiTarget: String
    private set

  @CommandLine.Option(
    names = ["--access-api-cert-host"],
    description =
      [
        "Expected hostname (DNS-ID) in the Access public API server's TLS certificate.",
        "This overrides derivation of the TLS DNS-ID from --access-api-target.",
      ],
    required = false,
    defaultValue = CommandLine.Option.NULL_VALUE,
  )
  var accessApiCertHost: String? = null
    private set

  @set:CommandLine.Option(
    names = ["--debug-verbose-grpc-client-logging"],
    description = ["Enables full gRPC request and response logging for outgoing gRPCs"],
    defaultValue = "false",
  )
  var debugVerboseGrpcClientLogging by Delegates.notNull<Boolean>()
    private set

  @CommandLine.Option(
    names = ["--event-group-metadata-descriptor-cache-duration"],
    description =
      [
        "How long the event group metadata descriptors are cached for before refreshing in format 1d1h1m1s1ms1ns"
      ],
    defaultValue = "1h",
    required = false,
  )
  lateinit var eventGroupMetadataDescriptorCacheDuration: Duration
    private set

  // TODO(world-federation-of-advertisers/cross-media-measurement#1937): Remove these flags as
  // part of determining a better way to set the model line when the VID Model Repo is adopted.
  @CommandLine.Option(
    names = ["--default-vid-model-line"],
    description = ["The default VID model line to be used by EDPs when fulfilling requisitions."],
    defaultValue = "",
    required = false,
  )
  lateinit var defaultVidModelLine: String
    private set

  @CommandLine.Option(
    names = ["--measurement-consumer-model-line"],
    description =
      [
        "Key-value pair of MeasurementConsumer resource name and VID ModelLine resource name. " +
          "This can be specified multiple times. Entries in this map override the default VID " +
          "ModelLine."
      ],
    required = false,
  )
  var measurementConsumerModelLines: Map<String, String> = emptyMap()
    private set

  @CommandLine.Option(
    names = ["--base-impression-qualification-filter"],
    description = ["ImpressionQualificationFilters that are always used in BasicReports"],
    required = false,
  )
  var baseImpressionQualificationFilters: List<String> = emptyList()
    private set

  // TODO(world-federation-of-advertisers/cross-media-measurement#2220): Remove this flag when LLv2
  // is deprecated.
  @set:CommandLine.Option(
    names = ["--allow-sampling-interval-wrapping"],
    description = ["Enable random sampling interval to wrap around 1."],
    defaultValue = "false",
  )
  var allowSamplingIntervalWrapping by Delegates.notNull<Boolean>()
    private set

  @CommandLine.Option(
    names = ["--pdp-name"],
    description = ["The Population DataProvider resource name."],
    required = true,
  )
  lateinit var populationDataProvider: String
    private set
}
