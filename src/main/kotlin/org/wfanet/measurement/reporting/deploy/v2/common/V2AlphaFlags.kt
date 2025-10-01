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

import java.io.File
import picocli.CommandLine

/** Flags specific to the V2Alpha API version. */
class V2AlphaFlags {
  @CommandLine.Option(
    names = ["--measurement-consumer-config-file"],
    description = ["File path to a MeasurementConsumerConfig textproto"],
    required = true,
  )
  lateinit var measurementConsumerConfigFile: File
    private set

  @CommandLine.Option(
    names = ["--signing-private-key-store-dir"],
    description = ["File path to the signing private key store directory"],
    required = true,
  )
  lateinit var signingPrivateKeyStoreDir: File
    private set

  @CommandLine.Option(
    names = ["--metric-spec-config-file"],
    description = ["File path to a MetricSpecConfig textproto"],
    required = true,
  )
  lateinit var metricSpecConfigFile: File
    private set

  @CommandLine.Option(
    names = ["--basic-report-metric-spec-config-file"],
    description =
      [
        "File path to a MetricSpecConfig textproto for BasicReports. " +
          "--metric-spec-config-file is used if this is not set."
      ],
    required = false,
  )
  var basicReportMetricSpecConfigFile: File? = null
    private set
}
