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

package org.wfanet.measurement.securecomputation.deploy.gcloud.deadletter

import kotlinx.coroutines.flow.Flow
import org.wfanet.measurement.common.commandLineMain
import picocli.CommandLine.Option

/** Command-line flags for [DeadLetterQueueListener]. */
class DeadLetterQueueListenerFlags {
  @Option(
    names = ["--dead-letter-subscription-id"],
    description = ["PubSub subscription ID for the dead letter queue"],
    required = true
  )
  lateinit var deadLetterSubscriptionId: String
    private set

  @Option(
    names = ["--project-id"],
    description = ["Google Cloud project ID"],
    required = true
  )
  lateinit var projectId: String
    private set

  @Option(
    names = ["--work-items-api-target"],
    description = ["gRPC target (authority) for the WorkItems API"],
    required = true
  )
  lateinit var workItemsApiTarget: String
    private set

  @Option(
    names = ["--tls-cert-file"],
    description = ["File containing the TLS certificate for the client"],
    required = false
  )
  var tlsCertFile: String? = null
    private set

  @Option(
    names = ["--tls-key-file"],
    description = ["File containing the TLS private key for the client"],
    required = false
  )
  var tlsKeyFile: String? = null
    private set

  @Option(
    names = ["--cert-collection-file"],
    description = ["File containing the trusted CA certificates"],
    required = false
  )
  var certCollectionFile: String? = null
    private set
}