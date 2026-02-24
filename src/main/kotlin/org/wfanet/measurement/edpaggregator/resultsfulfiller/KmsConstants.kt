/*
 * Copyright 2025 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.edpaggregator.resultsfulfiller

/** Constants for KMS URI handling. */
object KmsConstants {
  /**
   * Regex pattern for parsing GCP KMS key URIs.
   *
   * Format: gcp-kms://projects/{project}/locations/{location}/keyRings/{keyring}/cryptoKeys/{key}
   *
   * Capture groups:
   * 1. project
   * 2. location
   * 3. keyRing
   * 4. keyName
   */
  val GCP_KMS_KEY_URI_REGEX =
    Regex("gcp-kms://projects/([^/]+)/locations/([^/]+)/keyRings/([^/]+)/cryptoKeys/([^/]+)")

  /**
   * Regex pattern for parsing AWS KMS key URIs.
   *
   * Format: aws-kms://arn:aws:kms:{region}:{account}:key/{keyId}
   *
   * Capture groups:
   * 1. region
   * 2. account
   * 3. keyId
   */
  val AWS_KMS_KEY_URI_REGEX = Regex("aws-kms://arn:aws:kms:([^:]+):([^:]+):key/(.+)")
}
