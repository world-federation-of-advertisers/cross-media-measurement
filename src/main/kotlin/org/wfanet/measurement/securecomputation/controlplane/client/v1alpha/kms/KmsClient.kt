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

package org.wfanet.measurement.securecomputation.controlplane.client.v1alpha.kms

abstract class KmsDecryptionParams

/**
 * Interface representing a generic KMS (Key Management Service) client for decrypting keys.
 * Implementations of this interface can provide functionality for different cloud providers
 * (e.g., Google Cloud, AWS, Azure), where each implementation handles the decryption
 * using the provider's respective KMS service.
 */
interface KmsClient {

  /**
   * Decrypts a given encrypted key using the specified cloud provider's KMS.
   *
   * @param params The parameters required for the decryption operation,
   *               encapsulated in a subclass of [KmsDecryptionParams].
   *               Each cloud provider implementation will define its own
   *               set of parameters.
   *
   * @return The decrypted symmetric key as a [ByteArray].
   *
   * @throws IllegalArgumentException If the provided parameters are invalid or incompatible.
   * @throws Exception If an error occurs during the decryption process.
   */
  fun decryptKey(params: KmsDecryptionParams): ByteArray
}
