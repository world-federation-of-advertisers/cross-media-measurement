/*
 * Copyright 2026 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.edpaggregator.vidlabeler

import com.google.crypto.tink.KmsClient
import org.wfanet.measurement.edpaggregator.StorageConfig
import org.wfanet.measurement.edpaggregator.v1alpha.VidLabelerParams

/** Interface for the VID labeling core processing logic. */
interface VidLabeler {
  /**
   * Labels a batch of raw impressions with VIDs.
   *
   * @param vidLabelerParams parameters for this labeling batch.
   * @param storageConfig storage configuration for reading/writing blobs.
   * @param decryptKmsClient KMS client for decrypting raw impressions.
   * @param encryptKmsClient KMS client for encrypting labeled output.
   */
  suspend fun labelBatch(
    vidLabelerParams: VidLabelerParams,
    storageConfig: StorageConfig,
    decryptKmsClient: KmsClient,
    encryptKmsClient: KmsClient,
  )
}
