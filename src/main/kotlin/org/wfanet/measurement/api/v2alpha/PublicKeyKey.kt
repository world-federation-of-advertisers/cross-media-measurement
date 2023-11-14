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

package org.wfanet.measurement.api.v2alpha

import org.wfanet.measurement.common.api.ChildResourceKey
import org.wfanet.measurement.common.api.ResourceKey

/** [ResourceKey] of the parent of a PublicKey resource. */
sealed interface PublicKeyParentKey : ResourceKey

/** [ResourceKey] of a PublicKey resource. */
sealed interface PublicKeyKey : ChildResourceKey {
  override val parentKey: PublicKeyParentKey

  companion object FACTORY : ResourceKey.Factory<PublicKeyKey> {
    override fun fromName(resourceName: String): PublicKeyKey? {
      return DataProviderPublicKeyKey.fromName(resourceName)
        ?: MeasurementConsumerPublicKeyKey.fromName(resourceName)
    }
  }
}
