/*
 * Copyright 2022 The Cross-Media Measurement Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.wfanet.measurement.common.api

interface ResourceNameLookup<T> {
  /** Returns the resource name for [lookupKey], or `null` if not found. */
  suspend fun getResourceName(lookupKey: T): String?
}

fun <K> ResourceNameLookup<K>.toResourceKeyLookup(
  keyFactory: ResourceKey.Factory<*>
): ResourceKeyLookup<K> {
  return ResourceKeyLookupImpl(this, keyFactory)
}

private class ResourceKeyLookupImpl<K>(
  private val resourceNameLookup: ResourceNameLookup<K>,
  private val keyFactory: ResourceKey.Factory<*>
) : ResourceKeyLookup<K> {
  override suspend fun getResourceKey(lookupKey: K): ResourceKey? {
    return resourceNameLookup.getResourceName(lookupKey)?.let { keyFactory.fromName(it) }
  }
}
