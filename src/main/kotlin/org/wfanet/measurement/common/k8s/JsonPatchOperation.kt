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

package org.wfanet.measurement.common.k8s

/** A serializable [JSON Patch](https://jsonpatch.com/) operation. */
internal sealed class JsonPatchOperation
private constructor(
  /** Operation type. */
  @Suppress("unused") // Serialized field.
  val op: String
) {
  data class Replace(val path: String, val value: Any) : JsonPatchOperation("replace")
  data class Add(val path: String, val value: Any) : JsonPatchOperation("add")

  companion object {
    /** Creates a "replace" operation. */
    fun replace(path: String, value: Any) = Replace(path, value)
    /** Creates an "add" operation. */
    fun add(path: String, value: Any) = Add(path, value)
  }
}
