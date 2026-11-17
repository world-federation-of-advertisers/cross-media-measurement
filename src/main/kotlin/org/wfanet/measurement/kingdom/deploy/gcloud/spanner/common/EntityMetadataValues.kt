// Copyright 2026 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common

import com.google.protobuf.Struct
import com.google.protobuf.Value

/** Values indexed for entity metadata search. Field names and nulls are not searchable. */
object EntityMetadataValues {
  fun flatten(metadata: Struct): List<String> = buildList {
    // Sort map entries so the stored representation is independent of protobuf map ordering.
    for (value in metadata.fieldsMap.toSortedMap().values) {
      addValue(value)
    }
  }

  private val valueAppenders: Map<Value.KindCase, MutableList<String>.(Value) -> Unit> =
    mapOf(
      Value.KindCase.STRING_VALUE to { add(it.stringValue) },
      Value.KindCase.NUMBER_VALUE to { add(it.numberValue.toString()) },
      Value.KindCase.BOOL_VALUE to { add(it.boolValue.toString()) },
      Value.KindCase.STRUCT_VALUE to { addAll(flatten(it.structValue)) },
      Value.KindCase.LIST_VALUE to
        { value ->
          for (element in value.listValue.valuesList) {
            addValue(element)
          }
        },
      Value.KindCase.NULL_VALUE to {},
      Value.KindCase.KIND_NOT_SET to {},
    )

  private fun MutableList<String>.addValue(value: Value) {
    valueAppenders.getValue(value.kindCase).invoke(this, value)
  }
}
