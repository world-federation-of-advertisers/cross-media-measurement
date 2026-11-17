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

  private fun MutableList<String>.addValue(value: Value) {
    @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA") // Protobuf accessors cannot return null.
    when (value.kindCase) {
      Value.KindCase.STRING_VALUE -> add(value.stringValue)
      Value.KindCase.NUMBER_VALUE -> add(value.numberValue.toString())
      Value.KindCase.BOOL_VALUE -> add(value.boolValue.toString())
      Value.KindCase.STRUCT_VALUE -> addAll(flatten(value.structValue))
      Value.KindCase.LIST_VALUE -> value.listValue.valuesList.forEach { addValue(it) }
      Value.KindCase.NULL_VALUE,
      Value.KindCase.KIND_NOT_SET -> Unit
    }
  }
}
