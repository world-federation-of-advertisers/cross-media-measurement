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

package org.wfanet.measurement.edpaggregator.rawimpressions

import org.wfanet.measurement.storage.ParquetValue.KindCase

/**
 * Fails fast if any column a mapping needs is absent from the raw-impression parquet schema, or is
 * present but has an incompatible type (schema-drift detection, cross-media-measurement#3993).
 *
 * A [LabelerInputMapper] silently leaves a target field unset when its source column is missing
 * from a row, and throws per-row when a column's type is not what the mapping expects — so a
 * renamed, dropped, or retyped column would otherwise corrupt every impression (or fail deep into
 * processing) with no early signal. Callers read a representative file's schema (via
 * `ParquetBlob.readSchema()`) once at startup and pass its column names + [KindCase]s as [schema]
 * here.
 *
 * @param columnKinds the columns the mapping reads, each mapped to the [KindCase]s the mapper
 *   accepts for it (e.g. [LabelerInputMapper.referencedColumnKinds]).
 * @param schema the column name -> [KindCase] of the raw-impression parquet file.
 * @param context human-readable description of what is being validated (upload / file), for the
 *   error message.
 * @throws IllegalArgumentException if any referenced column is missing from [schema] or its schema
 *   [KindCase] is not among the accepted kinds.
 */
fun validateColumnsAgainstSchema(
  columnKinds: Map<String, Set<KindCase>>,
  schema: Map<String, KindCase>,
  context: String,
) {
  val missing = columnKinds.keys - schema.keys
  require(missing.isEmpty()) {
    "Raw-impression schema drift for $context: mapped column(s) $missing are not present in the " +
      "parquet schema (available columns: ${schema.keys.sorted()}). A column was likely renamed " +
      "or dropped; update the model line's labeler_input_field_mapping or the upstream schema."
  }
  val mismatched =
    columnKinds
      .filterKeys { it in schema }
      .filter { (column, accepted) -> schema.getValue(column) !in accepted }
  require(mismatched.isEmpty()) {
    "Raw-impression schema drift for $context: column(s) have an incompatible type: " +
      mismatched.entries.joinToString { (column, accepted) ->
        "'$column' is ${schema.getValue(column)} but the mapping expects " + "one of $accepted"
      } +
      ". The upstream column type likely changed; update the labeler_input_field_mapping or the " +
      "upstream schema."
  }
}
