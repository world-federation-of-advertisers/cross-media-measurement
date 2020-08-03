// Copyright 2020 The Measurement System Authors
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

package org.wfanet.measurement.db.gcp

import com.google.cloud.spanner.ResultSet
import com.google.cloud.spanner.Struct
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.flowOn

/** Converts a ResultSet into a [Flow] of [Struct]s. */
fun ResultSet.asFlow(): Flow<Struct> = flow {
  while (next()) {
    emit(currentRowAsStruct)
  }
}.flowOn(spannerDispatcher())

/** Returns a [Sequence] of [Struct]s from a [ResultSet]. */
fun ResultSet.asSequence(): Sequence<Struct> = sequence {
  while (next()) {
    yield(currentRowAsStruct)
  }
}

/**
 * Returns the unique element in a [ResultSet], or null if it's empty. Throws an error if there are
 * more than one element.
 */
fun ResultSet.singleOrNull(): Struct? = asSequence().singleOrNull()

/**
 * Returns the unique element in a [ResultSet]. Throws an error if the number of elements is not 1.
 */
fun ResultSet.single(): Struct = asSequence().single()
