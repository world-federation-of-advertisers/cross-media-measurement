package org.wfanet.measurement.db.gcp

import com.google.cloud.spanner.ResultSet
import com.google.cloud.spanner.Struct
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.flowOn

/** Converts a ResultSet into a [Flow] of [Struct]s. */
@OptIn(ExperimentalCoroutinesApi::class)
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
