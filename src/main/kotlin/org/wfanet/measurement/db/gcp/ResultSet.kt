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
fun ResultSet.sequence(): Sequence<Struct> =
  generateSequence { if (next()) currentRowAsStruct else null }

fun ResultSet.singleOrNull(): Struct? = sequence().singleOrNull()
