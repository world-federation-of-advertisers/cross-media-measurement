package org.wfanet.measurement.db.gcp

import com.google.cloud.Timestamp
import com.google.cloud.spanner.ResultSet
import com.google.cloud.spanner.Struct
import com.google.cloud.spanner.Type

private fun <T> Struct.nullOrValue(
  column: String,
  typeCode: Type.Code,
  getter: Struct.(String) -> T
): T? {
  val columnType = getColumnType(column).code
  check(columnType == typeCode) { "Cannot read $typeCode from $column, it has type $columnType" }
  return if (isNull(column)) null else getter(column)
}

/** Returns the value of a String column even if it is null. */
fun Struct.getNullableString(column: String): String? =
  nullOrValue(column, Type.Code.STRING, Struct::getString)

/** Returns the value of a Timestamp column even if it is null. */
fun Struct.getNullableTimestamp(column: String): Timestamp? =
  nullOrValue(column, Type.Code.TIMESTAMP, Struct::getTimestamp)

/** Returns the value of a INT64 column even if it is null. */
fun Struct.getNullableLong(column: String): Long? =
  nullOrValue(column, Type.Code.INT64, Struct::getLong)

fun ResultSet.getAtMostOne(): Struct? =
  if (next()) currentRowAsStruct.also { check(!next()) { "Found more than one row" } }
  else null
