// Copyright 2021 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.gcloud.spanner

import com.google.cloud.Timestamp
import com.google.cloud.spanner.Mutation
import com.google.cloud.spanner.Value
import com.google.common.truth.Truth.assertThat
import com.google.protobuf.Field
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.common.numberAsLong
import org.wfanet.measurement.common.toJson
import org.wfanet.measurement.gcloud.common.toGcloudByteArray

@RunWith(JUnit4::class)
class MutationsTest {
  @Test
  fun `insertMutation builds Mutation`() {
    val boolValue = true
    val longValue = 1234L
    val doubleValue = 12.34
    val stringValue = "stringValue"
    val timestamp = Timestamp.now()
    val field = Field.newBuilder().setName("field_name").build()
    val cardinality = Field.Cardinality.CARDINALITY_REPEATED

    val mutation: Mutation =
      insertMutation("DummyTable") {
        set("BoolColumn" to boolValue)
        set("LongColumn" to longValue)
        set("DoubleColumn" to doubleValue)
        set("StringColumn" to stringValue)
        set("TimestampColumn" to timestamp)
        set("EnumColumn" to cardinality)
        set("ProtoBytesColumn" to field)
        setJson("ProtoJsonColumn" to field)
      }

    assertThat(mutation.operation).isEqualTo(Mutation.Op.INSERT)
    assertThat(mutation.table).isEqualTo("DummyTable")
    assertThat(mutation.asMap())
      .containsExactly(
        "BoolColumn",
        Value.bool(boolValue),
        "LongColumn",
        Value.int64(longValue),
        "DoubleColumn",
        Value.float64(doubleValue),
        "StringColumn",
        Value.string(stringValue),
        "TimestampColumn",
        Value.timestamp(timestamp),
        "EnumColumn",
        Value.int64(cardinality.numberAsLong),
        "ProtoBytesColumn",
        Value.bytes(field.toGcloudByteArray()),
        "ProtoJsonColumn",
        Value.string(field.toJson())
      )
  }

  @Test
  fun `updateMutation builds Mutation`() {
    val mutation: Mutation = updateMutation("DummyTable") { set("LongColumn" to 1234L) }

    assertThat(mutation.operation).isEqualTo(Mutation.Op.UPDATE)
    assertThat(mutation.table).isEqualTo("DummyTable")
    assertThat(mutation.asMap()).containsExactly("LongColumn", Value.int64(1234L))
  }

  @Test
  fun `insertOrUpdateMutation builds Mutation`() {
    val mutation: Mutation = insertOrUpdateMutation("DummyTable") { set("LongColumn" to 1234L) }

    assertThat(mutation.operation).isEqualTo(Mutation.Op.INSERT_OR_UPDATE)
    assertThat(mutation.table).isEqualTo("DummyTable")
    assertThat(mutation.asMap()).containsExactly("LongColumn", Value.int64(1234L))
  }
}
