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

import com.google.common.truth.Truth.assertThat
import com.google.protobuf.Timestamp
import kotlin.test.assertFailsWith
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.edpaggregator.v1alpha.LabelerInputFieldMapping
import org.wfanet.measurement.edpaggregator.v1alpha.ageBucket
import org.wfanet.measurement.edpaggregator.v1alpha.ageRange
import org.wfanet.measurement.edpaggregator.v1alpha.bucketLookup
import org.wfanet.measurement.edpaggregator.v1alpha.compositeIdentity
import org.wfanet.measurement.edpaggregator.v1alpha.enumLookup
import org.wfanet.measurement.edpaggregator.v1alpha.labelerInputFieldMapping
import org.wfanet.measurement.edpaggregator.v1alpha.minMaxColumns
import org.wfanet.measurement.edpaggregator.v1alpha.scalarColumn
import org.wfanet.measurement.storage.ParquetValue
import org.wfanet.measurement.storage.ParquetValue.KindCase
import org.wfanet.measurement.storage.parquetValue
import org.wfanet.virtualpeople.common.Gender

@RunWith(JUnit4::class)
class LabelerInputMapperTest {
  @Test
  fun `projects scalar and nested message fields`() {
    val mapper =
      mapperOf(
        scalar("timestamp_usec", "ts"),
        scalar("profile_info.email_user_info.user_id", "email"),
      )

    val row =
      mapOf(
        "eid" to parquetValue { stringValue = "event-1" },
        "ts" to parquetValue { int64Value = 1_700_000_000_000_000L },
        "email" to parquetValue { stringValue = "user@example.com" },
      )

    val input = mapper.project(row)

    assertThat(input.eventId.id).isEqualTo("event-1")
    assertThat(input.timestampUsec).isEqualTo(1_700_000_000_000_000L)
    assertThat(input.profileInfo.emailUserInfo.userId).isEqualTo("user@example.com")
  }

  @Test
  fun `skips columns that are absent or NULL`() {
    val mapper = mapperOf(scalar("timestamp_usec", "ts"))

    val row =
      mapOf(
        "eid" to parquetValue { stringValue = "event-1" },
        // "ts" present but NULL (no kind set).
        "ts" to ParquetValue.getDefaultInstance(),
      )

    val input = mapper.project(row)

    assertThat(input.eventId.id).isEqualTo("event-1")
    assertThat(input.hasTimestampUsec()).isFalse()
  }

  @Test
  fun `coerces a parquet timestamp into timestamp_usec micros`() {
    val mapper = mapperOf(scalar("timestamp_usec", "ts"))
    val row =
      mapOf(
        "ts" to
          parquetValue {
            timestampValue = Timestamp.newBuilder().setSeconds(1_700L).setNanos(123_000).build()
          }
      )

    assertThat(mapper.project(row).timestampUsec).isEqualTo(1_700_000_123L)
  }

  @Test
  fun `widens int32 column to int64 field`() {
    val mapper = mapperOf(scalar("timestamp_usec", "ts"))
    assertThat(mapper.project(mapOf("ts" to parquetValue { int32Value = 42 })).timestampUsec)
      .isEqualTo(42L)
  }

  @Test
  fun `stores an unsigned 32-bit value above Int_MAX without overflow`() {
    // geo.country_id is a proto uint32; 3_000_000_000 is a valid uint32 but exceeds Int.MAX.
    val mapper = mapperOf(scalar("geo.country_id", "country"))
    val unsignedValue = 3_000_000_000L

    val input =
      mapper.project(mapOf("country" to parquetValue { uint32Value = unsignedValue.toInt() }))

    assertThat(Integer.toUnsignedLong(input.geo.countryId)).isEqualTo(unsignedValue)
  }

  @Test
  fun `translates a raw string to an enum via lookup table`() {
    val mapper =
      mapperOf(
        labelerInputFieldMapping {
          fieldPath = GENDER_PATH
          enumLookup = enumLookup {
            column = "g"
            lookupTable["M"] = "GENDER_MALE"
            lookupTable["F"] = "GENDER_FEMALE"
          }
        }
      )

    val male = mapper.project(mapOf("g" to parquetValue { stringValue = "M" }))
    val female = mapper.project(mapOf("g" to parquetValue { stringValue = "F" }))

    assertThat(male.profileInfo.emailUserInfo.demo.demoBucket.gender).isEqualTo(Gender.GENDER_MALE)
    assertThat(female.profileInfo.emailUserInfo.demo.demoBucket.gender)
      .isEqualTo(Gender.GENDER_FEMALE)
  }

  @Test
  fun `falls back to default_enum_value for an unmapped raw value`() {
    val mapper =
      mapperOf(
        labelerInputFieldMapping {
          fieldPath = GENDER_PATH
          enumLookup = enumLookup {
            column = "g"
            lookupTable["M"] = "GENDER_MALE"
            defaultEnumValue = "GENDER_UNKNOWN"
          }
        }
      )

    val input = mapper.project(mapOf("g" to parquetValue { stringValue = "unknown-code" }))

    assertThat(input.profileInfo.emailUserInfo.demo.demoBucket.gender)
      .isEqualTo(Gender.GENDER_UNKNOWN)
  }

  @Test
  fun `throws when an enum raw value has no mapping and no default`() {
    val mapper =
      mapperOf(
        labelerInputFieldMapping {
          fieldPath = GENDER_PATH
          enumLookup = enumLookup {
            column = "g"
            lookupTable["M"] = "GENDER_MALE"
          }
        }
      )

    assertFailsWith<IllegalArgumentException> {
      mapper.project(mapOf("g" to parquetValue { stringValue = "X" }))
    }
  }

  @Test
  fun `rejects a lookup_table entry naming a non-existent enum value at construction`() {
    // The lookup target is resolved to its EnumValueDescriptor once at construction, so an entry
    // that names a non-existent enum value fails fast at startup (not on the first matching row).
    val ex =
      assertFailsWith<IllegalArgumentException> {
        mapperOf(
          labelerInputFieldMapping {
            fieldPath = GENDER_PATH
            enumLookup = enumLookup {
              column = "g"
              lookupTable["M"] = "NOT_A_GENDER"
            }
          }
        )
      }
    assertThat(ex).hasMessageThat().contains("NOT_A_GENDER")
  }

  @Test
  fun `rejects a default_enum_value naming a non-existent enum value at construction`() {
    assertFailsWith<IllegalArgumentException> {
      mapperOf(
        labelerInputFieldMapping {
          fieldPath = GENDER_PATH
          enumLookup = enumLookup {
            column = "g"
            lookupTable["M"] = "GENDER_MALE"
            defaultEnumValue = "NOT_A_GENDER"
          }
        }
      )
    }
  }

  @Test
  fun `rejects an enum_lookup targeting a non-enum field at construction`() {
    assertFailsWith<IllegalArgumentException> {
      mapperOf(
        labelerInputFieldMapping {
          fieldPath = "timestamp_usec"
          enumLookup = enumLookup { column = "g" }
        }
      )
    }
  }

  @Test
  fun `fills an age range from a single age column`() {
    val mapper =
      mapperOf(
        labelerInputFieldMapping {
          fieldPath = AGE_PATH
          ageRange = ageRange { singleAgeColumn = "age" }
        }
      )

    val age = mapper.project(mapOf("age" to parquetValue { int32Value = 33 })).ageBucket()

    assertThat(age.minAge).isEqualTo(33)
    assertThat(age.maxAge).isEqualTo(33)
  }

  @Test
  fun `fills an age range from min and max columns`() {
    val mapper =
      mapperOf(
        labelerInputFieldMapping {
          fieldPath = AGE_PATH
          ageRange = ageRange {
            minMaxColumns = minMaxColumns {
              minAgeColumn = "min"
              maxAgeColumn = "max"
            }
          }
        }
      )

    val age =
      mapper
        .project(
          mapOf(
            "min" to parquetValue { int32Value = 18 },
            "max" to parquetValue { int32Value = 24 },
          )
        )
        .ageBucket()

    assertThat(age.minAge).isEqualTo(18)
    assertThat(age.maxAge).isEqualTo(24)
  }

  @Test
  fun `looks up a closed string age bucket in the bucket_table`() {
    val mapper = ageBucketMapper()
    val age = mapper.project(mapOf("bucket" to parquetValue { stringValue = "18-24" })).ageBucket()
    assertThat(age.minAge).isEqualTo(18)
    assertThat(age.maxAge).isEqualTo(24)
  }

  @Test
  fun `looks up an open-ended string age bucket verbatim from the bucket_table`() {
    val mapper = ageBucketMapper()
    val age = mapper.project(mapOf("bucket" to parquetValue { stringValue = "65+" })).ageBucket()
    assertThat(age.minAge).isEqualTo(65)
    assertThat(age.maxAge).isEqualTo(1000)
  }

  @Test
  fun `throws on an age bucket value not in the bucket_table`() {
    val mapper = ageBucketMapper()
    assertFailsWith<IllegalArgumentException> {
      mapper.project(mapOf("bucket" to parquetValue { stringValue = "unknown" }))
    }
  }

  @Test
  fun `sets the leaf from the first non-null composite identity column`() {
    val mapper =
      mapperOf(
        labelerInputFieldMapping {
          fieldPath = "profile_info.email_user_info.user_id"
          compositeIdentity = compositeIdentity {
            columns += "primary"
            columns += "fallback"
          }
        }
      )

    // primary NULL -> falls back to the second column.
    val input =
      mapper.project(
        mapOf(
          "primary" to ParquetValue.getDefaultInstance(),
          "fallback" to parquetValue { stringValue = "from-fallback" },
        )
      )

    assertThat(input.profileInfo.emailUserInfo.userId).isEqualTo("from-fallback")
  }

  @Test
  fun `requires the event_id_id field path at construction`() {
    val ex =
      assertFailsWith<IllegalArgumentException> {
        LabelerInputMapper(listOf(scalar("timestamp_usec", "ts")))
      }
    assertThat(ex).hasMessageThat().contains("event_id.id")
  }

  @Test
  fun `rejects a duplicate field path at construction`() {
    assertFailsWith<IllegalArgumentException> {
      mapperOf(scalar("timestamp_usec", "a"), scalar("timestamp_usec", "b"))
    }
  }

  @Test
  fun `rejects an entry with no source at construction`() {
    assertFailsWith<IllegalArgumentException> {
      mapperOf(labelerInputFieldMapping { fieldPath = "timestamp_usec" })
    }
  }

  @Test
  fun `rejects an unknown field path at construction`() {
    assertFailsWith<IllegalArgumentException> { mapperOf(scalar("not_a_field", "c")) }
  }

  @Test
  fun `rejects a message-typed leaf for a scalar source at construction`() {
    assertFailsWith<IllegalArgumentException> { mapperOf(scalar("event_id", "c")) }
  }

  @Test
  fun `rejects a type-incompatible column at projection`() {
    val mapper = LabelerInputMapper(listOf(scalar("event_id.id", "eid2")))
    assertFailsWith<IllegalArgumentException> {
      mapper.project(mapOf("eid2" to parquetValue { int64Value = 5 }))
    }
  }

  @Test
  fun `referencedColumnKinds lists every mapped column with its accepted kinds`() {
    val mapper =
      mapperOf(
        labelerInputFieldMapping {
          fieldPath = GENDER_PATH
          enumLookup = enumLookup { column = "g" }
        },
        labelerInputFieldMapping {
          fieldPath = AGE_PATH
          ageRange = ageRange {
            minMaxColumns = minMaxColumns {
              minAgeColumn = "min"
              maxAgeColumn = "max"
            }
          }
        },
        labelerInputFieldMapping {
          fieldPath = "profile_info.email_user_info.user_id"
          compositeIdentity = compositeIdentity {
            columns += "c1"
            columns += "c2"
          }
        },
      )

    val kinds = mapper.referencedColumnKinds()
    assertThat(kinds.keys).containsExactly("eid", "g", "min", "max", "c1", "c2")
    // event-id + composite user_id leaves + enum-lookup column are read as strings.
    assertThat(kinds["eid"]).containsExactly(KindCase.STRING_VALUE)
    assertThat(kinds["g"]).containsExactly(KindCase.STRING_VALUE)
    assertThat(kinds["c1"]).containsExactly(KindCase.STRING_VALUE)
    // age min/max columns accept integral kinds.
    assertThat(kinds["min"]).contains(KindCase.INT64_VALUE)
  }

  @Test
  fun `validateColumnsAgainstSchema throws when a mapped column is missing`() {
    val ex =
      assertFailsWith<IllegalArgumentException> {
        validateColumnsAgainstSchema(
          mapOf("a" to setOf(KindCase.STRING_VALUE), "missing" to setOf(KindCase.STRING_VALUE)),
          mapOf("a" to KindCase.STRING_VALUE),
          "ctx",
        )
      }
    assertThat(ex).hasMessageThat().contains("missing")
  }

  @Test
  fun `validateColumnsAgainstSchema throws when a column has an incompatible type`() {
    val ex =
      assertFailsWith<IllegalArgumentException> {
        validateColumnsAgainstSchema(
          mapOf("a" to setOf(KindCase.STRING_VALUE)),
          mapOf("a" to KindCase.INT64_VALUE),
          "ctx",
        )
      }
    assertThat(ex).hasMessageThat().contains("incompatible")
  }

  @Test
  fun `validateColumnsAgainstSchema passes when every column exists with a compatible kind`() {
    validateColumnsAgainstSchema(
      mapOf(
        "a" to setOf(KindCase.STRING_VALUE),
        "b" to setOf(KindCase.INT32_VALUE, KindCase.INT64_VALUE),
      ),
      mapOf("a" to KindCase.STRING_VALUE, "b" to KindCase.INT64_VALUE, "c" to KindCase.BOOL_VALUE),
      "ctx",
    )
  }

  companion object {
    private const val GENDER_PATH = "profile_info.email_user_info.demo.demo_bucket.gender"
    private const val AGE_PATH = "profile_info.email_user_info.demo.demo_bucket.age"

    /**
     * A scalar mapping entry (built outside any proto DSL scope to dodge factory-name shadowing).
     */
    private fun scalar(fieldPath: String, column: String): LabelerInputFieldMapping =
      labelerInputFieldMapping {
        this.fieldPath = fieldPath
        scalar = scalarColumn { this.column = column }
      }

    /** A mapper that always includes the required `event_id.id` scalar mapping plus [extra]. */
    private fun mapperOf(vararg extra: LabelerInputFieldMapping): LabelerInputMapper =
      LabelerInputMapper(listOf(scalar("event_id.id", "eid")) + extra)

    private fun ageBucketMapper(): LabelerInputMapper =
      mapperOf(
        labelerInputFieldMapping {
          fieldPath = AGE_PATH
          ageRange = ageRange {
            bucketLookup = bucketLookup {
              column = "bucket"
              bucketTable["18-24"] = ageBucket {
                minAge = 18
                maxAge = 24
              }
              bucketTable["65+"] = ageBucket {
                minAge = 65
                maxAge = 1000
              }
            }
          }
        }
      )

    private fun org.wfanet.virtualpeople.common.LabelerInput.ageBucket() =
      profileInfo.emailUserInfo.demo.demoBucket.age
  }
}
