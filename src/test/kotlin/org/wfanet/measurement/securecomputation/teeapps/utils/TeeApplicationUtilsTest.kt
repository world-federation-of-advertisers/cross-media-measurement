package org.wfanet.measurement.securecomputation.teeapps.utils

import org.junit.Test
import com.google.common.truth.extensions.proto.ProtoTruth.assertThat
import com.google.common.truth.Truth.assertThat
import org.wfanet.measurement.api.v2alpha.event_templates.testing.Person
import org.wfanet.measurement.api.v2alpha.event_templates.testing.person
import org.wfanet.virtualpeople.common.Gender
import org.wfanet.virtualpeople.common.ageRange
import org.wfanet.virtualpeople.common.demoBucket
import org.wfanet.measurement.api.v2alpha.event_templates.testing.Person.Gender as PersonGender
import com.google.protobuf.ExtensionRegistry
import com.google.protobuf.TypeRegistry
import org.wfanet.measurement.api.v2alpha.event_templates.testing.Person.AgeGroup
import org.wfanet.virtualpeople.common.demoInfo
import com.google.protobuf.Any


class TeeApplicationUtilsTest {
  @Test
  fun `mapMessage should correctly map fields from source to target message`() {
    val originalMessage = demoBucket {
      gender = Gender.GENDER_MALE
      age = ageRange {
        minAge = 18
        maxAge = 34
      }
    }


    val fieldNameMap = mapOf(
      "gender" to "gender",
      "age.min_age" to "age_group",
      "age.max_age" to "age_group"

    )

    val fieldValueMap = mapOf(
      "gender.GENDER_MALE" to "MALE",
      "gender.GENDER_FEMALE" to "FEMALE",
      "age.min_age.18" to "YEARS_18_TO_34",
      "age.max_age.34" to "YEARS_18_TO_34",
    )

    val newMessageDescriptor = Person.getDescriptor()

    val newMessage = TeeApplicationUtils.convertMessage(
      originalMessage,
      newMessageDescriptor,
      fieldNameMap,
      fieldValueMap
    )

    val expectedMessage = person {
      gender = PersonGender.MALE
      ageGroup = AgeGroup.YEARS_18_TO_34
    }
    val typeRegistry = TypeRegistry.newBuilder().add(Person.getDescriptor()).build()
    assertThat(newMessage.descriptorForType).isEqualTo(expectedMessage.descriptorForType)
    assertThat(Any.pack(newMessage)).unpackingAnyUsing(typeRegistry, ExtensionRegistry.getEmptyRegistry()).isEqualTo(Any.pack(expectedMessage))
    assertThat(newMessage.toByteString() == expectedMessage.toByteString())
  }

  @Test
  fun `mapMessage should correctly map fields from source to target message with nested messages`() {
    val originalMessage = demoInfo {
      demoBucket = demoBucket {
        gender = Gender.GENDER_MALE
        age = ageRange {
          minAge = 18
          maxAge = 34
        }
      }
    }


    val fieldNameMap = mapOf(
      "demo_bucket.gender" to "gender",
      "demo_bucket.age.min_age" to "age_group",
      "demo_bucket.age.max_age" to "age_group"

    )

    val fieldValueMap = mapOf(
      "demo_bucket.gender.GENDER_MALE" to "MALE",
      "demo_bucket.gender.GENDER_FEMALE" to "FEMALE",
      "demo_bucket.age.min_age.18" to "YEARS_18_TO_34",
      "demo_bucket.age.max_age.34" to "YEARS_18_TO_34",
    )

    val newMessageDescriptor = Person.getDescriptor()

    val newMessage = TeeApplicationUtils.convertMessage(
      originalMessage,
      newMessageDescriptor,
      fieldNameMap,
      fieldValueMap
    )

    val expectedMessage = person {
      gender = PersonGender.MALE
      ageGroup = AgeGroup.YEARS_18_TO_34
    }

    val typeRegistry = TypeRegistry.newBuilder().add(Person.getDescriptor()).build()
    assertThat(newMessage.descriptorForType).isEqualTo(expectedMessage.descriptorForType)
    assertThat(Any.pack(newMessage)).unpackingAnyUsing(typeRegistry, ExtensionRegistry.getEmptyRegistry()).isEqualTo(Any.pack(expectedMessage))
    assertThat(newMessage.toByteString() == expectedMessage.toByteString())
  }
}
