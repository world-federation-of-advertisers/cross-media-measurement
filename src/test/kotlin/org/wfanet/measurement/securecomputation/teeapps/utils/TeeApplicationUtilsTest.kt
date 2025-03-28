package org.wfanet.measurement.securecomputation.teeapps.utils

import org.junit.Test
import com.google.common.truth.Truth.assertThat
import org.wfanet.measurement.api.v2alpha.event_templates.testing.Person
import org.wfanet.measurement.api.v2alpha.event_templates.testing.person
import org.wfanet.virtualpeople.common.Gender
import org.wfanet.virtualpeople.common.ageRange
import org.wfanet.virtualpeople.common.demoBucket
import org.wfanet.measurement.api.v2alpha.event_templates.testing.Person.Gender as PersonGender
import org.wfanet.measurement.api.v2alpha.event_templates.testing.Person.AgeGroup

class TeeApplicationUtilsTest {
  @Test
  fun `mapMessage should correctly map fields from source to target message`() {
//    val originalMessage = common {
//      gender = Gender.GENDER_MALE
//      age = ageRange {
//        minAge = 18
//        maxAge = 24
//      }
//    }

    val originalMessage = demoBucket {
      gender = Gender.GENDER_MALE
      age = ageRange {
        minAge = 18
        maxAge = 34
      }
    }


    val fieldNameMap = mapOf(
      "gender" to "gender",
      "age" to "age_group"
    )

    val fieldValueMap = mapOf(
      "GENDER_MALE" to "MALE",
      "GENDER_FEMALE" to "FEMALE",
       originalMessage.age.toString() to "YEARS_18_TO_34"
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


    assertThat(newMessage.descriptorForType == expectedMessage.descriptorForType)
    assertThat(newMessage.toByteString() == expectedMessage.toByteString())
  }
}
