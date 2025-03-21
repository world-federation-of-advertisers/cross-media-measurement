package org.wfanet.measurement.securecomputation.teeapps.utils

import org.junit.Before
import org.junit.Test
import org.wfanet.measurement.api.v2alpha.event_templates.testing.Person
import org.wfanet.panelmatch.common.beam.testing.assertThat
import org.wfanet.virtualpeople.common.AgeRange
import org.wfanet.virtualpeople.common.Gender
import org.wfanet.virtualpeople.common.ageRange
import org.wfanet.virtualpeople.common.demoBucket

class TeeApplicationUtilsTest {
  @Test
  fun `mapMessage should correctly map fields from source to target message`() {
    val originalMessage = demoBucket {
      gender = Gender.GENDER_MALE
      age = ageRange {
        minAge = 18
        maxAge = 24
      }
    }

    val fieldNameMap = mapOf(
      "gender" to "gender",
      "age" to "age_group"
    )

    val fieldValueMap = mapOf(
      "GENDER_MALE" to "MALE",
      "GENDER_FEMALE" to "FEMALE",
    )

    val newMessageDescriptor = Person.getDescriptor()

    val a = TeeApplicationUtils.convertMessage(
      originalMessage,
      newMessageDescriptor,
      fieldNameMap,
      fieldValueMap
    )

    println("JOJI $a")

    assert(1==2)
  }
}
