package org.wfanet.measurement.securecomputation.teeapps.utils

import org.junit.Before
import org.junit.Test
import org.wfanet.measurement.api.v2alpha.event_templates.testing.Person
import org.wfanet.virtualpeople.common.AgeRange
import org.wfanet.virtualpeople.common.Gender
import org.wfanet.virtualpeople.common.ageRange
import org.wfanet.virtualpeople.common.demoBucket

class TeeApplicationUtilsTest {
  private lateinit var utils: TeeApplicationUtils

  @Before
  fun setup() {
    utils = TeeApplicationUtils()
  }

  @Test
  fun `mapMessage should correctly map fields from source to target message`() {
    val originalMessage = demoBucket {
      gender = Gender.GENDER_MALE
      age = ageRange {
        minAge = 18
        maxAge = 24
      }
    }

    val newMessageDescriptor = Person.getDescriptor()

    val fieldMapping = mapOf(
      "gender" to FieldDescriptorAndMapper(
        Person.getDescriptor().findFieldByName("gender"),
        object : FieldMapper {
          override fun getMappedValue(sourceValue: Any): Any {
            val genderEnumValue = sourceValue as Int
            return when (genderEnumValue) {
              0 -> Person.Gender.GENDER_UNSPECIFIED
              1 -> Person.Gender.MALE
              2 -> Person.Gender.FEMALE
              else -> throw IllegalArgumentException("Illegal enum value for Gender")
            }
          }
        }
      ),
      "age" to FieldDescriptorAndMapper(
        Person.getDescriptor().findFieldByName("age_group"),
        object : FieldMapper {
          override fun getMappedValue(sourceValue: Any): Any {
            return try {
              val ageRange = sourceValue as AgeRange
              val minAge = ageRange.minAge
              val maxAge = ageRange.maxAge

              // TODO: When is age group unspecified. When is Person.AgeGroup.AGE_GROUP_UNSPECIFIED returned
              if (minAge >= 18 && maxAge <= 34) {
                Person.AgeGroup.YEARS_18_TO_34
              } else if (minAge >= 35 && maxAge <= 54) {
                Person.AgeGroup.YEARS_35_TO_54
              } else if (minAge >= 55) {
                Person.AgeGroup.YEARS_18_TO_34_VALUE
              } else Person.AgeGroup.UNRECOGNIZED
            } catch (e: IllegalArgumentException) {
              println("Error: The value is not an AgeRange: $e")
            }
          }
        }
      )
    )

    utils.convertMessage(
      originalMessage,
      newMessageDescriptor,
      fieldMapping
    )
  }
}
