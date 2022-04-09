package org.wfanet.measurement.eventdataprovider.privacybudgetmanagement

import com.google.common.truth.Truth.assertThat
import java.time.LocalDate
import java.time.ZoneOffset
import kotlin.test.assertFailsWith
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.api.v2alpha.MeasurementSpecKt.reachAndFrequency
import org.wfanet.measurement.api.v2alpha.MeasurementSpecKt.vidSamplingInterval
import org.wfanet.measurement.api.v2alpha.RequisitionSpecKt
import org.wfanet.measurement.api.v2alpha.RequisitionSpecKt.eventFilter
import org.wfanet.measurement.api.v2alpha.RequisitionSpecKt.eventGroupEntry
import org.wfanet.measurement.api.v2alpha.measurementSpec
import org.wfanet.measurement.api.v2alpha.requisitionSpec
import org.wfanet.measurement.api.v2alpha.timeInterval
import org.wfanet.measurement.common.toProtoTime

private val MEASUREMENT_SPEC = measurementSpec {
  reachAndFrequency = reachAndFrequency {
    vidSamplingInterval = vidSamplingInterval {
      start = 0.0f
      width = 0.01f
    }
  }
}

@RunWith(JUnit4::class)
class PrivacyBucketMapperTest {
  @Test
  fun `Mapper fails for invalid filter expression`() {
    val requisitionSpec = requisitionSpec {
      eventGroups += eventGroupEntry {
        key = "eventGroups/someEventGroup"
        value =
          RequisitionSpecKt.EventGroupEntryKt.value {
            collectionInterval = timeInterval {
              startTime =
                LocalDate.now().minusDays(1).atStartOfDay().toInstant(ZoneOffset.UTC).toProtoTime()
              endTime = LocalDate.now().atStartOfDay().toInstant(ZoneOffset.UTC).toProtoTime()
            }
            filter = eventFilter { expression = "privacy_budget.age.value" }
          }
      }
    }

    assertFailsWith<PrivacyBudgetManagerException> {
      getPrivacyBucketGroups(MEASUREMENT_SPEC, requisitionSpec)
    }
  }

  @Test
  fun `Mapper succeeds for filter expression with only privacy budget Fields`() {

    val requisitionSpec = requisitionSpec {
      eventGroups += eventGroupEntry {
        key = "eventGroups/someEventGroup"
        value =
          RequisitionSpecKt.EventGroupEntryKt.value {
            collectionInterval = timeInterval {
              startTime =
                LocalDate.now().minusDays(1).atStartOfDay().toInstant(ZoneOffset.UTC).toProtoTime()
              endTime = LocalDate.now().atStartOfDay().toInstant(ZoneOffset.UTC).toProtoTime()
            }
            filter = eventFilter {
              expression = "privacy_budget.age.value in [0] && privacy_budget.gender.value == 1"
            }
          }
      }
    }

    assertThat(getPrivacyBucketGroups(MEASUREMENT_SPEC, requisitionSpec))
      .containsExactly(
        PrivacyBucketGroup(
          "ACME",
          LocalDate.parse("2022-04-08"),
          LocalDate.parse("2022-04-08"),
          AgeGroup.RANGE_18_34,
          Gender.FEMALE,
          0.0f,
          0.01f
        ),
        PrivacyBucketGroup(
          "ACME",
          LocalDate.parse("2022-04-07"),
          LocalDate.parse("2022-04-07"),
          AgeGroup.RANGE_18_34,
          Gender.FEMALE,
          0.0f,
          0.01f
        ),
        PrivacyBucketGroup(
          "ACME",
          LocalDate.parse("2022-04-08"),
          LocalDate.parse("2022-04-08"),
          AgeGroup.RANGE_18_34,
          Gender.FEMALE,
          0.01f,
          0.01f
        ),
        PrivacyBucketGroup(
          "ACME",
          LocalDate.parse("2022-04-07"),
          LocalDate.parse("2022-04-07"),
          AgeGroup.RANGE_18_34,
          Gender.FEMALE,
          0.01f,
          0.01f
        ),
      )
    // .forEach{println(it)}
  }

  //   @Test
  //   fun `Mapper succeeds for filter expression privacy budget and non privacy budget fields`() {
  //     val measurementSpec = measurementSpec {
  //       reachAndFrequency = reachAndFrequency {
  //         vidSamplingInterval = vidSamplingInterval {
  //           start = 0.0f
  //           width = 0.03f
  //         }
  //       }
  //     }

  //     val requisitionSpec = requisitionSpec {
  //       eventGroups += eventGroupEntry {
  //         key = "eventGroups/someEventGroup"
  //         value =
  //           RequisitionSpecKt.EventGroupEntryKt.value {
  //             collectionInterval = timeInterval {
  //               startTime =
  //                 LocalDate.now()
  //                   .minusDays(100)
  //                   .atStartOfDay()
  //                   .toInstant(ZoneOffset.UTC)
  //                   .toProtoTime()
  //               endTime = LocalDate.now().atStartOfDay().toInstant(ZoneOffset.UTC).toProtoTime()
  //             }
  //             filter = eventFilter {
  //               expression = "privacy_budget.age.value in [0,1] && privacy_budget.gender.value ==
  // 1"
  //               // expression = "privacy_budget.age.value "
  //             }
  //           }
  //       }
  //     }

  //     val buckets = getPrivacyBucketGroups(measurementSpec, requisitionSpec)
  //     println("NUM BUCKETS!!  ${buckets.size}")
  //     // .forEach{println(it)}
  //   }
}
