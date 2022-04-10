package org.wfanet.measurement.eventdataprovider.privacybudgetmanagement

import java.time.LocalDate
import java.time.ZoneOffset
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
  //   @Test
  //   fun `Mapper fails for invalid filter expression`() {
  //     val requisitionSpec = requisitionSpec {
  //       eventGroups += eventGroupEntry {
  //         key = "eventGroups/someEventGroup"
  //         value =
  //           RequisitionSpecKt.EventGroupEntryKt.value {
  //             collectionInterval = timeInterval {
  //               startTime =
  //
  // LocalDate.now().minusDays(1).atStartOfDay().toInstant(ZoneOffset.UTC).toProtoTime()
  //               endTime = LocalDate.now().atStartOfDay().toInstant(ZoneOffset.UTC).toProtoTime()
  //             }
  //             filter = eventFilter { expression = "privacy_budget.age.value" }
  //           }
  //       }
  //     }

  //     assertFailsWith<PrivacyBudgetManagerException> {
  //       getPrivacyBucketGroups(MEASUREMENT_SPEC, requisitionSpec)
  //     }
  //   }

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
            filter = eventFilter { expression = "privacy_budget.age.value == 1" }
          }
      }
    }
    val buckets = getPrivacyBucketGroups(MEASUREMENT_SPEC, requisitionSpec)
    println("AAAAAA SIZE")
    println(buckets.size)
  }

  //   @Test
  //   fun `Mapper succeeds for filter expression with only privacy budget Fields`() {

  //     val requisitionSpec = requisitionSpec {
  //       eventGroups += eventGroupEntry {
  //         key = "eventGroups/someEventGroup"
  //         value =
  //           RequisitionSpecKt.EventGroupEntryKt.value {
  //             collectionInterval = timeInterval {
  //               startTime =
  //
  // LocalDate.now().minusDays(1).atStartOfDay().toInstant(ZoneOffset.UTC).toProtoTime()
  //               endTime = LocalDate.now().atStartOfDay().toInstant(ZoneOffset.UTC).toProtoTime()
  //             }
  //             filter = eventFilter {
  //               expression = "privacy_budget.age.value in [0] && privacy_budget.gender.value ==
  // 1"
  //             }
  //           }
  //       }
  //     }

  //     assertThat(getPrivacyBucketGroups(MEASUREMENT_SPEC, requisitionSpec))
  //       .containsExactly(
  //         PrivacyBucketGroup(
  //           "ACME",
  //           LocalDate.now(),
  //           LocalDate.now(),
  //           AgeGroup.RANGE_18_34,
  //           Gender.FEMALE,
  //           0.0f,
  //           0.01f
  //         ),
  //         PrivacyBucketGroup(
  //           "ACME",
  //           LocalDate.now().minusDays(1),
  //           LocalDate.now().minusDays(1),
  //           AgeGroup.RANGE_18_34,
  //           Gender.FEMALE,
  //           0.0f,
  //           0.01f
  //         ),
  //         PrivacyBucketGroup(
  //           "ACME",
  //           LocalDate.now(),
  //           LocalDate.now(),
  //           AgeGroup.RANGE_18_34,
  //           Gender.FEMALE,
  //           0.01f,
  //           0.01f
  //         ),
  //         PrivacyBucketGroup(
  //           "ACME",
  //           LocalDate.now().minusDays(1),
  //           LocalDate.now().minusDays(1),
  //           AgeGroup.RANGE_18_34,
  //           Gender.FEMALE,
  //           0.01f,
  //           0.01f
  //         ),
  //       )
  //   }
}
