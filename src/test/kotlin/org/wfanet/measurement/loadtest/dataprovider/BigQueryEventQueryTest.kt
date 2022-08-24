/**
 * Copyright 2022 The Cross-Media Measurement Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 * ```
 *      http://www.apache.org/licenses/LICENSE-2.0
 * ```
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package org.wfanet.measurement.loadtest.dataprovider

import com.google.common.truth.Truth.assertThat
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import java.util.logging.Logger
import org.junit.Rule
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
// import org.mockito.kotlin.any
// import org.wfanet.measurement.api.v2alpha.CertificatesGrpcKt
import org.wfanet.measurement.api.v2alpha.RequisitionSpecKt.eventFilter
import org.wfanet.measurement.api.v2alpha.event_templates.testing.TestBannerTemplate.Gender
import org.wfanet.measurement.api.v2alpha.event_templates.testing.TestBannerTemplateKt.gender
import org.wfanet.measurement.api.v2alpha.event_templates.testing.TestPrivacyBudgetTemplate.AgeRange
import org.wfanet.measurement.api.v2alpha.event_templates.testing.TestPrivacyBudgetTemplateKt
import org.wfanet.measurement.api.v2alpha.event_templates.testing.TestPrivacyBudgetTemplateKt.ageRange
import org.wfanet.measurement.api.v2alpha.event_templates.testing.testBannerTemplate
import org.wfanet.measurement.api.v2alpha.event_templates.testing.testEvent
import org.wfanet.measurement.api.v2alpha.event_templates.testing.testPrivacyBudgetTemplate
// import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
// import org.wfanet.measurement.common.grpc.testing.mockService

private val NONMATCHING_BANNER_GENDER = Gender.Value.GENDER_MALE
private val MATCHING_BANNER_GENDER = Gender.Value.GENDER_FEMALE
private val NONMATCHING_PRIVACY_AGE_RANGE = AgeRange.Value.AGE_18_TO_24
private val MATCHING_PRIVACY_AGE_RANGE = AgeRange.Value.AGE_35_TO_54
private val MATCHING_EVENT_FILTER = eventFilter {
  expression = "privacy_budget.age.value == 1 || banner_ad.gender.value == 2"
}

@RunWith(JUnit4::class)
class BigQueryEventQueryTest {
  // private val certificatesServiceMock: CertificatesGrpcKt.CertificatesCoroutineImplBase = mockService {
  //   onBlocking { getCertificate(any()) }.thenReturn(4)
  // }
  //
  // @get:Rule
  // val grpcTestServerRule = GrpcTestServerRule {
  //   addService(certificatesServiceMock)
  // }
  //
  // private val certificatesStub: CertificatesGrpcKt.CertificatesCoroutineStub by lazy {
  //   CertificatesGrpcKt.CertificatesCoroutineStub(grpcTestServerRule.channel)
  // }
  private val logger: Logger = Logger.getLogger(this::class.java.name)


  @Test
  fun `bigquery correctness test`() {
    val formatter = DateTimeFormatter.ofPattern("yyyy/MM/dd")
    // val date = LocalDate.parse("2022/07/05", formatter)
    when ( LocalDate.parse("2022/07/05", formatter)) {
      is LocalDate -> logger.info(LocalDate.parse("2022/07/01", formatter).toString())
      else -> logger.info("Failure case")
    }

    val nonMatchingEvents =
      (1..5).associateWith {
        listOf(
          testEvent {
            this.bannerAd = testBannerTemplate {
              gender = gender { value = NONMATCHING_BANNER_GENDER }
            }
            this.privacyBudget = testPrivacyBudgetTemplate {
              age = ageRange { value = NONMATCHING_PRIVACY_AGE_RANGE }
            }
          }
        )
      }
    val eventQuery = FilterEventQuery(nonMatchingEvents)
    val userVids = eventQuery.getUserVirtualIds(MATCHING_EVENT_FILTER)
    // logger.info(date.toString())
    assertThat(userVids.toList()).isNotEmpty()
  }

  @Test
  fun `filters when no matching conditions`() {
    val nonMatchingEvents =
      (1..5).associateWith {
        listOf(
          testEvent {
            this.bannerAd = testBannerTemplate {
              gender = gender { value = NONMATCHING_BANNER_GENDER }
            }
            this.privacyBudget = testPrivacyBudgetTemplate {
              age = ageRange { value = NONMATCHING_PRIVACY_AGE_RANGE }
            }
          }
        )
      }
    val eventQuery = FilterEventQuery(nonMatchingEvents)
    val userVids = eventQuery.getUserVirtualIds(MATCHING_EVENT_FILTER)
    assertThat(userVids.toList()).isEmpty()
  }

  @Test
  fun `filters matching conditions`() {
    val privacyTemplateMatchingVids = (1..10)
    val bannerTemplateMatchingVids = (6..10)
    val nonMatchingVids = (11..20)
    val matchingVids = privacyTemplateMatchingVids + bannerTemplateMatchingVids

    val nonMatchingEvents =
      nonMatchingVids.associateWith {
        testEvent {
          this.bannerAd = testBannerTemplate {
            gender = gender { value = NONMATCHING_BANNER_GENDER }
          }
          this.privacyBudget = testPrivacyBudgetTemplate {
            age = ageRange { value = NONMATCHING_PRIVACY_AGE_RANGE }
          }
        }
      }
    val privacyMatchingEvents =
      privacyTemplateMatchingVids.associateWith {
        testEvent {
          this.bannerAd = testBannerTemplate {
            gender = gender { value = NONMATCHING_BANNER_GENDER }
          }
          this.privacyBudget = testPrivacyBudgetTemplate {
            age = ageRange { value = MATCHING_PRIVACY_AGE_RANGE }
          }
        }
      }
    val bannerMatchingEvents =
      bannerTemplateMatchingVids.associateWith {
        testEvent {
          this.bannerAd = testBannerTemplate { gender = gender { value = MATCHING_BANNER_GENDER } }
          this.privacyBudget = testPrivacyBudgetTemplate {
            age = ageRange { value = NONMATCHING_PRIVACY_AGE_RANGE }
          }
        }
      }
    val allEvents =
      (1..20).associateWith {
        listOfNotNull(privacyMatchingEvents[it], bannerMatchingEvents[it], nonMatchingEvents[it])
      }

    val filterEventQuery = FilterEventQuery(allEvents)
    val userVids = filterEventQuery.getUserVirtualIds(MATCHING_EVENT_FILTER)
    val expectedVids = matchingVids.map { it.toLong() }
    assertThat(userVids.toList().sorted()).isEqualTo(expectedVids.sorted())
  }
}
