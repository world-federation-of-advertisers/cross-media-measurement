package org.wfanet.measurement.eventdataprovider.privacybudgetmanagement

import com.google.common.truth.Truth.assertThat
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import kotlin.test.assertFailsWith
import kotlin.test.assertEquals
import org.wfanet.measurement.api.v2alpha.MeasurementSpec
import org.wfanet.measurement.api.v2alpha.RequisitionSpec
import org.wfanet.measurement.api.v2alpha.measurementSpec
import org.wfanet.measurement.api.v2alpha.requisitionSpec
import org.wfanet.measurement.eventdataprovider.privacybudgetmanagement.privacyBucketGroups

@RunWith(JUnit4::class)
class PrivacyBucketMapperTest {
  @Test
  fun `Mapper Fails for wrong MeasurementSpec`() {
    assertFailsWith(PrivacyBudgetManagementInternalException::class) { privacyBucketGroups(measurementSpec{}, requisitionSpec{}) }
  }
}
