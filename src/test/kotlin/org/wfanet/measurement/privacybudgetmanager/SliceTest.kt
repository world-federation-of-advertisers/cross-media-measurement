/*
 * Copyright 2025 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.privacybudgetmanager

import com.google.common.truth.Truth.assertThat
import java.time.LocalDate
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4

@RunWith(JUnit4::class)
class SliceTest {
  @Test
  fun `Slice successfully merges same row keys`() {
    val slice = Slice()
    val rowKey = LedgerRowKey("edp", "mc1", "eg1", LocalDate.of(2024, 1, 1))

    val charges1 = charges {
      populationIndexToCharges[123] =
        ChargesKt.intervalCharges {
          vidIntervalIndexToCharges[0] = acdpCharge {
            rho = 1.23f
            theta = 4.56f
          }
          vidIntervalIndexToCharges[1] = acdpCharge {
            rho = 7.89f
            theta = 10.11f
          }
        }
      populationIndexToCharges[456] =
        ChargesKt.intervalCharges {
          vidIntervalIndexToCharges[5] = acdpCharge {
            rho = 12.13f
            theta = 14.15f
          }
        }
    }

    val charges2 = charges {
      populationIndexToCharges[0] =
        ChargesKt.intervalCharges {
          vidIntervalIndexToCharges[10] = acdpCharge {
            rho = 2.3f
            theta = 5.5f
          }
        }
      populationIndexToCharges[123] =
        ChargesKt.intervalCharges {
          vidIntervalIndexToCharges[1] = acdpCharge {
            rho = 10.0f
            theta = 10.0f
          }
        }
      populationIndexToCharges[456] =
        ChargesKt.intervalCharges {
          vidIntervalIndexToCharges[5] = acdpCharge {
            rho = 12.13f
            theta = 14.15f
          }
        }
    }

    slice.merge(rowKey, charges2)
    slice.merge(rowKey, charges1)
    val returnedCharges = slice.get(rowKey)

    val expectedCharges = charges {
      populationIndexToCharges[0] =
        ChargesKt.intervalCharges {
          vidIntervalIndexToCharges[10] = acdpCharge {
            rho = 2.3f
            theta = 5.5f
          }
        }
      populationIndexToCharges[123] =
        ChargesKt.intervalCharges {
          vidIntervalIndexToCharges[0] = acdpCharge {
            rho = 1.23f
            theta = 4.56f
          }
          vidIntervalIndexToCharges[1] = acdpCharge {
            rho = 17.89f
            theta = 20.11f
          }
        }
      populationIndexToCharges[456] =
        ChargesKt.intervalCharges {
          vidIntervalIndexToCharges[5] = acdpCharge {
            rho = 24.26f
            theta = 28.3f
          }
        }
    }

    assertThat(returnedCharges).isEqualTo(expectedCharges)
  }
}
