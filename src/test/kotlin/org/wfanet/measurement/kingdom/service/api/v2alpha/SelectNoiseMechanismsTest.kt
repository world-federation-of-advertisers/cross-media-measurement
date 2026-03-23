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

package org.wfanet.measurement.kingdom.service.api.v2alpha

import io.grpc.Status
import io.grpc.StatusRuntimeException
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.internal.kingdom.ProtocolConfig.NoiseMechanism as InternalNoiseMechanism
import org.wfanet.measurement.internal.kingdom.dataProviderRequirements as internalDataProviderRequirements

@RunWith(JUnit4::class)
class SelectNoiseMechanismsTest {

  @Test
  fun `EDPs with empty allowed lists default to CONTINUOUS_GAUSSIAN`() {
    val serverMechanisms =
      listOf(InternalNoiseMechanism.NONE, InternalNoiseMechanism.CONTINUOUS_GAUSSIAN)
    val requirements =
      listOf(
        internalDataProviderRequirements {},
        internalDataProviderRequirements {},
      )

    val result = MeasurementsService.selectNoiseMechanisms(serverMechanisms, requirements)
    assertEquals(listOf(InternalNoiseMechanism.CONTINUOUS_GAUSSIAN), result)
  }

  @Test
  fun `returns server mechanisms when requirements list is empty`() {
    val serverMechanisms =
      listOf(
        InternalNoiseMechanism.NONE,
        InternalNoiseMechanism.CONTINUOUS_LAPLACE,
        InternalNoiseMechanism.CONTINUOUS_GAUSSIAN,
      )

    val result = MeasurementsService.selectNoiseMechanisms(serverMechanisms, emptyList())
    assertEquals(serverMechanisms.toSet(), result.toSet())
  }

  @Test
  fun `intersects server mechanisms with single EDP requirements`() {
    val serverMechanisms =
      listOf(
        InternalNoiseMechanism.NONE,
        InternalNoiseMechanism.CONTINUOUS_LAPLACE,
        InternalNoiseMechanism.CONTINUOUS_GAUSSIAN,
      )
    val requirements =
      listOf(
        internalDataProviderRequirements {
          allowedNoiseMechanisms += InternalNoiseMechanism.NONE
          allowedNoiseMechanisms += InternalNoiseMechanism.CONTINUOUS_GAUSSIAN
        }
      )

    val result = MeasurementsService.selectNoiseMechanisms(serverMechanisms, requirements)
    assertEquals(
      setOf(InternalNoiseMechanism.NONE, InternalNoiseMechanism.CONTINUOUS_GAUSSIAN),
      result.toSet(),
    )
  }

  @Test
  fun `intersects across multiple EDPs`() {
    val serverMechanisms =
      listOf(
        InternalNoiseMechanism.NONE,
        InternalNoiseMechanism.CONTINUOUS_LAPLACE,
        InternalNoiseMechanism.CONTINUOUS_GAUSSIAN,
      )
    val requirements =
      listOf(
        internalDataProviderRequirements {
          allowedNoiseMechanisms += InternalNoiseMechanism.NONE
          allowedNoiseMechanisms += InternalNoiseMechanism.CONTINUOUS_GAUSSIAN
        },
        internalDataProviderRequirements {
          allowedNoiseMechanisms += InternalNoiseMechanism.CONTINUOUS_GAUSSIAN
        },
      )

    val result = MeasurementsService.selectNoiseMechanisms(serverMechanisms, requirements)
    assertEquals(listOf(InternalNoiseMechanism.CONTINUOUS_GAUSSIAN), result)
  }

  @Test
  fun `intersects across three EDPs to single common mechanism`() {
    val serverMechanisms =
      listOf(
        InternalNoiseMechanism.NONE,
        InternalNoiseMechanism.CONTINUOUS_LAPLACE,
        InternalNoiseMechanism.CONTINUOUS_GAUSSIAN,
      )
    val requirements =
      listOf(
        internalDataProviderRequirements {
          allowedNoiseMechanisms += InternalNoiseMechanism.NONE
          allowedNoiseMechanisms += InternalNoiseMechanism.CONTINUOUS_GAUSSIAN
        },
        internalDataProviderRequirements {
          allowedNoiseMechanisms += InternalNoiseMechanism.CONTINUOUS_LAPLACE
          allowedNoiseMechanisms += InternalNoiseMechanism.CONTINUOUS_GAUSSIAN
        },
        internalDataProviderRequirements {
          allowedNoiseMechanisms += InternalNoiseMechanism.CONTINUOUS_GAUSSIAN
        },
      )

    val result = MeasurementsService.selectNoiseMechanisms(serverMechanisms, requirements)
    assertEquals(listOf(InternalNoiseMechanism.CONTINUOUS_GAUSSIAN), result)
  }

  @Test
  fun `empty list defaults to CONTINUOUS_GAUSSIAN and intersects with explicit EDP`() {
    val serverMechanisms =
      listOf(InternalNoiseMechanism.NONE, InternalNoiseMechanism.CONTINUOUS_GAUSSIAN)
    val requirements =
      listOf(
        internalDataProviderRequirements {
          allowedNoiseMechanisms += InternalNoiseMechanism.NONE
          allowedNoiseMechanisms += InternalNoiseMechanism.CONTINUOUS_GAUSSIAN
        },
        internalDataProviderRequirements {},
      )

    val result = MeasurementsService.selectNoiseMechanisms(serverMechanisms, requirements)
    assertEquals(listOf(InternalNoiseMechanism.CONTINUOUS_GAUSSIAN), result)
  }

  @Test
  fun `all EDPs with empty lists narrows to CONTINUOUS_GAUSSIAN`() {
    val serverMechanisms =
      listOf(
        InternalNoiseMechanism.NONE,
        InternalNoiseMechanism.CONTINUOUS_LAPLACE,
        InternalNoiseMechanism.CONTINUOUS_GAUSSIAN,
      )
    val requirements =
      listOf(
        internalDataProviderRequirements {},
        internalDataProviderRequirements {},
        internalDataProviderRequirements {},
      )

    val result = MeasurementsService.selectNoiseMechanisms(serverMechanisms, requirements)
    assertEquals(listOf(InternalNoiseMechanism.CONTINUOUS_GAUSSIAN), result)
  }

  @Test
  fun `server has single mechanism that all EDPs support`() {
    val serverMechanisms = listOf(InternalNoiseMechanism.CONTINUOUS_GAUSSIAN)
    val requirements =
      listOf(
        internalDataProviderRequirements {
          allowedNoiseMechanisms += InternalNoiseMechanism.NONE
          allowedNoiseMechanisms += InternalNoiseMechanism.CONTINUOUS_GAUSSIAN
        },
        internalDataProviderRequirements {
          allowedNoiseMechanisms += InternalNoiseMechanism.CONTINUOUS_GAUSSIAN
        },
      )

    val result = MeasurementsService.selectNoiseMechanisms(serverMechanisms, requirements)
    assertEquals(listOf(InternalNoiseMechanism.CONTINUOUS_GAUSSIAN), result)
  }

  @Test
  fun `throws when intersection is empty`() {
    val serverMechanisms = listOf(InternalNoiseMechanism.CONTINUOUS_LAPLACE)
    val requirements =
      listOf(
        internalDataProviderRequirements {
          allowedNoiseMechanisms += InternalNoiseMechanism.CONTINUOUS_GAUSSIAN
        }
      )

    val exception =
      assertFailsWith<StatusRuntimeException> {
        MeasurementsService.selectNoiseMechanisms(serverMechanisms, requirements)
      }
    assertEquals(Status.Code.INVALID_ARGUMENT, exception.status.code)
  }

  @Test
  fun `throws when two EDPs have disjoint requirements`() {
    val serverMechanisms =
      listOf(
        InternalNoiseMechanism.NONE,
        InternalNoiseMechanism.CONTINUOUS_LAPLACE,
        InternalNoiseMechanism.CONTINUOUS_GAUSSIAN,
      )
    val requirements =
      listOf(
        internalDataProviderRequirements {
          allowedNoiseMechanisms += InternalNoiseMechanism.NONE
        },
        internalDataProviderRequirements {
          allowedNoiseMechanisms += InternalNoiseMechanism.CONTINUOUS_GAUSSIAN
        },
      )

    assertFailsWith<StatusRuntimeException> {
      MeasurementsService.selectNoiseMechanisms(serverMechanisms, requirements)
    }
  }

  @Test
  fun `throws when EDP requirement not in server mechanisms`() {
    val serverMechanisms = listOf(InternalNoiseMechanism.NONE)
    val requirements =
      listOf(
        internalDataProviderRequirements {
          allowedNoiseMechanisms += InternalNoiseMechanism.CONTINUOUS_GAUSSIAN
        }
      )

    assertFailsWith<StatusRuntimeException> {
      MeasurementsService.selectNoiseMechanisms(serverMechanisms, requirements)
    }
  }

  @Test
  fun `throws when empty list EDP defaults to CG but server only has NONE`() {
    val serverMechanisms = listOf(InternalNoiseMechanism.NONE)
    val requirements = listOf(internalDataProviderRequirements {})

    assertFailsWith<StatusRuntimeException> {
      MeasurementsService.selectNoiseMechanisms(serverMechanisms, requirements)
    }
  }

  @Test
  fun `throws when empty list EDP conflicts with NONE-only explicit EDP`() {
    val serverMechanisms =
      listOf(InternalNoiseMechanism.NONE, InternalNoiseMechanism.CONTINUOUS_GAUSSIAN)
    val requirements =
      listOf(
        internalDataProviderRequirements {
          allowedNoiseMechanisms += InternalNoiseMechanism.NONE
        },
        internalDataProviderRequirements {},
      )

    assertFailsWith<StatusRuntimeException> {
      MeasurementsService.selectNoiseMechanisms(serverMechanisms, requirements)
    }
  }

  @Test
  fun `returns multiple mechanisms when multiple are common`() {
    val serverMechanisms =
      listOf(
        InternalNoiseMechanism.NONE,
        InternalNoiseMechanism.CONTINUOUS_LAPLACE,
        InternalNoiseMechanism.CONTINUOUS_GAUSSIAN,
      )
    val requirements =
      listOf(
        internalDataProviderRequirements {
          allowedNoiseMechanisms += InternalNoiseMechanism.NONE
          allowedNoiseMechanisms += InternalNoiseMechanism.CONTINUOUS_LAPLACE
          allowedNoiseMechanisms += InternalNoiseMechanism.CONTINUOUS_GAUSSIAN
        }
      )

    val result = MeasurementsService.selectNoiseMechanisms(serverMechanisms, requirements)
    assertEquals(
      setOf(
        InternalNoiseMechanism.NONE,
        InternalNoiseMechanism.CONTINUOUS_LAPLACE,
        InternalNoiseMechanism.CONTINUOUS_GAUSSIAN,
      ),
      result.toSet(),
    )
  }

  @Test
  fun `mixed empty and restrictive requirements narrows to CONTINUOUS_GAUSSIAN`() {
    val serverMechanisms =
      listOf(
        InternalNoiseMechanism.NONE,
        InternalNoiseMechanism.CONTINUOUS_LAPLACE,
        InternalNoiseMechanism.CONTINUOUS_GAUSSIAN,
      )
    val requirements =
      listOf(
        internalDataProviderRequirements {},
        internalDataProviderRequirements {
          allowedNoiseMechanisms += InternalNoiseMechanism.NONE
          allowedNoiseMechanisms += InternalNoiseMechanism.CONTINUOUS_GAUSSIAN
        },
        internalDataProviderRequirements {},
      )

    val result = MeasurementsService.selectNoiseMechanisms(serverMechanisms, requirements)
    assertEquals(listOf(InternalNoiseMechanism.CONTINUOUS_GAUSSIAN), result)
  }
}
