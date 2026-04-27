/*
 * Copyright 2026 The Cross-Media Measurement Authors
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
import org.wfanet.measurement.internal.kingdom.dataProviderCapabilities as internalDataProviderCapabilities

@RunWith(JUnit4::class)
class SelectNoiseMechanismsTest {

  @Test
  fun `throws when capabilities list is empty`() {
    val serverMechanisms =
      listOf(InternalNoiseMechanism.NONE, InternalNoiseMechanism.CONTINUOUS_GAUSSIAN)

    assertFailsWith<IllegalArgumentException> {
      MeasurementsService.selectNoiseMechanisms(serverMechanisms, emptyList())
    }
  }

  @Test
  fun `throws when server noise mechanisms list is empty`() {
    val capabilities =
      listOf(internalDataProviderCapabilities { noiseMechanismNoneSupported = true })

    assertFailsWith<IllegalArgumentException> {
      MeasurementsService.selectNoiseMechanisms(emptyList(), capabilities)
    }
  }

  @Test
  fun `EDP with default capabilities supports only CONTINUOUS_GAUSSIAN`() {
    val serverMechanisms =
      listOf(InternalNoiseMechanism.NONE, InternalNoiseMechanism.CONTINUOUS_GAUSSIAN)
    val capabilities = listOf(internalDataProviderCapabilities {})

    val result = MeasurementsService.selectNoiseMechanisms(serverMechanisms, capabilities)
    assertEquals(listOf(InternalNoiseMechanism.CONTINUOUS_GAUSSIAN), result)
  }

  @Test
  fun `EDP with noise_mechanism_none_supported returns both NONE and CONTINUOUS_GAUSSIAN`() {
    val serverMechanisms =
      listOf(InternalNoiseMechanism.NONE, InternalNoiseMechanism.CONTINUOUS_GAUSSIAN)
    val capabilities =
      listOf(
        internalDataProviderCapabilities {
          noiseMechanismNoneSupported = true
          isPanelProjection = true
        }
      )

    val result = MeasurementsService.selectNoiseMechanisms(serverMechanisms, capabilities)
    assertEquals(
      setOf(InternalNoiseMechanism.NONE, InternalNoiseMechanism.CONTINUOUS_GAUSSIAN),
      result.toSet(),
    )
  }

  @Test
  fun `intersects across multiple EDPs where one does not support NONE`() {
    val serverMechanisms =
      listOf(InternalNoiseMechanism.NONE, InternalNoiseMechanism.CONTINUOUS_GAUSSIAN)
    val capabilities =
      listOf(
        internalDataProviderCapabilities { noiseMechanismNoneSupported = true },
        internalDataProviderCapabilities {},
      )

    val result = MeasurementsService.selectNoiseMechanisms(serverMechanisms, capabilities)
    assertEquals(listOf(InternalNoiseMechanism.CONTINUOUS_GAUSSIAN), result)
  }

  @Test
  fun `intersects across three EDPs to CONTINUOUS_GAUSSIAN only`() {
    val serverMechanisms =
      listOf(InternalNoiseMechanism.NONE, InternalNoiseMechanism.CONTINUOUS_GAUSSIAN)
    val capabilities =
      listOf(
        internalDataProviderCapabilities { noiseMechanismNoneSupported = true },
        internalDataProviderCapabilities {},
        internalDataProviderCapabilities { noiseMechanismNoneSupported = true },
      )

    val result = MeasurementsService.selectNoiseMechanisms(serverMechanisms, capabilities)
    assertEquals(listOf(InternalNoiseMechanism.CONTINUOUS_GAUSSIAN), result)
  }

  @Test
  fun `all EDPs support NONE and server supports both`() {
    val serverMechanisms =
      listOf(InternalNoiseMechanism.NONE, InternalNoiseMechanism.CONTINUOUS_GAUSSIAN)
    val capabilities =
      listOf(
        internalDataProviderCapabilities {
          noiseMechanismNoneSupported = true
          isPanelProjection = true
        },
        internalDataProviderCapabilities {
          noiseMechanismNoneSupported = true
          isPanelProjection = true
        },
      )

    val result = MeasurementsService.selectNoiseMechanisms(serverMechanisms, capabilities)
    assertEquals(
      setOf(InternalNoiseMechanism.NONE, InternalNoiseMechanism.CONTINUOUS_GAUSSIAN),
      result.toSet(),
    )
  }

  @Test
  fun `server has single mechanism that all EDPs support`() {
    val serverMechanisms = listOf(InternalNoiseMechanism.CONTINUOUS_GAUSSIAN)
    val capabilities =
      listOf(
        internalDataProviderCapabilities { noiseMechanismNoneSupported = true },
        internalDataProviderCapabilities {},
      )

    val result = MeasurementsService.selectNoiseMechanisms(serverMechanisms, capabilities)
    assertEquals(listOf(InternalNoiseMechanism.CONTINUOUS_GAUSSIAN), result)
  }

  @Test
  fun `throws when server only supports NONE but EDP does not`() {
    val serverMechanisms = listOf(InternalNoiseMechanism.NONE)
    val capabilities = listOf(internalDataProviderCapabilities {})

    val exception =
      assertFailsWith<StatusRuntimeException> {
        MeasurementsService.selectNoiseMechanisms(serverMechanisms, capabilities)
      }
    assertEquals(Status.Code.INVALID_ARGUMENT, exception.status.code)
  }

  @Test
  fun `server only supports NONE and panel-projection EDP supports NONE`() {
    val serverMechanisms = listOf(InternalNoiseMechanism.NONE)
    val capabilities =
      listOf(
        internalDataProviderCapabilities {
          noiseMechanismNoneSupported = true
          isPanelProjection = true
        }
      )

    val result = MeasurementsService.selectNoiseMechanisms(serverMechanisms, capabilities)
    assertEquals(listOf(InternalNoiseMechanism.NONE), result)
  }

  @Test
  fun `default threshold excludes NONE when any EDP is not panel-projection`() {
    val serverMechanisms =
      listOf(InternalNoiseMechanism.NONE, InternalNoiseMechanism.CONTINUOUS_GAUSSIAN)
    val capabilities =
      listOf(
        internalDataProviderCapabilities {
          noiseMechanismNoneSupported = true
          isPanelProjection = true
        },
        internalDataProviderCapabilities {
          noiseMechanismNoneSupported = true
          isPanelProjection = false
        },
      )

    val result = MeasurementsService.selectNoiseMechanisms(serverMechanisms, capabilities)

    assertEquals(listOf(InternalNoiseMechanism.CONTINUOUS_GAUSSIAN), result)
  }

  @Test
  fun `large threshold preserves NONE regardless of panel-projection composition`() {
    val serverMechanisms =
      listOf(InternalNoiseMechanism.NONE, InternalNoiseMechanism.CONTINUOUS_GAUSSIAN)
    val capabilities =
      listOf(
        internalDataProviderCapabilities {
          noiseMechanismNoneSupported = true
          isPanelProjection = false
        },
        internalDataProviderCapabilities {
          noiseMechanismNoneSupported = true
          isPanelProjection = false
        },
      )

    val result =
      MeasurementsService.selectNoiseMechanisms(
        serverMechanisms,
        capabilities,
        maxNonPanelProjectionEdpsForNoneNoise = Int.MAX_VALUE,
      )

    assertEquals(
      setOf(InternalNoiseMechanism.NONE, InternalNoiseMechanism.CONTINUOUS_GAUSSIAN),
      result.toSet(),
    )
  }

  @Test
  fun `negative threshold throws`() {
    val serverMechanisms =
      listOf(InternalNoiseMechanism.NONE, InternalNoiseMechanism.CONTINUOUS_GAUSSIAN)
    val capabilities =
      listOf(
        internalDataProviderCapabilities {
          noiseMechanismNoneSupported = true
          isPanelProjection = true
        }
      )

    assertFailsWith<IllegalArgumentException> {
      MeasurementsService.selectNoiseMechanisms(
        serverMechanisms,
        capabilities,
        maxNonPanelProjectionEdpsForNoneNoise = -1,
      )
    }
  }

  @Test
  fun `threshold of 0 excludes NONE when any EDP is not panel-projection`() {
    val serverMechanisms =
      listOf(InternalNoiseMechanism.NONE, InternalNoiseMechanism.CONTINUOUS_GAUSSIAN)
    val capabilities =
      listOf(
        internalDataProviderCapabilities {
          noiseMechanismNoneSupported = true
          isPanelProjection = true
        },
        internalDataProviderCapabilities {
          noiseMechanismNoneSupported = true
          isPanelProjection = false
        },
      )

    val result =
      MeasurementsService.selectNoiseMechanisms(
        serverMechanisms,
        capabilities,
        maxNonPanelProjectionEdpsForNoneNoise = 0,
      )

    assertEquals(listOf(InternalNoiseMechanism.CONTINUOUS_GAUSSIAN), result)
  }

  @Test
  fun `threshold of 0 keeps NONE when all EDPs are panel-projection`() {
    val serverMechanisms =
      listOf(InternalNoiseMechanism.NONE, InternalNoiseMechanism.CONTINUOUS_GAUSSIAN)
    val capabilities =
      listOf(
        internalDataProviderCapabilities {
          noiseMechanismNoneSupported = true
          isPanelProjection = true
        },
        internalDataProviderCapabilities {
          noiseMechanismNoneSupported = true
          isPanelProjection = true
        },
      )

    val result =
      MeasurementsService.selectNoiseMechanisms(
        serverMechanisms,
        capabilities,
        maxNonPanelProjectionEdpsForNoneNoise = 0,
      )

    assertEquals(
      setOf(InternalNoiseMechanism.NONE, InternalNoiseMechanism.CONTINUOUS_GAUSSIAN),
      result.toSet(),
    )
  }

  @Test
  fun `threshold of 1 keeps NONE with exactly one non-panel-projection EDP`() {
    val serverMechanisms =
      listOf(InternalNoiseMechanism.NONE, InternalNoiseMechanism.CONTINUOUS_GAUSSIAN)
    val capabilities =
      listOf(
        internalDataProviderCapabilities {
          noiseMechanismNoneSupported = true
          isPanelProjection = true
        },
        internalDataProviderCapabilities {
          noiseMechanismNoneSupported = true
          isPanelProjection = false
        },
      )

    val result =
      MeasurementsService.selectNoiseMechanisms(
        serverMechanisms,
        capabilities,
        maxNonPanelProjectionEdpsForNoneNoise = 1,
      )

    assertEquals(
      setOf(InternalNoiseMechanism.NONE, InternalNoiseMechanism.CONTINUOUS_GAUSSIAN),
      result.toSet(),
    )
  }

  @Test
  fun `threshold of 1 excludes NONE with two non-panel-projection EDPs`() {
    val serverMechanisms =
      listOf(InternalNoiseMechanism.NONE, InternalNoiseMechanism.CONTINUOUS_GAUSSIAN)
    val capabilities =
      listOf(
        internalDataProviderCapabilities {
          noiseMechanismNoneSupported = true
          isPanelProjection = false
        },
        internalDataProviderCapabilities {
          noiseMechanismNoneSupported = true
          isPanelProjection = false
        },
      )

    val result =
      MeasurementsService.selectNoiseMechanisms(
        serverMechanisms,
        capabilities,
        maxNonPanelProjectionEdpsForNoneNoise = 1,
      )

    assertEquals(listOf(InternalNoiseMechanism.CONTINUOUS_GAUSSIAN), result)
  }

  @Test
  fun `threshold throws when only NONE is available and policy excludes it`() {
    val serverMechanisms = listOf(InternalNoiseMechanism.NONE)
    val capabilities =
      listOf(
        internalDataProviderCapabilities {
          noiseMechanismNoneSupported = true
          isPanelProjection = false
        }
      )

    val exception =
      assertFailsWith<StatusRuntimeException> {
        MeasurementsService.selectNoiseMechanisms(
          serverMechanisms,
          capabilities,
          maxNonPanelProjectionEdpsForNoneNoise = 0,
        )
      }
    assertEquals(Status.Code.INVALID_ARGUMENT, exception.status.code)
  }

  @Test
  fun `threshold does not affect CONTINUOUS_GAUSSIAN-only selection`() {
    val serverMechanisms = listOf(InternalNoiseMechanism.CONTINUOUS_GAUSSIAN)
    val capabilities =
      listOf(
        internalDataProviderCapabilities { isPanelProjection = false },
        internalDataProviderCapabilities { isPanelProjection = false },
      )

    val result =
      MeasurementsService.selectNoiseMechanisms(
        serverMechanisms,
        capabilities,
        maxNonPanelProjectionEdpsForNoneNoise = 0,
      )

    assertEquals(listOf(InternalNoiseMechanism.CONTINUOUS_GAUSSIAN), result)
  }
}
