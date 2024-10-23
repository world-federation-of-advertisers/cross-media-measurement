/*
 * Copyright 2023 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.reporting.service.api.v2alpha

/** [Exception] which indicates an error that noise mechanism is not specified. */
class NoiseMechanismUnspecifiedException(message: String? = null, cause: Throwable? = null) :
  Exception(message, cause)

/** [Exception] which indicates an error that noise mechanism is unrecognized. */
class NoiseMechanismUnrecognizedException(message: String? = null, cause: Throwable? = null) :
  Exception(message, cause)

/** [Exception] which indicates an error that measurement variance is not computable. */
class MeasurementVarianceNotComputableException(message: String? = null, cause: Throwable? = null) :
  Exception(message, cause)

/** [Exception] which indicates an error that metric result is not computable. */
class MetricResultNotComputableException(message: String? = null, cause: Throwable? = null) :
  Exception(message, cause)
