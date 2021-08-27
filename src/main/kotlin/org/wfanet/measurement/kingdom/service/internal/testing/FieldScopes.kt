package org.wfanet.measurement.kingdom.service.internal.testing

import com.google.common.truth.extensions.proto.FieldScope
import com.google.common.truth.extensions.proto.FieldScopes
import org.wfanet.measurement.internal.kingdom.ExchangeStep
import org.wfanet.measurement.internal.kingdom.ExchangeStepAttemptDetails

internal val EXCHANGE_STEP_RESPONSE_IGNORED_FIELDS: FieldScope =
  FieldScopes.allowingFieldDescriptors(ExchangeStep.getDescriptor().findFieldByName("update_time"))

internal val EXCHANGE_STEP_ATTEMPT_RESPONSE_IGNORED_FIELDS: FieldScope =
  FieldScopes.allowingFieldDescriptors(
    ExchangeStepAttemptDetails.getDescriptor().findFieldByName("start_time"),
    ExchangeStepAttemptDetails.getDescriptor().findFieldByName("update_time"),
    ExchangeStepAttemptDetails.DebugLog.getDescriptor().findFieldByName("time")
  )
