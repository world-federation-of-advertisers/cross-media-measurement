// Copyright 2024 The Cross-Media Measurement Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.wfanet.panelmatch.client.common

import com.google.common.truth.Truth.assertThat
import com.google.protobuf.stringValue
import java.time.LocalDate
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.api.v2alpha.DataProviderKey
import org.wfanet.measurement.api.v2alpha.ExchangeWorkflow as V2AlphaExchangeWorkflow
import org.wfanet.measurement.api.v2alpha.ExchangeWorkflowKt.StepKt as V2AlphaStepKt
import org.wfanet.measurement.api.v2alpha.ExchangeWorkflowKt.exchangeIdentifiers as v2AlphaExchangeIdentifiers
import org.wfanet.measurement.api.v2alpha.ExchangeWorkflowKt.schedule as v2AlphaSchedule
import org.wfanet.measurement.api.v2alpha.ExchangeWorkflowKt.step as v2AlphaStep
import org.wfanet.measurement.api.v2alpha.ModelProviderKey
import org.wfanet.measurement.api.v2alpha.copy
import org.wfanet.measurement.api.v2alpha.exchangeWorkflow as v2AlphaExchangeWorkflow
import org.wfanet.measurement.common.pack
import org.wfanet.measurement.common.toProtoDate
import org.wfanet.panelmatch.client.internal.ExchangeWorkflow
import org.wfanet.panelmatch.client.internal.ExchangeWorkflowKt.StepKt
import org.wfanet.panelmatch.client.internal.ExchangeWorkflowKt.exchangeIdentifiers
import org.wfanet.panelmatch.client.internal.ExchangeWorkflowKt.schedule
import org.wfanet.panelmatch.client.internal.ExchangeWorkflowKt.step
import org.wfanet.panelmatch.client.internal.copy
import org.wfanet.panelmatch.client.internal.exchangeWorkflow

@RunWith(JUnit4::class)
class ProtoConversionsTest {

  @Test
  fun toInternalConvertsMinimalWorkflow() {
    assertThat(V2ALPHA_MINIMAL_WORKFLOW.toInternal()).isEqualTo(INTERNAL_MINIMAL_WORKFLOW)
  }

  @Test
  fun toInternalConvertsWorkflowWithAllSteps() {
    assertThat(V2ALPHA_WORKFLOW_WITH_ALL_STEPS.toInternal())
      .isEqualTo(INTERNAL_WORKFLOW_WITH_ALL_STEPS)
  }

  @Test
  fun toInternalConvertsCommonStepFields() {
    assertThat(
        V2ALPHA_MINIMAL_WORKFLOW.copy {
            steps += v2AlphaStep {
              stepId = "step-01"
              party = V2AlphaExchangeWorkflow.Party.DATA_PROVIDER
              inputLabels.put("foo-key", "foo-value")
              inputLabels.put("bar-key", "bar-value")
              inputStep = V2AlphaStepKt.inputStep {}
            }
            steps += v2AlphaStep {
              stepId = "step-02"
              party = V2AlphaExchangeWorkflow.Party.MODEL_PROVIDER
              inputLabels.put("bar-key", "bar-value")
              inputLabels.put("baz-key", "baz-value")
              inputStep = V2AlphaStepKt.inputStep {}
            }
          }
          .toInternal()
      )
      .isEqualTo(
        INTERNAL_MINIMAL_WORKFLOW.copy {
          steps += step {
            stepId = "step-01"
            party = ExchangeWorkflow.Party.DATA_PROVIDER
            inputLabels.put("foo-key", "foo-value")
            inputLabels.put("bar-key", "bar-value")
            inputStep = StepKt.inputStep {}
          }
          steps += step {
            stepId = "step-02"
            party = ExchangeWorkflow.Party.MODEL_PROVIDER
            inputLabels.put("bar-key", "bar-value")
            inputLabels.put("baz-key", "baz-value")
            inputStep = StepKt.inputStep {}
          }
        }
      )
  }

  companion object {

    private val DATA_PROVIDER_KEY = DataProviderKey("some-data-provider")
    private val MODEL_PROVIDER_KEY = ModelProviderKey("some-model-provider")
    private val FIRST_EXCHANGE_DATE = LocalDate.parse("2024-01-01")
    private const val CRON_SCHEDULE = "@daily"

    private val V2ALPHA_MINIMAL_WORKFLOW = v2AlphaExchangeWorkflow {
      exchangeIdentifiers = v2AlphaExchangeIdentifiers {
        dataProvider = DATA_PROVIDER_KEY.toName()
        modelProvider = MODEL_PROVIDER_KEY.toName()
        sharedStorageOwner = V2AlphaExchangeWorkflow.Party.DATA_PROVIDER
        storage = V2AlphaExchangeWorkflow.StorageType.GOOGLE_CLOUD_STORAGE
      }
      firstExchangeDate = FIRST_EXCHANGE_DATE.toProtoDate()
      repetitionSchedule = v2AlphaSchedule { cronExpression = CRON_SCHEDULE }
    }

    private val INTERNAL_MINIMAL_WORKFLOW = exchangeWorkflow {
      exchangeIdentifiers = exchangeIdentifiers {
        dataProviderId = DATA_PROVIDER_KEY.dataProviderId
        modelProviderId = MODEL_PROVIDER_KEY.modelProviderId
        sharedStorageOwner = ExchangeWorkflow.Party.DATA_PROVIDER
        storage = ExchangeWorkflow.StorageType.GOOGLE_CLOUD_STORAGE
      }
      firstExchangeDate = FIRST_EXCHANGE_DATE.toProtoDate()
      repetitionSchedule = schedule { cronExpression = CRON_SCHEDULE }
    }

    private val V2ALPHA_WORKFLOW_WITH_ALL_STEPS =
      V2ALPHA_MINIMAL_WORKFLOW.copy {
        steps += v2AlphaStep {
          party = V2AlphaExchangeWorkflow.Party.DATA_PROVIDER
          copyFromSharedStorageStep =
            V2AlphaStepKt.copyFromSharedStorageStep {
              copyOptions =
                V2AlphaStepKt.copyOptions {
                  labelType = V2AlphaExchangeWorkflow.Step.CopyOptions.LabelType.BLOB
                }
            }
        }
        steps += v2AlphaStep {
          party = V2AlphaExchangeWorkflow.Party.DATA_PROVIDER
          copyToSharedStorageStep =
            V2AlphaStepKt.copyToSharedStorageStep {
              copyOptions =
                V2AlphaStepKt.copyOptions {
                  labelType = V2AlphaExchangeWorkflow.Step.CopyOptions.LabelType.MANIFEST
                }
            }
        }
        steps += v2AlphaStep {
          party = V2AlphaExchangeWorkflow.Party.DATA_PROVIDER
          intersectAndValidateStep =
            V2AlphaStepKt.intersectAndValidateStep {
              maxSize = 1000
              maximumNewItemsAllowed = 10
            }
        }
        steps += v2AlphaStep {
          party = V2AlphaExchangeWorkflow.Party.DATA_PROVIDER
          commutativeDeterministicEncryptStep = V2AlphaStepKt.commutativeDeterministicEncryptStep {}
        }
        steps += v2AlphaStep {
          party = V2AlphaExchangeWorkflow.Party.DATA_PROVIDER
          commutativeDeterministicReencryptStep =
            V2AlphaStepKt.commutativeDeterministicReEncryptStep {}
        }
        steps += v2AlphaStep {
          party = V2AlphaExchangeWorkflow.Party.DATA_PROVIDER
          commutativeDeterministicDecryptStep = V2AlphaStepKt.commutativeDeterministicDecryptStep {}
        }
        steps += v2AlphaStep {
          party = V2AlphaExchangeWorkflow.Party.DATA_PROVIDER
          inputStep = V2AlphaStepKt.inputStep {}
        }
        steps += v2AlphaStep {
          party = V2AlphaExchangeWorkflow.Party.DATA_PROVIDER
          generateCommutativeDeterministicKeyStep =
            V2AlphaStepKt.generateCommutativeDeterministicKeyStep {}
        }
        steps += v2AlphaStep {
          party = V2AlphaExchangeWorkflow.Party.DATA_PROVIDER
          generateSerializedRlweKeyPairStep =
            V2AlphaStepKt.generateSerializedRlweKeyPairStep {
              parameters = stringValue { value = "some-value" }.pack()
            }
        }
        steps += v2AlphaStep {
          party = V2AlphaExchangeWorkflow.Party.DATA_PROVIDER
          executePrivateMembershipQueriesStep =
            V2AlphaStepKt.executePrivateMembershipQueriesStep {
              parameters = stringValue { value = "some-value" }.pack()
              encryptedQueryResultFileCount = 100
              shardCount = 1024
              bucketsPerShard = 4
              maxQueriesPerShard = 4096
            }
        }
        steps += v2AlphaStep {
          party = V2AlphaExchangeWorkflow.Party.DATA_PROVIDER
          buildPrivateMembershipQueriesStep =
            V2AlphaStepKt.buildPrivateMembershipQueriesStep {
              parameters = stringValue { value = "some-value" }.pack()
              encryptedQueryBundleFileCount = 100
              queryIdToIdsFileCount = 1
              shardCount = 1024
              bucketsPerShard = 4
              queriesPerShard = 4096
              addPaddingQueries = true
            }
        }
        steps += v2AlphaStep {
          party = V2AlphaExchangeWorkflow.Party.DATA_PROVIDER
          decryptPrivateMembershipQueryResultsStep =
            V2AlphaStepKt.decryptPrivateMembershipQueryResultsStep {
              parameters = stringValue { value = "some-value" }.pack()
              decryptEventDataSetFileCount = 100
            }
        }
        steps += v2AlphaStep {
          party = V2AlphaExchangeWorkflow.Party.DATA_PROVIDER
          generateCertificateStep = V2AlphaStepKt.generateCertificateStep {}
        }
        steps += v2AlphaStep {
          party = V2AlphaExchangeWorkflow.Party.DATA_PROVIDER
          preprocessEventsStep = V2AlphaStepKt.preprocessEventsStep {}
        }
        steps += v2AlphaStep {
          party = V2AlphaExchangeWorkflow.Party.DATA_PROVIDER
          copyFromPreviousExchangeStep =
            V2AlphaStepKt.copyFromPreviousExchangeStep { previousBlobKey = "some-blob-key" }
        }
        steps += v2AlphaStep {
          party = V2AlphaExchangeWorkflow.Party.DATA_PROVIDER
          generateLookupKeysStep = V2AlphaStepKt.generateLookupKeysStep {}
        }
        steps += v2AlphaStep {
          party = V2AlphaExchangeWorkflow.Party.DATA_PROVIDER
          hybridEncryptStep = V2AlphaStepKt.hybridEncryptStep {}
        }
        steps += v2AlphaStep {
          party = V2AlphaExchangeWorkflow.Party.DATA_PROVIDER
          hybridDecryptStep = V2AlphaStepKt.hybridDecryptStep {}
        }
        steps += v2AlphaStep {
          party = V2AlphaExchangeWorkflow.Party.DATA_PROVIDER
          generateHybridEncryptionKeyPairStep = V2AlphaStepKt.generateHybridEncryptionKeyPairStep {}
        }
        steps += v2AlphaStep {
          party = V2AlphaExchangeWorkflow.Party.DATA_PROVIDER
          generateRandomBytesStep = V2AlphaStepKt.generateRandomBytesStep { byteCount = 32 }
        }
        steps += v2AlphaStep {
          party = V2AlphaExchangeWorkflow.Party.DATA_PROVIDER
          assignJoinKeyIdsStep = V2AlphaStepKt.assignJoinKeyIdsStep {}
        }
      }

    private val INTERNAL_WORKFLOW_WITH_ALL_STEPS =
      INTERNAL_MINIMAL_WORKFLOW.copy {
        steps += step {
          party = ExchangeWorkflow.Party.DATA_PROVIDER
          copyFromSharedStorageStep =
            StepKt.copyFromSharedStorageStep {
              copyOptions =
                StepKt.copyOptions { labelType = ExchangeWorkflow.Step.CopyOptions.LabelType.BLOB }
            }
        }
        steps += step {
          party = ExchangeWorkflow.Party.DATA_PROVIDER
          copyToSharedStorageStep =
            StepKt.copyToSharedStorageStep {
              copyOptions =
                StepKt.copyOptions {
                  labelType = ExchangeWorkflow.Step.CopyOptions.LabelType.MANIFEST
                }
            }
        }
        steps += step {
          party = ExchangeWorkflow.Party.DATA_PROVIDER
          intersectAndValidateStep =
            StepKt.intersectAndValidateStep {
              maxSize = 1000
              maximumNewItemsAllowed = 10
            }
        }
        steps += step {
          party = ExchangeWorkflow.Party.DATA_PROVIDER
          commutativeDeterministicEncryptStep = StepKt.commutativeDeterministicEncryptStep {}
        }
        steps += step {
          party = ExchangeWorkflow.Party.DATA_PROVIDER
          commutativeDeterministicReencryptStep = StepKt.commutativeDeterministicReEncryptStep {}
        }
        steps += step {
          party = ExchangeWorkflow.Party.DATA_PROVIDER
          commutativeDeterministicDecryptStep = StepKt.commutativeDeterministicDecryptStep {}
        }
        steps += step {
          party = ExchangeWorkflow.Party.DATA_PROVIDER
          inputStep = StepKt.inputStep {}
        }
        steps += step {
          party = ExchangeWorkflow.Party.DATA_PROVIDER
          generateCommutativeDeterministicKeyStep =
            StepKt.generateCommutativeDeterministicKeyStep {}
        }
        steps += step {
          party = ExchangeWorkflow.Party.DATA_PROVIDER
          generateSerializedRlweKeyPairStep =
            StepKt.generateSerializedRlweKeyPairStep {
              parameters = stringValue { value = "some-value" }.pack()
            }
        }
        steps += step {
          party = ExchangeWorkflow.Party.DATA_PROVIDER
          executePrivateMembershipQueriesStep =
            StepKt.executePrivateMembershipQueriesStep {
              parameters = stringValue { value = "some-value" }.pack()
              encryptedQueryResultFileCount = 100
              shardCount = 1024
              bucketsPerShard = 4
              maxQueriesPerShard = 4096
            }
        }
        steps += step {
          party = ExchangeWorkflow.Party.DATA_PROVIDER
          buildPrivateMembershipQueriesStep =
            StepKt.buildPrivateMembershipQueriesStep {
              parameters = stringValue { value = "some-value" }.pack()
              encryptedQueryBundleFileCount = 100
              queryIdToIdsFileCount = 1
              shardCount = 1024
              bucketsPerShard = 4
              queriesPerShard = 4096
              addPaddingQueries = true
            }
        }
        steps += step {
          party = ExchangeWorkflow.Party.DATA_PROVIDER
          decryptPrivateMembershipQueryResultsStep =
            StepKt.decryptPrivateMembershipQueryResultsStep {
              parameters = stringValue { value = "some-value" }.pack()
              decryptEventDataSetFileCount = 100
            }
        }
        steps += step {
          party = ExchangeWorkflow.Party.DATA_PROVIDER
          generateCertificateStep = StepKt.generateCertificateStep {}
        }
        steps += step {
          party = ExchangeWorkflow.Party.DATA_PROVIDER
          preprocessEventsStep = StepKt.preprocessEventsStep {}
        }
        steps += step {
          party = ExchangeWorkflow.Party.DATA_PROVIDER
          copyFromPreviousExchangeStep =
            StepKt.copyFromPreviousExchangeStep { previousBlobKey = "some-blob-key" }
        }
        steps += step {
          party = ExchangeWorkflow.Party.DATA_PROVIDER
          generateLookupKeysStep = StepKt.generateLookupKeysStep {}
        }
        steps += step {
          party = ExchangeWorkflow.Party.DATA_PROVIDER
          hybridEncryptStep = StepKt.hybridEncryptStep {}
        }
        steps += step {
          party = ExchangeWorkflow.Party.DATA_PROVIDER
          hybridDecryptStep = StepKt.hybridDecryptStep {}
        }
        steps += step {
          party = ExchangeWorkflow.Party.DATA_PROVIDER
          generateHybridEncryptionKeyPairStep = StepKt.generateHybridEncryptionKeyPairStep {}
        }
        steps += step {
          party = ExchangeWorkflow.Party.DATA_PROVIDER
          generateRandomBytesStep = StepKt.generateRandomBytesStep { byteCount = 32 }
        }
        steps += step {
          party = ExchangeWorkflow.Party.DATA_PROVIDER
          assignJoinKeyIdsStep = StepKt.assignJoinKeyIdsStep {}
        }
      }
  }
}
