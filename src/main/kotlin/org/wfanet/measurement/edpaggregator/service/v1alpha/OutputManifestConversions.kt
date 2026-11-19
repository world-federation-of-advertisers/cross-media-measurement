// Copyright 2025 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.edpaggregator.service.v1alpha

import org.wfanet.measurement.edpaggregator.v1alpha.OutputBlobReference
import org.wfanet.measurement.edpaggregator.v1alpha.outputBlobReference
import org.wfanet.measurement.internal.edpaggregator.OutputBlobReference as InternalOutputBlobReference
import org.wfanet.measurement.internal.edpaggregator.outputBlobReference as internalOutputBlobReference

/** Converts an immutable output version without resolving storage. */
fun OutputBlobReference.toInternal(): InternalOutputBlobReference = internalOutputBlobReference {
  blobUri = this@toInternal.blobUri
  blobGeneration = this@toInternal.blobGeneration
}

/** Converts an immutable output version without resolving storage. */
fun InternalOutputBlobReference.toPublic(): OutputBlobReference = outputBlobReference {
  blobUri = this@toPublic.blobUri
  blobGeneration = this@toPublic.blobGeneration
}
