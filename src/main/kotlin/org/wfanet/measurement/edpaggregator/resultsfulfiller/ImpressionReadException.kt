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

package org.wfanet.measurement.edpaggregator.resultsfulfiller

/**
 * Exception thrown when event reading operations fail.
 *
 * This exception provides structured error reporting for failures in the event reading pipeline,
 * particularly when accessing or parsing impression data from storage blobs.
 *
 * ## Error Codes
 *
 * The [code] property indicates the specific type of failure:
 * - [Code.BLOB_NOT_FOUND]: The requested blob does not exist in storage
 * - [Code.INVALID_FORMAT]: The blob exists but contains malformed or unparseable data
 *
 * @property blobKey the storage key or URI of the blob that caused the failure. This helps identify
 *   the specific resource that couldn't be accessed or parsed.
 * @property code the error code indicating the type of failure. Used for programmatic error
 *   handling and recovery strategies.
 * @property message optional human-readable description providing additional context about the
 *   failure. If null, only the error code and blob key are available.
 * @constructor Creates a new impression read exception with the specified error details.
 */
class ImpressionReadException(val blobKey: String, val code: Code, message: String? = null) :
  Exception(message) {

  /**
   * Categorizes impression reading failures.
   *
   * These codes enable consistent error handling across the event reading pipeline and help
   * determine appropriate recovery strategies.
   */
  enum class Code {
    /** The specified blob does not exist in storage. */
    BLOB_NOT_FOUND,

    /** The blob exists but contains invalid or unparseable data. */
    INVALID_FORMAT,
  }

  override fun toString(): String {
    return "ImpressionReadException(blobKey='$blobKey', code=$code" +
      (message?.let { ", message='$it'" } ?: "") +
      ")"
  }
}
