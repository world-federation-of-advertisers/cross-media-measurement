// Copyright 2023 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.duchy.deploy.aws.server

import picocli.CommandLine

/** Common command-line flags for connecting to a single Postgres database. */
class S3Flags {
  @CommandLine.Option(
    names = ["--s3-storage-bucket"],
    description = ["The name of the s3 bucket used for default private storage."],
  )
  lateinit var s3Bucket: String
    private set

  @CommandLine.Option(
    names = ["--s3-region"],
    description = ["The region the s3 bucket is located in."],
  )
  lateinit var s3Region: String
    private set
}
