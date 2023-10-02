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

package k8s

#AMPIngestEndpoint: string @tag("amp_ingest_endpoint")
#AMPRegion:         string @tag("amp_region")

#PostgresConfig: {
	region:     string @tag("postgres_region")
	host:       string @tag("postgres_host")
	port:       string @tag("postgres_port")
	secretName: string @tag("postgres_credential_secret_name")
}

#AwsS3Config: {
	bucket: string @tag("s3_bucket")
	region: string @tag("s3_region")
	flags: [
		"--s3-storage-bucket=" + bucket,
		"--s3-region=" + region,
	]
}
