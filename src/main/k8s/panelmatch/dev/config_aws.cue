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

package k8s

#AwsS3Config: {
	bucket: string @tag("s3_bucket")
	region: string @tag("s3_region")
	flags: [
		"--s3-storage-bucket=" + bucket,
		"--s3-region=" + region,
	]
}

#AwsPrivateCaConfig: #PrivateCaConfig & {
	privateCaArn: string

	_extraFlags: [
		"--certificate-authority-arn=" + privateCaArn,
		"--certificate-authority-csr-signature-algorithm=ECDSA_WITH_SHA256",
	]
}

#AwsKmsConfig: {
	kmsKeyArn: string
	flags: [
		"--tink-key-uri=aws-kms://\(kmsKeyArn)",
	]
}
