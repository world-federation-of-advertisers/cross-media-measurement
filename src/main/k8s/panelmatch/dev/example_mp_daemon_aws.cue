// Copyright 2022 The Cross-Media Measurement Authors
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

_exchangeDaemonConfig: {
	secretName:         string @tag("secret_name")
	partyName:          string @tag("party_name")
	partyType:          "MODEL_PROVIDER"
	cloudStorageBucket: "halo-edp-test-bucket"
	serviceAccountName: "mp-workflow"
	clientTls: {
		certFile: ""
		keyFile:  ""
	}
	tinkKeyUri: ""
}

// Blank strings are populated by the Terraform-backed deployment supporting
// the AWS daemon.
_defaultAwsConfig: {
	containerPrefix: ""
	region:          ""
	storageBucket:   ""
	kingdomApi:      ""
	certArn:         ""
	commonName:      ""
	orgName:         ""
	dns:             ""
}
