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

_partyType:                 string @tag("party_type")
_partyId:                   string @tag("party_id")
_recurringExchangeIds:      string @tag("recurring_exchange_ids")
_clusterServiceAccountName: string @tag("cluster_service_account_name")
_kmsKeyArn:                 string @tag("kms_key_arn")
_privateCaRegion:           string @tag("private_ca_region")
_privateCaName:             string @tag("private_ca_name")
_privateCaPoolId:           string @tag("private_ca_pool_id")
_privateCaArn:              string @tag("private_ca_arn")
_certCommonName:            string @tag("cert_common_name")
_certOrganization:          string @tag("cert_organization")
_certDnsName:               string @tag("cert_dns_name")

#KingdomlessExchangeDaemonResourceConfig: {
	resources: ResourceRequirements=#ResourceRequirements & {
		requests: {
			cpu:    "100m"
			memory: "2Gi"
		}
		limits: {
			memory: ResourceRequirements.requests.memory
		}
	}
}

#KingdomlessExchangeDaemon: {
	_daemonConfig: #KingdomlessExchangeDaemonConfig & {
		partyType:            _partyType
		partyId:              _partyId
		recurringExchangeIds: _recurringExchangeIds
	}
	_s3Config:  #AwsS3Config
	_kmsConfig: #AwsKmsConfig & {
		kmsKeyArn: _kmsKeyArn
	}
	_privateCaConfig: #AwsPrivateCaConfig & {
		region:           _privateCaRegion
		name:             _privateCaName
		poolId:           _privateCaPoolId
		privateCaArn:     _privateCaArn
		certCommonName:   _certCommonName
		certOrganization: _certOrganization
		certDnsName:      _certDnsName
	}

	args: _daemonConfig.flags + _s3Config.flags + _kmsConfig.flags + _privateCaConfig.flags
}

objectSets: [cronJobs, networkPolicies]

cronJobs: [Name=_]: #CronJob & {
	_name:      Name
	_component: "kingdomless-exchange"
	_podSpec: _container: resources: #KingdomlessExchangeDaemonResourceConfig.resources

	spec: {
		schedule:          "*/10 * * * *" // Every 10 minutes
		concurrencyPolicy: "Forbid"
	}
}

cronJobs: {
	"kingdomless-exchange-cronjob": {
		_jvmFlags: "-Xmx1024m"
		_podSpec: {
			serviceAccountName: _clusterServiceAccountName
		}
		_podSpec: _container: {
			image:           #ImageConfig.image
			imagePullPolicy: "Always"
			args:            #KingdomlessExchangeDaemon.args
		}
	}
}

networkPolicies: [Name=_]: #NetworkPolicy & {
	_name:    Name
	_appName: Name
}

networkPolicies: {
	"kingdomless-exchange-cronjob": {
		_ingresses: {
			// No ingress.
		}
		_egresses: {
			// Need to be able to send traffic to storage and Kingdom.
			any: {}
		}
	}
}
