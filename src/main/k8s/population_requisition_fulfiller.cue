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

import "list"

// TODO: Update target reference to allow for deployment outside same cluster as Kingdom
#KingdomPublicApiTarget: (#Target & {name: "v2alpha-public-api-server"}).target

#PopulationRequisitionFulfillerConfig: {
	dataProviderDisplayName:      string
	dataProviderResourceName:     string
	dataProviderCertResourceName: string
	throttlerMinimumInterval:     string | *"2s"
	eventMessageDescriptor:       string
	populationKeyAndInfoList: [...#PopulationKeyAndInfo]
}

#PopulationKeyAndInfo: {
	populationResourceName: string
	populationSpecFile:     string
	eventMessageTypeUrl:    string
}

#PopulationRequisitionFulfiller: {
	_config:                                   #PopulationRequisitionFulfillerConfig
	_imageConfig:                              #ImageConfig
	_populationRequisitionFulfillerSecretName: string

	let DisplayName = _config.dataProviderDisplayName

	_populationFlags: {
		let flagLists = [ for config in _config.populationKeyAndInfoList {[
			"--population-resource-name=\(config.populationResourceName)",
			"--population-spec=\(config.populationSpecFile)",
			"--event-message-type-url=\(config.eventMessageTypeUrl)",
		]}]
		list.FlattenN(flagLists, 2)
	}

	deployment: #Deployment & {
		_name:       DisplayName + "-requisition-fulfiller"
		_secretName: _populationRequisitionFulfillerSecretName
		_system:     "population"
		_container: {
			image: _imageConfig.image
			args:  [
				"--kingdom-public-api-target=\(#KingdomPublicApiTarget)",
				"--kingdom-public-api-cert-host=localhost",
				"--data-provider-resource-name=\(_config.dataProviderResourceName)",
				"--data-provider-display-name=\(DisplayName)",
				"--data-provider-certificate-resource-name=\(_config.dataProviderCertResourceName)",
				"--data-provider-encryption-private-keyset=/var/run/secrets/files/\(DisplayName)_enc_private.tink",
				"--data-provider-consent-signaling-private-key-der-file=/var/run/secrets/files/\(DisplayName)_cs_private.der",
				"--data-provider-consent-signaling-certificate-der-file=/var/run/secrets/files/\(DisplayName)_cs_cert.der",
				"--throttler-minimum-interval=\(_config.throttlerMinimumInterval)",
				"--tls-cert-file=/var/run/secrets/files/\(DisplayName)_root.pem",
				"--tls-key-file=/var/run/secrets/files/\(DisplayName)_root.key",
				"--cert-collection-file=/var/run/secrets/files/kingdom_root.pem",
				"--event-message-descriptor-set=\(_config.eventMessageDescriptor)",
			] + _populationFlags
		}
		spec: template: spec: {
			_dependencies: [
				"v2alpha-public-api-server",
			]
			_mounts: "config-files": #ConfigMapMount
		}
	}

	networkPolicies: [Name=_]: #NetworkPolicy & {
		_name: Name
	}

	networkPolicies: {
		"\(deployment._name)": {
			_app_label: deployment.spec.template.metadata.labels.app
			_destinationMatchLabels: [
				"v2alpha-public-api-server-app",
			]
		}
	}
}
