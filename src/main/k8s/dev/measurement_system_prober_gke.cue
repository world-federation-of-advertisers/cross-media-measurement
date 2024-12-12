package k8s

_secret_name:      string @tag("secret_name")
_mc_resource_name: string @tag("mc_name")
_mc_api_key:       string @tag("mc_api_key")
_edp1:             string @tag("edp1_name")
_edp2:             string @tag("edp2_name")

#KingdomPublicApiTarget: (#Target & {name: "v2alpha-public-api-server"}).target

objectSets: [ for objectSet in measurementSystemProber {objectSet}]

measurementSystemProber: #MeasurementSystemProber & {
	_mcName:                   _mc_resource_name
	_apiKey:                   _mc_api_key
	_secretName:               _secret_name
	_verboseGrpcClientLogging: true
	_edpResourceNames: [_edp1, _edp2]
	_kingdomPublicApiTarget: #KingdomPublicApiTarget

	networkPolicies: {
		"measurement-system-prober": {
			_app_label: "measurement-system-prober-app"
			_destinationMatchLabels: [
				"gcp-kingdom-data-server-app",
			]
		}
	}
}
