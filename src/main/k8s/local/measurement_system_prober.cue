package k8s

_secret_name:      string @tag("secret_name")
_mc_resource_name: string @tag("mc_name")
_mc_api_key:       string @tag("mc_api_key")
_edp1:             string @tag("edp1_name")
_edp2:             string @tag("edp2_name")
_edp3:             string @tag("edp3_name")
_edp4:             string @tag("edp4_name")
_edp5:             string @tag("edp5_name")
_edp6:             string @tag("edp6_name")

#KingdomPublicApiTarget: (#Target & {name: "v2alpha-public-api-server"}).target

objectSets: [for objectSet in measurementSystemProber {objectSet}]

measurementSystemProber: #MeasurementSystemProber & {
	_mc_name:                   _mc_resource_name
	_api_key:                   _mc_api_key
	_kingdom_secret_name:       _secret_name
	_verboseGrpcClientLogging:  true
	_edp1_name:                 _edp1
	_edp2_name:                 _edp2
	_edp3_name:                 _edp3
	_edp4_name:                 _edp4
	_edp5_name:                 _edp5
	_edp6_name:                 _edp6
	_kingdom_public_api_target: #KingdomPublicApiTarget
}
