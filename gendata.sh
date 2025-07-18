bazel run //src/main/kotlin/org/wfanet/measurement/loadtest/edpaggregator/tools:GenerateSyntheticData -- \
	--event-group-reference-id=event-group-reference-id/edpa-eg-reference-id-1 \
	--population-spec-resource-path=./src/main/proto/wfa/measurement/loadtest/dataprovider/360m_population_spec.textproto \
	--data-spec-resource-path=./src/main/proto/wfa/measurement/loadtest/dataprovider/90day_1billion_data_spec.textproto --output-bucket=halotest \
	--kms-type=NONE
