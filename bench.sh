bazel run --jvmopt=-Xmx20g //src/main/kotlin/org/wfanet/measurement/edpaggregator/resultsfulfiller:ResultsFulfillerPipeline \
	-- \
	--start-date 2025-01-01 \
	--end-date 2025-03-30 \
	--total-events 100000000 \
	--synthetic-unique-vids 10000000 \
	--use-parallel-pipeline
# --jvm_flags=-agentpath:/opt/jprofiler/bin/linux-x64/libjprofilerti.so=port=8849 \
