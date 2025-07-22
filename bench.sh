# Option 1: GUI mode (connect from JProfiler GUI on port 8849)
bazel run --jvmopt=-Xmx20g //src/main/kotlin/org/wfanet/measurement/edpaggregator/resultsfulfiller:HardcodedImpressionsResultsFulfillerMain \
	-- \
	--impressions-path /storage/impressions \
	--start-date 2025-01-01 \
	--end-date 2025-01-01 \
	--event-group-reference-id edpa-eg-reference-id-1 --impressions-uri gs://halotest
# --jvm_flags=-agentpath:/opt/jprofiler/bin/linux-x64/libjprofilerti.so=port=8849 \

# Option 2: Offline mode (generates /tmp/jprofiler/benchmark.jps file)
#bazel run \
#	//src/main/kotlin/org/wfanet/measurement/edpaggregator/resultsfulfiller:HardcodedImpressionsResultsFulfillerMain \
#	-- \
#	--jvm_flags=-agentpath:/opt/jprofiler/bin/linux-x64/libjprofilerti.so=offline,config=/tmp/jprofiler/offline.config \
#	--impressions-path /storage/impressions \
#	--start-date 2025-01-01 \
#	--end-date 2025-01-05 \
#	--event-group-reference-id edpa-eg-reference-id-1 --impressions-uri gs://halotest

# --master-key-file /home/mmg/storage/master-key.bin \
