# Option 1: GUI mode (connect from JProfiler GUI on port 8849)
bazel run --jvmopt=-Xmx20g //src/main/kotlin/org/wfanet/measurement/edpaggregator/resultsfulfiller:HardcodedImpressionsResultsFulfillerMain \
	-- \
	--start-date 2025-01-01 \
	--end-date 2025-02-02 \
	--total-events 100000000 \
	--use-parallel-pipeline
# --jvm_flags=-agentpath:/opt/jprofiler/bin/linux-x64/libjprofilerti.so=port=8849 \

# Option 2: Offline mode (generates /tmp/jprofiler/benchmark.jps file)
#bazel run \
#	//src/main/kotlin/org/wfanet/measurement/edpaggregator/resultsfulfiller:HardcodedImpressionsResultsFulfillerMain \
#	-- \
#	--jvm_flags=-agentpath:/opt/jprofiler/bin/linux-x64/libjprofilerti.so=offline,config=/tmp/jprofiler/offline.config \
#	--start-date 2025-01-01 \
#	--end-date 2025-01-05 \
#	--synthetic-events-per-second 100000 \
#	--synthetic-unique-vids 5000000 \
#	--synthetic-average-frequency 10

# --master-key-file /home/mmg/storage/master-key.bin \
