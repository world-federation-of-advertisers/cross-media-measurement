bazel run //src/main/kotlin/org/wfanet/measurement/edpaggregator/resultsfulfiller:HardcodedImpressionsResultsFulfillerMain \
	-- \
	--impressions-path /storage/impressions \
	--start-date 2025-01-01 \
	--end-date 2025-02-28 \
	--event-group-reference-id edpa-eg-reference-id-1 --impressions-uri gs://halotest
# --master-key-file /home/mmg/storage/master-key.bin \
