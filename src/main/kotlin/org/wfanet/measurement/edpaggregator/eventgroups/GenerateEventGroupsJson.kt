package org.wfanet.measurement.edpaggregator.eventgroups


import com.google.protobuf.util.JsonFormat
import org.wfanet.measurement.edpaggregator.eventgroups.v1alpha.EventGroup
import org.wfanet.measurement.edpaggregator.eventgroups.v1alpha.EventGroups
import org.wfanet.measurement.edpaggregator.eventgroups.v1alpha.eventGroup
import org.wfanet.measurement.edpaggregator.eventgroups.v1alpha.EventGroupKt.metadata as eventGroupMetadata
import org.wfanet.measurement.edpaggregator.eventgroups.v1alpha.EventGroupKt.MetadataKt.AdMetadataKt.campaignMetadata
import org.wfanet.measurement.edpaggregator.eventgroups.v1alpha.EventGroupKt.MetadataKt.adMetadata
import com.google.type.interval
import com.google.protobuf.timestamp
import org.wfanet.measurement.common.commandLineMain
import java.io.File
import java.time.Instant
import java.time.temporal.ChronoUnit

class GenerateEventGroupsJson : Runnable {
    override fun run() {
        val totalGroups = 10_000
        val eventGroupsBuilder = EventGroups.newBuilder()

        val now = Instant.now()
        val start = now.minus(30, ChronoUnit.DAYS)
        val end = now

        repeat(totalGroups) { i ->
            val eventGroup = eventGroup {
                eventGroupReferenceId = "test-eg-$i"
                measurementConsumer = "measurementConsumers/Rcn7fKd25C8"
                dataAvailabilityInterval = interval {
                    startTime = timestamp { seconds = start.epochSecond }
                    endTime = timestamp { seconds = end.epochSecond }
                }
                eventGroupMetadata = eventGroupMetadata {
                    adMetadata = adMetadata {
                        campaignMetadata = campaignMetadata {
                            brand = "brand-$i"
                            campaign = "campaign-$i"
                        }
                    }
                }
                mediaTypes += EventGroup.MediaType.VIDEO
            }
            eventGroupsBuilder.addEventGroups(eventGroup)
        }

        val json = JsonFormat.printer().includingDefaultValueFields().print(eventGroupsBuilder)
        val file = File(System.getProperty("user.dir"), "event_groups_2_10000.json")
        println("----- path: ${file.path}")
        file.writeText(json)

//        File("event_groups_10000.json").writeText(json)
        println("✅ Generated event_groups_10000.json with $totalGroups EventGroups.")
    }

    companion object {
        @JvmStatic
        fun main(args: Array<String>) {
            GenerateEventGroupsJson().run()
        }
    }

}