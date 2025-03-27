package org.wfanet.measurement.edpaggregator.eventgroups

@RunWith(JUnit4::class)
class EventGroupSyncTest {

  private val campaigns =
    listOf(
      campaignMetadata {
        eventGroupReferenceId = "reference-id-1"
        campaignName = "campaign-1"
        measurementConsumerName = "measurement-consumer-1"
        brandName = "brand-1"
        startTime = timestamp { seconds = 200 }
        endTime = timestamp { seconds = 300 }
        mediaTypes += listOf("VIDEO", "DISPLAY")
      },
      campaignMetadata {
        eventGroupReferenceId = "reference-id-2"
        campaignName = "campaign-2"
        measurementConsumerName = "measurement-consumer-2"
        brandName = "brand-2"
        startTime = timestamp { seconds = 200 }
        endTime = timestamp { seconds = 300 }
        mediaTypes += listOf("OTHER")
      },
      campaignMetadata {
        eventGroupReferenceId = "reference-id-3"
        campaignName = "campaign-2"
        measurementConsumerName = "measurement-consumer-2"
        brandName = "brand-2"
        startTime = timestamp { seconds = 200 }
        endTime = timestamp { seconds = 300 }
        mediaTypes += listOf("OTHER")
      },
    )

  private val eventGroupsServiceMock: EventGroupsCoroutineImplBase = mockService {
    onBlocking { updateEventGroup(any<UpdateEventGroupRequest>()) }
      .thenAnswer { invocation -> invocation.getArgument<UpdateEventGroupRequest>(0).eventGroup }
    onBlocking { createEventGroup(any<CreateEventGroupRequest>()) }
      .thenAnswer { invocation -> invocation.getArgument<CreateEventGroupRequest>(0).eventGroup }
    onBlocking { listEventGroups(any<ListEventGroupsRequest>()) }
      .thenAnswer {
        listEventGroupsResponse {
          eventGroups += campaigns[0].toEventGroup()
          eventGroups += campaigns[1].toEventGroup()
          eventGroups +=
            campaigns[2].toEventGroup().copy {
              this.eventGroupMetadata = eventGroupMetadata {
                this.adMetadata = adMetadata {
                  this.campaignMetadata = eventGroupCampaignMetadata {
                    brandName = "new-brand-name"
                  }
                }
              }
            }
        }
      }
  }

  private val eventGroupsStub: EventGroupsCoroutineStub by lazy {
    EventGroupsCoroutineStub(grpcTestServerRule.channel)
  }

  @get:Rule val grpcTestServerRule = GrpcTestServerRule { addService(eventGroupsServiceMock) }

  @Test
  fun `sync registersUnregisteredEventGroups`() {
    val testCampaigns =
      campaigns + campaigns[0].copy { eventGroupReferenceId = "some-new-reference-id" }
    val eventGroupSync = EventGroupSync("edp-name", eventGroupsStub, testCampaigns)
    runBlocking { eventGroupSync.sync() }
    verifyBlocking(eventGroupsServiceMock, times(1)) { createEventGroup(any()) }
  }

  @Test
  fun `sync updatesExistingEventGroups`() {
    val eventGroupSync = EventGroupSync("edp-name", eventGroupsStub, campaigns)
    runBlocking { eventGroupSync.sync() }
    verifyBlocking(eventGroupsServiceMock, times(1)) { updateEventGroup(any()) }
  }

  @Test
  fun sync_returnsMapOfEventGroupReferenceIdsToEventGroups() {
    runBlocking {
      val eventGroupSync = EventGroupSync("edp-name", eventGroupsStub, campaigns)
      val result = runBlocking { eventGroupSync.sync() }
      assertThat(result)
        .isEqualTo(
          mapOf(
            "reference-id-1" to "resource-name-for-reference-id-1",
            "reference-id-2" to "resource-name-for-reference-id-2",
            "reference-id-3" to "resource-name-for-reference-id-3",
          )
        )
    }
  }
}
