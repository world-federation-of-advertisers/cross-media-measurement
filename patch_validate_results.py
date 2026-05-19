import sys

filepath = "/home/stevenwjones/cross-media-measurement/src/main/kotlin/org/wfanet/measurement/integration/common/reporting/v2/InProcessEdpAggregatorLifeOfAReportTest.kt"
with open(filepath, "r") as f:
    content = f.read()

# 1. Add assertSingleEdpNoNoiseResults helper method after assertNoNoiseResults
old_after_no_noise = '''    assertWithMessage("cross-publisher reach < sum of individual EDP reaches")
      .that(reportingUnitCumulative.reach)
      .isLessThan(componentReaches.sum())
  }

  private fun assertRunningBasicReport('''

new_after_no_noise = '''    assertWithMessage("cross-publisher reach < sum of individual EDP reaches")
      .that(reportingUnitCumulative.reach)
      .isLessThan(componentReaches.sum())
  }

  private fun assertSingleEdpNoNoiseResults(
    basicReport: BasicReport,
    expectedReach: Long,
    expectedImpressions: Long,
    expectedKPlusReach: List<Long>,
  ) {
    assertWithMessage("result groups").that(basicReport.resultGroupsList).hasSize(1)

    val resultGroup = basicReport.resultGroupsList.single()
    val totalResults =
      resultGroup.resultsList.filter {
        it.metadata.metricFrequency.selectorCase == MetricFrequencySpec.SelectorCase.TOTAL
      }
    assertWithMessage("total results").that(totalResults).hasSize(1)

    val result = totalResults.single()
    val reportingUnitCumulative = result.metricSet.reportingUnit.cumulative

    assertWithMessage("population size").that(result.metricSet.populationSize).isGreaterThan(0)

    assertWithMessage("reach")
      .that(reportingUnitCumulative.reach)
      .isEqualTo(expectedReach)

    assertWithMessage("impressions")
      .that(reportingUnitCumulative.impressions)
      .isEqualTo(expectedImpressions)

    assertWithMessage("k+ reach")
      .that(reportingUnitCumulative.kPlusReachList)
      .containsExactlyElementsIn(expectedKPlusReach)
      .inOrder()

    assertWithMessage("number of components").that(result.metricSet.componentsCount).isEqualTo(1)

    val component = result.metricSet.componentsList.single()
    val componentCumulative = component.value.cumulative

    assertWithMessage("component reach")
      .that(componentCumulative.reach)
      .isEqualTo(expectedReach)

    assertWithMessage("component impressions")
      .that(componentCumulative.impressions)
      .isEqualTo(expectedImpressions)
  }

  private fun assertRunningBasicReport('''

content = content.replace(old_after_no_noise, new_after_no_noise, 1)

# 2. Add expected value constants for single-EDP and multi-entity-key
old_constants = '''    private const val EXPECTED_CROSS_PUBLISHER_REACH = 5330L
    private const val EXPECTED_CROSS_PUBLISHER_IMPRESSIONS = 8860L
    private val EXPECTED_K_PLUS_REACH = listOf(5330L, 2572L, 647L, 311L, 0L)
    private const val EXPECTED_EDP_SPEC1_REACH = 3937L
    private const val EXPECTED_EDP_SPEC2_REACH = 3638L'''

new_constants = '''    private const val EXPECTED_CROSS_PUBLISHER_REACH = 5330L
    private const val EXPECTED_CROSS_PUBLISHER_IMPRESSIONS = 8860L
    private val EXPECTED_CROSS_PUBLISHER_K_PLUS_REACH = listOf(5330L, 2572L, 647L, 311L, 0L)
    private const val EXPECTED_EDP_SPEC1_REACH = 3937L
    private const val EXPECTED_EDP_SPEC2_REACH = 3638L

    private const val EXPECTED_SINGLE_EDP_SPEC2_REACH = 3638L
    private const val EXPECTED_SINGLE_EDP_SPEC2_IMPRESSIONS = 4276L
    private val EXPECTED_SINGLE_EDP_SPEC2_K_PLUS_REACH = listOf(3638L, 638L, 0L, 0L, 0L)

    private const val EXPECTED_MULTI_ENTITY_KEY_REACH = 1811L
    private const val EXPECTED_MULTI_ENTITY_KEY_IMPRESSIONS = 2137L
    private val EXPECTED_MULTI_ENTITY_KEY_K_PLUS_REACH = listOf(1811L, 326L, 0L, 0L, 0L)'''

content = content.replace(old_constants, new_constants, 1)

# 2b. Rename EXPECTED_K_PLUS_REACH references to EXPECTED_CROSS_PUBLISHER_K_PLUS_REACH
content = content.replace(
    'expectedKPlusReach = EXPECTED_K_PLUS_REACH,',
    'expectedKPlusReach = EXPECTED_CROSS_PUBLISHER_K_PLUS_REACH,',
)

# 3. Update ref-id-only test: replace state assertion + logging with result validation
old_ref_test = '''    assertThat(completedBasicReport.state).isEqualTo(BasicReport.State.SUCCEEDED)
    logger.info("REF-ID-ONLY REPORT RESULTS: $completedBasicReport")
  }

  @Test
  fun `basic report with creative-id entity-key-only event groups succeeds`'''

new_ref_test = '''    assertThat(completedBasicReport.state).isEqualTo(BasicReport.State.SUCCEEDED)
    assertStructuralResults(completedBasicReport)
    assertSingleEdpNoNoiseResults(
      completedBasicReport,
      expectedReach = EXPECTED_SINGLE_EDP_SPEC2_REACH,
      expectedImpressions = EXPECTED_SINGLE_EDP_SPEC2_IMPRESSIONS,
      expectedKPlusReach = EXPECTED_SINGLE_EDP_SPEC2_K_PLUS_REACH,
    )
  }

  @Test
  fun `basic report with creative-id entity-key-only event groups succeeds`'''

content = content.replace(old_ref_test, new_ref_test, 1)

# 4. Split creative-id test: the existing one becomes cross-publisher,
#    and we add a single-EDP creative-id test.
old_creative_test = '''  @Test
  fun `basic report with creative-id entity-key-only event groups succeeds`() = runBlocking {
    val creativeIdEventGroups = getCreativeIdOnlyEventGroups()
    check(creativeIdEventGroups.size >= 2) {
      "Expected at least 2 creative-id event groups, got ${creativeIdEventGroups.size}"
    }

    val createBasicReportRequest =
      buildCreateBasicReportRequest(
        creativeIdEventGroups,
        "creative-id-only-campaign",
        "creative-id-only-basicreport",
        includeIqfFilter = false,
      )

    val createdBasicReport =
      reportingBasicReportsClient
        .withCallCredentials(credentials)
        .createBasicReport(createBasicReportRequest)

    executeBasicReportsReportsJob(createdBasicReport.name)
    executeReportProcessorJob()

    val completedBasicReport =
      reportingBasicReportsClient
        .withCallCredentials(credentials)
        .getBasicReport(getBasicReportRequest { name = createdBasicReport.name })

    assertThat(completedBasicReport.state).isEqualTo(BasicReport.State.SUCCEEDED)
    logger.info("CREATIVE-ID-ONLY REPORT RESULTS: $completedBasicReport")
  }'''

new_creative_test = '''  @Test
  fun `basic report with cross-publisher creative-id event groups succeeds`() = runBlocking {
    val creativeIdEventGroups = getCreativeIdOnlyEventGroups()
    check(creativeIdEventGroups.size >= 2) {
      "Expected at least 2 creative-id event groups, got ${creativeIdEventGroups.size}"
    }

    val createBasicReportRequest =
      buildCreateBasicReportRequest(
        creativeIdEventGroups,
        "creative-id-cross-pub-campaign",
        "creative-id-cross-pub-basicreport",
        includeIqfFilter = false,
      )

    val createdBasicReport =
      reportingBasicReportsClient
        .withCallCredentials(credentials)
        .createBasicReport(createBasicReportRequest)

    executeBasicReportsReportsJob(createdBasicReport.name)
    executeReportProcessorJob()

    val completedBasicReport =
      reportingBasicReportsClient
        .withCallCredentials(credentials)
        .getBasicReport(getBasicReportRequest { name = createdBasicReport.name })

    assertThat(completedBasicReport.state).isEqualTo(BasicReport.State.SUCCEEDED)
    assertStructuralResults(completedBasicReport)
    assertNoNoiseResults(
      completedBasicReport,
      expectedCrossPublisherReach = EXPECTED_CROSS_PUBLISHER_REACH,
      expectedCrossPublisherImpressions = EXPECTED_CROSS_PUBLISHER_IMPRESSIONS,
      expectedKPlusReach = EXPECTED_CROSS_PUBLISHER_K_PLUS_REACH,
      expectedEdpSpec1Reach = EXPECTED_EDP_SPEC1_REACH,
      expectedEdpSpec2Reach = EXPECTED_EDP_SPEC2_REACH,
    )
  }

  @Test
  fun `basic report with single-edp creative-id event group succeeds`() = runBlocking {
    val allEventGroups = listReportingEventGroups()
    val singleCreativeIdEventGroup =
      allEventGroups.filter {
        it.eventGroupReferenceId == EDP1_CREATIVE_EVENT_GROUP_REF_ID
      }
    check(singleCreativeIdEventGroup.isNotEmpty()) {
      "No single creative-id event group found for edp1"
    }

    val createBasicReportRequest =
      buildCreateBasicReportRequest(
        singleCreativeIdEventGroup,
        "creative-id-single-edp-campaign",
        "creative-id-single-edp-basicreport",
        includeIqfFilter = false,
      )

    val createdBasicReport =
      reportingBasicReportsClient
        .withCallCredentials(credentials)
        .createBasicReport(createBasicReportRequest)

    executeBasicReportsReportsJob(createdBasicReport.name)
    executeReportProcessorJob()

    val completedBasicReport =
      reportingBasicReportsClient
        .withCallCredentials(credentials)
        .getBasicReport(getBasicReportRequest { name = createdBasicReport.name })

    assertThat(completedBasicReport.state).isEqualTo(BasicReport.State.SUCCEEDED)
    assertStructuralResults(completedBasicReport)
    assertSingleEdpNoNoiseResults(
      completedBasicReport,
      expectedReach = EXPECTED_SINGLE_EDP_SPEC2_REACH,
      expectedImpressions = EXPECTED_SINGLE_EDP_SPEC2_IMPRESSIONS,
      expectedKPlusReach = EXPECTED_SINGLE_EDP_SPEC2_K_PLUS_REACH,
    )
  }'''

content = content.replace(old_creative_test, new_creative_test, 1)

# 5. Update multi-entity-key test: replace state assertion + logging with result validation
old_multi_test = '''      assertThat(completedBasicReport.state).isEqualTo(BasicReport.State.SUCCEEDED)
      logger.info("MULTI-ENTITY-KEY REPORT RESULTS: $completedBasicReport")
    }

  companion object {'''

new_multi_test = '''      assertThat(completedBasicReport.state).isEqualTo(BasicReport.State.SUCCEEDED)
      assertStructuralResults(completedBasicReport)
      assertSingleEdpNoNoiseResults(
        completedBasicReport,
        expectedReach = EXPECTED_MULTI_ENTITY_KEY_REACH,
        expectedImpressions = EXPECTED_MULTI_ENTITY_KEY_IMPRESSIONS,
        expectedKPlusReach = EXPECTED_MULTI_ENTITY_KEY_K_PLUS_REACH,
      )
    }

  companion object {'''

content = content.replace(old_multi_test, new_multi_test, 1)

with open(filepath, "w") as f:
    f.write(content)
print("OK: added result validation to all entity key tests")
