# Copyright 2025 The Cross-Media Measurement Authors
#
# Licensed under the Apache License, Version 2.0 (the 'License');
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an 'AS IS' BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import unittest

from google.protobuf import text_format

from wfa.measurement.internal.reporting.postprocessing import report_post_processor_result_pb2

from noiseninja.noised_measurements import Measurement, MeasurementSet

from src.main.python.wfa.measurement.reporting.postprocessing.report.report import (
    MetricReport,
    Report,
)
from wfa.measurement.internal.reporting.postprocessing import report_summary_v2_pb2

StatusCode = report_post_processor_result_pb2.ReportPostProcessorStatus.StatusCode

NOISE_CORRECTION_TOLERANCE = 0.1

from tools.post_process_report_summary_v2 import ReportSummaryV2Processor


class TestPostProcessReportSummaryV2(unittest.TestCase):

    def test_report_summary_with_a_single_reach_is_processed_correctly(self):
        report_summary_textproto = """
          cmms_measurement_consumer_id: "NsQ4CS3K1to"
          external_report_result_id: 1896307399873472987
          groupings {
            path: "person.social_grade_group"
            value { enum_value: "C2_D_E" }
          }
          event_filters {
            terms {
              path: "person.age_group"
              value { enum_value: "YEARS_18_TO_34" }
            }
          }
          population: 5733810
          report_summary_set_results {
            external_reporting_set_result_id: 7237497766817761156
            impression_filter: "custom"
            set_operation: "union"
            data_providers: "abc123"
            metric_frequency_spec { weekly: MONDAY }
            non_cumulative_results {
              key {
                non_cumulative_start { year: 2021 month: 3 day: 14 }
                end { year: 2021 month: 3 day: 15 }
              }
              reach {
                value: 116000
                standard_deviation: 137750.79990420336
                metric: "reach_7237497766817761156_non_cumulative_2021_03_15"
              }
            }
          }
        """

        report_summary = text_format.Parse(
            report_summary_textproto,
            report_summary_v2_pb2.ReportSummaryV2(),
        )

        result = ReportSummaryV2Processor(report_summary).process()

        self.assertEqual(
            result.status.status_code,
            StatusCode.SOLUTION_FOUND_WITH_HIGHS,
        )
        self.assertEqual(
            result.updated_measurements["reach_7237497766817761156_non_cumulative_2021_03_15"],
            116000,
        )

    def test_report_summary_v2_is_parsed_correctly(self):
        report_summary = get_report_summary_v2(
            'src/test/python/wfa/measurement/reporting/postprocessing/tools/sample_report_summary_v2.textproto'
        )
        reportSummaryProcessor = ReportSummaryV2Processor(report_summary)

        reportSummaryProcessor._process_union_results()

        edp1 = frozenset({'EDP_ONE'})
        edp2 = frozenset({'EDP_TWO'})
        edp3 = frozenset({'EDP_THREE'})
        edp12 = frozenset({'EDP_ONE', 'EDP_TWO'})
        edp123 = frozenset({'EDP_ONE', 'EDP_TWO', 'EDP_THREE'})

        expected_weekly_cumulative_reaches = {
            'ami': {
                edp1: [
                    Measurement(9992500, 10000, 'm_001'),
                    Measurement(11998422, 10000, 'm_002'),
                ],
                edp2: [
                    Measurement(5000000, 0, 'm_012'),
                    Measurement(6000000, 0, 'm_013'),
                ],
                edp3: [
                    Measurement(800000, 0, 'm_023'),
                    Measurement(1000000, 0, 'm_024'),
                ],
                edp123: [
                    Measurement(15830545, 10000, 'm_036'),
                    Measurement(19010669, 10000, 'm_037'),
                ],
            },
            'mrc': {
                edp1: [
                    Measurement(9501618, 10000, 'm_047'),
                    Measurement(11389309, 10000, 'm_048'),
                ],
                edp2: [
                    Measurement(4750000, 0, 'm_058'),
                    Measurement(5700000, 0, 'm_059'),
                ],
                edp3: [
                    Measurement(760000, 0, 'm_069'),
                    Measurement(950000, 0, 'm_070'),
                ],
                edp12: [
                    Measurement(13427250, 10000, 'm_080'),
                    Measurement(15920317, 10000, 'm_081'),
                ],
            },
            'custom': {
                edp1: [
                    Measurement(9984642, 10000, 'm_091'),
                    Measurement(12020226, 10000, 'm_092'),
                ],
                edp2: [
                    Measurement(5000000, 0, 'm_102'),
                    Measurement(6000000, 0, 'm_103'),
                ],
                edp3: [
                    Measurement(800000, 0, 'm_113'),
                    Measurement(1000000, 0, 'm_114'),
                ],
                edp123: [
                    Measurement(15799013, 10000, 'm_124'),
                    Measurement(19015392, 10000, 'm_125'),
                ],
            },
        }

        expected_whole_campaign_measurements = {
            'ami': {
                edp1:
                    MeasurementSet(
                        reach=Measurement(11978894, 10000, 'm_003'),
                        k_reach={
                            1: Measurement(6182655, 10000, 'm_004-bin-1'),
                            2: Measurement(3091328, 10000, 'm_004-bin-2'),
                            3: Measurement(1545664, 10000, 'm_004-bin-3'),
                            4: Measurement(772832, 10000, 'm_004-bin-4'),
                            5: Measurement(386415, 10000, 'm_004-bin-5'),
                        },
                        impression=Measurement(22870892, 10000, 'm_005'),
                    ),
                edp2:
                    MeasurementSet(
                        reach=Measurement(6000000, 0, 'm_014'),
                        k_reach={
                            1: Measurement(3096774, 0, 'm_015-bin-1'),
                            2: Measurement(1548387, 0, 'm_015-bin-2'),
                            3: Measurement(774194, 0, 'm_015-bin-3'),
                            4: Measurement(387097, 0, 'm_015-bin-4'),
                            5: Measurement(193548, 0, 'm_015-bin-5'),
                        },
                        impression=Measurement(11216125, 0, 'm_016'),
                    ),
                edp3:
                    MeasurementSet(
                        reach=Measurement(1000000, 0, 'm_025'),
                        k_reach={
                            1: Measurement(516129, 0, 'm_026-bin-1'),
                            2: Measurement(258065, 0, 'm_026-bin-2'),
                            3: Measurement(129033, 0, 'm_026-bin-3'),
                            4: Measurement(64517, 0, 'm_026-bin-4'),
                            5: Measurement(32256, 0, 'm_026-bin-5'),
                        },
                        impression=Measurement(1844136, 0, 'm_027'),
                    ),
                edp12:
                    MeasurementSet(
                        reach=Measurement(16686873, 10000, 'm_034'),
                        impression=Measurement(34113188, 10000, 'm_035'),
                    ),
                edp123:
                    MeasurementSet(
                        reach=Measurement(19021738, 10000, 'm_038'),
                        k_reach={
                            1: Measurement(9817671, 10000, 'm_039-bin-1'),
                            2: Measurement(4908836, 10000, 'm_039-bin-2'),
                            3: Measurement(2454418, 10000, 'm_039-bin-3'),
                            4: Measurement(1227209, 10000, 'm_039-bin-4'),
                            5: Measurement(613604, 10000, 'm_039-bin-5'),
                        },
                        impression=Measurement(35926461, 10000, 'm_040'),
                    ),
            },
            'mrc': {
                edp1:
                    MeasurementSet(
                        reach=Measurement(11382243, 10000, 'm_049'),
                        k_reach={
                            1: Measurement(5874706, 10000, 'm_050-bin-1'),
                            2: Measurement(2937353, 10000, 'm_050-bin-2'),
                            3: Measurement(1468677, 10000, 'm_050-bin-3'),
                            4: Measurement(734339, 10000, 'm_050-bin-4'),
                            5: Measurement(367168, 10000, 'm_050-bin-5'),
                        },
                        impression=Measurement(21696322, 10000, 'm_051'),
                    ),
                edp2:
                    MeasurementSet(
                        reach=Measurement(5700000, 0, 'm_060'),
                        k_reach={
                            1: Measurement(2941935, 0, 'm_061-bin-1'),
                            2: Measurement(1470968, 0, 'm_061-bin-2'),
                            3: Measurement(735484, 0, 'm_061-bin-3'),
                            4: Measurement(367742, 0, 'm_061-bin-4'),
                            5: Measurement(183871, 0, 'm_061-bin-5'),
                        },
                        impression=Measurement(10645760, 0, 'm_062'),
                    ),
                edp3:
                    MeasurementSet(
                        reach=Measurement(950000, 0, 'm_071'),
                        k_reach={
                            1: Measurement(490323, 0, 'm_072-bin-1'),
                            2: Measurement(245162, 0, 'm_072-bin-2'),
                            3: Measurement(122581, 0, 'm_072-bin-3'),
                            4: Measurement(61291, 0, 'm_072-bin-4'),
                            5: Measurement(30643, 0, 'm_072-bin-5'),
                        },
                        impression=Measurement(1751669, 0, 'm_073'),
                    ),
                edp12:
                    MeasurementSet(
                        reach=Measurement(15908881, 10000, 'm_082'),
                        k_reach={
                            1: Measurement(8211035, 10000, 'm_083-bin-1'),
                            2: Measurement(4105518, 10000, 'm_083-bin-2'),
                            3: Measurement(2052759, 10000, 'm_083-bin-3'),
                            4: Measurement(1026380, 10000, 'm_083-bin-4'),
                            5: Measurement(513189, 10000, 'm_083-bin-5'),
                        },
                        impression=Measurement(32337826, 10000, 'm_084'),
                    ),
            },
            'custom': {
                edp1:
                    MeasurementSet(
                        reach=Measurement(12017026, 10000, 'm_093'),
                        k_reach={
                            1: Measurement(6202336, 10000, 'm_094-bin-1'),
                            2: Measurement(3101168, 10000, 'm_094-bin-2'),
                            3: Measurement(1550584, 10000, 'm_094-bin-3'),
                            4: Measurement(775292, 10000, 'm_094-bin-4'),
                            5: Measurement(387646, 10000, 'm_094-bin-5'),
                        },
                        impression=Measurement(22871159, 10000, 'm_095'),
                    ),
                edp2:
                    MeasurementSet(
                        reach=Measurement(6000000, 0, 'm_104'),
                        k_reach={
                            1: Measurement(3096774, 0, 'm_105-bin-1'),
                            2: Measurement(1548387, 0, 'm_105-bin-2'),
                            3: Measurement(774194, 0, 'm_105-bin-3'),
                            4: Measurement(387097, 0, 'm_105-bin-4'),
                            5: Measurement(193548, 0, 'm_105-bin-5'),
                        },
                        impression=Measurement(11216125, 0, 'm_106'),
                    ),
                edp3:
                    MeasurementSet(
                        reach=Measurement(1000000, 0, 'm_115'),
                        k_reach={
                            1: Measurement(516129, 0, 'm_116-bin-1'),
                            2: Measurement(258065, 0, 'm_116-bin-2'),
                            3: Measurement(129033, 0, 'm_116-bin-3'),
                            4: Measurement(64517, 0, 'm_116-bin-4'),
                            5: Measurement(32256, 0, 'm_116-bin-5'),
                        },
                        impression=Measurement(1844136, 0, 'm_117'),
                    ),
                edp123:
                    MeasurementSet(
                        reach=Measurement(19030737, 10000, 'm_126'),
                        k_reach={
                            1: Measurement(9822316, 10000, 'm_127-bin-1'),
                            2: Measurement(4911158, 10000, 'm_127-bin-2'),
                            3: Measurement(2455579, 10000, 'm_127-bin-3'),
                            4: Measurement(1227790, 10000, 'm_127-bin-4'),
                            5: Measurement(613894, 10000, 'm_127-bin-5'),
                        },
                        impression=Measurement(35936915, 10000, 'm_128'),
                    ),
            },
        }

        expected_weekly_non_cumulative_measurements = {
            'ami': {
                edp1: [
                    MeasurementSet(
                        reach=Measurement(10008130, 10000, 'm_006'),
                        k_reach={
                            1: Measurement(5165486, 10000, 'm_007-bin-1'),
                            2: Measurement(2582743, 10000, 'm_007-bin-2'),
                            3: Measurement(1291372, 10000, 'm_007-bin-3'),
                            4: Measurement(645686, 10000, 'm_007-bin-4'),
                            5: Measurement(322843, 10000, 'm_007-bin-5'),
                        },
                        impression=Measurement(18379493, 10000, 'm_008'),
                    ),
                    MeasurementSet(
                        reach=Measurement(2452001, 10000, 'm_009'),
                        k_reach={
                            1: Measurement(1265549, 10000, 'm_010-bin-1'),
                            2: Measurement(632775, 10000, 'm_010-bin-2'),
                            3: Measurement(316388, 10000, 'm_010-bin-3'),
                            4: Measurement(158194, 10000, 'm_010-bin-4'),
                            5: Measurement(79095, 10000, 'm_010-bin-5'),
                        },
                        impression=Measurement(4471035, 10000, 'm_011'),
                    ),
                ],
                edp2: [
                    MeasurementSet(
                        reach=Measurement(5000000, 0, 'm_017'),
                        k_reach={
                            1: Measurement(2580645, 0, 'm_018-bin-1'),
                            2: Measurement(1290323, 0, 'm_018-bin-2'),
                            3: Measurement(645162, 0, 'm_018-bin-3'),
                            4: Measurement(322581, 0, 'm_018-bin-4'),
                            5: Measurement(161289, 0, 'm_018-bin-5'),
                        },
                        impression=Measurement(9193546, 0, 'm_019'),
                    ),
                    MeasurementSet(
                        reach=Measurement(1100000, 0, 'm_020'),
                        k_reach={
                            1: Measurement(567742, 0, 'm_021-bin-1'),
                            2: Measurement(283871, 0, 'm_021-bin-2'),
                            3: Measurement(141936, 0, 'm_021-bin-3'),
                            4: Measurement(70968, 0, 'm_021-bin-4'),
                            5: Measurement(35483, 0, 'm_021-bin-5'),
                        },
                        impression=Measurement(2022579, 0, 'm_022'),
                    ),
                ],
                edp3: [
                    MeasurementSet(
                        reach=Measurement(800000, 0, 'm_028'),
                        k_reach={
                            1: Measurement(412903, 0, 'm_029-bin-1'),
                            2: Measurement(206452, 0, 'm_029-bin-2'),
                            3: Measurement(103226, 0, 'm_029-bin-3'),
                            4: Measurement(51613, 0, 'm_029-bin-4'),
                            5: Measurement(25806, 0, 'm_029-bin-5'),
                        },
                        impression=Measurement(1470967, 0, 'm_030'),
                    ),
                    MeasurementSet(
                        reach=Measurement(202952, 0, 'm_031'),
                        k_reach={
                            1: Measurement(104749, 0, 'm_032-bin-1'),
                            2: Measurement(52375, 0, 'm_032-bin-2'),
                            3: Measurement(26188, 0, 'm_032-bin-3'),
                            4: Measurement(13094, 0, 'm_032-bin-4'),
                            5: Measurement(6546, 0, 'm_032-bin-5'),
                        },
                        impression=Measurement(373169, 0, 'm_033'),
                    ),
                ],
                edp123: [
                    MeasurementSet(
                        reach=Measurement(15829304, 10000, 'm_041'),
                        k_reach={
                            1: Measurement(8169963, 10000, 'm_042-bin-1'),
                            2: Measurement(4084982, 10000, 'm_042-bin-2'),
                            3: Measurement(2042491, 10000, 'm_042-bin-3'),
                            4: Measurement(1021246, 10000, 'm_042-bin-4'),
                            5: Measurement(510622, 10000, 'm_042-bin-5'),
                        },
                        impression=Measurement(29046331, 10000, 'm_043'),
                    ),
                    MeasurementSet(
                        reach=Measurement(3761510, 10000, 'm_044'),
                        k_reach={
                            1: Measurement(1941425, 10000, 'm_045-bin-1'),
                            2: Measurement(970713, 10000, 'm_045-bin-2'),
                            3: Measurement(485357, 10000, 'm_045-bin-3'),
                            4: Measurement(242679, 10000, 'm_045-bin-4'),
                            5: Measurement(121336, 10000, 'm_045-bin-5'),
                        },
                        impression=Measurement(6904573, 10000, 'm_046'),
                    ),
                ],
            },
            'mrc': {
                edp1: [
                    MeasurementSet(
                        reach=Measurement(9503446, 10000, 'm_052'),
                        k_reach={
                            1: Measurement(4905004, 10000, 'm_053-bin-1'),
                            2: Measurement(2452502, 10000, 'm_053-bin-2'),
                            3: Measurement(1226251, 10000, 'm_053-bin-3'),
                            4: Measurement(613126, 10000, 'm_053-bin-4'),
                            5: Measurement(306563, 10000, 'm_053-bin-5'),
                        },
                        impression=Measurement(17473517, 10000, 'm_054'),
                    ),
                    MeasurementSet(
                        reach=Measurement(2289252, 10000, 'm_055'),
                        k_reach={
                            1: Measurement(1181549, 10000, 'm_056-bin-1'),
                            2: Measurement(590775, 10000, 'm_056-bin-2'),
                            3: Measurement(295388, 10000, 'm_056-bin-3'),
                            4: Measurement(147694, 10000, 'm_056-bin-4'),
                            5: Measurement(73846, 10000, 'm_056-bin-5'),
                        },
                        impression=Measurement(4236753, 10000, 'm_057'),
                    ),
                ],
                edp2: [
                    MeasurementSet(
                        reach=Measurement(4750000, 0, 'm_063'),
                        k_reach={
                            1: Measurement(2451613, 0, 'm_064-bin-1'),
                            2: Measurement(1225807, 0, 'm_064-bin-2'),
                            3: Measurement(612904, 0, 'm_064-bin-3'),
                            4: Measurement(306452, 0, 'm_064-bin-4'),
                            5: Measurement(153224, 0, 'm_064-bin-5'),
                        },
                        impression=Measurement(8733867, 0, 'm_065'),
                    ),
                    MeasurementSet(
                        reach=Measurement(1039801, 0, 'm_066'),
                        k_reach={
                            1: Measurement(536671, 0, 'm_067-bin-1'),
                            2: Measurement(268336, 0, 'm_067-bin-2'),
                            3: Measurement(134168, 0, 'm_067-bin-3'),
                            4: Measurement(67084, 0, 'm_067-bin-4'),
                            5: Measurement(33542, 0, 'm_067-bin-5'),
                        },
                        impression=Measurement(1911893, 0, 'm_068'),
                    ),
                ],
                edp3: [
                    MeasurementSet(
                        reach=Measurement(760000, 0, 'm_074'),
                        k_reach={
                            1: Measurement(392258, 0, 'm_075-bin-1'),
                            2: Measurement(196129, 0, 'm_075-bin-2'),
                            3: Measurement(98065, 0, 'm_075-bin-3'),
                            4: Measurement(49033, 0, 'm_075-bin-4'),
                            5: Measurement(24515, 0, 'm_075-bin-5'),
                        },
                        impression=Measurement(1397418, 0, 'm_076'),
                    ),
                    MeasurementSet(
                        reach=Measurement(192662, 0, 'm_077'),
                        k_reach={
                            1: Measurement(99438, 0, 'm_078-bin-1'),
                            2: Measurement(49719, 0, 'm_078-bin-2'),
                            3: Measurement(24860, 0, 'm_078-bin-3'),
                            4: Measurement(12430, 0, 'm_078-bin-4'),
                            5: Measurement(6215, 0, 'm_078-bin-5'),
                        },
                        impression=Measurement(354251, 0, 'm_079'),
                    ),
                ],
                edp12: [
                    MeasurementSet(
                        reach=Measurement(13426464, 10000, 'm_085'),
                        k_reach={
                            1: Measurement(6929788, 10000, 'm_086-bin-1'),
                            2: Measurement(3464894, 10000, 'm_086-bin-2'),
                            3: Measurement(1732447, 10000, 'm_086-bin-3'),
                            4: Measurement(866224, 10000, 'm_086-bin-4'),
                            5: Measurement(433111, 10000, 'm_086-bin-5'),
                        },
                        impression=Measurement(26215389, 10000, 'm_087'),
                    ),
                    MeasurementSet(
                        reach=Measurement(3278136, 10000, 'm_088'),
                        k_reach={
                            1: Measurement(1691941, 10000, 'm_089-bin-1'),
                            2: Measurement(845971, 10000, 'm_089-bin-2'),
                            3: Measurement(422986, 10000, 'm_089-bin-3'),
                            4: Measurement(211493, 10000, 'm_089-bin-4'),
                            5: Measurement(105745, 10000, 'm_089-bin-5'),
                        },
                        impression=Measurement(6135862, 10000, 'm_090'),
                    ),
                ],
            },
            'custom': {
                edp1: [
                    MeasurementSet(
                        reach=Measurement(10000981, 10000, 'm_096'),
                        k_reach={
                            1: Measurement(5161797, 10000, 'm_097-bin-1'),
                            2: Measurement(2580899, 10000, 'm_097-bin-2'),
                            3: Measurement(1290450, 10000, 'm_097-bin-3'),
                            4: Measurement(645225, 10000, 'm_097-bin-4'),
                            5: Measurement(322610, 10000, 'm_097-bin-5'),
                        },
                        impression=Measurement(18382797, 10000, 'm_098'),
                    ),
                    MeasurementSet(
                        reach=Measurement(2441042, 10000, 'm_099'),
                        k_reach={
                            1: Measurement(1259893, 10000, 'm_100-bin-1'),
                            2: Measurement(629947, 10000, 'm_100-bin-2'),
                            3: Measurement(314974, 10000, 'm_100-bin-3'),
                            4: Measurement(157487, 10000, 'm_100-bin-4'),
                            5: Measurement(78741, 10000, 'm_100-bin-5'),
                        },
                        impression=Measurement(4488362, 10000, 'm_101'),
                    ),
                ],
                edp2: [
                    MeasurementSet(
                        reach=Measurement(5000000, 0, 'm_107'),
                        k_reach={
                            1: Measurement(2580645, 0, 'm_108-bin-1'),
                            2: Measurement(1290323, 0, 'm_108-bin-2'),
                            3: Measurement(645162, 0, 'm_108-bin-3'),
                            4: Measurement(322581, 0, 'm_108-bin-4'),
                            5: Measurement(161289, 0, 'm_108-bin-5'),
                        },
                        impression=Measurement(9193546, 0, 'm_109'),
                    ),
                    MeasurementSet(
                        reach=Measurement(1100000, 0, 'm_110'),
                        k_reach={
                            1: Measurement(567742, 0, 'm_111-bin-1'),
                            2: Measurement(283871, 0, 'm_111-bin-2'),
                            3: Measurement(141936, 0, 'm_111-bin-3'),
                            4: Measurement(70968, 0, 'm_111-bin-4'),
                            5: Measurement(35483, 0, 'm_111-bin-5'),
                        },
                        impression=Measurement(2022579, 0, 'm_112'),
                    ),
                ],
                edp3: [
                    MeasurementSet(
                        reach=Measurement(800000, 0, 'm_118'),
                        k_reach={
                            1: Measurement(412903, 0, 'm_119-bin-1'),
                            2: Measurement(206452, 0, 'm_119-bin-2'),
                            3: Measurement(103226, 0, 'm_119-bin-3'),
                            4: Measurement(51613, 0, 'm_119-bin-4'),
                            5: Measurement(25806, 0, 'm_119-bin-5'),
                        },
                        impression=Measurement(1470967, 0, 'm_120'),
                    ),
                    MeasurementSet(
                        reach=Measurement(202952, 0, 'm_121'),
                        k_reach={
                            1: Measurement(104749, 0, 'm_122-bin-1'),
                            2: Measurement(52375, 0, 'm_122-bin-2'),
                            3: Measurement(26188, 0, 'm_122-bin-3'),
                            4: Measurement(13094, 0, 'm_122-bin-4'),
                            5: Measurement(6546, 0, 'm_122-bin-5'),
                        },
                        impression=Measurement(373169, 0, 'm_123'),
                    ),
                ],
                edp123: [
                    MeasurementSet(
                        reach=Measurement(15819974, 10000, 'm_129'),
                        k_reach={
                            1: Measurement(8165148, 10000, 'm_130-bin-1'),
                            2: Measurement(4082574, 10000, 'm_130-bin-2'),
                            3: Measurement(2041287, 10000, 'm_130-bin-3'),
                            4: Measurement(1020644, 10000, 'm_130-bin-4'),
                            5: Measurement(510321, 10000, 'm_130-bin-5'),
                        },
                        impression=Measurement(29052805, 10000, 'm_131'),
                    ),
                    MeasurementSet(
                        reach=Measurement(3751542, 10000, 'm_132'),
                        k_reach={
                            1: Measurement(1936280, 10000, 'm_133-bin-1'),
                            2: Measurement(968140, 10000, 'm_133-bin-2'),
                            3: Measurement(484070, 10000, 'm_133-bin-3'),
                            4: Measurement(242035, 10000, 'm_133-bin-4'),
                            5: Measurement(121017, 10000, 'm_133-bin-5'),
                        },
                        impression=Measurement(6884110, 10000, 'm_134'),
                    ),
                ],
            },
        }

        self.assertEqual(
            reportSummaryProcessor._weekly_cumulative_reaches,
            expected_weekly_cumulative_reaches,
        )
        self.assertEqual(
            reportSummaryProcessor._whole_campaign_measurements,
            expected_whole_campaign_measurements,
        )
        self.assertEqual(
            reportSummaryProcessor._weekly_non_cumulative_measurements,
            expected_weekly_non_cumulative_measurements,
        )

    def test_report_is_built_from_report_summary_v2_correctly(self):
        report_summary = get_report_summary_v2(
            'src/test/python/wfa/measurement/reporting/postprocessing/tools/sample_report_summary_v2.textproto'
        )
        reportSummaryProcessor = ReportSummaryV2Processor(report_summary)

        edp1 = frozenset({'EDP_ONE'})
        edp2 = frozenset({'EDP_TWO'})
        edp3 = frozenset({'EDP_THREE'})
        edp12 = frozenset({'EDP_ONE', 'EDP_TWO'})
        edp123 = frozenset({'EDP_ONE', 'EDP_TWO', 'EDP_THREE'})

        report = reportSummaryProcessor._build_report()

        expected_report = Report(
            metric_reports={
                "ami":
                    MetricReport(
                        weekly_cumulative_reaches={
                            edp1: [
                                Measurement(9992500, 10000, 'm_001'),
                                Measurement(11998422, 10000, 'm_002'),
                            ],
                            edp2: [
                                Measurement(5000000, 0, 'm_012'),
                                Measurement(6000000, 0, 'm_013'),
                            ],
                            edp3: [
                                Measurement(800000, 0, 'm_023'),
                                Measurement(1000000, 0, 'm_024'),
                            ],
                            edp123: [
                                Measurement(15830545, 10000, 'm_036'),
                                Measurement(19010669, 10000, 'm_037'),
                            ],
                        },
                        whole_campaign_measurements={
                            edp1:
                                MeasurementSet(
                                    reach=Measurement(11978894, 10000, 'm_003'),
                                    k_reach={
                                        1:
                                            Measurement(6182655, 10000,
                                                        'm_004-bin-1'),
                                        2:
                                            Measurement(3091328, 10000,
                                                        'm_004-bin-2'),
                                        3:
                                            Measurement(1545664, 10000,
                                                        'm_004-bin-3'),
                                        4:
                                            Measurement(772832, 10000,
                                                        'm_004-bin-4'),
                                        5:
                                            Measurement(386415, 10000,
                                                        'm_004-bin-5'),
                                    },
                                    impression=Measurement(
                                        22870892, 10000, 'm_005'),
                                ),
                            edp2:
                                MeasurementSet(
                                    reach=Measurement(6000000, 0, 'm_014'),
                                    k_reach={
                                        1:
                                            Measurement(3096774, 0,
                                                        'm_015-bin-1'),
                                        2:
                                            Measurement(1548387, 0,
                                                        'm_015-bin-2'),
                                        3:
                                            Measurement(774194, 0,
                                                        'm_015-bin-3'),
                                        4:
                                            Measurement(387097, 0,
                                                        'm_015-bin-4'),
                                        5:
                                            Measurement(193548, 0,
                                                        'm_015-bin-5'),
                                    },
                                    impression=Measurement(
                                        11216125, 0, 'm_016'),
                                ),
                            edp3:
                                MeasurementSet(
                                    reach=Measurement(1000000, 0, 'm_025'),
                                    k_reach={
                                        1:
                                            Measurement(516129, 0,
                                                        'm_026-bin-1'),
                                        2:
                                            Measurement(258065, 0,
                                                        'm_026-bin-2'),
                                        3:
                                            Measurement(129033, 0,
                                                        'm_026-bin-3'),
                                        4:
                                            Measurement(64517, 0,
                                                        'm_026-bin-4'),
                                        5:
                                            Measurement(32256, 0,
                                                        'm_026-bin-5'),
                                    },
                                    impression=Measurement(1844136, 0, 'm_027'),
                                ),
                            edp12:
                                MeasurementSet(
                                    reach=Measurement(16686873, 10000, 'm_034'),
                                    impression=Measurement(
                                        34113188, 10000, 'm_035'),
                                ),
                            edp123:
                                MeasurementSet(
                                    reach=Measurement(19021738, 10000, 'm_038'),
                                    k_reach={
                                        1:
                                            Measurement(9817671, 10000,
                                                        'm_039-bin-1'),
                                        2:
                                            Measurement(4908836, 10000,
                                                        'm_039-bin-2'),
                                        3:
                                            Measurement(2454418, 10000,
                                                        'm_039-bin-3'),
                                        4:
                                            Measurement(1227209, 10000,
                                                        'm_039-bin-4'),
                                        5:
                                            Measurement(613604, 10000,
                                                        'm_039-bin-5'),
                                    },
                                    impression=Measurement(
                                        35926461, 10000, 'm_040'),
                                ),
                        },
                        weekly_non_cumulative_measurements={
                            edp1: [
                                MeasurementSet(
                                    reach=Measurement(10008130, 10000, 'm_006'),
                                    k_reach={
                                        1:
                                            Measurement(5165486, 10000,
                                                        'm_007-bin-1'),
                                        2:
                                            Measurement(2582743, 10000,
                                                        'm_007-bin-2'),
                                        3:
                                            Measurement(1291372, 10000,
                                                        'm_007-bin-3'),
                                        4:
                                            Measurement(645686, 10000,
                                                        'm_007-bin-4'),
                                        5:
                                            Measurement(322843, 10000,
                                                        'm_007-bin-5'),
                                    },
                                    impression=Measurement(
                                        18379493, 10000, 'm_008'),
                                ),
                                MeasurementSet(
                                    reach=Measurement(2452001, 10000, 'm_009'),
                                    k_reach={
                                        1:
                                            Measurement(1265549, 10000,
                                                        'm_010-bin-1'),
                                        2:
                                            Measurement(632775, 10000,
                                                        'm_010-bin-2'),
                                        3:
                                            Measurement(316388, 10000,
                                                        'm_010-bin-3'),
                                        4:
                                            Measurement(158194, 10000,
                                                        'm_010-bin-4'),
                                        5:
                                            Measurement(79095, 10000,
                                                        'm_010-bin-5'),
                                    },
                                    impression=Measurement(
                                        4471035, 10000, 'm_011'),
                                ),
                            ],
                            edp2: [
                                MeasurementSet(
                                    reach=Measurement(5000000, 0, 'm_017'),
                                    k_reach={
                                        1:
                                            Measurement(2580645, 0,
                                                        'm_018-bin-1'),
                                        2:
                                            Measurement(1290323, 0,
                                                        'm_018-bin-2'),
                                        3:
                                            Measurement(645162, 0,
                                                        'm_018-bin-3'),
                                        4:
                                            Measurement(322581, 0,
                                                        'm_018-bin-4'),
                                        5:
                                            Measurement(161289, 0,
                                                        'm_018-bin-5'),
                                    },
                                    impression=Measurement(9193546, 0, 'm_019'),
                                ),
                                MeasurementSet(
                                    reach=Measurement(1100000, 0, 'm_020'),
                                    k_reach={
                                        1:
                                            Measurement(567742, 0,
                                                        'm_021-bin-1'),
                                        2:
                                            Measurement(283871, 0,
                                                        'm_021-bin-2'),
                                        3:
                                            Measurement(141936, 0,
                                                        'm_021-bin-3'),
                                        4:
                                            Measurement(70968, 0,
                                                        'm_021-bin-4'),
                                        5:
                                            Measurement(35483, 0,
                                                        'm_021-bin-5'),
                                    },
                                    impression=Measurement(2022579, 0, 'm_022'),
                                ),
                            ],
                            edp3: [
                                MeasurementSet(
                                    reach=Measurement(800000, 0, 'm_028'),
                                    k_reach={
                                        1:
                                            Measurement(412903, 0,
                                                        'm_029-bin-1'),
                                        2:
                                            Measurement(206452, 0,
                                                        'm_029-bin-2'),
                                        3:
                                            Measurement(103226, 0,
                                                        'm_029-bin-3'),
                                        4:
                                            Measurement(51613, 0,
                                                        'm_029-bin-4'),
                                        5:
                                            Measurement(25806, 0,
                                                        'm_029-bin-5'),
                                    },
                                    impression=Measurement(1470967, 0, 'm_030'),
                                ),
                                MeasurementSet(
                                    reach=Measurement(202952, 0, 'm_031'),
                                    k_reach={
                                        1:
                                            Measurement(104749, 0,
                                                        'm_032-bin-1'),
                                        2:
                                            Measurement(52375, 0,
                                                        'm_032-bin-2'),
                                        3:
                                            Measurement(26188, 0,
                                                        'm_032-bin-3'),
                                        4:
                                            Measurement(13094, 0,
                                                        'm_032-bin-4'),
                                        5:
                                            Measurement(6546, 0, 'm_032-bin-5'),
                                    },
                                    impression=Measurement(373169, 0, 'm_033'),
                                ),
                            ],
                            edp123: [
                                MeasurementSet(
                                    reach=Measurement(15829304, 10000, 'm_041'),
                                    k_reach={
                                        1:
                                            Measurement(8169963, 10000,
                                                        'm_042-bin-1'),
                                        2:
                                            Measurement(4084982, 10000,
                                                        'm_042-bin-2'),
                                        3:
                                            Measurement(2042491, 10000,
                                                        'm_042-bin-3'),
                                        4:
                                            Measurement(1021246, 10000,
                                                        'm_042-bin-4'),
                                        5:
                                            Measurement(510622, 10000,
                                                        'm_042-bin-5'),
                                    },
                                    impression=Measurement(
                                        29046331, 10000, 'm_043'),
                                ),
                                MeasurementSet(
                                    reach=Measurement(3761510, 10000, 'm_044'),
                                    k_reach={
                                        1:
                                            Measurement(1941425, 10000,
                                                        'm_045-bin-1'),
                                        2:
                                            Measurement(970713, 10000,
                                                        'm_045-bin-2'),
                                        3:
                                            Measurement(485357, 10000,
                                                        'm_045-bin-3'),
                                        4:
                                            Measurement(242679, 10000,
                                                        'm_045-bin-4'),
                                        5:
                                            Measurement(121336, 10000,
                                                        'm_045-bin-5'),
                                    },
                                    impression=Measurement(
                                        6904573, 10000, 'm_046'),
                                ),
                            ],
                        }),
                "mrc":
                    MetricReport(
                        weekly_cumulative_reaches={
                            edp1: [
                                Measurement(9501618, 10000, 'm_047'),
                                Measurement(11389309, 10000, 'm_048'),
                            ],
                            edp2: [
                                Measurement(4750000, 0, 'm_058'),
                                Measurement(5700000, 0, 'm_059'),
                            ],
                            edp3: [
                                Measurement(760000, 0, 'm_069'),
                                Measurement(950000, 0, 'm_070'),
                            ],
                            edp12: [
                                Measurement(13427250, 10000, 'm_080'),
                                Measurement(15920317, 10000, 'm_081'),
                            ],
                        },
                        whole_campaign_measurements={
                            edp1:
                                MeasurementSet(
                                    reach=Measurement(11382243, 10000, 'm_049'),
                                    k_reach={
                                        1:
                                            Measurement(5874706, 10000,
                                                        'm_050-bin-1'),
                                        2:
                                            Measurement(2937353, 10000,
                                                        'm_050-bin-2'),
                                        3:
                                            Measurement(1468677, 10000,
                                                        'm_050-bin-3'),
                                        4:
                                            Measurement(734339, 10000,
                                                        'm_050-bin-4'),
                                        5:
                                            Measurement(367168, 10000,
                                                        'm_050-bin-5'),
                                    },
                                    impression=Measurement(
                                        21696322, 10000, 'm_051'),
                                ),
                            edp2:
                                MeasurementSet(
                                    reach=Measurement(5700000, 0, 'm_060'),
                                    k_reach={
                                        1:
                                            Measurement(2941935, 0,
                                                        'm_061-bin-1'),
                                        2:
                                            Measurement(1470968, 0,
                                                        'm_061-bin-2'),
                                        3:
                                            Measurement(735484, 0,
                                                        'm_061-bin-3'),
                                        4:
                                            Measurement(367742, 0,
                                                        'm_061-bin-4'),
                                        5:
                                            Measurement(183871, 0,
                                                        'm_061-bin-5'),
                                    },
                                    impression=Measurement(
                                        10645760, 0, 'm_062'),
                                ),
                            edp3:
                                MeasurementSet(
                                    reach=Measurement(950000, 0, 'm_071'),
                                    k_reach={
                                        1:
                                            Measurement(490323, 0,
                                                        'm_072-bin-1'),
                                        2:
                                            Measurement(245162, 0,
                                                        'm_072-bin-2'),
                                        3:
                                            Measurement(122581, 0,
                                                        'm_072-bin-3'),
                                        4:
                                            Measurement(61291, 0,
                                                        'm_072-bin-4'),
                                        5:
                                            Measurement(30643, 0,
                                                        'm_072-bin-5'),
                                    },
                                    impression=Measurement(1751669, 0, 'm_073'),
                                ),
                            edp12:
                                MeasurementSet(
                                    reach=Measurement(15908881, 10000, 'm_082'),
                                    k_reach={
                                        1:
                                            Measurement(8211035, 10000,
                                                        'm_083-bin-1'),
                                        2:
                                            Measurement(4105518, 10000,
                                                        'm_083-bin-2'),
                                        3:
                                            Measurement(2052759, 10000,
                                                        'm_083-bin-3'),
                                        4:
                                            Measurement(1026380, 10000,
                                                        'm_083-bin-4'),
                                        5:
                                            Measurement(513189, 10000,
                                                        'm_083-bin-5'),
                                    },
                                    impression=Measurement(
                                        32337826, 10000, 'm_084'),
                                ),
                        },
                        weekly_non_cumulative_measurements={
                            edp1: [
                                MeasurementSet(
                                    reach=Measurement(9503446, 10000, 'm_052'),
                                    k_reach={
                                        1:
                                            Measurement(4905004, 10000,
                                                        'm_053-bin-1'),
                                        2:
                                            Measurement(2452502, 10000,
                                                        'm_053-bin-2'),
                                        3:
                                            Measurement(1226251, 10000,
                                                        'm_053-bin-3'),
                                        4:
                                            Measurement(613126, 10000,
                                                        'm_053-bin-4'),
                                        5:
                                            Measurement(306563, 10000,
                                                        'm_053-bin-5'),
                                    },
                                    impression=Measurement(
                                        17473517, 10000, 'm_054'),
                                ),
                                MeasurementSet(
                                    reach=Measurement(2289252, 10000, 'm_055'),
                                    k_reach={
                                        1:
                                            Measurement(1181549, 10000,
                                                        'm_056-bin-1'),
                                        2:
                                            Measurement(590775, 10000,
                                                        'm_056-bin-2'),
                                        3:
                                            Measurement(295388, 10000,
                                                        'm_056-bin-3'),
                                        4:
                                            Measurement(147694, 10000,
                                                        'm_056-bin-4'),
                                        5:
                                            Measurement(73846, 10000,
                                                        'm_056-bin-5'),
                                    },
                                    impression=Measurement(
                                        4236753, 10000, 'm_057'),
                                ),
                            ],
                            edp2: [
                                MeasurementSet(
                                    reach=Measurement(4750000, 0, 'm_063'),
                                    k_reach={
                                        1:
                                            Measurement(2451613, 0,
                                                        'm_064-bin-1'),
                                        2:
                                            Measurement(1225807, 0,
                                                        'm_064-bin-2'),
                                        3:
                                            Measurement(612904, 0,
                                                        'm_064-bin-3'),
                                        4:
                                            Measurement(306452, 0,
                                                        'm_064-bin-4'),
                                        5:
                                            Measurement(153224, 0,
                                                        'm_064-bin-5'),
                                    },
                                    impression=Measurement(8733867, 0, 'm_065'),
                                ),
                                MeasurementSet(
                                    reach=Measurement(1039801, 0, 'm_066'),
                                    k_reach={
                                        1:
                                            Measurement(536671, 0,
                                                        'm_067-bin-1'),
                                        2:
                                            Measurement(268336, 0,
                                                        'm_067-bin-2'),
                                        3:
                                            Measurement(134168, 0,
                                                        'm_067-bin-3'),
                                        4:
                                            Measurement(67084, 0,
                                                        'm_067-bin-4'),
                                        5:
                                            Measurement(33542, 0,
                                                        'm_067-bin-5'),
                                    },
                                    impression=Measurement(1911893, 0, 'm_068'),
                                ),
                            ],
                            edp3: [
                                MeasurementSet(
                                    reach=Measurement(760000, 0, 'm_074'),
                                    k_reach={
                                        1:
                                            Measurement(392258, 0,
                                                        'm_075-bin-1'),
                                        2:
                                            Measurement(196129, 0,
                                                        'm_075-bin-2'),
                                        3:
                                            Measurement(98065, 0,
                                                        'm_075-bin-3'),
                                        4:
                                            Measurement(49033, 0,
                                                        'm_075-bin-4'),
                                        5:
                                            Measurement(24515, 0,
                                                        'm_075-bin-5'),
                                    },
                                    impression=Measurement(1397418, 0, 'm_076'),
                                ),
                                MeasurementSet(
                                    reach=Measurement(192662, 0, 'm_077'),
                                    k_reach={
                                        1: Measurement(99438, 0, 'm_078-bin-1'),
                                        2: Measurement(49719, 0, 'm_078-bin-2'),
                                        3: Measurement(24860, 0, 'm_078-bin-3'),
                                        4: Measurement(12430, 0, 'm_078-bin-4'),
                                        5: Measurement(6215, 0, 'm_078-bin-5'),
                                    },
                                    impression=Measurement(354251, 0, 'm_079'),
                                ),
                            ],
                            edp12: [
                                MeasurementSet(
                                    reach=Measurement(13426464, 10000, 'm_085'),
                                    k_reach={
                                        1:
                                            Measurement(6929788, 10000,
                                                        'm_086-bin-1'),
                                        2:
                                            Measurement(3464894, 10000,
                                                        'm_086-bin-2'),
                                        3:
                                            Measurement(1732447, 10000,
                                                        'm_086-bin-3'),
                                        4:
                                            Measurement(866224, 10000,
                                                        'm_086-bin-4'),
                                        5:
                                            Measurement(433111, 10000,
                                                        'm_086-bin-5'),
                                    },
                                    impression=Measurement(
                                        26215389, 10000, 'm_087'),
                                ),
                                MeasurementSet(
                                    reach=Measurement(3278136, 10000, 'm_088'),
                                    k_reach={
                                        1:
                                            Measurement(1691941, 10000,
                                                        'm_089-bin-1'),
                                        2:
                                            Measurement(845971, 10000,
                                                        'm_089-bin-2'),
                                        3:
                                            Measurement(422986, 10000,
                                                        'm_089-bin-3'),
                                        4:
                                            Measurement(211493, 10000,
                                                        'm_089-bin-4'),
                                        5:
                                            Measurement(105745, 10000,
                                                        'm_089-bin-5'),
                                    },
                                    impression=Measurement(
                                        6135862, 10000, 'm_090'),
                                ),
                            ],
                        },
                    ),
                "custom":
                    MetricReport(
                        weekly_cumulative_reaches={
                            edp1: [
                                Measurement(9984642, 10000, 'm_091'),
                                Measurement(12020226, 10000, 'm_092'),
                            ],
                            edp2: [
                                Measurement(5000000, 0, 'm_102'),
                                Measurement(6000000, 0, 'm_103'),
                            ],
                            edp3: [
                                Measurement(800000, 0, 'm_113'),
                                Measurement(1000000, 0, 'm_114'),
                            ],
                            edp123: [
                                Measurement(15799013, 10000, 'm_124'),
                                Measurement(19015392, 10000, 'm_125'),
                            ],
                        },
                        whole_campaign_measurements={
                            edp1:
                                MeasurementSet(
                                    reach=Measurement(12017026, 10000, 'm_093'),
                                    k_reach={
                                        1:
                                            Measurement(6202336, 10000,
                                                        'm_094-bin-1'),
                                        2:
                                            Measurement(3101168, 10000,
                                                        'm_094-bin-2'),
                                        3:
                                            Measurement(1550584, 10000,
                                                        'm_094-bin-3'),
                                        4:
                                            Measurement(775292, 10000,
                                                        'm_094-bin-4'),
                                        5:
                                            Measurement(387646, 10000,
                                                        'm_094-bin-5'),
                                    },
                                    impression=Measurement(
                                        22871159, 10000, 'm_095'),
                                ),
                            edp2:
                                MeasurementSet(
                                    reach=Measurement(6000000, 0, 'm_104'),
                                    k_reach={
                                        1:
                                            Measurement(3096774, 0,
                                                        'm_105-bin-1'),
                                        2:
                                            Measurement(1548387, 0,
                                                        'm_105-bin-2'),
                                        3:
                                            Measurement(774194, 0,
                                                        'm_105-bin-3'),
                                        4:
                                            Measurement(387097, 0,
                                                        'm_105-bin-4'),
                                        5:
                                            Measurement(193548, 0,
                                                        'm_105-bin-5'),
                                    },
                                    impression=Measurement(
                                        11216125, 0, 'm_106'),
                                ),
                            edp3:
                                MeasurementSet(
                                    reach=Measurement(1000000, 0, 'm_115'),
                                    k_reach={
                                        1:
                                            Measurement(516129, 0,
                                                        'm_116-bin-1'),
                                        2:
                                            Measurement(258065, 0,
                                                        'm_116-bin-2'),
                                        3:
                                            Measurement(129033, 0,
                                                        'm_116-bin-3'),
                                        4:
                                            Measurement(64517, 0,
                                                        'm_116-bin-4'),
                                        5:
                                            Measurement(32256, 0,
                                                        'm_116-bin-5'),
                                    },
                                    impression=Measurement(1844136, 0, 'm_117'),
                                ),
                            edp123:
                                MeasurementSet(
                                    reach=Measurement(19030737, 10000, 'm_126'),
                                    k_reach={
                                        1:
                                            Measurement(9822316, 10000,
                                                        'm_127-bin-1'),
                                        2:
                                            Measurement(4911158, 10000,
                                                        'm_127-bin-2'),
                                        3:
                                            Measurement(2455579, 10000,
                                                        'm_127-bin-3'),
                                        4:
                                            Measurement(1227790, 10000,
                                                        'm_127-bin-4'),
                                        5:
                                            Measurement(613894, 10000,
                                                        'm_127-bin-5'),
                                    },
                                    impression=Measurement(
                                        35936915, 10000, 'm_128'),
                                ),
                        },
                        weekly_non_cumulative_measurements={
                            edp1: [
                                MeasurementSet(
                                    reach=Measurement(10000981, 10000, 'm_096'),
                                    k_reach={
                                        1:
                                            Measurement(5161797, 10000,
                                                        'm_097-bin-1'),
                                        2:
                                            Measurement(2580899, 10000,
                                                        'm_097-bin-2'),
                                        3:
                                            Measurement(1290450, 10000,
                                                        'm_097-bin-3'),
                                        4:
                                            Measurement(645225, 10000,
                                                        'm_097-bin-4'),
                                        5:
                                            Measurement(322610, 10000,
                                                        'm_097-bin-5'),
                                    },
                                    impression=Measurement(
                                        18382797, 10000, 'm_098'),
                                ),
                                MeasurementSet(
                                    reach=Measurement(2441042, 10000, 'm_099'),
                                    k_reach={
                                        1:
                                            Measurement(1259893, 10000,
                                                        'm_100-bin-1'),
                                        2:
                                            Measurement(629947, 10000,
                                                        'm_100-bin-2'),
                                        3:
                                            Measurement(314974, 10000,
                                                        'm_100-bin-3'),
                                        4:
                                            Measurement(157487, 10000,
                                                        'm_100-bin-4'),
                                        5:
                                            Measurement(78741, 10000,
                                                        'm_100-bin-5'),
                                    },
                                    impression=Measurement(
                                        4488362, 10000, 'm_101'),
                                ),
                            ],
                            edp2: [
                                MeasurementSet(
                                    reach=Measurement(5000000, 0, 'm_107'),
                                    k_reach={
                                        1:
                                            Measurement(2580645, 0,
                                                        'm_108-bin-1'),
                                        2:
                                            Measurement(1290323, 0,
                                                        'm_108-bin-2'),
                                        3:
                                            Measurement(645162, 0,
                                                        'm_108-bin-3'),
                                        4:
                                            Measurement(322581, 0,
                                                        'm_108-bin-4'),
                                        5:
                                            Measurement(161289, 0,
                                                        'm_108-bin-5'),
                                    },
                                    impression=Measurement(9193546, 0, 'm_109'),
                                ),
                                MeasurementSet(
                                    reach=Measurement(1100000, 0, 'm_110'),
                                    k_reach={
                                        1:
                                            Measurement(567742, 0,
                                                        'm_111-bin-1'),
                                        2:
                                            Measurement(283871, 0,
                                                        'm_111-bin-2'),
                                        3:
                                            Measurement(141936, 0,
                                                        'm_111-bin-3'),
                                        4:
                                            Measurement(70968, 0,
                                                        'm_111-bin-4'),
                                        5:
                                            Measurement(35483, 0,
                                                        'm_111-bin-5'),
                                    },
                                    impression=Measurement(2022579, 0, 'm_112'),
                                ),
                            ],
                            edp3: [
                                MeasurementSet(
                                    reach=Measurement(800000, 0, 'm_118'),
                                    k_reach={
                                        1:
                                            Measurement(412903, 0,
                                                        'm_119-bin-1'),
                                        2:
                                            Measurement(206452, 0,
                                                        'm_119-bin-2'),
                                        3:
                                            Measurement(103226, 0,
                                                        'm_119-bin-3'),
                                        4:
                                            Measurement(51613, 0,
                                                        'm_119-bin-4'),
                                        5:
                                            Measurement(25806, 0,
                                                        'm_119-bin-5'),
                                    },
                                    impression=Measurement(1470967, 0, 'm_120'),
                                ),
                                MeasurementSet(
                                    reach=Measurement(202952, 0, 'm_121'),
                                    k_reach={
                                        1:
                                            Measurement(104749, 0,
                                                        'm_122-bin-1'),
                                        2:
                                            Measurement(52375, 0,
                                                        'm_122-bin-2'),
                                        3:
                                            Measurement(26188, 0,
                                                        'm_122-bin-3'),
                                        4:
                                            Measurement(13094, 0,
                                                        'm_122-bin-4'),
                                        5:
                                            Measurement(6546, 0, 'm_122-bin-5'),
                                    },
                                    impression=Measurement(373169, 0, 'm_123'),
                                ),
                            ],
                            edp123: [
                                MeasurementSet(
                                    reach=Measurement(15819974, 10000, 'm_129'),
                                    k_reach={
                                        1:
                                            Measurement(8165148, 10000,
                                                        'm_130-bin-1'),
                                        2:
                                            Measurement(4082574, 10000,
                                                        'm_130-bin-2'),
                                        3:
                                            Measurement(2041287, 10000,
                                                        'm_130-bin-3'),
                                        4:
                                            Measurement(1020644, 10000,
                                                        'm_130-bin-4'),
                                        5:
                                            Measurement(510321, 10000,
                                                        'm_130-bin-5'),
                                    },
                                    impression=Measurement(
                                        29052805, 10000, 'm_131'),
                                ),
                                MeasurementSet(
                                    reach=Measurement(3751542, 10000, 'm_132'),
                                    k_reach={
                                        1:
                                            Measurement(1936280, 10000,
                                                        'm_133-bin-1'),
                                        2:
                                            Measurement(968140, 10000,
                                                        'm_133-bin-2'),
                                        3:
                                            Measurement(484070, 10000,
                                                        'm_133-bin-3'),
                                        4:
                                            Measurement(242035, 10000,
                                                        'm_133-bin-4'),
                                        5:
                                            Measurement(121017, 10000,
                                                        'm_133-bin-5'),
                                    },
                                    impression=Measurement(
                                        6884110, 10000, 'm_134'),
                                ),
                            ],
                        },
                    ),
            },
            metric_subsets_by_parent={'ami': ['mrc', 'custom']},
            cumulative_inconsistency_allowed_edp_combinations={},
        )

        self.assertEqual(expected_report._metric_reports.keys(),
                         expected_report._metric_reports.keys())
        for key in report._metric_reports.keys():
            self.assertEqual(
                report._metric_reports[key]._weekly_cumulative_reaches,
                expected_report._metric_reports[key]._weekly_cumulative_reaches)
            self.assertEqual(
                report._metric_reports[key]._whole_campaign_measurements,
                expected_report._metric_reports[key].
                _whole_campaign_measurements)
            self.assertEqual(
                report._metric_reports[key]._weekly_non_cumulative_measurements,
                expected_report._metric_reports[key].
                _weekly_non_cumulative_measurements)

    def test_report_summary_v2_is_corrected_successfully(self):
        report_summary_v2 = get_report_summary_v2(
            "src/test/python/wfa/measurement/reporting/postprocessing/tools/sample_report_summary_v2.textproto"
        )
        report_post_processor_result = ReportSummaryV2Processor(
            report_summary_v2).process()

        self.assertEqual(
            report_post_processor_result.pre_correction_report_summary_v2,
            report_summary_v2)
        self.assertEqual(report_post_processor_result.status.status_code,
                         StatusCode.SOLUTION_FOUND_WITH_HIGHS)
        self.assertLess(
            report_post_processor_result.status.primal_equality_residual,
            NOISE_CORRECTION_TOLERANCE)
        self.assertLess(
            report_post_processor_result.status.primal_inequality_residual,
            NOISE_CORRECTION_TOLERANCE)
        self.assertEqual(len(report_post_processor_result.updated_measurements),
                         278)


def read_file_to_string(filename: str) -> str:
    try:
        with open(filename, 'r') as file:
            return file.read()
    except FileNotFoundError:
        pass


def get_report_summary_v2(filename: str):
    report_summary_textproto = read_file_to_string(filename)
    report_summary = text_format.Parse(report_summary_textproto,
                                       report_summary_v2_pb2.ReportSummaryV2())
    return report_summary


if __name__ == '__main__':
    unittest.main()
